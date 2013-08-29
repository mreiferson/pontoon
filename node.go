// pontoon is a library implementing the Raft distributed consensus algorithm
//
// (excerpts from the paper https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)
//
// Raft implements consensus by ﬁrst electing a distinguished leader, then giving the leader complete
// responsibility for managing the replicated log. The leader accepts log entries from clients,
// replicates them on other servers, and tells servers when it is safe to apply log entries to their
// state machines. Having a leader simplifies the algorithm in several ways. For example, a leader can
// make decisions unilaterally without fear of conﬂicting decisions made elsewhere. In addition, Raft
// ensures that the leader's log is always "the truth"; logs can occasionally become inconsistent after
// crashes, but the leader resolves these situations by forcing the other servers’ logs into agreement
// with its own. A leader can fail or become disconnected from the other servers, in which case a new
// leader is elected.
//
// Ensuring safety is critical to any consensus algorithm. In Raft clients only interact with the
// leader, so the only behavior they see is that of the leader; as a result, safety can be deﬁned in
// terms of leaders. The Raft safety property is this: if a leader has applied a particular log entry
// to its state machine (in which case the results of that command could be visible to clients), then
// no other server may apply a different command for the same log entry. Raft has two interlocking
// policies that ensure safety.
//
//  * First, the leader decides when it is safe to apply a log entry to its state machine; such an
//    entry is called committed.
//
//  * Second, the leader election process ensures that no server can be elected as leader unless its
//    log contains all committed entries; this preserves the property that the leader’s log is "the
//    truth".
//
// Given the leader approach, Raft decomposes the consensus problem into three relatively independent
// subproblems:
//
//  * Leader election: a new leader must be chosen when an existing leader fails, and Raft must
//    guarantee that exactly one leader is chosen.
//
//  * Log replication: the leader must accept log entries from clients and replicate them faithfully
//    across the cluster, forcing all other logs to agree with its own.
//
//  * Safety: how Raft decides when a log entry has been committed, and how it ensures that leaders
//    always hold all committed entries in their logs.
package pontoon

import (
	"bytes"
	"log"
	"math/rand"
	"sync"
	"time"
)

type Node struct {
	sync.RWMutex

	ID           string
	State        int
	Term         int64
	ElectionTerm int64
	VotedFor     string
	Votes        int
	Cluster      []*Peer
	Uncommitted  map[int64]*CommandRequest

	Log          Logger
	Transport    Transporter
	StateMachine Applyer

	ElectionTimeout time.Duration

	exitChan                  chan int
	voteResponseChan          chan VoteResponse
	requestVoteChan           chan VoteRequest
	requestVoteResponseChan   chan VoteResponse
	appendEntriesChan         chan EntryRequest
	appendEntriesResponseChan chan EntryResponse
	commandChan               chan CommandRequest

	endElectionChan      chan int
	finishedElectionChan chan int
}

func NewNode(id string, transport Transporter, logger Logger, applyer Applyer) *Node {
	node := &Node{
		ID: id,

		Log:          logger,
		Transport:    transport,
		StateMachine: applyer,

		Uncommitted: make(map[int64]*CommandRequest),

		ElectionTimeout: 500 * time.Millisecond,

		exitChan:                  make(chan int),
		voteResponseChan:          make(chan VoteResponse),
		requestVoteChan:           make(chan VoteRequest),
		requestVoteResponseChan:   make(chan VoteResponse),
		appendEntriesChan:         make(chan EntryRequest),
		appendEntriesResponseChan: make(chan EntryResponse),
		commandChan:               make(chan CommandRequest),
	}
	return node
}

func (n *Node) Serve() error {
	return n.Transport.Serve(n)
}

func (n *Node) Start() {
	go n.ioLoop()
}

func (n *Node) Exit() error {
	return n.Transport.Close()
}

func (n *Node) AddToCluster(member string) {
	p := &Peer{
		ID: member,
	}
	n.Cluster = append(n.Cluster, p)
}

func (n *Node) RequestVote(vr VoteRequest) (VoteResponse, error) {
	n.requestVoteChan <- vr
	return <-n.requestVoteResponseChan, nil
}

func (n *Node) AppendEntries(er EntryRequest) (EntryResponse, error) {
	n.appendEntriesChan <- er
	return <-n.appendEntriesResponseChan, nil
}

func (n *Node) Command(cr CommandRequest) {
	n.commandChan <- cr
}

// Raft uses randomized election timeouts to ensure that split votes are rare and that they are
// resolved quickly. To prevent split votes in the ﬁrst place, election timeouts are chosen randomly
// from an interval between a ﬁxed minimum value and twice that value (currently 150-300ms in our
// implementation). This spreads out the servers so that in most cases only a single server will time
// out; it wins the election and sends heartbeats before any other servers time out. The same mechanism
// is used to handle split votes. Each candidate restarts its (randomized) election timeout at the
// start of an election, and it waits for that timeout to elapse before starting the next election;
// this reduces the likelihood of another split vote in the new election.
func (n *Node) randomElectionTimeout() time.Duration {
	return n.ElectionTimeout + time.Duration(rand.Int63n(int64(n.ElectionTimeout)))
}

//
//                                  times out,
//  startup                        new election
//     |                             .-----.
//     |                             |     |
//     v         times out,          |     v     receives votes from
// +----------+  starts election  +-----------+  majority of servers  +--------+
// | Follower |------------------>| Candidate |---------------------->| Leader |
// +----------+                   +-----------+                       +--------+
//     ^ ^                              |                                 |
//     | |    discovers current leader  |                                 |
//     | |                 or new term  |                                 |
//     | '------------------------------'                                 |
//     |                                                                  |
//     |                               discovers server with higher term  |
//     '------------------------------------------------------------------'
//
func (n *Node) ioLoop() {
	log.Printf("[%s] starting ioLoop()", n.ID)

	electionTimer := time.NewTimer(n.randomElectionTimeout())
	followerTimer := time.NewTicker(250 * time.Millisecond)
	for {
		select {
		case vreq := <-n.requestVoteChan:
			vresp, _ := n.doRequestVote(vreq)
			n.requestVoteResponseChan <- vresp
		case ereq := <-n.appendEntriesChan:
			eresp, _ := n.doAppendEntries(ereq)
			n.appendEntriesResponseChan <- eresp
		case creq := <-n.commandChan:
			n.doCommand(creq)
			// optimize client response time by triggering a follower update immediately
			n.updateFollowers()
		case <-electionTimer.C:
			n.electionTimeout()
		case vresp := <-n.voteResponseChan:
			n.voteResponse(vresp)
			continue
		case <-followerTimer.C:
			// this also acts as a heartbeat
			n.updateFollowers()
			continue
		case <-n.exitChan:
			n.stepDown()
			goto exit
		}

		randElectionTimeout := n.randomElectionTimeout()
		// try to re-use the timer first before creating a new one
		if !electionTimer.Reset(randElectionTimeout) {
			electionTimer = time.NewTimer(randElectionTimeout)
		}

		log.Printf("[%s] looping... reset election timeout to %s", n.ID, randElectionTimeout)
	}

exit:
	log.Printf("[%s] exiting ioLoop()", n.ID)
}

func (n *Node) setTerm(term int64) {
	// check freshness
	if term <= n.Term {
		return
	}

	n.Term = term
	n.State = Follower
	n.VotedFor = ""
	n.Votes = 0
}

func (n *Node) nextTerm() {
	log.Printf("[%s] nextTerm()", n.ID)

	n.Term++
	n.State = Follower
	n.VotedFor = ""
	n.Votes = 0
}

func (n *Node) stepDown() {
	if n.State == Follower {
		return
	}

	log.Printf("[%s] stepDown()", n.ID)

	n.State = Follower
	n.VotedFor = ""
	n.Votes = 0
}

func (n *Node) promoteToLeader() {
	log.Printf("[%s] promoteToLeader()", n.ID)

	n.State = Leader
	for _, peer := range n.Cluster {
		peer.NextIndex = n.Log.Index()
	}
}

// To begin an election, a follower increments its current term and transitions to candidate state. It
// then issues RequestVote RPCs in parallel to each of the other servers in the cluster. If the
// candidate receives no response for an RPC, it reissues the RPC repeatedly until a response arrives
// or the election concludes. A candidate continues in this state until one of three things happens:
//
//  (a) it wins the election
//  (b) another server establishes itself as leader
//  (c) a period of time goes by with no winner
func (n *Node) electionTimeout() {
	if n.State == Leader {
		return
	}

	log.Printf("[%s] electionTimeout()", n.ID)

	// kill any current election (idempotent)
	n.endElection()

	n.nextTerm()
	n.runForLeader()
}

func (n *Node) voteResponse(vresp VoteResponse) {
	if n.State != Candidate {
		return
	}

	if vresp.Term != n.ElectionTerm {
		if vresp.Term > n.ElectionTerm {
			// we discovered a higher term
			n.endElection()
			n.setTerm(vresp.Term)
		}
		return
	}

	if vresp.VoteGranted {
		n.voteGranted()
	}
}

// A candidate wins an election if it receives votes from a majority of the servers in the full cluster
// for the same term. Each server will vote for at most one candidate in a given term, on a
// ﬁrst-come-ﬁrst-served basis (note: Section 5.4 adds an additional restriction on votes). The
// majority rule ensures that only one candidate can win. Once a candidate wins an election, it becomes
// leader. It then sends heartbeat messages to every other server to establish its authority and
// prevent new elections.
func (n *Node) voteGranted() {
	n.Votes++

	majority := (len(n.Cluster)+1)/2 + 1

	log.Printf("[%s] voteGranted() %d >= %d", n.ID, n.Votes, majority)

	if n.Votes >= majority {
		// we won election, end it and promote
		n.endElection()
		n.promoteToLeader()
	}
}

func (n *Node) endElection() {
	if n.State != Candidate || n.endElectionChan == nil {
		return
	}

	log.Printf("[%s] endElection()", n.ID)

	close(n.endElectionChan)
	<-n.finishedElectionChan

	n.endElectionChan = nil
	n.finishedElectionChan = nil
}

func (n *Node) updateFollowers() {
	var er EntryRequest

	if n.State != Leader {
		return
	}

	for _, peer := range n.Cluster {
		if n.Log.LastIndex() < peer.NextIndex {
			// heartbeat
			_, prevLogIndex, prevLogTerm := n.Log.GetEntryForRequest(n.Log.LastIndex())
			er = newEntryRequest(-1, n.ID, n.Term, prevLogIndex, prevLogTerm, []byte("NOP"))
		} else {
			entry, prevLogIndex, prevLogTerm := n.Log.GetEntryForRequest(peer.NextIndex)
			er = newEntryRequest(entry.CmdID, n.ID, n.Term, prevLogIndex, prevLogTerm, entry.Data)
		}

		log.Printf("[%s] updating follower %s - %+v", n.ID, peer.ID, er)
		_, err := n.Transport.AppendEntriesRPC(peer.ID, er)
		if err != nil {
			log.Printf("[%s] error in AppendEntriesRPC() to %s - %s", n.ID, peer.ID, err)
			peer.NextIndex--
			if peer.NextIndex < 0 {
				peer.NextIndex = 0
			}
			continue
		}

		if er.CmdID == -1 {
			// skip commit checks for heartbeats
			continue
		}

		// TODO: keep track of CommitIndex and send to followers

		// TODO: keep track of whether or not the leader has propagated an entry for the *current* term

		// A log entry may only be considered committed if the entry is stored on a majority of the
		// servers; in addition, at least one entry from the leader’s current term must also be stored
		// on a majority of the servers.
		majority := int32((len(n.Cluster)+1)/2 + 1)
		cr := n.Uncommitted[er.CmdID]
		cr.ReplicationCount++
		if cr.ReplicationCount >= majority && cr.State != Committed {
			cr.State = Committed
			log.Printf("[%s] !!! apply %+v", n.ID, cr)
			err := n.StateMachine.Apply(cr)
			if err != nil {
				// TODO: what do we do here?
			}
			cr.ResponseChan <- CommandResponse{LeaderID: n.VotedFor, Success: true}
		}
		peer.NextIndex++
	}
}

func (n *Node) runForLeader() {
	log.Printf("[%s] runForLeader()", n.ID)

	n.State = Candidate
	n.Votes++
	n.ElectionTerm = n.Term
	n.endElectionChan = make(chan int)
	n.finishedElectionChan = make(chan int)

	// build the vote request before launching the go-routine
	// so that we do not need to lock
	vreq := VoteRequest{
		Term: n.Term,
		// TODO: this should use Node.ID
		CandidateID:  n.Transport.String(),
		LastLogIndex: n.Log.Index(),
		LastLogTerm:  n.Log.Term(),
	}
	go n.gatherVotes(vreq)
}

func (n *Node) gatherVotes(vreq VoteRequest) {
	for _, peer := range n.Cluster {
		go func(p string) {
			vresp, err := n.Transport.RequestVoteRPC(p, vreq)
			if err != nil {
				log.Printf("[%s] error in RequestVoteRPC() to %s - %s", n.ID, p, err)
				return
			}
			n.voteResponseChan <- vresp
		}(peer.ID)
	}

	// TODO: retry failed requests

	<-n.endElectionChan
	close(n.finishedElectionChan)
}

func (n *Node) doRequestVote(vr VoteRequest) (VoteResponse, error) {
	log.Printf("[%s] doRequestVote() %+v", n.ID, vr)

	if vr.Term < n.Term {
		return VoteResponse{n.Term, false}, nil
	}

	if vr.Term > n.Term {
		n.endElection()
		n.setTerm(vr.Term)
	}

	if n.VotedFor != "" && n.VotedFor != vr.CandidateID {
		return VoteResponse{n.Term, false}, nil
	}

	if n.Log.FresherThan(vr.LastLogIndex, vr.LastLogTerm) {
		return VoteResponse{n.Term, false}, nil
	}

	log.Printf("[%s] granting vote to %s", n.ID, vr.CandidateID)

	n.VotedFor = vr.CandidateID
	return VoteResponse{n.Term, true}, nil
}

func (n *Node) doAppendEntries(er EntryRequest) (EntryResponse, error) {
	if er.Term < n.Term {
		return EntryResponse{Term: n.Term, Success: false}, nil
	}

	// While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming
	// to be leader. If the leader’s term (included in its RPC) is at least as large as the candidate’s
	// current term, then the candidate recognizes the leader as legitimate and steps down, meaning that it
	// returns to follower state. If the term in the RPC is older than the candidate’s current term, then
	// the candidate rejects the RPC and continues in candidate state.
	if n.State != Follower {
		n.endElection()
		n.stepDown()
	}

	if er.Term > n.Term {
		n.setTerm(er.Term)
	}

	// TODO: compare EntryRequest.CommitIndex against local and update

	err := n.Log.Check(er.PrevLogIndex, er.PrevLogTerm, er.PrevLogIndex+1, er.Term)
	if err != nil {
		log.Printf("[%s] Check error - %s", n.ID, err)
		return EntryResponse{Term: n.Term, Success: false}, nil
	}

	if bytes.Compare(er.Data, []byte("NOP")) == 0 {
		log.Printf("[%s] HEARTBEAT", n.ID)
		return EntryResponse{Term: n.Term, Success: true}, nil
	}

	e := &Entry{
		CmdID: er.CmdID,
		Index: er.PrevLogIndex + 1,
		Term:  er.Term,
		Data:  er.Data,
	}

	log.Printf("[%s] ... appending %+v", n.ID, e)

	err = n.Log.Append(e)
	if err != nil {
		log.Printf("[%s] Append error - %s", n.ID, err)
	}

	return EntryResponse{Term: n.Term, Success: err == nil}, nil
}

func (n *Node) doCommand(cr CommandRequest) {
	if n.State != Leader {
		cr.ResponseChan <- CommandResponse{LeaderID: n.VotedFor, Success: false}
	}

	e := &Entry{
		CmdID: cr.ID,
		Index: n.Log.Index(),
		Term:  n.Term,
		Data:  cr.Body,
	}

	log.Printf("[%s] ... appending %+v", n.ID, e)

	err := n.Log.Append(e)
	if err != nil {
		cr.ResponseChan <- CommandResponse{LeaderID: n.VotedFor, Success: false}
	}

	// TODO: should check uncommitted before re-appending, etc.
	cr.State = Logged
	cr.ReplicationCount++
	n.Uncommitted[cr.ID] = &cr
}
