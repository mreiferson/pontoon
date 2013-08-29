// - Increment currentTerm, vote for self
// - Reset election timeout
// - Send RequestVote RPCs to all other servers, wait for either:
//   - Votes received from majority of servers: become leader
//   - AppendEntries RPC received from new leader: step down
//   - Election timeout elapses without election resolution: increment term, start new election
//   - Discover higher term: step down
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

func (n *Node) ioLoop() {
	log.Printf("[%s] starting ioLoop()", n.ID)

	electionTimeout := 500 * time.Millisecond
	randElectionTimeout := electionTimeout + time.Duration(rand.Int63n(int64(electionTimeout)))
	electionTimer := time.NewTimer(randElectionTimeout)

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

		randElectionTimeout := electionTimeout + time.Duration(rand.Int63n(int64(electionTimeout)))
		log.Printf("[%s] looping/resetting election timeout %s", n.ID, randElectionTimeout)
		if !electionTimer.Reset(randElectionTimeout) {
			electionTimer = time.NewTimer(randElectionTimeout)
		}
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

func (n *Node) electionTimeout() {
	if n.State == Leader {
		return
	}

	log.Printf("[%s] electionTimeout()", n.ID)

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

	if n.State != Follower {
		n.endElection()
		n.stepDown()
	}

	if er.Term > n.Term {
		n.setTerm(er.Term)
	}

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
