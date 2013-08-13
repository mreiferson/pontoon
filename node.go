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

const (
	Follower = iota
	Candidate
	Leader
)

type Transporter interface {
	Serve(node *Node) error
	Close() error
	String() string
	RequestVoteRPC(address string, voteRequest VoteRequest) (VoteResponse, error)
	AppendEntriesRPC(address string, entryRequest EntryRequest) (EntryResponse, error)
}

type Node struct {
	sync.RWMutex

	ID           string
	State        int
	Term         int64
	ElectionTerm int64
	VotedFor     string
	Votes        int
	Log          *Log
	Cluster      []*Peer
	Transport    Transporter

	exitChan         chan int
	voteResponseChan chan VoteResponse

	requestVoteChan         chan VoteRequest
	requestVoteResponseChan chan VoteResponse

	appendEntriesChan         chan EntryRequest
	appendEntriesResponseChan chan EntryResponse

	commandChan         chan CommandRequest
	commandResponseChan chan CommandResponse

	endElectionChan      chan int
	finishedElectionChan chan int
}

func NewNode(id string, transport Transporter) *Node {
	node := &Node{
		ID:        id,
		Log:       &Log{},
		Transport: transport,

		exitChan:         make(chan int),
		voteResponseChan: make(chan VoteResponse),

		requestVoteChan:         make(chan VoteRequest),
		requestVoteResponseChan: make(chan VoteResponse),

		appendEntriesChan:         make(chan EntryRequest),
		appendEntriesResponseChan: make(chan EntryResponse),

		commandChan:         make(chan CommandRequest),
		commandResponseChan: make(chan CommandResponse),
	}
	go node.stateMachine()
	return node
}

func (n *Node) Start() error {
	return n.Transport.Serve(n)
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

func (n *Node) stateMachine() {
	log.Printf("[%s] starting stateMachine()", n.ID)

	electionTimeout := 500 * time.Millisecond
	randElectionTimeout := electionTimeout + time.Duration(rand.Int63n(int64(electionTimeout)))
	electionTimer := time.NewTimer(randElectionTimeout)

	followerTimer := time.NewTicker(10 * time.Millisecond)
	heartbeatTimer := time.NewTicker(100 * time.Millisecond)

	for {
		select {
		case <-n.exitChan:
			n.stepDown()
			goto exit
		case vreq := <-n.requestVoteChan:
			vresp, _ := n.doRequestVote(vreq)
			n.requestVoteResponseChan <- vresp
		case ereq := <-n.appendEntriesChan:
			eresp, _ := n.doAppendEntries(ereq)
			n.appendEntriesResponseChan <- eresp
		case creq := <-n.commandChan:
			cresp, _ := n.doCommand(creq)
			n.commandResponseChan <- cresp
		case <-electionTimer.C:
			n.electionTimeout()
		case vresp := <-n.voteResponseChan:
			n.voteResponse(vresp)
		case <-followerTimer.C:
			if n.State != Leader {
				continue
			}
			n.updateFollowers()
		case <-heartbeatTimer.C:
			n.sendHeartbeat()
			continue
		}

		randElectionTimeout := electionTimeout + time.Duration(rand.Int63n(int64(electionTimeout)))
		if !electionTimer.Reset(randElectionTimeout) {
			electionTimer = time.NewTimer(randElectionTimeout)
		}
	}

exit:
	log.Printf("[%s] exiting StateMachine()", n.ID)
}

func (n *Node) setTerm(term int64) {
	n.Lock()
	defer n.Unlock()

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
	n.Lock()
	defer n.Unlock()

	log.Printf("[%s] nextTerm()", n.ID)

	n.Term++
	n.State = Follower
	n.VotedFor = ""
	n.Votes = 0
}

func (n *Node) stepDown() {
	n.Lock()
	defer n.Unlock()

	if n.State == Follower {
		return
	}

	log.Printf("[%s] stepDown()", n.ID)

	n.State = Follower
	n.VotedFor = ""
	n.Votes = 0
}

func (n *Node) promoteToLeader() {
	n.Lock()
	defer n.Unlock()

	log.Printf("[%s] promoteToLeader()", n.ID)

	n.State = Leader
	for _, peer := range n.Cluster {
		peer.NextIndex = n.Log.Index
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
	n.Lock()
	n.Votes++
	votes := n.Votes
	n.Unlock()

	majority := (len(n.Cluster)+1)/2 + 1

	log.Printf("[%s] voteGranted() %d >= %d", n.ID, votes, majority)

	if votes >= majority {
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
	for _, peer := range n.Cluster {
		if (n.Log.Index - 1) < peer.NextIndex {
			continue
		}
		er := n.newEntryRequest(peer.NextIndex - 1, n.Log.Get(peer.NextIndex).Data)
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
		peer.NextIndex++
	}
}

func (n *Node) runForLeader() {
	n.Lock()
	defer n.Unlock()

	log.Printf("[%s] runForLeader()", n.ID)

	n.State = Candidate
	n.Votes++
	n.ElectionTerm = n.Term
	n.endElectionChan = make(chan int)
	n.finishedElectionChan = make(chan int)

	go n.gatherVotes()
}

func (n *Node) gatherVotes() {
	vreq := VoteRequest{
		Term:         n.Term,
		CandidateID:  n.Transport.String(),
		LastLogIndex: n.Log.Index,
		LastLogTerm:  n.Log.Term,
	}

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

func (n *Node) RequestVote(vr VoteRequest) (VoteResponse, error) {
	n.requestVoteChan <- vr
	return <-n.requestVoteResponseChan, nil
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
	if bytes.Compare(er.Data, []byte("NOP")) != 0 {
		err := n.Log.Append(er.PrevLogIndex+1, er.Term, er.Data)
		if err != nil {
			log.Printf("[%s] Append error - %s", n.ID, err)
			return EntryResponse{Term: n.Term, Success: false}, nil
		}
	}

	return EntryResponse{Term: n.Term, Success: true}, nil
}

func (n *Node) AppendEntries(er EntryRequest) (EntryResponse, error) {
	n.appendEntriesChan <- er
	return <-n.appendEntriesResponseChan, nil
}

func (n *Node) doCommand(cr CommandRequest) (CommandResponse, error) {
	n.RLock()
	state := n.State
	leaderID := n.ID
	n.RUnlock()

	if state != Leader {
		return CommandResponse{leaderID}, nil
	}

	err := n.Log.Append(n.Log.Index, n.Log.Term, cr.Body)
	if err != nil {
		return CommandResponse{leaderID}, nil
	}

	return CommandResponse{leaderID}, nil
}

func (n *Node) Command(cr CommandRequest) (CommandResponse, error) {
	n.commandChan <- cr
	return <-n.commandResponseChan, nil
}

func (n *Node) sendHeartbeat() {
	n.RLock()
	state := n.State
	er := n.newEntryRequest(n.Log.Index - 1, []byte("NOP"))
	n.RUnlock()

	if state != Leader {
		return
	}

	log.Printf("[%s] sendHeartbeat()", n.ID)

	for _, peer := range n.Cluster {
		go func(p string) {
			_, err := n.Transport.AppendEntriesRPC(p, er)
			if err != nil {
				log.Printf("[%s] error in AppendEntriesRPC() to %s - %s", n.ID, p, err)
				return
			}
		}(peer.ID)
	}
}

func (n *Node) newEntryRequest(index int64, data []byte) EntryRequest {
	entry := n.Log.Get(index)
	prevLogIndex := int64(-1)
	prevLogTerm := int64(-1)
	if entry != nil {
		prevLogIndex = entry.Index
		prevLogTerm = entry.Term
	}
	return EntryRequest{
		LeaderID:     n.ID,
		Term:         n.Term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Data:         data,
	}
}
