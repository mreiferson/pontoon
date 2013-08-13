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

	heartbeatInterval := 100 * time.Millisecond
	heartbeatTimer := time.NewTicker(heartbeatInterval)

	for {
		select {
		case <-n.exitChan:
			n.StepDown()
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
			n.ElectionTimeout()
		case vresp := <-n.voteResponseChan:
			n.VoteResponse(vresp)
		case <-heartbeatTimer.C:
			n.SendHeartbeat()
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

func (n *Node) SetTerm(term int64) {
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

func (n *Node) NextTerm() {
	n.Lock()
	defer n.Unlock()

	log.Printf("[%s] NextTerm()", n.ID)

	n.Term++
	n.State = Follower
	n.VotedFor = ""
	n.Votes = 0
}

func (n *Node) StepDown() {
	n.Lock()
	defer n.Unlock()

	if n.State == Follower {
		return
	}

	log.Printf("[%s] StepDown()", n.ID)

	n.State = Follower
	n.VotedFor = ""
	n.Votes = 0
}

func (n *Node) PromoteToLeader() {
	n.Lock()
	defer n.Unlock()

	log.Printf("[%s] PromoteToLeader()", n.ID)

	n.State = Leader
	for _, peer := range n.Cluster {
		peer.NextIndex = n.Log.Index + 1
	}
}

func (n *Node) ElectionTimeout() {
	if n.State == Leader {
		return
	}

	log.Printf("[%s] ElectionTimeout()", n.ID)

	n.NextTerm()
	n.RunForLeader()
}

func (n *Node) VoteResponse(vresp VoteResponse) {
	if n.State != Candidate {
		return
	}

	if vresp.Term != n.ElectionTerm {
		if vresp.Term > n.ElectionTerm {
			// we discovered a higher term
			n.EndElection()
			n.SetTerm(vresp.Term)
		}
		return
	}

	if vresp.VoteGranted {
		n.VoteGranted()
	}
}

func (n *Node) VoteGranted() {
	n.Lock()
	n.Votes++
	votes := n.Votes
	n.Unlock()

	majority := (len(n.Cluster)+1)/2 + 1

	log.Printf("[%s] VoteGranted() %d >= %d", n.ID, votes, majority)

	if votes >= majority {
		// we won election, end it and promote
		n.EndElection()
		n.PromoteToLeader()
	}
}

func (n *Node) EndElection() {
	if n.State != Candidate || n.endElectionChan == nil {
		return
	}

	log.Printf("[%s] EndElection()", n.ID)

	close(n.endElectionChan)
	<-n.finishedElectionChan

	n.endElectionChan = nil
	n.finishedElectionChan = nil
}

// - Increment currentTerm, vote for self
// - Reset election timeout
// - Send RequestVote RPCs to all other servers, wait for either:
//   - Votes received from majority of servers: become leader
//   - AppendEntries RPC received from new leader: step down
//   - Election timeout elapses without election resolution: increment term, start new election
//   - Discover higher term: step down
func (n *Node) RunForLeader() {
	n.Lock()
	defer n.Unlock()

	log.Printf("[%s] RunForLeader()", n.ID)

	n.State = Candidate
	n.Votes++
	n.ElectionTerm = n.Term
	n.endElectionChan = make(chan int)
	n.finishedElectionChan = make(chan int)

	go n.GatherVotes()
}

func (n *Node) GatherVotes() {
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
				log.Printf("[%s] error in RequestVoteRPC() to %s - %s", n.ID, p, err.Error())
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
		n.EndElection()
		n.SetTerm(vr.Term)
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
		n.EndElection()
		n.StepDown()
	}

	if er.Term > n.Term {
		n.SetTerm(er.Term)
	}

	err := n.Log.Check(er.PrevLogIndex, er.PrevLogTerm, er.PrevLogIndex+1, er.Term)
	if err != nil {
		log.Printf("[%s] Check error - %s", n.ID, err)
		return EntryResponse{Term: n.Term, Success: false}, nil
	}
	if bytes.Compare(er.Data, []byte("NOP")) != 0 {
		err := n.Log.Append(er.PrevLogIndex, er.Term, er.Data)
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

	return CommandResponse{}, nil
}

func (n *Node) Command(cr CommandRequest) (CommandResponse, error) {
	n.commandChan <- cr
	return <-n.commandResponseChan, nil
}

func (n *Node) SendHeartbeat() {
	n.RLock()
	state := n.State

	var prevEntry *Entry
	prevLogIndex := n.Log.Index - 1
	prevLogTerm := int64(-1)
	if prevLogIndex >= 0 {
		prevEntry = n.Log.Entries[prevLogIndex]
		prevLogTerm = prevEntry.Term
	}
	er := EntryRequest{
		LeaderID:     n.ID,
		Term:         n.Term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Data:         []byte("NOP"),
	}

	n.RUnlock()

	if state != Leader {
		return
	}

	log.Printf("[%s] SendHeartbeat()", n.ID)

	for _, peer := range n.Cluster {
		go func(p string) {
			_, err := n.Transport.AppendEntriesRPC(p, er)
			if err != nil {
				log.Printf("[%s] error in AppendEntriesRPC() to %s - %s", n.ID, p, err.Error())
				return
			}
		}(peer.ID)
	}
}
