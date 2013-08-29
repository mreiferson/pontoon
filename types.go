package pontoon

// At any given time each server is in one of three states:
// leader, follower, or candidate.
const (
	Follower = iota
	Candidate
	Leader
)

const (
	Initialized = iota
	Logged
	Committed
)

type Transporter interface {
	Serve(node *Node) error
	Close() error
	String() string
	RequestVoteRPC(address string, voteRequest VoteRequest) (VoteResponse, error)
	AppendEntriesRPC(address string, entryRequest EntryRequest) (EntryResponse, error)
}

type Logger interface {
	Check(prevLogIndex int64, prevLogTerm int64, index int64, term int64) error
	Append(e *Entry) error
	FresherThan(index int64, term int64) bool
	Get(index int64) *Entry
	GetEntryForRequest(index int64) (*Entry, int64, int64)
	Index() int64
	LastIndex() int64
	Term() int64
}

type Applyer interface {
	Apply(cr *CommandRequest) error
}

type Peer struct {
	ID        string
	NextIndex int64
}

type CommandRequest struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	Body             []byte `json:"body"`
	ResponseChan     chan CommandResponse
	ReplicationCount int32
	State            int32
}

type CommandResponse struct {
	LeaderID string
	Success  bool
}

type VoteRequest struct {
	Term         int64  `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int64  `json:"last_log_index"`
	LastLogTerm  int64  `json:"last_log_term"`
}

type VoteResponse struct {
	Term        int64 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}
