package pontoon

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

type Logger interface {
	Check(prevLogIndex int64, prevLogTerm int64, index int64, term int64) error
	Append(index int64, term int64, data []byte) error
	FresherThan(index int64, term int64) bool
	Get(index int64) *Entry
	Index() int64
	LastIndex() int64
	Term() int64
}

type Peer struct {
	ID        string
	NextIndex int64
}

type CommandRequest struct {
	ID              int64
	Name            string
	Body            []byte
	responseChannel chan bool
}

type CommandResponse struct {
	LeaderID string
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

type EntryRequest struct {
	Term           int64  `json:"term"`
	LeaderID       string `json:"leader_id"`
	PrevLogIndex   int64  `json:"prev_log_index"`
	PrevLogTerm    int64  `json:"prev_log_term"`
	Data           []byte `json:"data"`
	commandRequest *CommandRequest
}

type EntryResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}
