package pontoon

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
