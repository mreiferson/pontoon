package pontoon

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
	Term int64 `json:"term"`
}

type EntryResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}
