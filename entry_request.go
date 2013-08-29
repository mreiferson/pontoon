package pontoon

type EntryRequest struct {
	CmdID        int64  `json:"cmd_id"`
	Term         int64  `json:"term"`
	LeaderID     string `json:"leader_id"`
	PrevLogIndex int64  `json:"prev_log_index"`
	PrevLogTerm  int64  `json:"prev_log_term"`
	Data         []byte `json:"data"`
}

type EntryResponse struct {
	Term    int64 `json:"term"`
	Success bool  `json:"success"`
}

func newEntryRequest(cmdID int64, leaderID string, term int64, prevLogIndex int64, prevLogTerm int64, data []byte) EntryRequest {
	return EntryRequest{
		CmdID:        cmdID,
		LeaderID:     leaderID,
		Term:         term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Data:         data,
	}
}
