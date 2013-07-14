package pontoon

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

func (n *Node) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		ApiResponse(w, 405, nil)
	}

	switch req.URL.Path {
	case "/ping":
		n.pingHandler(w, req)
	case "/request_vote":
		n.requestVoteHandler(w, req)
	case "/append_entries":
		n.appendEntriesHandler(w, req)
	default:
		ApiResponse(w, 404, nil)
	}
}

func ApiResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	response, err := json.Marshal(data)
	if err != nil {
		response = []byte("INTERNAL_ERROR")
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.WriteHeader(statusCode)
	w.Write(response)
}

func (n *Node) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
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

func (n *Node) requestVoteHandler(w http.ResponseWriter, req *http.Request) {
	var vr VoteRequest

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		ApiResponse(w, 500, nil)
		return
	}

	err = json.Unmarshal(data, &vr)
	if err != nil {
		ApiResponse(w, 500, nil)
		return
	}

	resp, err := n.RequestVote(vr)
	if err != nil {
		ApiResponse(w, 500, nil)
		return
	}

	ApiResponse(w, 200, resp)
}

type EntryRequest struct {
}

type EntryResponse struct {
}

func (n *Node) appendEntriesHandler(w http.ResponseWriter, req *http.Request) {
	var er EntryRequest

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		ApiResponse(w, 500, nil)
		return
	}

	err = json.Unmarshal(data, &er)
	if err != nil {
		ApiResponse(w, 500, nil)
		return
	}

	resp, err := n.AppendEntries(er)
	if err != nil {
		ApiResponse(w, 500, nil)
	}

	ApiResponse(w, 200, resp)
}
