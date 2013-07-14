package pontoon

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type HTTPTransport struct {
	Address  string
	node     *Node
	listener net.Listener
}

func (t *HTTPTransport) Serve(node *Node) error {
	t.node = node

	httpListener, err := net.Listen("tcp", t.Address)
	if err != nil {
		log.Fatalf("FATAL: listen (%s) failed - %s", t.Address, err.Error())
	}
	t.listener = httpListener

	go func() {
		log.Printf("[%s] starting HTTP server", t.node.ID)
		server := &http.Server{
			Handler: t,
		}
		err := server.Serve(httpListener)
		// theres no direct way to detect this error because it is not exposed
		if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("ERROR: http.Serve() - %s", err.Error())
		}
		close(t.node.exitChan)
		log.Printf("[%s] exiting Serve()", t.node.ID)
	}()

	return nil
}

func (t *HTTPTransport) Close() error {
	return t.listener.Close()
}

func (t *HTTPTransport) String() string {
	return t.listener.Addr().String()
}

func (t *HTTPTransport) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		ApiResponse(w, 405, nil)
	}

	switch req.URL.Path {
	case "/ping":
		t.pingHandler(w, req)
	case "/request_vote":
		t.requestVoteHandler(w, req)
	case "/append_entries":
		t.appendEntriesHandler(w, req)
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

func (t *HTTPTransport) pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func (t *HTTPTransport) requestVoteHandler(w http.ResponseWriter, req *http.Request) {
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

	resp, err := t.node.RequestVote(vr)
	if err != nil {
		ApiResponse(w, 500, nil)
		return
	}

	ApiResponse(w, 200, resp)
}

func (t *HTTPTransport) appendEntriesHandler(w http.ResponseWriter, req *http.Request) {
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

	resp, err := t.node.AppendEntries(er)
	if err != nil {
		ApiResponse(w, 500, nil)
	}

	ApiResponse(w, 200, resp)
}

func (t *HTTPTransport) RequestVoteRPC(address string, voteRequest VoteRequest) (VoteResponse, error) {
	endpoint := fmt.Sprintf("http://%s/request_vote", address)
	log.Printf("[%s] RequestVoteRPC %+v to %s", t.node.ID, voteRequest, endpoint)
	data, err := ApiRequest("POST", endpoint, voteRequest, 100*time.Millisecond)
	if err != nil {
		return VoteResponse{}, err
	}
	term, _ := data.Get("term").Int64()
	voteGranted, _ := data.Get("vote_granted").Bool()
	vresp := VoteResponse{
		Term:        term,
		VoteGranted: voteGranted,
	}
	return vresp, nil
}

func (t *HTTPTransport) AppendEntriesRPC(address string, entryRequest EntryRequest) (EntryResponse, error) {
	endpoint := fmt.Sprintf("http://%s/append_entries", address)
	log.Printf("[%s] AppendEntriesRPC %+v to %s", t.node.ID, entryRequest, endpoint)
	_, err := ApiRequest("POST", endpoint, entryRequest, 500*time.Millisecond)
	if err != nil {
		return EntryResponse{}, err
	}
	return EntryResponse{}, nil
}
