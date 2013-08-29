package pontoon

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.Ldate | log.Lmicroseconds)
}

func gimmeNodes(num int) []*Node {
	var nodes []*Node

	for i := 0; i < num; i++ {
		transport := &HTTPTransport{Address: "127.0.0.1:0"}
		logger := &Log{}
		applyer := &StateMachine{}
		node := NewNode(fmt.Sprintf("%d", i), transport, logger, applyer)
		nodes = append(nodes, node)
		nodes[i].Serve()
	}

	// let them start serving
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < len(nodes); i++ {
		for j := 0; j < len(nodes); j++ {
			if j != i {
				nodes[i].AddToCluster(nodes[j].Transport.String())
			}
		}
	}

	for _, node := range nodes {
		node.Start()
	}

	return nodes
}

func countLeaders(nodes []*Node) int {
	leaders := 0
	for i := 0; i < len(nodes); i++ {
		nodes[i].RLock()
		if nodes[i].State == Leader {
			leaders++
		}
		nodes[i].RUnlock()
	}
	return leaders
}

func findLeader(nodes []*Node) *Node {
	for i := 0; i < len(nodes); i++ {
		nodes[i].RLock()
		if nodes[i].State == Leader {
			nodes[i].RUnlock()
			return nodes[i]
		}
		nodes[i].RUnlock()
	}
	return nil
}

func startCluster(num int) ([]*Node, *Node) {
	nodes := gimmeNodes(num)
	for {
		time.Sleep(50 * time.Millisecond)
		if countLeaders(nodes) == 1 {
			break
		}
	}
	leader := findLeader(nodes)
	return nodes, leader
}

func stopCluster(nodes []*Node) {
	for _, node := range nodes {
		node.Exit()
	}
}

func TestStartup(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	nodes, _ := startCluster(3)
	defer stopCluster(nodes)
	time.Sleep(250 * time.Millisecond)
	if countLeaders(nodes) != 1 {
		t.Fatalf("leaders should still be one")
	}
}

func TestNodeKill(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	nodes, leader := startCluster(3)
	defer stopCluster(nodes)
	leader.Exit()
	for {
		time.Sleep(50 * time.Millisecond)
		if countLeaders(nodes) == 1 {
			break
		}
	}
}

func TestCommand(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	nodes, leader := startCluster(5)
	defer stopCluster(nodes)
	for i := int64(0); i < 10; i++ {
		responseChan := make(chan CommandResponse, 1)
		cr := CommandRequest{
			ID:           i + 1,
			Name:         "SUP",
			Body:         []byte("BODY"),
			ResponseChan: responseChan,
		}
		leader.Command(cr)
		<-responseChan
	}

	time.Sleep(10 * time.Second)
}
