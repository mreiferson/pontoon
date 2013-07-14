package pontoon

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func gimmeNodes(num int) []*Node {
	var nodes []*Node

	for i := 0; i < num; i++ {
		transport := &HTTPTransport{Address: "127.0.0.1:0"}
		node := NewNode(fmt.Sprintf("%d", i), transport)
		nodes = append(nodes, node)
		nodes[i].Start()
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

func TestStartup(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	nodes := gimmeNodes(3)

	for {
		time.Sleep(50 * time.Millisecond)
		if countLeaders(nodes) == 1 {
			break
		}
	}

	time.Sleep(250 * time.Millisecond)

	if countLeaders(nodes) != 1 {
		t.Fatalf("leaders should still be one")
	}
}

func TestNodeKill(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	nodes := gimmeNodes(3)

	for {
		time.Sleep(50 * time.Millisecond)
		if countLeaders(nodes) == 1 {
			break
		}
	}

	leader := findLeader(nodes)
	leader.Exit()

	for {
		time.Sleep(50 * time.Millisecond)
		if countLeaders(nodes) == 1 {
			break
		}
	}
}
