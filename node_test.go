package pontoon

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestStartup(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	numNodes := 3

	var nodes []*Node
	for i := 0; i < numNodes; i++ {
		node := NewNode(fmt.Sprintf("%d", i))
		nodes = append(nodes, node)
		nodes[i].Serve("127.0.0.1:0")
	}

	// let them start serving
	time.Sleep(100 * time.Millisecond)
	
	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if j != i {
				nodes[i].AddToCluster(nodes[j].httpListener.Addr().String())
			}
		}
	}

	for {
		time.Sleep(50 * time.Millisecond)

		leaders := 0
		for i := 0; i < numNodes; i++ {
			nodes[i].RLock()
			if nodes[i].State == Leader {
				leaders++
			}
			nodes[i].RUnlock()
		}

		if leaders == 1 {
			break
		}
	}

	time.Sleep(250 * time.Millisecond)

	leaders := 0
	for i := 0; i < numNodes; i++ {
		nodes[i].RLock()
		if nodes[i].State == Leader {
			leaders++
		}
		nodes[i].RUnlock()
	}

	if leaders != 1 {
		t.Fatalf("leaders should still be one")
	}
}
