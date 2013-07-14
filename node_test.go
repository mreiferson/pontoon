package pontoon

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestStartup(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	node1 := NewNode("1")
	node2 := NewNode("2")
	node3 := NewNode("3")

	node1.Serve("127.0.0.1:0")
	node2.Serve("127.0.0.1:0")
	node3.Serve("127.0.0.1:0")

	time.Sleep(100 * time.Millisecond)

	node1.AddToCluster(node2.httpListener.Addr().String())
	node1.AddToCluster(node3.httpListener.Addr().String())

	node2.AddToCluster(node1.httpListener.Addr().String())
	node2.AddToCluster(node3.httpListener.Addr().String())

	node3.AddToCluster(node1.httpListener.Addr().String())
	node3.AddToCluster(node2.httpListener.Addr().String())

	for {
		node1.RLock()
		if node1.State == Leader {
			break
		}
		node1.RUnlock()

		node2.RLock()
		if node2.State == Leader {
			break
		}
		node2.RUnlock()

		node3.RLock()
		if node3.State == Leader {
			break
		}
		node3.RUnlock()

		time.Sleep(10 * time.Millisecond)
	}
}
