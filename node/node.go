package node

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/eshyong/lettuce/db"
	"github.com/eshyong/lettuce/server"
)

type Node struct {
	store  *db.Store
	server *server.Server
	master net.Conn
}

func NewNode(address string, port string) *Node {
	// Connect to the master node.
	m, err := net.Dial("tcp", address+":"+port)
	if err != nil {
		log.Fatal(err)
	}
	return &Node{store: db.NewStore(), server: server.NewServer(), master: m}
}

func (node *Node) Run() {
	client := node.server.Serve()
	masterChan := make(chan string)
	go node.handleSignals()
	for {
		select {
		case input := <-client:

		}
	}
}

func (node *Node) handleSignals() {
	// Create a channel to catch os signals.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	// Block on this channel until signal is received, then flush db.
	sig := <-c
	fmt.Println(sig, "signal received, flushing buffers to disk...")
	node.store.Flush()
	fmt.Println("Goodbye!")
	os.Exit(0)
}

func talkToMaster() {

}
