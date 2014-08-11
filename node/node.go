package node

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/eshyong/lettuce/db"
	"github.com/eshyong/lettuce/server"
)

const (
	NODE_LISTENING_PORT = "8001"
	NODE_CAPACITY       = 4
)

type Node struct {
	store    *db.Store
	server   *server.Server
	others   []net.Conn
	isMaster bool
	master   string
	self     string
}

func NewNode() *Node {
	// Connect to the master node.
	o := make([]net.Conn, 0, NODE_CAPACITY)
	node := &Node{store: db.NewStore(), server: server.NewServer(), others: o}
	return node
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

func (node *Node) readConfig() []string {
	file, err := os.Open("config/nodes")
	if err != nil {
		log.Fatal("Configuration file not found, unable to load nodes.")
	}

	// Create a buffered reader and read each line.
	scanner := bufio.NewScanner(file)
	addrList := make([]string, 0, 5)
	for scanner.Scan() {
		// Config file lines have the format: "name address"
		// where address is in IP address form.
		substrs := strings.Split(scanner.Text(), " ")
		addr := substrs[0]
		name := substrs[1]

		// Special nodes.
		if name == "master" {
			node.master = addr
		}
		if name == "self" {
			node.self = addr
			continue
		}
		addrList = append(addrList, addr)
	}
	return addrList
}

func (node *Node) connectToOthers() {
	// Attempt to connect to other nodes in the network.
	addrList := node.readConfig()
	wait := make(chan bool)
	for _, a := range addrList {
		// Try to connect to each node asynchronously.
		go func(address string) {
			conn, err := net.DialTimeout("tcp", address, time.Second*5)
			if err != nil {
				fmt.Println("Unable to connect to", address)
			} else {
				fmt.Println("Connected to", address)
				node.others = append(node.others, conn)
			}
			wait <- true
		}(a + ":" + NODE_LISTENING_PORT)
	}

	// Wait channel hack.
	for i := 0; i < len(addrList); i++ {
		<-wait
	}
}

func (node *Node) listenForOthers() {
	// Listen for other nodes.
	listener, err := net.Listen("tcp", "0.0.0.0:"+NODE_LISTENING_PORT)
	if err != nil {
		log.Fatal(err)
	}

	for {
		// Accept connections.
		conn, err := listener.Accept()
		if err != nil && err != io.EOF {
			fmt.Println(err)
			continue
		}
		fmt.Println("Connected to", conn.LocalAddr())
		sendMessage(conn)
		if len(node.others) < NODE_CAPACITY {
			node.others = append(node.others, conn)
		}
	}
}

func sendMessage(conn net.Conn) {
	conn.Write([]byte("hello world\n"))
}

func (node *Node) Run() {
	go node.listenForOthers()
	node.connectToOthers()
	fmt.Println("Welcome to Lettuce! You can connect through another window by running lettuce-cli.")
	node.server.Serve()
	for {
		time.Sleep(time.Millisecond)
	}
}
