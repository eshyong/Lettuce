package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

type Cli struct {
	outbound chan string
	inbound  chan string
	conn     net.Conn
	running  bool
}

func newCli() *Cli {
	c, err := net.Dial("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	cli := &Cli{outbound: make(chan string), inbound: make(chan string), conn: c, running: true}
	return cli
}

func (cli *Cli) run() {
	// Make sure connection socket gets cleaned up.
	defer cli.conn.Close()

	// Set up goroutines for reading user input and sending over the wire.
	go cli.getMessage()
	go cli.getInput()

	// Prompt user.
	fmt.Print("> ")
	for cli.running {
		select {
		case message, ok := <-cli.inbound:
			if !ok {
				cli.running = false
				return
			}
			fmt.Print(message)
			fmt.Print("> ")
		case input, ok := <-cli.outbound:
			if !ok {
				cli.running = false
				return
			}
			go cli.sendMessage(input)
		}
	}
}

func (cli *Cli) getInput() {
	// Send a message from Stdin through a gochannel.
	defer close(cli.outbound)
	reader := bufio.NewReader(os.Stdin)
	for cli.running {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		cli.outbound <- input
	}
}

func (cli *Cli) sendMessage(message string) {
	// Send commands to the server through a TCPConn.
	n, err := cli.conn.Write([]byte(message))
	if n == 0 {
		fmt.Println("server disconnected")
	}
	if err != nil {
		fmt.Println(err)
	}
}

func (cli *Cli) getMessage() {
	// Receive messages from the server.
	defer close(cli.inbound)
	reader := bufio.NewReader(cli.conn)
	for {
		message, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("server disconnected")
			break
		}
		// Send server messages over gochannel.
		cli.inbound <- message
	}
}

func main() {
	cli := newCli()
	cli.run()
}
