package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

type Client struct {
	outbound chan string
	inbound  chan string
	conn     net.Conn
	running  bool
}

func newClient() *Client {
	c, err := net.Dial("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	client := &Client{outbound: make(chan string), inbound: make(chan string), conn: c, running: true}
	return client
}

func (client *Client) run() {
	// Make sure connection socket gets cleaned up.
	defer client.conn.Close()

	// Set up goroutines for reading user input and sending over the wire.
	go client.getMessage()
	go client.getInput()

	// Event loop
	for client.running {
		select {
		case message, ok := <-client.inbound:
			if !ok {
				client.running = false
				return
			}
			fmt.Println(string(message))
		case input, ok := <-client.outbound:
			if !ok {
				client.running = false
				return
			}
			go client.sendMessage(input)
		}
	}
}

func (client *Client) getInput() {
	// Send a message from Stdin through a gochannel.
	defer close(client.outbound)
	reader := bufio.NewReader(os.Stdin)
	for client.running {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err)
		}
		client.outbound <- input
	}
}

func (client *Client) sendMessage(message string) {
	// Send commands to the server through a TCPConn.
	n, err := client.conn.Write([]byte(message))
	if n == 0 {
		fmt.Println("Server disconnected")
	}
	if err != nil {
		fmt.Println(err)
	}
}

func (client *Client) getMessage() {
	// Receive messages from the server.
	message := make([]byte, 1024)
	defer close(client.inbound)
	for {
		n, err := client.conn.Read(message)
		if n == 0 {
			fmt.Println("server disconnected")
			break
		}
		if err != nil {
			fmt.Println(err)
			break
		}
		// Send server messages over gochannel.
		client.inbound <- string(message)
	}
}

func main() {
	client := newClient()
	client.run()
}
