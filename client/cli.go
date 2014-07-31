package main

import (
	"fmt"
	"log"
	"net"
)

type Client struct {
	outbound chan string
	inbound  chan string
	conn     net.Conn
}

func newClient() *Client {
	c, err := net.Dial("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	client := &Client{outbound: make(chan string), inbound: make(chan string), conn: c}
	return client
}

func (client *Client) getMessage() {
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
		client.inbound <- string(message)
	}
}

func (client *Client) getInput() {
	var input string
	for {
		fmt.Scanln(input)
		client.outbound <- input
	}
}

func (client *Client) run() {
	defer client.conn.Close()
	go client.getMessage()
	go client.getInput()
	for {
		select {
		case message, ok := <-client.inbound:
			if !ok {
				return
			}
			fmt.Println(string(message))
			//		case input, ok := <-client.outbound:
			//			client.sendMessage()
		}
	}
}

func main() {
	client := newClient()
	client.run()
}
