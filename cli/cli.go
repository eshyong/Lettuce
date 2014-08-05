package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

type Cli struct {
	conn net.Conn
}

func newCli() *Cli {
	c, err := net.Dial("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	cli := &Cli{conn: c}
	return cli
}

func (cli *Cli) run() {
	// Make sure connection socket gets cleaned up.
	defer cli.conn.Close()

	// Set up goroutines for reading user input and sending over the wire.
	inbound := cli.getMessage()
	outbound := cli.getInput()

	// Prompt user.
	fmt.Print("> ")
	for {
		select {
		case message, ok := <-inbound:
			if !ok {
				return
			}
			fmt.Print(message)
			fmt.Print("> ")
		case input, ok := <-outbound:
			if !ok {
				return
			}
			go cli.sendMessage(input)
		}
	}
}

func (cli *Cli) getInput() chan string {
	c := make(chan string)
	go func() {
		// Use buffered io for easier reading.
		defer close(c)
		reader := bufio.NewReader(os.Stdin)
		for {
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println(err)
				break
			}
			// Send cli input over the channel.
			c <- input
		}
	}()
	return c
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

func (cli *Cli) getMessage() chan string {
	c := make(chan string)
	go func() {
		// Use buffered io for easier reading.
		defer close(c)
		reader := bufio.NewReader(cli.conn)
		for {
			message, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("server disconnected")
				break
			}
			// Send server messagesover the channel.
			c <- message
		}
	}()
	return c
}

func main() {
	cli := newCli()
	cli.run()
}
