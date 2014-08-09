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

func NewCli() *Cli {
	c, err := net.Dial("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to server", c.RemoteAddr())
	cli := &Cli{conn: c}
	return cli
}

func (cli *Cli) Run() {
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
			if message != "" {
				fmt.Println(message)
			}
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
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			// Send cli input over the channel.
			c <- scanner.Text()
		}
		// Non-EOF error.
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
		}
	}()
	return c
}

func (cli *Cli) sendMessage(message string) {
	// Send commands to the server through a TCPConn.
	n, err := fmt.Fprintln(cli.conn, message)
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
		scanner := bufio.NewScanner(cli.conn)
		for scanner.Scan() {
			// Send server messages over the channel.
			c <- scanner.Text()
		}
		fmt.Println("server disconnected")
		if err := scanner.Err(); err != nil {
			// Some error other than EOF.
			fmt.Println(err)
		}
	}()
	return c
}

func main() {
	cli := NewCli()
	cli.Run()
}
