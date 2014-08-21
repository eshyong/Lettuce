package cli

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/eshyong/lettuce/utils"
)

type Cli struct {
	server net.Conn
}

func NewCli() *Cli {
	c, err := net.Dial("tcp", utils.LOCALHOST+utils.DELIMITER+utils.CLI_CLIENT_PORT)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to server", c.RemoteAddr())
	cli := &Cli{server: c}
	return cli
}

func (cli *Cli) Run() {
	// Make sure serverection socket gets cleaned up.
	defer cli.server.Close()

	// Set up goroutines for reading user input and sending over the wire.
	serverIn := utils.InChanFromConn(cli.server, "server")
	serverOut := utils.OutChanFromConn(cli.server, "server")
	userIn := cli.getInput()

	// Prompt user.
	fmt.Print("> ")
	for {
		select {
		case message, ok := <-serverIn:
			if !ok {
				return
			}
			if message != "" {
				fmt.Println(message)
			}
			fmt.Print("> ")
		case input, ok := <-userIn:
			if !ok {
				return
			}
			serverOut <- input
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
