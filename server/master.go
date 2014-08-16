package server

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

const (
	CLI_CLIENT_PORT = "8000"
	DEADLINE        = time.Second * 10
	TIMEOUT         = time.Second * 5
	SERVER_PORT     = "8080"
)

type Master struct {
	primary net.Conn
	backup  net.Conn
}

func NewMaster() *Master {
	return &Master{primary: nil, backup: nil}
}

// This is run if no servers are discovered on startup. Alternates between polling and sleeping.
func (master *Master) WaitForConnections() {
	fmt.Println("Waiting for server connections...")
	listener, err := net.Listen("tcp", ":"+SERVER_PORT)
	if err != nil {
		log.Fatal("Unable to get a socket: ", err)
	}
	for master.primary == nil || master.backup == nil {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Unable to connect: ", err)
		}
		conn.SetDeadline(DEADLINE)
		if master.primary == nil {
			pingServer(conn, "primary")
			fmt.Println("Primary is running!")
			master.primary = conn
		} else if master.backup == nil {
			pingServer(conn, "backup")
			fmt.Println("Backup is running!")
			master.backup = conn
		}
	}
}

// Check server's status by sending a short message.
func pingServer(server net.Conn, request string) (string, error) {
	// SYN
	fmt.Println("Sending SYN...")
	n, err := fmt.Fprintln(server, "SYN:"+request)
	if n == 0 || err != nil {
		return "", errors.New("SYN failed!")
	}

	// ACK
	scanner := bufio.NewScanner(server)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return "", errors.New("No ACK received")
	}
	body := strings.Split(scanner.Text(), ":")[1]
	return body, nil
}

// Serves any number of clients. TODO: load test.
func (master *Master) Serve() {
	// Create a listener for clients.
	listener, err := net.Listen("tcp", ":"+CLI_CLIENT_PORT)
	if err != nil {
		log.Fatal("Couldn't get a socket: ", err)
	}
	defer listener.Close()
	fmt.Println("Welcome to lettuce! You can connect to this database by " +
		"running `lettuce-cli` in another window.")
	for {
		// Grab a connection.
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("client connected on address", conn.LocalAddr())

		// Handle client session.
		sesh := session(conn)
		go master.handleRequests(sesh)
	}
}

// Middleman function that shuttles data from client to the server.
func (master *Master) handleRequests(sesh chan string) {
	out := master.sendRequestsToServer()
	in := master.getRepliesFromServer()
	for {
		// Send user requests to a primary to execute.
		select {
		case request, ok := <-sesh:
			if !ok {
				return
			}
			out <- request
		case response, ok := <-in:
			if !ok {
				return
			}
			sesh <- response
		}
	}
}

// Asks the server to execute a user command.
func (master *Master) sendRequestsToServer() chan string {
	c := make(chan string)
	go func() {
		for {
			request := <-c
			n, err := fmt.Fprintln(master.primary, request)
			if n == 0 {
				fmt.Println("Server down")
			}
			if err != nil {
				fmt.Println(err)
			}
		}
	}()
	return c
}

// Gets a response from the server.
func (master *Master) getRepliesFromServer() chan string {
	c := make(chan string)
	go func() {
		scanner := bufio.NewScanner(master.primary)
		for scanner.Scan() {
			c <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
		}
	}()
	return c
}

// Client session: gets input from client and sends it to a channel to the master.
// Each session has its own socket connection.
func session(client net.Conn) chan string {
	c := make(chan string)
	go func() {
		// Get input from client user.
		input := getInputFromClient(client)
		defer client.Close()
		defer close(c)
		for {
			select {
			case command, ok := <-input:
				// Send to db server.
				if !ok {
					return
				}
				c <- command
			case reply := <-c:
				// Send db server's reply to the user.
				go sendReplyToClient(client, reply)
			}
		}
	}()
	return c
}

// Gets a database request from the client.
func getInputFromClient(client net.Conn) chan string {
	c := make(chan string)
	go func() {
		scanner := bufio.NewScanner(client)
		for scanner.Scan() {
			// Read from connected client.
			c <- scanner.Text()
		}
		fmt.Printf("client at %v disconnected\n", client.LocalAddr())
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
		}
	}()
	return c
}

// Sends a database response to the client.
func sendReplyToClient(client net.Conn, reply string) {
	n, err := fmt.Fprintln(client, reply)
	if n == 0 {
		fmt.Println("client disconnected")
	}
	if err != nil {
		fmt.Println(err)
	}
}
