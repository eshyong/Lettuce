package server

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

type Master struct {
	// DB servers that master manages.
	primary   net.Conn
	backup    net.Conn
	serverIn  <-chan string
	serverOut chan<- string
	sessions  map[string]chan<- string
	up        bool
	counter   uint64
}

func NewMaster() *Master {
	return &Master{primary: nil, backup: nil, sessions: make(map[string]chan<- string), up: true, counter: 0}
}

// This is run if no servers are discovered on startup. Alternates between polling and sleeping.
func (master *Master) WaitForConnections() {
	fmt.Println("Waiting for server connections...")
	listener, err := net.Listen("tcp", ":"+SERVER_PORT)
	if err != nil {
		log.Fatal("Unable to get a socket: ", err)
	}
	defer listener.Close()

	// Wait until a primary and a backup connect.
	for master.primary == nil || master.backup == nil {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Unable to connect: ", err)
		}

		if master.primary == nil {
			// Connect primary first.
			message, err := pingServer(conn, "primary")
			if err != nil {
				fmt.Println(err)
				continue
			}
			if message == OK {
				fmt.Println("Primary is running!")
			}
			master.primary = conn
		} else if master.backup == nil {
			// Then connect the backup.
			message, err := pingServer(conn, "backup")
			if err != nil {
				fmt.Println(err)
				continue
			}
			if message == OK {
				fmt.Println("Backup is running!")
			}
			master.backup = conn
		}
	}
	// Notify the backup servers to connect to the master.
	message, err := pingServer(master.backup, "primary="+LOCALHOST+":"+SERVER_PORT)
	if err != nil {
		log.Fatal(err)
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
	fmt.Println("Waiting for ACK...")
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

	// Create channels to listen on primary.
	master.serverIn = master.getServerReplies()
	master.serverOut = master.sendServerRequests()
	defer close(master.serverOut)

	// Funnel requests into a multiplexer.
	mux := master.funnelRequests()
	for {
		// Grab a connection.
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("client connected on address", conn.LocalAddr())

		// Create a new session ID, and add the session to our multiplexer set.
		id := "session" + strconv.FormatUint(master.counter, 10)
		master.sessions[id] = session(conn, mux, id)
		master.counter += 1
	}
}

// Creates a multiplexer for all client sessions to write to. Dispatches to the primary
// server, and determines which session channel to write back to.
func (master *Master) funnelRequests() chan<- string {
	multiplexer := make(chan string)
	go func() {
		defer close(multiplexer)
	loop:
		for {
			select {
			case request, _ := <-multiplexer:
				// Send any sessions request to the server.
				arr := strings.Split(request, ":")
				if len(arr) < 2 {
					fmt.Println("Invalid request", arr)
					break
				}
				sender, body := arr[0], arr[1]
				if body == "CLOSED" {
					// One of our client connections closed, delete the mapped value.
					delete(master.sessions, sender)
				} else {
					master.serverOut <- request
				}
			case reply, ok := <-master.serverIn:
				// Get a server reply, and determine which session to send to.
				if !ok {
					break loop
				}
				arr := strings.Split(reply, ":")
				if len(arr) < 2 {
					fmt.Println("Invalid reply", arr)
				} else {
					// session#NUM:reply -> "send reply to session#NUM"
					recipient := arr[0]
					body := arr[1]
					if channel, in := master.sessions[recipient]; in {
						channel <- body
					}
				}
			}
		}
	}()
	return multiplexer
}

func (master *Master) promoteBackup() bool {
	if master.backup == nil {
		master.WaitForConnections()
	}
	message, err := pingServer(master.backup, "primary")
	if err != nil {
		fmt.Println("Ping failed!")
		return false
	} else {
		if message == OK {
			// Assign primary to backup.
			fmt.Println("Promotion success")
			master.primary = master.backup
			master.backup = nil

			// Wait for backup to come online.

			// Create new channels for server.
			master.serverIn = master.getServerReplies()
			return true
		}
		fmt.Println("Server denied request")
		return false
	}
}

func (master *Master) sendServerRequests() chan<- string {
	serverOut := make(chan string)
	go func() {
		for {
			request, ok := <-serverOut
			if !ok {
				break
			}
			// Write requests to the server.
			n, err := fmt.Fprintln(master.primary, request)
			if n == 0 || err != nil {
				fmt.Println("sendServerRequest()", err)
				break
			}
		}
	}()
	return serverOut
}

func (master *Master) getServerReplies() <-chan string {
	serverIn := make(chan string)
	go func() {
		defer close(serverIn)
		scanner := bufio.NewScanner(master.primary)
		for scanner.Scan() {
			// Send server replies back down the channel.
			serverIn <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("getServerReply():", err)
		}
	}()
	return serverIn
}

// Client session: gets input from client and sends it to a channel to the master.
// Each session has its own socket connection.
func session(client net.Conn, mux chan<- string, id string) chan<- string {
	session := make(chan string)
	go func() {
		// Get IO from client user.
		clientIn := getInputFromClient(client)
		clientOut := sendReplyToClient(client)
		defer client.Close()
		defer close(clientOut)

		// TODO: associate client IDs for each session.
	loop:
		for {
			// Shuttle data between server and client.
			select {
			case request, ok := <-clientIn:
				if !ok {
					break loop
				}
				// Request format "ID:request".
				request = id + ":" + request
				mux <- request
			case reply, ok := <-session:
				if !ok {
					break loop
				}
				// No demarshaling required on the client side.
				clientOut <- reply
			}
		}
		mux <- id + ":CLOSED"
		fmt.Printf("Client at %v disconnected\n", client.LocalAddr())
	}()
	return session
}

// Creates a read-only channel to consume input from the client.
func getInputFromClient(client net.Conn) <-chan string {
	clientIn := make(chan string)
	go func() {
		defer close(clientIn)

		// Scan each line of input and feed it into the channel.
		scanner := bufio.NewScanner(client)
		for scanner.Scan() {
			clientIn <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("getInputFromClient():", err)
		}
	}()
	return clientIn
}

func sendReplyToClient(client net.Conn) chan<- string {
	clientOut := make(chan string)
	go func() {
		for {
			// Send replies to client.
			reply, ok := <-clientOut
			if !ok {
				break
			}
			n, err := fmt.Fprintln(client, reply)
			if n == 0 || err != nil {
				fmt.Println("sendReplyToClient():", err)
				break
			}
		}
	}()
	return clientOut
}
