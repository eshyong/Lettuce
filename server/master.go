package server

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/eshyong/lettuce/utils"
)

type Master struct {
	// DB servers that master manages.
	primary       net.Conn
	backup        net.Conn
	primaryIn     <-chan string
	primaryOut    chan<- string
	backupIn      <-chan string
	backupOut     chan<- string
	sessions      map[string]chan<- string
	counter       uint64
	pingedPrimary bool
	pingedBackup  bool
}

func NewMaster() *Master {
	return &Master{primary: nil, backup: nil,
		sessions:  make(map[string]chan<- string),
		primaryIn: nil, primaryOut: nil,
		backupIn: nil, backupOut: nil,
		counter:       0,
		pingedPrimary: false, pingedBackup: false}
}

// This is run if no servers are discovered on startup. Alternates between polling and sleeping.
func (master *Master) WaitForConnections() {
	fmt.Println("Waiting for server connections...")
	listener, err := net.Listen("tcp", utils.DELIMITER+utils.SERVER_PORT)
	if err != nil {
		log.Fatal("Unable to get a socket: ", err)
	}
	defer listener.Close()

	// Wait for a primary and backup to connect.
	for master.primary == nil {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Unable to connect: ", err)
		}
		master.checkServer(conn, true)
	}
	for master.backup == nil {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Unable to connect: ", err)
		}
		master.checkServer(conn, false)
	}
}

func (master *Master) checkServer(conn net.Conn, promote bool) (string, error) {
	var retmsg, request string
	if request = utils.STATUS; promote {
		request = utils.PROMOTE
	}

	// Send a STATUS message.
	message, err := pingServer(conn, request)
	if err != nil {
		return "", err
	}
	fmt.Println("message:", message)

	// Hopefully receive an "OK" in response.
	if message != utils.OK {
		return "", errors.New("Request rejected.")
	}
	if promote {
		retmsg = "Primary is running!"
		master.primary = conn
	} else {
		retmsg = "Backup is running!"
		master.backup = conn
	}
	return retmsg, nil
}

// Check server's status by sending a short message.
func pingServer(server net.Conn, request string) (string, error) {
	// SYN
	fmt.Println("Sending SYN...")
	n, err := fmt.Fprintln(server, utils.SYNDEL+request)
	if n == 0 || err != nil {
		return "", errors.New("SYN failed!")
	}

	// ACK
	fmt.Println("Waiting for ACK...")
	scanner := bufio.NewScanner(server)
	scanner.Scan()
	fmt.Println("scanner.Text():", scanner.Text())
	if err := scanner.Err(); err != nil {
		return "", errors.New("No ACK received")
	}

	// Get message
	message := strings.Split(scanner.Text(), utils.DELIMITER)
	if len(message) < 2 || message[0] != utils.ACK {
		return "", errors.New("Invalid message: " + scanner.Text())
	}
	return message[1], nil
}

// Serves any number of clients. TODO: load test.
func (master *Master) Serve() {
	// Create a listener for clients.
	listener, err := net.Listen("tcp", utils.DELIMITER+utils.CLI_CLIENT_PORT)
	if err != nil {
		log.Fatal("Couldn't get a socket: ", err)
	}
	defer listener.Close()

	// Create channels to listen on primary.
	master.primaryIn = utils.InChanFromConn(master.primary, "primary")
	master.primaryOut = utils.OutChanFromConn(master.primary, "primary")
	master.backupIn = utils.InChanFromConn(master.backup, "backup")
	master.backupOut = utils.OutChanFromConn(master.backup, "backup")
	defer close(master.primaryOut)

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
		id := utils.CLIENT + strconv.FormatUint(master.counter, 10)
		master.sessions[id] = session(conn, mux, id)
		master.counter += 1
	}
}

func (master *Master) pingServers() {
	// Ping primary and make sure server is up.
	fmt.Println("Pinging primary...")
	message, err := pingServer(master.primary, utils.STATUS)
	if err != nil || message != "OK" {
		// Promote backup
		log.Fatal("TODO: promote backup")
	}
	// Ping backup and make sure server is up.
	fmt.Println("Pinging backup...")
	message, err = pingServer(master.backup, utils.STATUS)
	if err != nil || message != "OK" {
		// Wait for backups to connect.
		log.Fatal("TODO: wait for backups")
	}
	fmt.Println("All servers up.")
}

// Creates a multiplexer for all client sessions to write to. Dispatches to the primary
// server, and determines which session channel to write back to.
func (master *Master) funnelRequests() chan<- string {
	multiplexer := make(chan string)
	go func() {
		defer close(multiplexer)
		begin := time.Now()
	loop:
		for {
			elapsed := time.Since(begin)
			select {
			case request, _ := <-multiplexer:
				master.handleClientRequest(request)
			case reply, ok := <-master.primaryIn:
				// Get a server reply, and determine which session to send to.
				if !ok {
					break loop
				}
				master.handlePrimaryIn(reply)
			case reply, ok := <-master.backupIn:
				// Get a ping from backup.
				if !ok {
					break loop
				}
				master.handleBackupIn(reply)
			default:
				// Ping servers and make sure they're up.
				if (master.pingedPrimary || master.pingedBackup) && elapsed > utils.DEADLINE {
					if master.pingedPrimary {
						// Promote backup
						continue
					}
					if master.pingedBackup {
						// Wait for other backups
						continue
					}
				}
				if elapsed > utils.WAIT_PERIOD {
					// Reset begin.
					begin = time.Now()

					master.primaryOut <- utils.SYNDEL + utils.STATUS
					master.backupOut <- utils.SYNDEL + utils.STATUS
					master.pingedPrimary = true
					master.pingedBackup = true
				}
			}
		}
	}()
	return multiplexer
}

// Send any sessions request to the server.
func (master *Master) handleClientRequest(request string) {
	// Check if message is in a valid format.
	arr := strings.Split(request, utils.DELIMITER)
	if len(arr) < 2 {
		fmt.Println("Invalid request", arr)
		return
	}
	sender, body := arr[0], arr[1]
	if body == utils.CLOSED {
		// One of our client connections closed, delete the mapped value.
		delete(master.sessions, sender)
	} else {
		// Otherwise send it out to the server.
		master.primaryOut <- request
	}
}

func (master *Master) handlePrimaryIn(reply string) {
	// Check if message is in a valid format.
	arr := strings.Split(reply, utils.DELIMITER)
	if len(arr) < 2 {
		fmt.Println("Invalid reply", arr)
		return
	}

	header, body := arr[0], arr[1]
	if header == utils.ACK {
		if !master.pingedPrimary {
			fmt.Println("Something interesting happened")
			return
		}
		if body != utils.OK {
			fmt.Println("Something wrong with the primary")
			// TODO: figure this out.
			return
		}
		// Everything is fine.
		fmt.Println("Primary is fine.")
		master.pingedPrimary = false
	} else if strings.Contains(header, utils.CLIENT) {
		// clientID:reply -> "send reply to CLIENT#"
		if channel, in := master.sessions[header]; in {
			channel <- body
		}
	} else {
		fmt.Println("Unknown protocol message:", reply)
		master.primaryOut <- utils.ERRDEL + utils.UNKNOWN
	}
}

func (master *Master) handleBackupIn(reply string) {
	// Check if message is in a valid format.
	arr := strings.Split(reply, utils.DELIMITER)
	if len(arr) < 2 {
		fmt.Println("Invalid reply", arr)
		return
	}
	header, body := arr[0], arr[1]
	if header != utils.ACK {
		fmt.Println("Unknown protocol message:", reply)
		master.primaryOut <- utils.ERRDEL + utils.UNKNOWN
	}
	if body != utils.OK {
		fmt.Println("Something wrong with the backup")
		// TODO: figure this out.
		return
	}
	// Everything is fine.
	fmt.Println("Backup is fine.")
	master.pingedBackup = false
}

func (master *Master) promoteBackup() bool {
	if master.backup == nil {
		master.WaitForConnections()
	}
	message, err := pingServer(master.backup, utils.PRIMARY)
	if err != nil {
		fmt.Println("Ping failed!")
		return false
	}
	if message == utils.OK {
		// Assign primary to backup.
		fmt.Println("Promotion success")
		master.primary = master.backup
		master.backup = nil

		// Wait for backup to come online.

		// Create new channels for server.
		master.primaryIn = utils.InChanFromConn(master.primary, "primary")
		return true
	}
	fmt.Println("Server denied request")
	return false
}

// Client session: gets input from client and sends it to a channel to the master.
// Each session has its own socket connection.
func session(client net.Conn, mux chan<- string, id string) chan<- string {
	session := make(chan string)
	go func() {
		// Get IO from client user.
		clientIn := utils.InChanFromConn(client, "client")
		clientOut := utils.OutChanFromConn(client, "client")
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
				request = id + utils.DELIMITER + request
				fmt.Println("request:", request)
				mux <- request
			case reply, ok := <-session:
				if !ok {
					break loop
				}
				// No demarshaling required on the client side.
				clientOut <- reply
			}
		}
		mux <- id + utils.DELIMITER + utils.CLOSED
		fmt.Printf("Client at %v disconnected\n", client.LocalAddr())
	}()
	return session
}
