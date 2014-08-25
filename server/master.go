package server

import (
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
	primary  net.Conn
	backup   net.Conn
	sessions map[string]chan<- string

	primaryIn  <-chan string
	primaryOut chan<- string

	backupIn  <-chan string
	backupOut chan<- string

	counter uint64
}

func NewMaster() *Master {
	return &Master{primary: nil, backup: nil,
		sessions:  make(map[string]chan<- string),
		primaryIn: nil, primaryOut: nil,
		backupIn: nil, backupOut: nil,
		counter: 0}
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

		// Ping the server to check if it's ok.
		in := utils.InChanFromConn(conn, "primary")
		out := utils.OutChanFromConn(conn, "primary")
		err = pingServer(in, out, true)

		if err != nil {
			fmt.Println(err)
			continue
		}

		// Add backup variables to our master.
		master.primary = conn
		master.primaryIn = in
		master.primaryOut = out
		if err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("Primary is running!")
	}
	for master.backup == nil {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Unable to connect: ", err)
		}

		// Ping the server to check if it's ok.
		in := utils.InChanFromConn(conn, "backup")
		out := utils.OutChanFromConn(conn, "backup")
		err = pingServer(in, out, false)

		if err != nil {
			fmt.Println(err)
			continue
		}

		// Add backup variables to our master.
		master.backup = conn
		master.backupIn = in
		master.backupOut = out
		fmt.Println("Backup is running!")
	}
}

func (master *Master) checkServers() {
	fmt.Println("Checking server status...")
	err := pingServer(master.primaryIn, master.primaryOut, false)
	if err != nil {
		// TODO: promote backup
		log.Fatal("checkServers() failed: primary")
	}
	fmt.Println("Primary is fine! Pinging backup...")
	err = pingServer(master.backupIn, master.backupOut, false)
	if err != nil {
		// TODO: Wait for backup
		log.Fatal("checkServers() failed: backup")
	}
	fmt.Println("Backup is fine!")
}

// Check server's status by sending a short message.
func pingServer(in <-chan string, out chan<- string, primary bool) error {
	request := utils.STATUS
	if primary {
		request = utils.PROMOTE
	}

	// Send a STATUS message.
	out <- utils.SYNDEL + request
	message, ok := <-in
	if !ok {
		return errors.New("Connection error")
	}
	fmt.Println("message:", message)

	arr := strings.Split(message, utils.DELIMITER)
	header, body := arr[0], arr[1]
	if header != utils.ACK {
		return errors.New("Invalid protocol")
	}

	// Hopefully receive an "OK" in response.
	if body != utils.OK {
		return errors.New("Request rejected.")
	}
	return nil
}

// Serves any number of clients. TODO: load test.
func (master *Master) Serve() {
	// Create a listener for clients.
	listener, err := net.Listen("tcp", utils.DELIMITER+utils.CLI_CLIENT_PORT)
	if err != nil {
		log.Fatal("Couldn't get a socket: ", err)
	}
	defer listener.Close()

	// Create channels to listen on primary and backup.
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
					// Primary disconnected.
					master.promoteBackup()
					goto longsleep
				}
				fmt.Println("case reply, ok := <-master.primaryIn:", reply)
				master.handlePrimaryIn(reply)
			case reply, ok := <-master.backupIn:
				// Get a ping from backup.
				if !ok {
					master.backup = nil
					master.backupIn = nil
					master.backupOut = nil
					master.waitForBackup()
					goto longsleep
				}
				fmt.Println("case reply, ok := <-master.backupIn:", reply)
				master.handleBackupIn(reply)
			default:
				// Ping servers and make sure they're up.
				if elapsed > utils.WAIT_PERIOD {
					master.checkServers()
					begin = time.Now()
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	longsleep:
		time.Sleep(time.Second)
		goto loop
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
		if body != utils.OK {
			fmt.Println("Something wrong with the primary")
			// TODO: figure this out.
			return
		}
		// Everything is fine.
		fmt.Println("Primary is fine.")
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
}

func (master *Master) promoteBackup() {
	// Completely borked.
	if master.backupOut == nil || master.backupIn == nil {
		log.Fatal("No backup available, exiting...")
	}

	// Send a message and wait for a response.
	fmt.Println("Promoting backup...")
	err := pingServer(master.backupIn, master.backupOut, true)
	if err != nil {
		log.Fatal(err)
	}

	// Switch everything primary to backup.
	fmt.Println("Promotion success!")
	master.primary = master.backup
	master.primaryIn = master.backupIn
	master.primaryOut = master.backupOut

	// Remove backup variables.
	master.backup = nil
	master.backupIn = nil
	master.backupOut = nil

	// Wait for backup to come online.
	master.waitForBackup()
}

func (master *Master) waitForBackup() {
	fmt.Println("Waiting for backups...")
	listener, err := net.Listen("tcp", utils.DELIMITER+utils.SERVER_PORT)
	if err != nil {
		log.Fatal("Unable to get a socket: ", err)
	}
	defer listener.Close()

	// Wait for a backup server to connect.
	for master.backup == nil {
		fmt.Println("Accepting...")
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error connecting to backup:", err)
		}

		fmt.Println("Backup is up!")
		master.backup = conn
		master.backupIn = utils.InChanFromConn(master.backup, "backup")
		master.backupOut = utils.OutChanFromConn(master.backup, "backup")
	}
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
	}()
	return session
}
