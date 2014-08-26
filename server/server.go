package server

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/eshyong/lettuce/db"
	"github.com/eshyong/lettuce/utils"
)

type Server struct {
	// Server can either have a backup or a primary, but not both.
	master net.Conn
	store  *db.Store
	// TODO: allow any arbitrary number of peers.
	peer net.Conn

	masterIn  <-chan string
	masterOut chan<- string

	peerIn  <-chan string
	peerOut chan<- string

	queue     []string
	isPrimary bool
}

func NewServer() *Server {
	return &Server{master: nil, store: db.NewStore(), peer: nil, isPrimary: false}
}

func (server *Server) ConnectToMaster() {
	/* master, err := readConfig()
	if err != nil {
		log.Fatal(err)
	} */
	// Connect to the master server.
	conn, err := net.DialTimeout("tcp", utils.LOCALHOST+utils.DELIMITER+utils.SERVER_PORT, utils.TIMEOUT)
	if err != nil {
		log.Fatal("Could not connect to master ", err)
	}
	in := utils.InChanFromConn(conn, "master")
	out := utils.OutChanFromConn(conn, "master")

	// TODO: check errors
	request, _ := <-in
	fmt.Println(request)
	err = server.handleMasterPing(out, request)
	if err != nil {
		log.Fatal(err)
	}

	server.master = conn
	server.masterIn = in
	server.masterOut = out

	if server.isPrimary {
		listener, err := net.Listen("tcp", utils.DELIMITER+utils.PEER_PORT)
		if err != nil {
			log.Fatal("Couldn't get a socket: ", err)
		}
		conn, err = listener.Accept()
		if err != nil {
			log.Fatal("Couldn't get a connection: ", err)
		}
	} else {
		conn = connectToPrimary(in)
	}
	server.peer = conn
	server.peerIn = utils.InChanFromConn(conn, "peer")
	server.peerOut = utils.OutChanFromConn(conn, "peer")
}

func connectToPrimary(in <-chan string) net.Conn {
	request, _ := <-in
	arr := strings.Split(request, utils.DELIMITER)
	if len(arr) < 2 {
		log.Fatal("Invalid message.")
	}
	header, body := arr[0], arr[1]

	if header != utils.SYN {
		log.Fatal("Unknown protocol.")
	}
	arr = strings.Split(body, utils.EQUALS)
	if len(arr) < 2 {
		log.Fatal("Invalid message: " + request)
	}
	name, address := arr[0], arr[1]
	if name != utils.PRIMARY {
		log.Fatal("Expected address of primary.")
	}
	conn, err := net.DialTimeout("tcp", address+utils.DELIMITER+utils.PEER_PORT, utils.TIMEOUT)
	if err != nil {
		log.Fatal("Couldn't connect to primary.")
	}
	return conn
}

func readConfig() (string, error) {
	file, err := os.Open("master.config")
	if err != nil {
		return "", err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// Lines are formatted as 'name address\n'.
		line := strings.Trim(scanner.Text(), " ")
		if line[0] == '#' {
			// Comments start with a '#'.
			continue
		}

		// Check the name and return address.
		entry := strings.Split(line, " ")
		name, address := entry[0], entry[1]
		if name == "master" {
			return address, nil
		}
	}
	return "", errors.New("couldn't find address in master.config")
}

func (server *Server) Serve() {
loop:
	for {
		// Receive a message from the master server.
		select {
		case message, ok := <-server.masterIn:
			if !ok {
				break loop
			}
			err := server.handleMasterRequests(server.masterOut, message)
			if err != nil {
				fmt.Println(err)
			}
		case message, ok := <-server.peerIn:
			if !ok {
				break
			}
			err := server.handlePeerMessage(server.peerOut, message)
			if err != nil {
				fmt.Println(err)
			}
		}
		if server.isPrimary && len(server.queue) > 0 {
			// Send diffs to the backup server.
			request := server.queue[0]
			server.peerOut <- utils.SYNDEL + utils.DIFF + utils.EQUALS + request
		}
	}
	fmt.Println("Shutting down...")
}

func (server *Server) handleMasterRequests(out chan<- string, message string) error {
	// Messages have the format 'HEADER:REQUEST'
	arr := strings.Split(message, utils.DELIMITER)
	if len(arr) < 2 {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Invalid request: " + message)
	}
	fmt.Println("master message:", message)

	// Handle a message from the server or a master request.
	header, request := arr[0], arr[1]
	if header == utils.SYN {
		// SYN message
		return server.handleMasterPing(out, message)
	} else if strings.Contains(header, utils.CLIENT) {
		// Client request
		if !server.isPrimary {
			// Refuse request as backup.
			out <- utils.ERRDEL + utils.NEG
			return errors.New("Not primary: " + request)
		}
		// Execute request and send reply to server.
		reply := header + utils.DELIMITER + server.store.Execute(request)
		out <- reply

		// Append to queue of requests to send to backup.
		server.queue = append(server.queue, request)
	} else {
		// Invalid request
		out <- utils.ERRDEL + utils.UNKNOWN
		return errors.New("Unrecognized request: " + request)
	}
	return nil
}

func (server *Server) handleMasterPing(out chan<- string, message string) error {
	arr := strings.Split(message, utils.DELIMITER)
	if len(arr) < 2 {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Invalid message: " + message)
	}
	header, request := arr[0], arr[1]
	if header != utils.SYN {
		return errors.New("Invalid message: " + message)
	}
	if request == utils.PROMOTE {
		if server.isPrimary {
			// This server is already a primary, so we reject the request.
			out <- utils.ACKDEL + utils.NEG
		} else {
			// Promote self to primary.
			server.isPrimary = true
			out <- utils.ACKDEL + utils.OK
		}
	} else if request == utils.STATUS {
		// Ping to check status?
		out <- utils.ACKDEL + utils.OK
	} else {
		// Some invalid message not covered by our protocol.
		out <- utils.ERRDEL + utils.UNKNOWN
		return errors.New("Unrecognized request: " + request)
	}
	return nil
}

func (server *Server) handlePeerMessage(out chan<- string, message string) error {
	fmt.Println("peer message:", message)
	var err error
	if server.isPrimary {
		err = server.handleBackupResponse(out, message)
	} else {
		err = server.handlePrimaryRequest(out, message)
	}
	return err
}

func (server *Server) handleBackupResponse(out chan<- string, message string) error {
	// We check if our last transaction went through; if there was a mistake we can retransmit it.
	fmt.Println("backup message:", message)
	arr := strings.Split(message, utils.DELIMITER)
	if len(arr) < 2 {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Invalid message: " + message)
	}
	header, body := arr[0], arr[1]
	if header == utils.ERR {
		return errors.New("Need to retransmit diff request: " + message)
	}
	if header != utils.ACK {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Unrecognized header: " + header)
	}
	if body != utils.OK {
		return errors.New("Request was rejected: " + body)
	}

	// Our transaction went through, pop from the front of the queue.
	server.queue = server.queue[1:]
	return nil
}

func (server *Server) handlePrimaryRequest(out chan<- string, message string) error {
	// Messages have the format 'HEADER:REQUEST'
	arr := strings.Split(message, utils.DELIMITER)
	if len(arr) < 2 {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Invalid message: " + message)
	}

	header, request := arr[0], arr[1]
	if header != utils.SYN {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Unrecognized header:" + header)
	}
	if !strings.Contains(request, utils.DIFF) {
		// Only handle DIFFs for now.
		out <- utils.ERRDEL + utils.UNKNOWN
		return errors.New("Unrecognized request:" + request)
	}
	arr = strings.Split(request, utils.EQUALS)
	if len(arr) < 2 {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Invalid message:" + request)
	}
	fmt.Println(server.store.Execute(arr[1]))
	out <- utils.ACKDEL + utils.OK
	return nil
}
