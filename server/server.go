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
)

type Server struct {
	// Server can either have a backup or a primary, but not both.
	master  net.Conn
	store   *db.Store
	primary net.Conn
	// TODO: allow any arbitrary number of backups.
	backup    net.Conn
	isPrimary bool
}

func NewServer() *Server {
	return &Server{master: nil, store: db.NewStore(), primary: nil, backup: nil, isPrimary: false}
}

// Returns true if this computer is designated as a primary.
func (server *Server) ConnectToMaster() {
	/* master, err := readConfig()
	if err != nil {
		log.Fatal(err)
	} */
	// Connect to the master server.
	conn, err := net.DialTimeout("tcp", LOCALHOST+":"+SERVER_PORT, TIMEOUT)
	if err != nil {
		log.Fatal("Could not connect to master ", err)
	}
	server.master = conn
}

func (server *Server) Setup() {
	server.ConnectToMaster()

	// Check if we are primary or backup.
	message, err := server.getPing()
	if err != nil {
		log.Fatal(err)
	}
	if message != "primary" && message != "backup" {
		// Some invalid protocol message.
		log.Fatal("Server replied with invalid message " + message + ", aborting")
	}
	// Acknowledge the request.
	fmt.Fprintln(server.master, ACK+DELIMITER+OK)

	// Return true for primary and false for backup.
	fmt.Println("Confirmed as " + message + ".")
	if message == "primary" {
		server.isPrimary = true
	} else {
		// Wait for to send us the address of primary.
		message, err := server.getPing()
		if err != nil {
			log.Fatal(err)
		}

		// Establish a connection with primary.
		conn, err := net.Dial("tcp", message+":"+SERVER_PORT)
		if err != nil {
			log.Fatal("Unable to connect to primary: ", err)
		}

		server.primary = conn
	}
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
	masterIn := server.getRequestsFromServer(server.master)
	masterOut := server.sendRepliesToServer(server.master)

	var otherIn, otherOut chan string
	if server.primary != nil {
		otherIn = server.getRequestsFromServer(server.primary)
		otherOut = server.sendRepliesToServer(server.primary)
	} else if server.backup != nil {
		otherIn = server.getRequestsFromServer(server.backup)
		otherOut = server.sendRepliesToServer(server.backup)
	}

loop:
	for {
		// Receive a message from the master server.
		select {
		case message, ok := <-masterIn:
			if !ok {
				break loop
			}
			server.handleMasterRequests(masterOut, message)
		case ping, ok := <-otherIn:
			if !ok {
				break loop
			}
			server.handleOtherPing(otherOut, ping)
		}

	}
}

func (server *Server) handleMasterRequests(out chan string, message string) {
	// Messages have the format 'HEADER:REQUEST'
	arr := strings.Split(message, ":")
	if len(arr) < 2 {
		fmt.Println("Invalid request:", arr)
		out <- ERR + DELIMITER + INVALID
		continue
	}

	// Handle a message from the server or a master request.
	header, request := arr[0], arr[1]
	if header == SYN {
		handleMasterPing(out, request)
	} else if header.contains(SESSION) {
		reply := header + DELIMITER + server.store.Execute(request)
		out <- reply
	} else {
		// Invalid request.
		out <- ERR + DELIMITER + INVALID
	}
}

func (server *Server) handleMasterPing(out chan string, request string) {
	if request == PROMOTE {
		// TODO: Check if you are the primary. If so, then we reject this request.
		// Otherwise, we reply with an ACK.
		if server.isPrimary {
			out <- ACK + DELIMITER + NEG
		} else {
			out <- ACK + DELIMITER + OK
		}
	} else if request == STATUS {
		// Ping to check status?
		out <- ACK + DELIMITER + OK
	} else {
		// Some invalid message not covered by our protocol.
		fmt.Println("Invalid request " + request)
		out <- ERR + DELIMITER + INVALID
	}
}

func (server *Server) handleOtherPing(out chan string, request string) {

}

func (server *Server) getRequestsFromServer(conn net.Conn) chan string {
	serverIn := make(chan string)
	go func() {
		defer close(serverIn)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			serverIn <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("*Server.getRequestsFromServer() ", err)
		}
	}()
	return serverIn
}

func (server *Server) sendRepliesToServer(conn net.Conn) chan string {
	serverOut := make(chan string)
	go func() {
		defer close(serverOut)
		for {
			// Get a reply from the main loop.
			reply, ok := <-serverOut
			if !ok {
				break
			}

			// Send replies to the master server.
			n, err := fmt.Fprintln(conn, reply)
			if n == 0 || err != nil {
				fmt.Println("*Server.sendRepliesToServer() ", err)
				break
			}
		}
		fmt.Println("Master disconnected, shutting down...")
	}()
	return serverOut
}
