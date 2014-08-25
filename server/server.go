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
	peer      net.Conn
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
	server.master = conn
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
	masterIn := utils.InChanFromConn(server.master, "master")
	masterOut := utils.OutChanFromConn(server.master, "master")

	/* var name string
	if server.isPrimary {
		name = "backup"
	} else {
		name = "primary"
	}
	peerIn := utils.InChanFromConn(server.peer, name)
	peerOut := utils.OutChanFromConn(server.peer, name) */

loop:
	for {
		// Receive a message from the master server.
		select {
		case message, ok := <-masterIn:
			if !ok {
				break loop
			}
			err := server.handleMasterRequests(masterOut, message)
			if err != nil {
				fmt.Println(err)
			}
			/*		case message, ok := <-peerIn:
					if !ok {
						break loop
					}
					err := server.handlePeerMessage(peerOut, message)
					if err != nil {
						fmt.Println(err)
					} */
		}
	}
}

func (server *Server) handleMasterRequests(masterOut chan<- string, message string) error {
	// Messages have the format 'HEADER:REQUEST'
	arr := strings.Split(message, utils.DELIMITER)
	if len(arr) < 2 {
		masterOut <- utils.ERRDEL + utils.INVALID
		return errors.New("Invalid request: " + message)
	}
	fmt.Println("master message:", message)

	// Handle a message from the server or a master request.
	header, request := arr[0], arr[1]
	if header == utils.SYN {
		// SYN message
		return server.handleMasterPing(masterOut, request)
	} else if strings.Contains(header, utils.CLIENT) {
		// Client request
		if !server.isPrimary {
			// Refuse request as backup.
			masterOut <- utils.ERRDEL + utils.NEG
			return errors.New("Not primary: " + request)
		}
		reply := header + utils.DELIMITER + server.store.Execute(request)
		//		peerOut <- utils.SYNDEL + utils.DIFF + "=" + request
		masterOut <- reply
	} else {
		// Invalid request
		masterOut <- utils.ERRDEL + utils.UNKNOWN
		return errors.New("Unrecognized request: " + request)
	}
	return nil
}

func (server *Server) handleMasterPing(out chan<- string, request string) error {
	if request == utils.PROMOTE {
		if server.isPrimary {
			// This server is already a primary, so we reject the request.
			out <- utils.ACKDEL + utils.NEG
		} else {
			server.isPrimary = true
			out <- utils.ACKDEL + utils.OK
		}
	} else if request == utils.STATUS {
		// Ping to check status?
		fmt.Println("Request for status.")
		out <- utils.ACKDEL + utils.OK
	} else {
		// Some invalid message not covered by our protocol.
		out <- utils.ERRDEL + utils.UNKNOWN
		return errors.New("Unrecognized request: " + request)
	}
	return nil
}

func (server *Server) handlePeerMessage(out chan<- string, message string) error {
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
	return nil
}

func (server *Server) handlePrimaryRequest(out chan<- string, message string) error {
	// Messages have the format 'HEADER:REQUEST'
	arr := strings.Split(message, utils.DELIMITER)
	if len(arr) < 2 {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Invalid message: " + message)
	}
	fmt.Println("peer message:", message)

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
	arr = strings.Split(request, "=")
	if len(arr) < 2 {
		out <- utils.ERRDEL + utils.INVALID
		return errors.New("Invalid message:" + request)
	}
	fmt.Println(server.store.Execute(arr[1]))
	return nil
}
