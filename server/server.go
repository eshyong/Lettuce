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

	//	otherIn, otherOut := server.connectToOther()

loop:
	for {
		// Receive a message from the master server.
		select {
		case message, ok := <-masterIn:
			if !ok {
				break loop
			}
			server.handleMasterRequests(masterOut, message)
			/*case ping, ok := <-otherIn:
			if !ok {
				break loop
			}
			server.handleOtherPing(otherOut, ping) */
		}

	}
}

func (server *Server) connectToOther() (<-chan string, chan<- string) {
	var in <-chan string
	var out chan<- string
	if server.isPrimary {
		in = utils.InChanFromConn(server.backup, "backup")
		out = utils.OutChanFromConn(server.backup, "backup")
	} else {
		in = utils.InChanFromConn(server.primary, "primary")
		out = utils.OutChanFromConn(server.primary, "primary")
	}
	return in, out
}

func (server *Server) handleMasterRequests(out chan<- string, message string) {
	// Messages have the format 'HEADER:REQUEST'
	arr := strings.Split(message, utils.DELIMITER)
	if len(arr) < 2 {
		fmt.Println("Invalid request:", arr)
		out <- utils.ERR + utils.DELIMITER + utils.INVALID
		return
	}
	fmt.Println(message)

	// Handle a message from the server or a master request.
	header, request := arr[0], arr[1]
	if header == utils.SYN {
		// SYN message
		server.handleMasterPing(out, request)
	} else if strings.Contains(header, utils.CLIENT) {
		// Client request
		reply := header + utils.DELIMITER + server.store.Execute(request)
		out <- reply
	} else {
		// Invalid request
		out <- utils.ERR + utils.DELIMITER + utils.INVALID
	}
}

func (server *Server) handleMasterPing(out chan<- string, request string) {
	if request == utils.PROMOTE {
		// TODO: Check if server is already the primary. If so, then we reject this request.
		// Otherwise, we reply with status code OK.
		if server.isPrimary {
			out <- utils.ACK + utils.DELIMITER + utils.NEG
		} else {
			out <- utils.ACK + utils.DELIMITER + utils.OK
		}
	} else if request == utils.STATUS {
		// Ping to check status?
		out <- utils.ACK + utils.DELIMITER + utils.OK
	} else {
		// Some invalid message not covered by our protocol.
		fmt.Println("Invalid request " + request)
		out <- utils.ERR + utils.DELIMITER + utils.INVALID
	}
}

func (server *Server) handleOtherPing(out chan<- string, request string) {

}
