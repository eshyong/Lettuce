package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

const (
	CLI_CLIENT_PORT = "8000"
	SERVER_PORT     = "8001"
)

type Master struct {
	listener net.Listener
	primary  net.Conn
	backup   net.Conn
}

type Session struct {
	conn net.Conn
}

func newSession(c net.Conn) *Session {
	return &Session{conn: c}
}

func NewMaster() *Master {
	l, err := net.Listen("tcp", ":"+CLI_CLIENT_PORT)
	if err != nil {
		log.Fatal("Listen: unable to get a port", err)
	}
	master := &Master{listener: l}
	master.connectToServers()
	return master
}

func (master *Master) Serve() {
	defer master.listener.Close()
	fmt.Println("Welcome to lettuce! You can connect to this database by " +
		"running `lettuce-cli` in another window.")
	for {
		// Grab a connection.
		conn, err := master.listener.Accept()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("client connected on address", conn.LocalAddr())

		// Handle client session.
		session := newSession(conn)
		sesh := session.run()
		go master.handleRequests(sesh)
	}
}

func (master *Master) connectToServers() {
	file, err := os.Open("config/servers")
	if err != nil {
		log.Fatal("Unable to open configuration file, aborting...", err)
	}
	scanner := bufio.NewScanner(file)
	wait := make(chan bool)
	n := 0
	for scanner.Scan() {
		n++
		go master.parseLineAndConnect(scanner.Text(), wait)
	}
	for i := 0; i < n; i++ {
		<-wait
	}
}

func (master *Master) parseLineAndConnect(line string, w chan bool) {
	arr := strings.Split(line, " ")
	server := arr[0]
	address := arr[1]

	if server == "primary" {
		if master.primary != nil {
			fmt.Println("config/servers: two primaries specified, default to last connection")
			// TODO: add to backups
		}
		primary, err := net.DialTimeout("tcp", address+":"+SERVER_PORT, time.Second*10)
		if err != nil {
			fmt.Println("Couldn't connect to primary:", err)
			return
		}
		fmt.Println("Connected to primary at address", primary.RemoteAddr())
		master.primary = primary
		fmt.Fprintln(master.primary, "true")
	} else {
		backup, err := net.DialTimeout("tcp", address+":"+SERVER_PORT, time.Second*10)
		if err != nil {
			fmt.Println("Couldn't connect to backup:", err)
			return
		}
		fmt.Println("Connected to backup at address", backup.RemoteAddr())
		master.backup = backup
		fmt.Fprintln(master.backup, "false")
	}
	w <- true
}

func (master *Master) handleRequests(sesh chan string) {
	out := master.sendRequests()
	in := master.getReplies()
	for {
		// Send user requests to a primary to execute.
		request, ok := <-sesh
		if !ok {
			return
		}

		out <- request
		response := <-in
		sesh <- response
	}
}

func (master *Master) sendRequests() chan string {
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

func (master *Master) getReplies() chan string {
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

func (session *Session) run() chan string {
	c := make(chan string)
	go func() {
		// Get input from client user.
		input := session.getInput()
		defer session.conn.Close()
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
				go session.sendReply(reply)
			}
		}
	}()
	return c
}

func (session *Session) getInput() chan string {
	c := make(chan string)
	go func() {
		scanner := bufio.NewScanner(session.conn)
		for scanner.Scan() {
			// Read from connected client.
			c <- scanner.Text()
		}
		fmt.Printf("client at %v disconnected\n", session.conn.RemoteAddr())
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
		}
	}()
	return c
}

func (session *Session) sendReply(reply string) {
	n, err := fmt.Fprintln(session.conn, reply)
	if n == 0 {
		fmt.Println("client disconnected")
	}
	if err != nil {
		fmt.Println(err)
	}
}
