package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

type Server struct {
	listener net.Listener
}

type Session struct {
	conn net.Conn
}

func NewServer() *Server {
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal(err)
	}
	return &Server{listener: l}
}

func newSession(c net.Conn) *Session {
	return &Session{conn: c}
}

func (server *Server) Serve() chan string {
	node := make(chan string)
	go func() {
		defer server.listener.Close()

		for {
			// Grab a connection.
			conn, err := server.listener.Accept()
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println("client connected on address", conn.LocalAddr())

			// Handle client session.
			session := newSession(conn)
			c := session.run()
			server.handleRequests(c, node)
		}
	}()
	return node
}

func (server *Server) handleRequests(sesh chan string, node chan string) {
	for {
		// Send user requests for the db to execute.
		request, ok := <-sesh
		if !ok {
			return
		}
		// Send request to the node, which will execute the command.
		sesh <- request

		// Get a response back from the node, which we send back to the session.
		response := <-sesh
		node <- response
	}
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
