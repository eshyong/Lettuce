package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/eshyong/lettuce/db"
)

type Server struct {
	listener net.Listener
	store    *db.Store
}

type Session struct {
	input  chan string
	output chan string
	conn   net.Conn
}

func newServer() *Server {
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal(err)
	}
	return &Server{listener: l,
		store: db.NewStore()}
}

func newSession(c net.Conn) *Session {
	return &Session{input: make(chan string, 1024),
		output: make(chan string, 1024),
		conn:   c}
}

func (server *Server) serve() {
	// Make sure listener gets cleaned up
	defer server.listener.Close()
	for {
		// Get a connection
		conn, err := server.listener.Accept()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("client %v connected\n", conn.RemoteAddr())

		// Handle client session
		session := newSession(conn)
		c := session.run()
		go server.handleRequests(c)
	}
}

func (server *Server) handleRequests(c chan string) {
	for {
		request, ok := <-c
		if !ok {
			return
		}
		request = request[:len(request)-1]
		val := server.store.Execute(request)
		c <- val
	}
}

func (session *Session) run() chan string {
	c := make(chan string)
	go func() {
		go session.getInput()
		defer session.conn.Close()
		defer close(c)
		for {
			select {
			case command, ok := <-session.input:
				if !ok {
					return
				}
				c <- command
			case reply := <-c:
				reply += "\n"
				go session.sendReply(reply)
			}
		}
	}()
	return c
}

func (session *Session) getInput() {
	reader := bufio.NewReader(session.conn)
	for {
		command, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Printf("client at %v disconnected: ", session.conn.RemoteAddr())
			} else {
				fmt.Println(err)
			}
			return
		}
		session.input <- command
	}
}

func (session *Session) sendReply(reply string) {
	n, err := session.conn.Write([]byte(reply))
	if n == 0 {
		fmt.Println("client disconnected")
	}
	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	server := newServer()
	fmt.Println("Welcome to Lettuce! You can connect through another window by running lettuce-cli.")
	server.serve()
}
