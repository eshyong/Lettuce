package main

import (
	"fmt"
	"log"
	"net"
)

type Server struct {
	input    chan string
	listener net.Listener
	// commands map[string]func(something)
}

func newServer() *Server {
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal(err)
	}
	server := &Server{input: make(chan string, 1024), listener: l}
	return server
}

func (server *Server) serve() {
	// Make sure listener gets cleaned up
	defer server.listener.Close()

	for {
		// Get a connection
		conn, err := server.listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		// Handle client session
		go server.session(conn)
	}
}

func (server *Server) session(conn net.Conn) {
	defer conn.Close()
	go server.getInput(conn)
	for {
		select {
		case command, ok := <-server.input:
			if !ok {
				return
			}
			server.runCommand(command)
		}
	}
}

func (server *Server) getInput(conn net.Conn) {
	// Make sure connection socket gets cleaned up.
	defer conn.Close()
	command := make([]byte, 1024)
	for {
		n, err := conn.Read(command)
		if n == 0 {
			fmt.Printf("client %v has disconnnected.\n", conn.LocalAddr())
			return
		}
		if err != nil {
			fmt.Println(err)
		}
		server.input <- string(command)
		for i := 0; command[i] != 0; i++ {
			command[i] = 0
		}
	}
}

func (server *Server) runCommand(command string) {
	// Do something here
	fmt.Print("user cmd: " + command)
}

func main() {
	server := newServer()
	server.serve()
}
