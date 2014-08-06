package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"

	"github.com/eshyong/lettuce/db"
)

type Server struct {
	listener net.Listener
	store    *db.Store
}

type Session struct {
	conn net.Conn
}

func newServer() *Server {
	l, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal(err)
	}
	return &Server{listener: l, store: db.NewStore()}
}

func newSession(c net.Conn) *Session {
	return &Session{conn: c}
}

func (server *Server) serve() {
	defer server.listener.Close()
	defer server.store.Flush()
	go server.handleSignals()
	for {
		// Grab a connection.
		conn, err := server.listener.Accept()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("client %v connected\n", conn.RemoteAddr())

		// Handle client session.
		session := newSession(conn)
		c := session.run()
		go server.handleRequests(c)
	}
}

func (server *Server) handleRequests(c chan string) {
	for {
		// Send user requests for the db to execute.
		request, ok := <-c
		if !ok {
			return
		}
		// Shave off a newline.
		request = request[:len(request)-1]
		val := server.store.Execute(request)
		c <- val
	}
}

func (server *Server) handleSignals() {
	// Create a channel to catch os signals.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)

	// Block on this channel until signal is received, then flush db.
	s := <-c
	fmt.Println(s, "signal received, flushing buffers to disk...")
	server.store.Flush()
	fmt.Println("Goodbye!")
	os.Exit(0)
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
				reply += "\n"
				go session.sendReply(reply)
			}
		}
	}()
	return c
}

func (session *Session) getInput() chan string {
	c := make(chan string)
	go func() {
		reader := bufio.NewReader(session.conn)
		for {
			// Read from connected client.
			command, err := reader.ReadString('\n')
			if err != nil {
				if err == io.EOF {
					fmt.Printf("client at %v disconnected\n", session.conn.RemoteAddr())
				} else {
					fmt.Println(err)
				}
				return
			}
			// Send over the channel
			c <- command
		}
	}()
	return c
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
