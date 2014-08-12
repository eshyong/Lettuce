package server

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"github.com/eshyong/lettuce/db"
)

type Server struct {
	master  net.Conn
	store   *db.Store
	primary bool
}

func NewServer() *Server {
	s := db.NewStore()
	listener, err := net.Listen("tcp", ":"+SERVER_PORT)
	if err != nil {
		log.Fatal("Unable to start server.", err)
	}
	defer listener.Close()

	fmt.Println("Waiting for master to connect...")
	conn, err := listener.Accept()
	if err != nil {
		log.Fatal("Unable to connect to master.", err)
	}

	dbServer := &Server{master: conn, store: s, primary: false}
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		fmt.Println("Could not receive message from master")
	} else {
		if scanner.Text() == "true" {
			dbServer.primary = true
		}
	}
	return dbServer
}

func (server *Server) getInput() chan string {
	c := make(chan string)
	go func() {
		defer close(c)
		scanner := bufio.NewScanner(server.master)
		for scanner.Scan() {
			c <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Println("Error", err)
		}
	}()
	return c
}

func (server *Server) sendReply() chan string {
	c := make(chan string)
	go func() {
		for {
			defer close(c)
			reply := <-c
			n, err := fmt.Fprintln(server.master, reply)
			if n == 0 {
				fmt.Println("Master disconnected")
				break
			}
			if err != nil {
				fmt.Println(err)
				break
			}
		}
	}()
	return c
}

func (server *Server) Serve() {
	fmt.Println("DB server running!")
	in := server.getInput()
	out := server.sendReply()
	for {
		request, ok := <-in
		if !ok {
			break
		}
		reply := server.store.Execute(request)
		out <- reply
	}
}
