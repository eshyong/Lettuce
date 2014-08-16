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
	master  net.Conn
	store   *db.Store
	primary bool
}

func NewServer() *Server {
	return &Server{master: nil, store: db.NewStore(), primary: false}
}

func (server *Server) connectToMaster() {
	master, err := readConfig()
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", master+":"+SERVER_PORT, TIMEOUT)
	if err != nil {
		log.Fatal("Could not connect to master ", err)
	}
	server.master = conn
	message, err := server.getPing()
	if err != nil {
		log.Fatal(err)
	}
	if message == "primary" || message == "backup" {
		fmt.Println("Confirmed as " + message + ".")
		if message == "primary" {
			server.primary = true
		}
	} else {
		// This should never happen unless someone tries to hijack the server.
		log.Fatal("Server replied with invalid message " + message + ", aborting")
	}
}

func readConfig() (string, error) {
	file, err := os.Open("master.config")
	if err != nil {
		return "", err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.Trim(scanner.Text(), " ")
		if line[0] != '#' {
			arr := strings.Split(line, " ")
			name := arr[0]
			address := arr[1]
			if name == "master" {
				return address, nil
			}
		}
	}
	return "", errors.New("couldn't find address in master.config")
}

func (server *Server) getPing() (string, error) {
	scanner := bufio.NewScanner(server.master)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return "", errors.New("Could not receive SYN from server")
	}
	message := strings.Split(scanner.Text(), ":")
	header := message[0]
	body := message[1]
	if header != "SYN" {
		fmt.Println("Unrecognized reply from server")
	}
	return body, nil
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
	server.connectToMaster()
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
