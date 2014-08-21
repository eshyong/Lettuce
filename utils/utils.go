package utils

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

const (
	// TCP constants.
	CLI_CLIENT_PORT = "8000"
	DEADLINE        = time.Second * 10
	TIMEOUT         = time.Second * 5
	SERVER_PORT     = "8080"

	// Protocol headers.
	ACK    = "ACK"
	SYN    = "SYN"
	ERR    = "ERR"
	CLIENT = "CLIENT"

	// Message delimiter.
	DELIMITER = ":"

	// Requests.
	PROMOTE = "PROMOTE"
	PRIMARY = "PRIMARY"
	BACKUP  = "BACKUP"
	STATUS  = "STATUS"

	// Status codes.
	OK      = "OK"
	NEG     = "NEG"
	INVALID = "INVALID"
	CLOSED  = "CLOSED"

	// For testing.
	LOCALHOST = "127.0.0.1"
)

func InChanFromConn(conn net.Conn, name string) <-chan string {
	in := make(chan string)
	go func() {
		defer close(in)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			in <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Println(name, err)
		}
		fmt.Println(name, " disconnected at ", conn.RemoteAddr())
	}()
	return in
}

func OutChanFromConn(conn net.Conn, name string) chan<- string {
	out := make(chan string)
	go func() {
		for {
			reply, ok := <-out
			if !ok {
				break
			}

			// Use Fprintln because bufio.WriteString buffers.
			n, err := fmt.Fprintln(conn, reply)
			if n == 0 {
				break
			}
			if err != nil {
				fmt.Println(err)
			}
		}
		fmt.Println(name, "disconnected at", conn.RemoteAddr())
	}()
	return out
}
