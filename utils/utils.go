package utils

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

var NO_DEADLINE = time.Time{}

const (
	// TCP constants.
	CLI_CLIENT_PORT = "8000"
	DEADLINE        = time.Second * 5
	TIMEOUT         = time.Second * 5
	WAIT_PERIOD     = time.Second * 15
	SERVER_PORT     = "8080"
	PEER_PORT       = "9000"

	// Protocol headers.
	ACK    = "ACK"
	SYN    = "SYN"
	ERR    = "ERR"
	CLIENT = "CLI"

	// For convenience.
	ACKDEL = ACK + DELIMITER
	SYNDEL = SYN + DELIMITER
	ERRDEL = ERR + DELIMITER
	CLIDEL = CLIENT + DELIMITER

	// Message delimiters.
	DELIMITER = ":"
	EQUALS    = "="

	// Requests.
	PROMOTE = "PROM"
	PRIMARY = "PRIM"
	BACKUP  = "BACK"
	STATUS  = "STAT"
	DIFF    = "DIFF"

	// Status codes.
	OK      = "OK"
	NEG     = "NEG"
	INVALID = "INVLD"
	UNKNOWN = "UNKN"
	CLOSED  = "CLOS"

	// For testing.
	LOCALHOST = "127.0.0.1"
)

func InChanFromConn(conn net.Conn, name string) <-chan string {
	in := make(chan string)
	go func() {
		defer close(in)
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			//			fmt.Println("Reading!", scanner.Text())
			in <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Println(name, err)
		}
		fmt.Println(name + " disconnected at " + conn.RemoteAddr().String())
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
			//			fmt.Println("Writing!", reply)
			n, err := fmt.Fprintln(conn, reply)
			if n == 0 {
				break
			}
			if err != nil {
				fmt.Println(err)
			}
		}
	}()
	return out
}
