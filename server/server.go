package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

func DoStuff(conn net.Conn) {
	message := make([]byte, 1024)
	reply := []byte("yo")
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Hour))
	for {
		// Read endlessly from client and echo on screen
		_, err := fmt.Fscan(conn, &message)
		if err != nil {
			log.Println("Fscan: " + err.Error())
			return
		}
		if message[0] != 0 {
			fmt.Println(os.Stdout, string(message))
			_, err = fmt.Fprint(conn, reply)
			if err != nil {
				log.Println("Fprint: " + err.Error())
				return
			}
		}
		message[0] = 0
	}
}

func Serve() {
	listener, err := net.Listen("tcp", ":8000")
	if err != nil {
		log.Fatal(err)
	}
	for {
		// Get a connection
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal(err)
		}

		// Set read deadline
		go DoStuff(conn)
	}
}

func main() {
	Serve()
}
