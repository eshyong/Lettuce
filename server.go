package server

import (
	"fmt"
	"log"
	"net"
)

func connect(address string, port string) {
	conn, err := net.Dial("tcp", address+port)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf(conn, "What's good\n")
}
