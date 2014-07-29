package main

import (
	"fmt"
	"io"
	"log"
	"net"
)

func main() {
	listen, err := net.Listen("tcp", ":2000")
	if err != nil {
		log.Fatal(err)
	}
	defer listen.Close()
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go func(c net.Conn) {
			io.Copy(c, c)
			c.Close()
			fmt.Printf("%v has disconnected.\n", c.RemoteAddr().String())
		}(conn)
		// time.Sleep(1000)
	}
}
