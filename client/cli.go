package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8000")
	if err != nil {
		log.Fatal(err)
	}
	conn.SetDeadline(time.Now().Add(10 * time.Hour))
	defer conn.Close()
	for {
		message := make([]byte, 1024)
		fmt.Fscan(os.Stdin, message)
		_, err := fmt.Fprint(conn, message)
		if err != nil {
			log.Println("server has disconnected")
			break
		}
	}
}
