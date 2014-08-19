package main

import (
	"fmt"

	"github.com/eshyong/lettuce/server"
)

func main() {
	s := server.NewServer()
	s.ConnectToMaster()
	fmt.Println("DB server running!")
	s.Serve()
}
