package main

import (
	"fmt"

	"github.com/eshyong/lettuce/server"
)

func main() {
	s := server.NewMaster()
	s.WaitForConnections()
	fmt.Println("Welcome to lettuce! You can connect to this database by " +
		"running `lettuce-cli` in another window.")
	s.Serve()
}
