package main

import (
	"fmt"

	"github.com/eshyong/lettuce/server"
)

func main() {
	m := server.NewMaster()
	m.WaitForConnections()
	fmt.Println("Welcome to lettuce! You can connect to this database by " +
		"running `lettuce-cli` in another window.")
	m.Serve()
}
