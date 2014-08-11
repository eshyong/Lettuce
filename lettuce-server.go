package main

import (
	"fmt"

	"github.com/eshyong/lettuce/server"
)

func main() {
	s := server.NewServer()
	fmt.Println("Welcome to Lettuce! You can connect through another window by running lettuce-cli.")
	s.Serve()
}
