package main

import "github.com/eshyong/lettuce/server"

func main() {
	s := server.NewServer()
	s.Serve()
}
