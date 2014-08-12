package main

import "github.com/eshyong/lettuce/server"

func main() {
	s := server.NewMaster()
	s.Serve()
}
