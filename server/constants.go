package server

import "time"

const (
	CLI_CLIENT_PORT = "8000"
	DEADLINE        = time.Second * 10
	TIMEOUT         = time.Second * 5
	SERVER_PORT     = "8080"
	// For testing
	LOCALHOST = "127.0.0.1"
)
