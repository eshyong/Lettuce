package server

import "time"

const (
	// TCP constants.
	CLI_CLIENT_PORT = "8000"
	DEADLINE        = time.Second * 10
	TIMEOUT         = time.Second * 5
	SERVER_PORT     = "8080"

	// Protocol headers.
	ACK    = "ACK"
	SYN    = "SYN"
	ERR    = "ERR"
	CLIENT = "CLIENT"

	// Message delimiter.
	DELIMITER = ":"

	// Requests.
	PROMOTE = "PROMOTE"
	PRIMARY = "PRIMARY"
	BACKUP  = "BACKUP"
	STATUS  = "STATUS"

	// Status codes.
	OK      = "OK"
	NEG     = "NEG"
	INVALID = "INVALID"

	// For testing.
	LOCALHOST = "127.0.0.1"
)
