package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

const (
	CLI_CLIENT_PORT = "8000"
	SERVER_PORT     = "8001"
)

type Master struct {
	clientListener net.Listener
	primary        net.Conn
	backup         net.Conn
	addresses      map[string]string
}

func NewMaster() *Master {
	l, err := net.Listen("tcp", ":"+CLI_CLIENT_PORT)
	if err != nil {
		log.Fatal("Listen: unable to get a port", err)
	}
	master := &Master{clientListener: l, addresses: make(map[string]string)}
	master.connectToServers()
	return master
}

// Connects to the primary and backup servers. If none are found, will wait
// for servers to come online.
func (master *Master) connectToServers() {
	file, err := os.Open("config/servers")
	if err != nil {
		log.Fatal("No server configuration file found, quitting", err)
	}

	// Read config file and connect to each address.
	master.parseAddresses(file)
	for server, _ := range master.addresses {
		if server == "primary" {
			go master.connectToPrimary()
		} else if server == "backup" {
			go master.connectToBackup()
		} else {
			fmt.Println("Invalid entry in config file, continuing...")
		}
	}
	time.Sleep(time.Second * 5)
	fmt.Println("timed out")

	// We need at least two servers to operate.
	if master.primary == nil || master.backup == nil {
		fmt.Println("No servers up, waiting for connections...")
		time.Sleep(time.Second * 2)
		master.waitForConnections()
	}
}

// Parses a config file, and attempts to connect to
func (master *Master) parseAddresses(file *os.File) {
	scanner := bufio.NewScanner(file)

	// Connect to addresses listed in config file.
	for scanner.Scan() {
		entry := scanner.Text()
		// Ignore comments.
		if entry[0] != '#' {
			// Split each entry by server name and address.
			arr := strings.Split(entry, " ")
			server := arr[0]
			address := arr[1]
			master.addresses[server] = address
		}
	}
}

// This is run if no servers are discovered on startup. Alternates between polling and sleeping.
func (master *Master) waitForConnections() {
	for master.primary == nil || master.backup == nil {
		go master.connectToPrimary()
		go master.connectToBackup()
		time.Sleep(time.Second * 5)
	}
}

// Connects to a primary server, timing out after 10 seconds.
func (master *Master) connectToPrimary() {
	primary, err := net.Dial("tcp", master.addresses["primary"]+":"+SERVER_PORT)
	if err == nil {
		fmt.Println("Primary is up at address", primary.RemoteAddr())
		fmt.Fprintln(primary, "true")
		master.primary = primary
	}
}

// Connects to a backup server, timing out after 10 seconds.
func (master *Master) connectToBackup() {
	backup, err := net.Dial("tcp", master.addresses["backup"]+":"+SERVER_PORT)
	if err == nil {
		fmt.Println("Backup is up at address", backup.RemoteAddr())
		fmt.Fprintln(backup, "false")
		master.backup = backup
	}
}

// Serves any number of clients. TODO: load test.
func (master *Master) Serve() {
	defer master.clientListener.Close()
	fmt.Println("Welcome to lettuce! You can connect to this database by " +
		"running `lettuce-cli` in another window.")
	for {
		// Grab a connection.
		conn, err := master.clientListener.Accept()
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("client connected on address", conn.LocalAddr())

		// Handle client session.
		sesh := session(conn)
		go master.handleRequests(sesh)
	}
}

// Middleman function that shuttles data from client to the server.
func (master *Master) handleRequests(sesh chan string) {
	out := master.sendRequestsToServer()
	in := master.getRepliesFromServer()
	for {
		// Send user requests to a primary to execute.
		select {
		case request, ok := <-sesh:
			if !ok {
				return
			}
			out <- request
		case response, ok := <-in:
			if !ok {
				return
			}
			sesh <- response
		}
	}
}

// Asks the server to execute a user command.
func (master *Master) sendRequestsToServer() chan string {
	c := make(chan string)
	go func() {
		for {
			request := <-c
			n, err := fmt.Fprintln(master.primary, request)
			if n == 0 {
				fmt.Println("Server down")
			}
			if err != nil {
				fmt.Println(err)
			}
		}
	}()
	return c
}

// Gets a response from the server.
func (master *Master) getRepliesFromServer() chan string {
	c := make(chan string)
	go func() {
		scanner := bufio.NewScanner(master.primary)
		for scanner.Scan() {
			c <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
		}
	}()
	return c
}

// Client session: gets input from client and sends it to a channel to the master.
// Each session has its own socket connection.
func session(client net.Conn) chan string {
	c := make(chan string)
	go func() {
		// Get input from client user.
		input := getInputFromClient(client)
		defer client.Close()
		defer close(c)
		for {
			select {
			case command, ok := <-input:
				// Send to db server.
				if !ok {
					return
				}
				c <- command
			case reply := <-c:
				// Send db server's reply to the user.
				go sendReplyToClient(client, reply)
			}
		}
	}()
	return c
}

// Gets a database request from the client.
func getInputFromClient(client net.Conn) chan string {
	c := make(chan string)
	go func() {
		scanner := bufio.NewScanner(client)
		for scanner.Scan() {
			// Read from connected client.
			c <- scanner.Text()
		}
		fmt.Printf("client at %v disconnected\n", client.LocalAddr())
		if err := scanner.Err(); err != nil {
			fmt.Println(err)
		}
	}()
	return c
}

// Sends a database response to the client.
func sendReplyToClient(client net.Conn, reply string) {
	n, err := fmt.Fprintln(client, reply)
	if n == 0 {
		fmt.Println("client disconnected")
	}
	if err != nil {
		fmt.Println(err)
	}
}
