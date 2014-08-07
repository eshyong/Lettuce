package db

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Integer constants.
const (
	MAXINT               = 9223372036854775807
	MININT               = -9223372036854775808
	INITIAL_LOG_CAPACITY = 4096
)

// Lookup table for function requests.
var funcmap = map[string]func(args []string, store *Store) string{
	"get":  getValue,
	"set":  setValue,
	"incr": incr,
	"decr": decr,
	"del":  del,
}

type Store struct {
	stringStore map[string]string
	logs        []Record
	lock        sync.Mutex
}

type Record struct {
	request   string
	timestamp time.Time
}

func NewStore() *Store {
	store := &Store{stringStore: make(map[string]string),
		logs: make([]Record, 0, INITIAL_LOG_CAPACITY),
		lock: sync.Mutex{}}
	// Try to read a database dump if one exists.
	file, err := os.Open("dump")
	if err != nil {
		fmt.Println("No existing database found.")
	} else {
		// Use a Scanner to get each line of the dump.
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()

			// The log follows the format: "key:val"
			index := strings.IndexByte(line, ':')
			if index == -1 {
				fmt.Println("Invalid database record")
				continue
			}

			// Parse and store the key-value pair.
			key := line[:index]
			val := line[index+1:]
			store.stringStore[key] = val
		}
	}
	return store
}

func NewRecord(r string) *Record {
	return &Record{request: r, timestamp: time.Now()}
}

func (store *Store) Flush() {
	// Flush all data to disk.
	store.writeLogs()
	store.writeDump()
}

func (store *Store) Execute(request string) string {
	return store.dispatch(request)
}

func (store *Store) dispatch(request string) string {
	// Commands are case insensitive, but arguments are not.
	args := strings.Split(request, " ")
	function := strings.ToLower(args[0])
	exec, ok := funcmap[function]
	if !ok {
		return "no such function"
	}
	// Keep a log of every passed transaction, and call the function.
	store.logRecord(request)
	return exec(args[1:], store)
}

func (store *Store) logRecord(r string) {
	request := Record{request: r, timestamp: time.Now()}
	store.logs = append(store.logs, request)
}

func (store *Store) writeLogs() {
	// Logs are append-only, and keep a complete record of all transactions in history.
	file, err := os.OpenFile("log", os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_SYNC, 0660)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	for i := 0; i < len(store.logs); i++ {
		// Write each record to the file.
		record := store.logs[i]
		entry := "\"" + record.request + "\", " + record.timestamp.String() + "\n"
		file.WriteString(entry)
	}
}

func (store *Store) writeDump() {
	// Dumps are overwritten each time the server is closed.
	file, err := os.Create("dump")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	for key, val := range store.stringStore {
		// Write each key-value pair, separated by colons, to the file.
		entry := key + ":" + val + "\n"
		file.WriteString(entry)
	}
	err = file.Sync()
	if err != nil {
		log.Fatal(err)
	}
}

func getValue(args []string, store *Store) string {
	// Get mutex lock and ensure release.
	store.lock.Lock()
	defer store.lock.Unlock()
	if len(args) != 1 {
		return "wrong number of arguments for \"GET\""
	}
	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")
	val, present := store.stringStore[key]
	if !present {
		return "<nil>"
	}
	return val
}

func setValue(args []string, store *Store) string {
	// Get mutex lock and ensure release.
	store.lock.Lock()
	defer store.lock.Unlock()
	if len(args) != 2 {
		return "wrong number of arguments for \"SET\""
	}

	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")
	val := strings.Trim(args[1], "\"")
	store.stringStore[key] = val
	return "OK"
}

func incr(args []string, store *Store) string {
	// Get mutex lock and ensure release.
	store.lock.Lock()
	defer store.lock.Unlock()
	if len(args) != 1 {
		return "wrong number of arguments for \"INCR\""
	}
	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")

	// Get string and try to parse it as an integer.
	val, present := store.stringStore[key]
	if !present {
		return "no such value in store"
	}
	intVal, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err.Error()
	}

	// Check for integer overflow, increment, and convert back into a string.
	if intVal == MAXINT {
		return "unable to \"INCR\", integer overflow"
	}
	result := strconv.FormatInt(intVal+1, 10)
	store.stringStore[key] = result
	return result
}

func decr(args []string, store *Store) string {
	// Get mutex lock and ensure release.
	store.lock.Lock()
	defer store.lock.Unlock()
	if len(args) != 1 {
		return "wrong number of arguments for \"DECR\""
	}
	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")

	// Get string and try to parse it as an integer.
	val, present := store.stringStore[key]
	if !present {
		return "no such value in store"
	}
	intVal, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err.Error()
	}

	// Check for integer underflow, decrement, and convert back into a string.
	if intVal == MININT {
		return "unable to \"DECR\", integer underflow"
	}
	result := strconv.FormatInt(intVal-1, 10)
	store.stringStore[key] = result
	return result
}

func del(args []string, store *Store) string {
	// Get mutex lock and ensure release.
	store.lock.Lock()
	defer store.lock.Unlock()
	if len(args) != 1 {
		return "wrong number of arguments for \"DEL\""
	}
	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")
	delete(store.stringStore, key)
	return "OK"
}
