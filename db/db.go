package db

import (
	"bufio"
	"container/list"
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
	MAXINT                = 9223372036854775807
	MININT                = -9223372036854775808
	INITIAL_LOG_CAPACITY  = 8192
	INITIAL_LIST_CAPACITY = 1024
)

// Lookup table for function requests.
var funcmap = map[string]func(args []string, store *Store) string{
	// String operations.
	"get":    getValue,
	"set":    setValue,
	"incr":   incr,
	"incrby": incrby,
	"decr":   decr,
	"del":    del,

	// List operations.
	"lpush":  lpush,
	"lpop":   lpop,
	"rpush":  rpush,
	"rpop":   rpop,
	"llen":   llen,
	"lrange": lrange,

	// Hash operations.
	"hset":    hset,
	"hget":    hget,
	"hlen":    hlen,
	"hkeys":   hkeys,
	"hvals":   hvals,
	"hgetall": hgetall,
}

type Store struct {
	stringStore map[string]string
	hashStore   map[string]map[string]string
	listStore   map[string]*list.List
	logs        []Record
	lock        sync.Mutex
}

type Record struct {
	request   string
	timestamp time.Time
}

func NewStore() *Store {
	store := &Store{listStore: make(map[string]*list.List),
		hashStore:   make(map[string]map[string]string),
		stringStore: make(map[string]string),
		logs:        make([]Record, 0, INITIAL_LOG_CAPACITY),
		lock:        sync.Mutex{}}
	// Try to read a database dump if one exists.
	store.readFromFile("dump")
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

func (store *Store) readFromFile(filename string) {
	file, err := os.Open(filename)
	if err == nil {
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
}

func (store *Store) Execute(request string) string {
	if request == "" {
		return request
	}

	// Commands are case insensitive, but arguments are not.
	args := strings.Split(request, " ")
	function := strings.ToLower(args[0])

	// Get function from map and run it with args.
	exec, ok := funcmap[function]
	if !ok {
		return "ERR no such function"
	}
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
	if len(args) != 1 {
		return "wrong number of arguments for \"GET\", expected 1"
	}
	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")
	val, present := store.stringStore[key]
	if !present {
		return "<nil>"
	}
	return "\"" + val + "\""
}

func setValue(args []string, store *Store) string {
	if len(args) != 2 {
		return "wrong number of arguments for \"SET\", expected 2"
	}

	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")
	val := strings.Trim(args[1], "\"")
	store.stringStore[key] = val
	return "OK"
}

func incr(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"INCR\", expected 1"
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
		return "cannot increment non-integer string"
	}

	// Check for integer overflow, increment, and convert back into a string.
	if intVal == MAXINT {
		return "unable to \"INCR\", integer overflow"
	}
	result := strconv.FormatInt(intVal+1, 10)
	store.stringStore[key] = result
	return "(int) " + result
}

func incrby(args []string, store *Store) string {
	if len(args) != 2 {
		return "wrong number of arguments for \"INCRBY\", expected 2"
	}
	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")

	// Try to parse incrby argument as an integer.
	plus, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return "invalid integer argument"
	}

	// Get string and try to parse it as an integer.
	val, present := store.stringStore[key]
	if !present {
		return "no such value in store"
	}
	intVal, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return "cannot increment non-integer value"
	}

	// Check for integer underflow, decrement, and convert back into a string.
	if intVal > MAXINT-plus {
		return "unable to \"INCRBY\", integer overflow"
	}
	result := strconv.FormatInt(intVal+plus, 10)
	store.stringStore[key] = result
	return "(int) " + result
}

func decr(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"DECR\", expected 1"
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
		return "cannot decrement non-integer value"
	}

	// Check for integer underflow, decrement, and convert back into a string.
	if intVal == MININT {
		return "unable to \"DECR\", integer underflow"
	}
	result := strconv.FormatInt(intVal-1, 10)
	store.stringStore[key] = result
	return "(int) " + result
}

func del(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"DEL\", expected 1"
	}
	// Trim surrounding quotes.
	key := strings.Trim(args[0], "\"")
	delete(store.stringStore, key)
	return "OK"
}

func lpush(args []string, store *Store) string {
	if len(args) != 2 {
		return "wrong number of arguments for \"LPUSH\", expected 2"
	}
	// Trim surrounding quotes.
	name, item := strings.Trim(args[0], "\""), strings.Trim(args[1], "\"")

	// Check if list is present, and create a new one if not.
	l, present := store.listStore[name]
	if !present {
		l = list.New()
	}
	// Append to list and return length of list.
	l.PushFront(item)
	store.listStore[name] = l
	return "(int) " + strconv.FormatInt(int64(l.Len()), 10)
}

func lpop(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"LPOP\", expected 1"
	}

	// Trim surrounding quotes.
	name := strings.Trim(args[0], "\"")

	// Check if list is present in store.
	l, present := store.listStore[name]
	if !present {
		return "<nil>"
	}
	// Pop from list and return item.
	item := l.Remove(l.Front()).(string)
	return "\"" + item + "\""
}

func rpush(args []string, store *Store) string {
	if len(args) != 2 {
		return "wrong number of arguments for \"RPUSH\", expected 2"
	}
	// Trim surrounding quotes.
	name, item := strings.Trim(args[0], "\""), strings.Trim(args[1], "\"")

	// Check if list is present, and create a new one if not.
	l, present := store.listStore[name]
	if !present {
		l = list.New()
	}
	// Append to list and return length of list.
	l.PushBack(item)
	store.listStore[name] = l
	return "(int) " + strconv.FormatInt(int64(l.Len()), 10)
}

func rpop(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"RPOP\", expected 1"
	}

	// Trim surrounding quotes.
	name := strings.Trim(args[0], "\"")

	// Check if list is present in store.
	l, present := store.listStore[name]
	if !present {
		return "<nil>"
	}
	// Pop from list and return item.
	item := l.Remove(l.Back()).(string)
	return "\"" + item + "\""
}

func llen(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"LLEN\", expected 1"
	}

	// Trim surrounding quotes.
	name := strings.Trim(args[0], "\"")

	// Check if list is present in store.
	l, present := store.listStore[name]
	if !present {
		return "(int) 0"
	}
	return "(int) " + strconv.FormatInt(int64(l.Len()), 10)
}

func lrange(args []string, store *Store) string {
	if len(args) != 3 {
		return "wrong number of arguments for \"LRANGE\", expected 3"
	}

	// Trim surrounding quotes
	name := strings.Trim(args[0], "\"")

	// Check if list is present in store
	l, present := store.listStore[name]
	if !present {
		return "empty list"
	}

	// Try to parse start and stop as integers.
	start, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return "invalid integer given as start index"
	}
	stop, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return "invalid integer given as stop index"
	}

	// Read until end of the list if stop is negative.
	if stop < 0 {
		stop = int64(l.Len())
	}

	// Start should be a positive integer.
	if start < 0 {
		return "start index must be positive"
	}
	if int(start) > l.Len() {
		return "empty list"
	}

	// Print out each item of the list until stop, or the end of the list.
	ret := ""
	for e, i := l.Front(), start; i < stop && e != nil; i, e = i+1, e.Next() {
		ret = ret + "\"" + e.Value.(string) + "\", "
	}
	ret = ret[:len(ret)-2]
	return ret
}

func hset(args []string, store *Store) string {
	if len(args) != 3 {
		return "wrong number of arguments for \"HSET\", expected 3"
	}

	ret, name := 1, strings.Trim(args[0], "\"")
	hash, present := store.hashStore[name]
	if !present {
		hash = make(map[string]string)
	}

	key, val := strings.Trim(args[1], "\""), strings.Trim(args[2], "\"")
	_, present = hash[key]
	if present {
		ret = 0
	}
	hash[key] = val
	store.hashStore[name] = hash
	return "(int) " + strconv.FormatInt(int64(ret), 10)
}

func hget(args []string, store *Store) string {
	if len(args) != 2 {
		return "wrong number of arguments for \"HGET\", expected 3"
	}

	name := strings.Trim(args[0], "\"")
	hash, present := store.hashStore[name]
	if !present {
		return "<nil>"
	}

	key := strings.Trim(args[1], "\"")
	val, present := hash[key]
	if !present {
		return "<nil>"
	}
	return "\"" + val + "\""
}

func hlen(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"HLEN\", expected 3"
	}

	name := strings.Trim(args[0], "\"")
	hash, present := store.hashStore[name]
	if !present {
		return "(int) 0"
	}
	return strconv.FormatInt(int64(len(hash)), 10)
}

func hkeys(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"HKEYS\", expected 1"
	}

	name := strings.Trim(args[0], "\"")
	hash, present := store.hashStore[name]
	if !present {
		return "empty hash"
	}
	ret := ""
	for key, _ := range hash {
		ret = ret + "\"" + key + "\", "
	}
	ret = ret[:len(ret)-2]
	return ""
}

func hvals(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"HVALS\", expected 1"
	}

	name := strings.Trim(args[0], "\"")
	hash, present := store.hashStore[name]
	if !present {
		return "empty hash"
	}
	ret := ""
	for _, val := range hash {
		ret = ret + "\"" + val + "\", "
	}
	ret = ret[:len(ret)-2]
	return ""
}

func hgetall(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"HGETALL\", expected 1"
	}

	name := strings.Trim(args[0], "\"")
	hash, present := store.hashStore[name]
	if !present {
		return "empty hash"
	}
	ret := ""
	for key, val := range hash {
		ret = ret + "\"" + key + "->" + val + "\", "
	}
	ret = ret[:len(ret)-2]
	return ret
}
