package db

import (
	"strconv"
	"strings"
	"time"
)

const (
	MAXINT = 9223372036854775807
	MININT = -9223372036854775808
)

var funcmap = map[string]func(args []string, store *Store) string{
	"get":  getValue,
	"set":  setValue,
	"incr": incr,
	"decr": decr,
}

type Store struct {
	stringStore map[string]string
}

type Request struct {
	Command   string
	Timestamp time.Time
}

func NewStore() *Store {
	return &Store{stringStore: make(map[string]string)}
}

func NewRequest(c string) *Request {
	return &Request{Command: c, Timestamp: time.Now()}
}

func (store *Store) Execute(request string) string {
	return store.dispatch(request)
}

func (store *Store) dispatch(request string) string {
	args := strings.Split(request, " ")
	exec, ok := funcmap[args[0]]
	if !ok {
		return "no such function"
	}
	return exec(args[1:], store)
}

func getValue(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"GET\""
	}
	val, present := store.stringStore[args[0]]
	if !present {
		return "no such value in store"
	}
	return val
}

func setValue(args []string, store *Store) string {
	if len(args) != 2 {
		return "wrong number of arguments for \"SET\""
	}
	store.stringStore[args[0]] = args[1]
	return "OK"
}

func incr(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"INCR\""
	}
	val, present := store.stringStore[args[0]]
	if !present {
		return "no such value in store"
	}
	intVal, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err.Error()
	}
	if intVal == MAXINT {
		return "unable to \"INCR\", integer overflow"
	}
	result := strconv.FormatInt(intVal+1, 10)
	store.stringStore[args[0]] = result
	return result
}

func decr(args []string, store *Store) string {
	if len(args) != 1 {
		return "wrong number of arguments for \"DECR\""
	}
	val, present := store.stringStore[args[0]]
	if !present {
		return "no such value in store"
	}
	intVal, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err.Error()
	}
	if intVal == MININT {
		return "unable to \"DECR\", integer underflow"
	}
	result := strconv.FormatInt(intVal-1, 10)
	store.stringStore[args[0]] = result
	return result
}
