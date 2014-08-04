package db

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var funcmap = map[string]func(args []string, store *Store) (string, error){
	"get": getValue,
	"set": setValue,
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

func (store *Store) Execute(request string) (string, error) {
	fmt.Println("db.Execute(request)")
	return store.dispatch(request)
}

func (store *Store) dispatch(request string) (string, error) {
	fmt.Println("dispatch")
	args := strings.Split(request, " ")
	exec, ok := funcmap[args[0]]
	if !ok {
		return "", errors.New("no such function")
	}
	return exec(args[1:], store)
}

func getValue(args []string, store *Store) (string, error) {
	if len(args) != 1 {
		return "", errors.New("wrong number of arguments for \"GET\"")
	}
	val, present := store.stringStore[args[0]]
	if !present {
		return val, errors.New("no such value in store")
	}
	return val, nil
}

func setValue(args []string, store *Store) (string, error) {
	if len(args) != 2 {
		return "", errors.New("wrong number of arguments for \"GET\"")
	}
	store.stringStore[args[0]] = args[1]
	return "OK", nil
}
