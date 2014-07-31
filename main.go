package main

import (
	"net/http"
)

func main() {
	var s Server
	http.ListenAndServe("localhost:8000", s)
}
