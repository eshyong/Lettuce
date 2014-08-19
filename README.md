Lettuce
=======
A key/value store with plans for data replication and failure tolerance. In progress!

Architecture
============
Lettuce is composed of a master server, which talks directly to the client and forwards requests to the DB. The other servers (primary, backup) execute client requests and keep a store in memory. The servers communicate amongst themselves to get diffs of their DB state. The master is in charge of managing the uptime of the servers, and will replace servers as necessary.

Unfortunately, the master is currently a centralized point of failure. TODO: Implement Paxos!

Usage
======
Requires Golang.
Make sure your GOPATH and environment variables are setup, and run `go install ./cmd/server/`, `go install ./cmd/master/`, and `go install ./cmd/cli`. Then run `master` on one machine, `server` in two other machines, and `cli` in the first machine.

Some Commands
=========
* `GET key`:           returns the value mapped by key, if present
* `SET key value`:     maps key to value 
* `INCR key`:          interprets the key as an integer counter, and increments it
* `DECR key`:          interprets the key as an integer counter, and decrements it
* `INCRBY key intval`: interprets the key as an integer counter, and increments it by intval
