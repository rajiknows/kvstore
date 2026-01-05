package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"kvstore/internal"
	"kvstore/internal/raft"
	"log"
	"net/http"
	"strconv"
	"strings"
)

func main() {
	var (
		id          int
		httpAddr    string
		raftAddr    string
		cluster     string
		clusterHttp string
	)

	flag.IntVar(&id, "id", 0, "node ID")
	flag.StringVar(&httpAddr, "http-addr", "localhost:8080", "client-facing HTTP server address")
	flag.StringVar(&raftAddr, "raft-addr", "localhost:9090", "raft server address")
	flag.StringVar(&cluster, "cluster", "", "comma-separated list of raft addresses in the cluster")
	flag.StringVar(&clusterHttp, "cluster-http", "", "comma-separated list of http addresses in the cluster")
	flag.Parse()

	if id == 0 {
		log.Fatal("id is required")
	}

	peerAddrs := strings.Split(cluster, ",")
	peerIds := make([]int, 0, len(peerAddrs))
	peerIdToAddr := make(map[int]string)
	for _, addr := range peerAddrs {
		parts := strings.Split(addr, "=")
		if len(parts) != 2 {
			log.Fatalf("invalid peer address: %s", addr)
		}
		peerId, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Fatalf("invalid peer id: %s", parts[0])
		}
		peerIds = append(peerIds, peerId)
		peerIdToAddr[peerId] = parts[1]
	}

	peerHttpAddrs := strings.Split(clusterHttp, ",")
	peerIdToHttpAddr := make(map[int]string)
	for _, addr := range peerHttpAddrs {
		parts := strings.Split(addr, "=")
		if len(parts) != 2 {
			log.Fatalf("invalid peer http address: %s", addr)
		}
		peerId, err := strconv.Atoi(parts[0])
		if err != nil {
			log.Fatalf("invalid peer id: %s", parts[0])
		}
		peerIdToHttpAddr[peerId] = parts[1]
	}

	store := internal.NewStore()
	commitChan := make(chan internal.Log)

	ready := make(chan any)
	raftServer := raft.NewServer(id, peerIds, ready, commitChan)
	raftServer.Serve(raftAddr)
	close(ready)

	go func() {
		for command := range commitChan {
			switch command.Op {
			case internal.OpPut:
				store.Put(string(command.Key), string(command.Value))
			case internal.OpDelete:
				store.Delete(string(command.Key))
			}
		}
	}()

	for peerId, addr := range peerIdToAddr {
		if peerId == id {
			continue
		}
		err := raftServer.ConnectToPeer(peerId, addr)
		if err != nil {
			log.Fatalf("failed to connect to peer %d at %s: %v", peerId, addr, err)
		}
	}

	http.HandleFunc("/get/", getHandler(store, raftServer, id, peerIdToHttpAddr))
	http.HandleFunc("/put/", putHandler(raftServer, id, peerIdToHttpAddr))
	http.HandleFunc("/delete/", deleteHandler(raftServer, id, peerIdToHttpAddr))

	fmt.Printf("Server running on %s\n", httpAddr)
	http.ListenAndServe(httpAddr, nil)
}

type PutRequest struct {
	Value string `json:"value"`
}

func putHandler(raftServer *raft.Server, id int, peerIdToHttpAddr map[int]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		leaderId := raftServer.GetLeaderId()
		if leaderId != id {
			leaderAddr, ok := peerIdToHttpAddr[leaderId]
			if !ok {
				http.Error(w, "leader not found", http.StatusServiceUnavailable)
				return
			}
			http.Redirect(w, r, "http://"+leaderAddr+r.URL.Path, http.StatusTemporaryRedirect)
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/put/")
		if key == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		var reqbody PutRequest
		if err := json.NewDecoder(r.Body).Decode(&reqbody); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}

		command := internal.Log{
			Op:    internal.OpPut,
			Key:   []byte(key),
			Value: []byte(reqbody.Value),
		}

		_, err := raftServer.Submit(command)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func deleteHandler(raftServer *raft.Server, id int, peerIdToHttpAddr map[int]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		leaderId := raftServer.GetLeaderId()
		if leaderId != id {
			leaderAddr, ok := peerIdToHttpAddr[leaderId]
			if !ok {
				http.Error(w, "leader not found", http.StatusServiceUnavailable)
				return
			}
			http.Redirect(w, r, "http://"+leaderAddr+r.URL.Path, http.StatusTemporaryRedirect)
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/delete/")
		if key == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		command := internal.Log{
			Op:  internal.OpDelete,
			Key: []byte(key),
		}

		_, err := raftServer.Submit(command)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func getHandler(store *internal.Kvstore, raftServer *raft.Server, id int, peerIdToHttpAddr map[int]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		leaderId := raftServer.GetLeaderId()
		if leaderId != id {
			leaderAddr, ok := peerIdToHttpAddr[leaderId]
			if !ok {
				http.Error(w, "leader not found", http.StatusServiceUnavailable)
				return
			}
			http.Redirect(w, r, "http://"+leaderAddr+r.URL.Path, http.StatusTemporaryRedirect)
			return
		}

		key := strings.TrimPrefix(r.URL.Path, "/get/")
		if key == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		val, exists, err := store.Get(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !exists {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}

		fmt.Fprintln(w, val)
	}
}
