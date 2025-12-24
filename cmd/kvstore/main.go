package main

import (
	"encoding/json"
	"fmt"
	"kvstore/internal"
	"net/http"
	"os"
	"strings"
	"sync"
)

func main() {
	store := internal.NewStore()

	logfile, err := os.OpenFile(
		"kvlog",
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		0644,
	)

	if err := internal.ReplayWAL(logfile, store); err != nil {
		panic(err)
	}

	if err != nil {
		panic("unable to open logfile")
	}
	defer logfile.Close()

	var logMu sync.Mutex

	http.HandleFunc("/get/", getHandler(store, logfile, &logMu))
	http.HandleFunc("/put/", putHandler(store, logfile, &logMu))
	http.HandleFunc("/delete/", deleteHandler(store, logfile, &logMu))

	fmt.Println("Server running on port 8080")
	http.ListenAndServe(":8080", nil)
}

type PutRequest struct {
	Value string `json:"value"`
}

func putHandler(store *internal.Kvstore, logfile *os.File, logmu *sync.Mutex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := strings.TrimPrefix(r.URL.Path, "/put/")
		if id == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		var reqbody PutRequest
		if err := json.NewDecoder(r.Body).Decode(&reqbody); err != nil {
			http.Error(w, "invalid json body", http.StatusBadRequest)
			return
		}

		entry := &internal.Log{
			Op:    2, // PUT
			Key:   []byte(id),
			Value: []byte(reqbody.Value),
		}

		if err := internal.WriteLog(logmu, logfile, entry); err != nil {
			http.Error(w, "wal write failed", http.StatusInternalServerError)
			return
		}

		if err := store.Put(id, reqbody.Value); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func deleteHandler(store *internal.Kvstore, logfile *os.File, logmu *sync.Mutex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		id := strings.TrimPrefix(r.URL.Path, "/delete/")
		if id == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		entry := &internal.Log{
			Op:  3, // DELETE
			Key: []byte(id),
		}

		if err := internal.WriteLog(logmu, logfile, entry); err != nil {
			http.Error(w, "wal write failed", http.StatusInternalServerError)
			return
		}

		if err := store.Delete(id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func getHandler(store *internal.Kvstore, logfile *os.File, logmu *sync.Mutex) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/get/")
		if id == "" {
			http.Error(w, "id is required", http.StatusBadRequest)
			return
		}

		val, exists, err := store.Get(id)
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
