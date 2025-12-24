package internal

import (
	"errors"
	"sync"
)

// this is the Basic interface of a kv-store
type Store interface {
	Get(key string) (val string, exists bool, err error)
	Put(key, val string) error
	Delete(key string) error
}

// the most basic form of a kv-store just a string  -> string map
type Kvstore struct {
	mu   sync.Mutex
	data map[string]string
}

func NewStore() *Kvstore {
	return &Kvstore{
		data: make(map[string]string),
	}
}
func (kv *Kvstore) Get(key string) (string, bool, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, exists := kv.data[key]
	return val, exists, nil
}

func (kv *Kvstore) Put(key, val string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data[key] = val
	return nil
}

func (kv *Kvstore) Delete(key string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, exists := kv.data[key]; !exists {
		return errors.New("key not found")
	}
	delete(kv.data, key)
	return nil
}
