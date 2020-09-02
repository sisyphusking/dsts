package cache

import "sync"

type msg struct {
	Key   string
	Value []byte
}

type ICache interface {
	Set(index uint64, key string, value []byte)
	Get(index uint64) (string, []byte, bool)
	Delete(index uint64)
}

type Cache struct {
	store map[uint64]msg
	mu    sync.RWMutex
}

func (c *Cache) Set(index uint64, key string, value []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[index] = msg{
		Key:   key,
		Value: value,
	}
}

func (c *Cache) Get(index uint64) (string, []byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	message, ok := c.store[index]
	return message.Key, message.Value, ok
}

func (c *Cache) Delete(index uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.store, index)
}

func New() *Cache {
	hashtable := make(map[uint64]msg)
	return &Cache{store: hashtable}
}
