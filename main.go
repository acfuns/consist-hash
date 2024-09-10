package main

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
)

func main() {
	ch := NewConsistentHash(3, nil)

	ch.Add("node1", "node2", "node3")

	fmt.Println(ch.Get("key1"))
	fmt.Println(ch.Get("key2"))
	fmt.Println(ch.Get("key3"))
	fmt.Println(ch.Get("key4"))
	fmt.Println(ch.Get("key12"))
	fmt.Println(ch.Get("key12132"))
	fmt.Println(ch.Get("keyawdaw3"))
}

type HashFunc func(key []byte) uint32

type ConsistentHash struct {
	hashFunc HashFunc
	replicas int
	ring     []int
	hashMap  map[int]string
}

func NewConsistentHash(replicas int, fn HashFunc) *ConsistentHash {
	m := &ConsistentHash{
		replicas: replicas,
		hashFunc: fn,
		hashMap:  make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *ConsistentHash) Add(nodes ...string) {
	for _, node := range nodes {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hashFunc(([]byte(strconv.Itoa(i) + node))))
			m.ring = append(m.ring, hash)
			m.hashMap[hash] = node
		}
	}

	sort.Ints(m.ring)
}

func (m *ConsistentHash) Get(key string) string {
	if len(m.ring) == 0 {
		return ""
	}
	hash := int(m.hashFunc(([]byte(key))))
	idx := sort.Search(len(m.ring), func(i int) bool { return m.ring[i] >= hash })
	if idx == len(m.ring) {
		idx = 0
	}

	return m.hashMap[m.ring[idx]]
}
