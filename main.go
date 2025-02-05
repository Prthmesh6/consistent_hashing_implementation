package main

import (
	"crypto/sha1"
	"fmt"
	"sort"
	"strconv"
)

// Hash function to map nodes and keys onto the ring
func hashKey(key string) uint32 {
	h := sha1.New()
	h.Write([]byte(key))
	hashBytes := h.Sum(nil)
	return (uint32(hashBytes[0])<<24 | uint32(hashBytes[1])<<16 | uint32(hashBytes[2])<<8 | uint32(hashBytes[3]))
}

// ConsistentHash struct to manage the hash ring
type ConsistentHash struct {
	replicas    int               // Virtual nodes per real node
	hashRing    []uint32          // Sorted hash values of nodes
	nodeMap     map[uint32]string // Hash to node mapping
	actualNodes map[string]bool   // Real nodes tracking
}

// NewConsistentHash initializes a consistent hashing ring
func NewConsistentHash(replicas int) *ConsistentHash {
	return &ConsistentHash{
		replicas:    replicas,
		nodeMap:     make(map[uint32]string),
		actualNodes: make(map[string]bool),
	}
}

// AddNode adds a new node with virtual replicas
func (ch *ConsistentHash) AddNode(node string) {
	if _, exists := ch.actualNodes[node]; exists {
		return // Node already exists
	}

	ch.actualNodes[node] = true
	for i := 0; i < ch.replicas; i++ {
		hash := hashKey(node + strconv.Itoa(i)) // Virtual node hashing
		ch.hashRing = append(ch.hashRing, hash)
		ch.nodeMap[hash] = node
	}

	sort.Slice(ch.hashRing, func(i, j int) bool { return ch.hashRing[i] < ch.hashRing[j] })
}

// RemoveNode removes a node from the ring
func (ch *ConsistentHash) RemoveNode(node string) {
	if _, exists := ch.actualNodes[node]; !exists {
		return
	}

	delete(ch.actualNodes, node)
	var newRing []uint32
	for _, hash := range ch.hashRing {
		if ch.nodeMap[hash] != node {
			newRing = append(newRing, hash)
		} else {
			delete(ch.nodeMap, hash)
		}
	}
	ch.hashRing = newRing
}

// GetNode finds the closest node for a given key
func (ch *ConsistentHash) GetNode(key string) string {
	if len(ch.hashRing) == 0 {
		return ""
	}

	hash := hashKey(key)
	idx := sort.Search(len(ch.hashRing), func(i int) bool { return ch.hashRing[i] >= hash })

	if idx == len(ch.hashRing) {
		idx = 0
	}
	return ch.nodeMap[ch.hashRing[idx]]
}

// Testing the consistent hashing implementation
func main() {
	ch := NewConsistentHash(3) // 3 virtual nodes per server

	// Add some nodes (servers)
	ch.AddNode("ServerA")
	ch.AddNode("ServerB")
	ch.AddNode("ServerC")

	// Simulate mapping of requests (e.g., S2 Cell IDs)
	keys := []string{"CellID_12345", "CellID_67890", "CellID_54321", "CellID_99999"}
	for _, key := range keys {
		fmt.Printf("Key %s is assigned to %s\n", key, ch.GetNode(key))
	}

	// Remove a server and reassign keys
	fmt.Println("\nRemoving ServerB...")
	ch.RemoveNode("ServerB")

	for _, key := range keys {
		fmt.Printf("Key %s is now assigned to %s\n", key, ch.GetNode(key))
	}
}
