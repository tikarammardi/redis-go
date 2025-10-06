package main

import (
	"sync"
	"time"
)

// Item represents a value with optional expiry
type Item struct {
	Value  string
	Expiry *time.Time
}

// IsExpired checks if the item has expired
func (i *Item) IsExpired() bool {
	return i.Expiry != nil && time.Now().After(*i.Expiry)
}

// InMemoryKeyValueStore implements KeyValueStore interface
type InMemoryKeyValueStore struct {
	store map[string]Item
	mutex sync.RWMutex
}

// NewInMemoryKeyValueStore creates a new in-memory key-value store
func NewInMemoryKeyValueStore() *InMemoryKeyValueStore {
	return &InMemoryKeyValueStore{
		store: make(map[string]Item),
	}
}

func (s *InMemoryKeyValueStore) Set(key, value string, expiry ...time.Duration) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	var expiryTime *time.Time
	if len(expiry) > 0 && expiry[0] > 0 {
		t := time.Now().Add(expiry[0])
		expiryTime = &t
	}

	s.store[key] = Item{
		Value:  value,
		Expiry: expiryTime,
	}
	return nil
}

func (s *InMemoryKeyValueStore) Get(key string) (string, bool) {
	s.mutex.RLock()
	item, found := s.store[key]
	s.mutex.RUnlock()

	if !found {
		return "", false
	}

	if item.IsExpired() {
		_ = s.Delete(key) // Handle the error silently as it's a cleanup operation
		return "", false
	}

	return item.Value, true
}

func (s *InMemoryKeyValueStore) Delete(key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.store, key)
	return nil
}

// ListNode represents a node in a doubly linked list
type ListNode struct {
	Value string
	Next  *ListNode
	Prev  *ListNode
}

// DoublyLinkedList represents a doubly linked list with length tracking
type DoublyLinkedList struct {
	head   *ListNode
	tail   *ListNode
	length int
}

// NewDoublyLinkedList creates a new doubly linked list
func NewDoublyLinkedList() *DoublyLinkedList {
	return &DoublyLinkedList{}
}

func (dll *DoublyLinkedList) PushFront(value string) {
	node := &ListNode{Value: value}

	if dll.head == nil {
		dll.head = node
		dll.tail = node
	} else {
		node.Next = dll.head
		dll.head.Prev = node
		dll.head = node
	}
	dll.length++
}

func (dll *DoublyLinkedList) PushBack(value string) {
	node := &ListNode{Value: value}

	if dll.tail == nil {
		dll.head = node
		dll.tail = node
	} else {
		node.Prev = dll.tail
		dll.tail.Next = node
		dll.tail = node
	}
	dll.length++
}

func (dll *DoublyLinkedList) PopFront() (string, bool) {
	if dll.head == nil {
		return "", false
	}

	value := dll.head.Value
	dll.head = dll.head.Next

	if dll.head != nil {
		dll.head.Prev = nil
	} else {
		dll.tail = nil
	}

	dll.length--
	return value, true
}

func (dll *DoublyLinkedList) PopFrontMultiple(count int) []string {
	if count <= 0 || dll.head == nil {
		return []string{}
	}

	values := make([]string, 0, count)
	for i := 0; i < count && dll.head != nil; i++ {
		value, _ := dll.PopFront()
		values = append(values, value)
	}

	return values
}

func (dll *DoublyLinkedList) Range(start, end int) []string {
	if dll.head == nil {
		return []string{}
	}

	// Handle negative indices
	if start < 0 {
		start = dll.length + start
	}
	if end < 0 {
		end = dll.length + end
	}

	// Bounds checking
	if start < 0 {
		start = 0
	}
	if end >= dll.length {
		end = dll.length - 1
	}
	if start > end || start >= dll.length {
		return []string{}
	}

	result := make([]string, 0, end-start+1)
	current := dll.head

	// Skip to start position
	for i := 0; i < start && current != nil; i++ {
		current = current.Next
	}

	// Collect values in range
	for i := start; i <= end && current != nil; i++ {
		result = append(result, current.Value)
		current = current.Next
	}

	return result
}

func (dll *DoublyLinkedList) Length() int {
	return dll.length
}

// InMemoryListStore implements ListStore interface
type InMemoryListStore struct {
	lists map[string]*DoublyLinkedList
	mutex sync.RWMutex
}

// NewInMemoryListStore creates a new in-memory list store
func NewInMemoryListStore() *InMemoryListStore {
	return &InMemoryListStore{
		lists: make(map[string]*DoublyLinkedList),
	}
}

func (s *InMemoryListStore) LPush(key string, values ...string) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	list, exists := s.lists[key]
	if !exists {
		list = NewDoublyLinkedList()
		s.lists[key] = list
	}

	for _, value := range values {
		list.PushFront(value)
	}

	return list.Length(), nil
}

func (s *InMemoryListStore) RPush(key string, values ...string) (int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	list, exists := s.lists[key]
	if !exists {
		list = NewDoublyLinkedList()
		s.lists[key] = list
	}

	for _, value := range values {
		list.PushBack(value)
	}

	return list.Length(), nil
}

func (s *InMemoryListStore) LPop(key string, count ...int) ([]string, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	list, exists := s.lists[key]
	if !exists {
		return nil, false
	}

	popCount := 1
	if len(count) > 0 && count[0] > 0 {
		popCount = count[0]
	}

	if popCount == 1 {
		value, ok := list.PopFront()
		if !ok {
			return nil, false
		}
		if list.Length() == 0 {
			delete(s.lists, key)
		}
		return []string{value}, true
	}

	values := list.PopFrontMultiple(popCount)
	if len(values) == 0 {
		return nil, false
	}

	if list.Length() == 0 {
		delete(s.lists, key)
	}

	return values, true
}

func (s *InMemoryListStore) LRange(key string, start, end int) ([]string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	list, exists := s.lists[key]
	if !exists {
		return nil, false
	}

	return list.Range(start, end), true
}

func (s *InMemoryListStore) LLen(key string) (int, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	list, exists := s.lists[key]
	if !exists {
		return 0, false
	}

	return list.Length(), true
}
