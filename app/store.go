package main

import "time"

type Item struct {
	Value  string
	Expiry *time.Time // nil means no expiry
}

var store = make(map[string]Item)

func setValue(key, value string, expiryMilliseconds ...int) {
	var expiry *time.Time

	if len(expiryMilliseconds) > 0 && expiryMilliseconds[0] > 0 {
		t := time.Now().Add(time.Duration(expiryMilliseconds[0]) * time.Millisecond)
		expiry = &t
	}

	store[key] = Item{
		Value:  value,
		Expiry: expiry,
	}
}

func getValue(key string) (string, bool) {
	item, found := store[key]
	if !found {
		return "", false
	}
	// Naive expiry check, will be improved in later stages maybe with a background cleanup goroutine
	if item.Expiry != nil && time.Now().After(*item.Expiry) {
		delete(store, key)
		return "", false
	}

	return item.Value, true
}

func deleteValue(key string) {
	delete(store, key)
}

type ListItem struct {
	Value  string
	Next   *ListItem
	Prev   *ListItem
	Length int
}

var listStore = make(map[string]*ListItem)

func rpushValue(key, value string) (int /*new length*/, error) {
	if head, found := listStore[key]; found {
		// Traverse to the end of the list
		current := head
		for current.Next != nil {
			current = current.Next
		}
		newItem := &ListItem{Value: value, Prev: current}
		current.Next = newItem
		head.Length++
		return head.Length, nil
	} else {
		// Create new list
		newItem := &ListItem{Value: value, Length: 1}
		listStore[key] = newItem
		return 1, nil
	}
}
