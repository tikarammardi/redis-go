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

func getListRange(key string, start, end int) ([]string, bool) {
	head, found := listStore[key]
	if !found {
		return nil, false
	}
	length := head.Length
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end || start >= length {
		return []string{}, true
	}

	result := []string{}
	current := head
	for i := 0; i < start; i++ {
		current = current.Next
	}
	for i := start; i <= end && current != nil; i++ {
		result = append(result, current.Value)
		current = current.Next
	}
	return result, true
}

func lpushValue(key, value string) (int /*new length*/, error) {
	if head, found := listStore[key]; found {
		newItem := &ListItem{Value: value, Next: head}
		head.Prev = newItem
		newItem.Length = head.Length + 1
		listStore[key] = newItem
		return newItem.Length, nil
	} else {
		// Create new list
		newItem := &ListItem{Value: value, Length: 1}
		listStore[key] = newItem
		return 1, nil
	}
}

func getListLength(key string) (int, bool) {
	head, found := listStore[key]
	if !found {
		return 0, false
	}
	return head.Length, true
}
