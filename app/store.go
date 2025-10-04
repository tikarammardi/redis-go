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
