package store

import "time"

// Item for string values
type Item struct {
	Value  string
	Expiry *time.Time
}

// ListNode for linked list
type ListNode struct {
	Value string
	Next  *ListNode
}

// List holds head/tail pointers and expiry
type List struct {
	Head   *ListNode
	Tail   *ListNode
	Expiry *time.Time
}

// Entry is a union: either a string entry or a list entry
type Entry struct {
	Str *Item
	Lst *List
}
