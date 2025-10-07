package main

import (
	"net"
	"time"
)

// CommandHandler interface following Single Responsibility Principle
type CommandHandler interface {
	Handle(parts []RespValue, conn net.Conn) error
}

// Store interfaces following Interface Segregation Principle
type KeyValueStore interface {
	Set(key, value string, expiry ...time.Duration) error
	Get(key string) (string, bool)
	Delete(key string) error
}

type ListStore interface {
	LPush(key string, values ...string) (int, error)
	RPush(key string, values ...string) (int, error)
	LPop(key string, count ...int) ([]string, bool)
	LRange(key string, start, end int) ([]string, bool)
	LLen(key string) (int, bool)
}

// Response writer interface for better testability
type ResponseWriter interface {
	WriteSimpleString(s string) error
	WriteBulkString(s string) error
	WriteInteger(i int) error
	WriteError(err string) error
	WriteArray(items []string) error
	WriteNullBulkString() error
	WriteNullArray() error
	WriteEmptyArray() error
	WriteStreamEntries(entries []StreamEntry) error
	WriteStreamReadResults(results []StreamReadResult) error
}

// Command processor interface
type CommandProcessor interface {
	Process(command RespValue, conn net.Conn) error
}

// Server interface
type Server interface {
	Start(address string) error
	Stop() error
}
