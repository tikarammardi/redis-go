package main

import (
	"net"
	"strconv"
	"strings"
	"time"
)

// Constants for error messages to follow DRY principle
const (
	ErrUnknownCommand = "ERR unknown command"
)

// PingHandler handles PING commands
type PingHandler struct {
	writer ResponseWriter
}

func NewPingHandler(writer ResponseWriter) *PingHandler {
	return &PingHandler{writer: writer}
}

func (h *PingHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) == 1 {
		return h.writer.WriteSimpleString("PONG")
	} else if len(parts) == 2 {
		msg, ok := parts[1].Value.(string)
		if !ok {
			return h.writer.WriteError(ErrUnknownCommand)
		}
		return h.writer.WriteSimpleString(msg)
	}
	return h.writer.WriteError(ErrUnknownCommand)
}

// EchoHandler handles ECHO commands
type EchoHandler struct {
	writer ResponseWriter
}

func NewEchoHandler(writer ResponseWriter) *EchoHandler {
	return &EchoHandler{writer: writer}
}

func (h *EchoHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) != 2 {
		return h.writer.WriteError(ErrUnknownCommand)
	}
	msg, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError(ErrUnknownCommand)
	}
	return h.writer.WriteBulkString(msg)
}

// SetHandler handles SET commands
type SetHandler struct {
	store  KeyValueStore
	writer ResponseWriter
}

func NewSetHandler(store KeyValueStore, writer ResponseWriter) *SetHandler {
	return &SetHandler{store: store, writer: writer}
}

func (h *SetHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) < 3 {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	if parts[1].Type != BulkString || parts[2].Type != BulkString {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	key := parts[1].Value.(string)
	value := parts[2].Value.(string)

	// Parse expiry options
	var expiry time.Duration
	for i := 3; i < len(parts); i += 2 {
		if i+1 >= len(parts) {
			return h.writer.WriteError(ErrUnknownCommand)
		}

		optionValue, ok := parts[i].Value.(string)
		if !ok {
			return h.writer.WriteError(ErrUnknownCommand)
		}

		timeValue, ok := parts[i+1].Value.(string)
		if !ok {
			return h.writer.WriteError(ErrUnknownCommand)
		}

		timeInt, err := strconv.Atoi(timeValue)
		if err != nil || timeInt <= 0 {
			return h.writer.WriteError("ERR invalid expire time in set")
		}

		switch strings.ToUpper(optionValue) {
		case "EX":
			expiry = time.Duration(timeInt) * time.Second
		case "PX":
			expiry = time.Duration(timeInt) * time.Millisecond
		default:
			return h.writer.WriteError(ErrUnknownCommand)
		}
	}

	var err error
	if expiry > 0 {
		err = h.store.Set(key, value, expiry)
	} else {
		err = h.store.Set(key, value)
	}

	if err != nil {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	return h.writer.WriteSimpleString("OK")
}

// GetHandler handles GET commands
type GetHandler struct {
	store  KeyValueStore
	writer ResponseWriter
}

func NewGetHandler(store KeyValueStore, writer ResponseWriter) *GetHandler {
	return &GetHandler{store: store, writer: writer}
}

func (h *GetHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) != 2 {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	key := parts[1].Value.(string)
	value, exists := h.store.Get(key)

	if !exists {
		return h.writer.WriteNullBulkString()
	}

	return h.writer.WriteBulkString(value)
}

// LPushHandler handles LPUSH commands
type LPushHandler struct {
	store  ListStore
	writer ResponseWriter
}

func NewLPushHandler(store ListStore, writer ResponseWriter) *LPushHandler {
	return &LPushHandler{store: store, writer: writer}
}

func (h *LPushHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) < 3 {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	if parts[1].Type != BulkString {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	key := parts[1].Value.(string)
	values := make([]string, 0, len(parts)-2)

	for i := 2; i < len(parts); i++ {
		if parts[i].Type != BulkString {
			return h.writer.WriteError(ErrUnknownCommand)
		}
		values = append(values, parts[i].Value.(string))
	}

	length, err := h.store.LPush(key, values...)
	if err != nil {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	return h.writer.WriteInteger(length)
}

// RPushHandler handles RPUSH commands
type RPushHandler struct {
	store  ListStore
	writer ResponseWriter
}

func NewRPushHandler(store ListStore, writer ResponseWriter) *RPushHandler {
	return &RPushHandler{store: store, writer: writer}
}

func (h *RPushHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) < 3 {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	if parts[1].Type != BulkString {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	key := parts[1].Value.(string)
	values := make([]string, 0, len(parts)-2)

	for i := 2; i < len(parts); i++ {
		if parts[i].Type != BulkString {
			return h.writer.WriteError(ErrUnknownCommand)
		}
		values = append(values, parts[i].Value.(string))
	}

	length, err := h.store.RPush(key, values...)
	if err != nil {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	return h.writer.WriteInteger(length)
}

// LPopHandler handles LPOP commands
type LPopHandler struct {
	store  ListStore
	writer ResponseWriter
}

func NewLPopHandler(store ListStore, writer ResponseWriter) *LPopHandler {
	return &LPopHandler{store: store, writer: writer}
}

func (h *LPopHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) < 2 || len(parts) > 3 {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	if parts[1].Type != BulkString {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	key := parts[1].Value.(string)
	count := 1

	if len(parts) == 3 {
		if parts[2].Type != BulkString {
			return h.writer.WriteError(ErrUnknownCommand)
		}
		var err error
		count, err = strconv.Atoi(parts[2].Value.(string))
		if err != nil || count <= 0 {
			return h.writer.WriteError("ERR invalid count for LPOP")
		}
	}

	values, exists := h.store.LPop(key, count)
	if !exists || len(values) == 0 {
		if count == 1 {
			return h.writer.WriteNullBulkString()
		}
		return h.writer.WriteEmptyArray()
	}

	if count == 1 {
		return h.writer.WriteBulkString(values[0])
	}

	return h.writer.WriteArray(values)
}

// LRangeHandler handles LRANGE commands
type LRangeHandler struct {
	store  ListStore
	writer ResponseWriter
}

func NewLRangeHandler(store ListStore, writer ResponseWriter) *LRangeHandler {
	return &LRangeHandler{store: store, writer: writer}
}

func (h *LRangeHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) != 4 {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	if parts[1].Type != BulkString || parts[2].Type != BulkString || parts[3].Type != BulkString {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	key := parts[1].Value.(string)
	startStr := parts[2].Value.(string)
	endStr := parts[3].Value.(string)

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	end, err := strconv.Atoi(endStr)
	if err != nil {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	items, exists := h.store.LRange(key, start, end)
	if !exists {
		return h.writer.WriteEmptyArray()
	}

	return h.writer.WriteArray(items)
}

// LLenHandler handles LLEN commands
type LLenHandler struct {
	store  ListStore
	writer ResponseWriter
}

func NewLLenHandler(store ListStore, writer ResponseWriter) *LLenHandler {
	return &LLenHandler{store: store, writer: writer}
}

func (h *LLenHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) != 2 {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	if parts[1].Type != BulkString {
		return h.writer.WriteError(ErrUnknownCommand)
	}

	key := parts[1].Value.(string)
	length, exists := h.store.LLen(key)

	if !exists {
		return h.writer.WriteInteger(0)
	}

	return h.writer.WriteInteger(length)
}

// BLPopHandler handles BLPOP commands
type BLPopHandler struct {
	store  ListStore
	writer ResponseWriter
}

func NewBLPopHandler(store ListStore, writer ResponseWriter) *BLPopHandler {
	return &BLPopHandler{store: store, writer: writer}
}

func (h *BLPopHandler) Handle(parts []RespValue, conn net.Conn) error {
	if len(parts) < 3 {
		return h.writer.WriteError("ERR wrong number of arguments for 'blpop' command")
	}

	// Last argument is timeout
	timeoutStr, ok := parts[len(parts)-1].Value.(string)
	if !ok || parts[len(parts)-1].Type != BulkString {
		return h.writer.WriteError("ERR timeout is not a float or out of range")
	}

	timeoutSeconds, err := strconv.ParseFloat(timeoutStr, 64)
	if err != nil || timeoutSeconds < 0 {
		return h.writer.WriteError("ERR timeout is not a float or out of range")
	}

	// Extract keys
	keys := make([]string, 0, len(parts)-2)
	for i := 1; i < len(parts)-1; i++ {
		if parts[i].Type != BulkString {
			return h.writer.WriteError("ERR wrong number of arguments for 'blpop' command")
		}
		keys = append(keys, parts[i].Value.(string))
	}

	// Try immediate pop
	for _, key := range keys {
		values, exists := h.store.LPop(key)
		if exists && len(values) > 0 {
			result := []string{key, values[0]}
			return h.writer.WriteArray(result)
		}
	}

	// Block with timeout
	if timeoutSeconds == 0 {
		// Block indefinitely
		for {
			for _, key := range keys {
				values, exists := h.store.LPop(key)
				if exists && len(values) > 0 {
					result := []string{key, values[0]}
					return h.writer.WriteArray(result)
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		// Block with timeout
		timeout := time.Duration(timeoutSeconds * float64(time.Second))
		deadline := time.Now().Add(timeout)

		for time.Now().Before(deadline) {
			for _, key := range keys {
				values, exists := h.store.LPop(key)
				if exists && len(values) > 0 {
					result := []string{key, values[0]}
					return h.writer.WriteArray(result)
				}
			}
			time.Sleep(10 * time.Millisecond)
		}

		// Timeout reached
		return h.writer.WriteNullArray()
	}
}
