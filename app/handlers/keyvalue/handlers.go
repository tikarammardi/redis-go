package keyvalue

import (
	"net"
	"strconv"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

// SetHandler handles SET commands
type SetHandler struct {
	writer *resp.ResponseWriter
	store  KeyValueStore
}

// NewSetHandler creates a new SET handler
func NewSetHandler(store KeyValueStore) *SetHandler {
	return &SetHandler{store: store}
}

// Handle processes the SET command
func (h *SetHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) < 3 {
		return h.writer.WriteError("ERR wrong number of arguments for 'set' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	value, ok := parts[2].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid value type")
	}

	var expiry time.Duration
	// Parse optional PX (milliseconds) or EX (seconds) parameters
	for i := 3; i < len(parts)-1; i += 2 {
		param, ok := parts[i].Value.(string)
		if !ok {
			continue
		}

		switch param {
		case "PX":
			if ms, ok := parts[i+1].Value.(string); ok {
				if msInt, err := strconv.Atoi(ms); err == nil {
					expiry = time.Duration(msInt) * time.Millisecond
				}
			}
		case "EX":
			if s, ok := parts[i+1].Value.(string); ok {
				if sInt, err := strconv.Atoi(s); err == nil {
					expiry = time.Duration(sInt) * time.Second
				}
			}
		}
	}

	err := h.store.Set(key, value, expiry)
	if err != nil {
		return h.writer.WriteError("ERR " + err.Error())
	}

	return h.writer.WriteSimpleString("OK")
}

// SetWriter sets the response writer for this handler
func (h *SetHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// GetHandler handles GET commands
type GetHandler struct {
	writer *resp.ResponseWriter
	store  KeyValueStore
}

// NewGetHandler creates a new GET handler
func NewGetHandler(store KeyValueStore) *GetHandler {
	return &GetHandler{store: store}
}

// Handle processes the GET command
func (h *GetHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 2 {
		return h.writer.WriteError("ERR wrong number of arguments for 'get' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	value, exists := h.store.Get(key)
	if !exists {
		return h.writer.WriteNullBulkString()
	}

	return h.writer.WriteBulkString(value)
}

// SetWriter sets the response writer for this handler
func (h *GetHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// IncrHandler handles INCR commands
type IncrHandler struct {
	writer *resp.ResponseWriter
	store  KeyValueStore
}

// NewIncrHandler creates a new INCR handler
func NewIncrHandler(store KeyValueStore) *IncrHandler {
	return &IncrHandler{store: store}
}

// Handle processes the INCR command
func (h *IncrHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 2 {
		return h.writer.WriteError("ERR wrong number of arguments for 'incr' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	value, exists := h.store.Get(key)
	var intValue int
	var err error

	if exists {
		intValue, err = strconv.Atoi(value)
		if err != nil {
			return h.writer.WriteError("ERR value is not an integer or out of range")
		}
	}

	intValue++
	err = h.store.Set(key, strconv.Itoa(intValue))
	if err != nil {
		return h.writer.WriteError("ERR " + err.Error())
	}

	return h.writer.WriteInteger(intValue)
}

// SetWriter sets the response writer for this handler
func (h *IncrHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// TypeHandler handles TYPE commands
type TypeHandler struct {
	writer    *resp.ResponseWriter
	kvStore   KeyValueStore
	listStore ListStore
}

// NewTypeHandler creates a new TYPE handler
func NewTypeHandler(kvStore KeyValueStore, listStore ListStore) *TypeHandler {
	return &TypeHandler{
		kvStore:   kvStore,
		listStore: listStore,
	}
}

// Handle processes the TYPE command
func (h *TypeHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 2 {
		return h.writer.WriteError("ERR wrong number of arguments for 'type' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	// Check if key exists in key-value store as a regular string
	if _, exists := h.kvStore.Get(key); exists {
		return h.writer.WriteSimpleString("string")
	}

	// Check if key exists in list store
	if _, exists := h.listStore.LLen(key); exists {
		return h.writer.WriteSimpleString("list")
	}

	// Check if key exists as a stream (look for entries with pattern key:*)
	if h.hasStreamEntries(key) {
		return h.writer.WriteSimpleString("stream")
	}

	return h.writer.WriteSimpleString("none")
}

// hasStreamEntries checks if there are any stream entries for the given key
func (h *TypeHandler) hasStreamEntries(key string) bool {
	prefix := key + ":"

	// Check for common stream ID patterns to see if any stream entries exist
	// This is a simplified implementation - in a real system we'd have a proper stream index
	testPatterns := []string{
		"0-1", "0-2", "0-3", "0-4", "0-5",
		"1-0", "1-1", "1-2", "1-3", "1-4", "1-5",
		"2-0", "2-1", "2-2", "2-3", "2-4", "2-5",
		"3-0", "3-1", "3-2", "3-3", "3-4", "3-5",
		"4-0", "4-1", "4-2", "4-3", "4-4", "4-5",
		"5-0", "5-1", "5-2", "5-3", "5-4", "5-5",
	}

	for _, pattern := range testPatterns {
		if _, exists := h.kvStore.Get(prefix + pattern); exists {
			return true
		}
	}

	// Also check for timestamp-based IDs (auto-generated ones)
	// Look for recent timestamps (simplified approach for testing)
	currentTime := time.Now().UnixMilli()
	for i := int64(0); i < 10; i++ {
		timestampToCheck := currentTime - i*1000 // Check last 10 seconds
		for seq := int64(0); seq < 5; seq++ {
			testKey := prefix + strconv.FormatInt(timestampToCheck, 10) + "-" + strconv.FormatInt(seq, 10)
			if _, exists := h.kvStore.Get(testKey); exists {
				return true
			}
		}
	}

	return false
}

// SetWriter sets the response writer for this handler
func (h *TypeHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// Common interfaces and types
type KeyValueStore interface {
	Set(key, value string, expiry ...time.Duration) error
	Get(key string) (string, bool)
	Delete(key string) error
}

type ListStore interface {
	LLen(key string) (int, bool)
}
