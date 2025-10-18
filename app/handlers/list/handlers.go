package list

import (
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"net"
	"strconv"
	"time"
)

// LPushHandler handles LPUSH commands
type LPushHandler struct {
	writer *resp.ResponseWriter
	store  ListStore
}

// NewLPushHandler creates a new LPUSH handler
func NewLPushHandler(store ListStore) *LPushHandler {
	return &LPushHandler{store: store}
}

// Handle processes the LPUSH command
func (h *LPushHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) < 3 {
		return h.writer.WriteError("ERR wrong number of arguments for 'lpush' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	values := make([]string, 0, len(parts)-2)
	for i := 2; i < len(parts); i++ {
		if val, ok := parts[i].Value.(string); ok {
			values = append(values, val)
		} else {
			return h.writer.WriteError("ERR invalid value type")
		}
	}

	length, err := h.store.LPush(key, values...)
	if err != nil {
		return h.writer.WriteError("ERR " + err.Error())
	}

	return h.writer.WriteInteger(length)
}

// SetWriter sets the response writer for this handler
func (h *LPushHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// RPushHandler handles RPUSH commands
type RPushHandler struct {
	writer *resp.ResponseWriter
	store  ListStore
}

// NewRPushHandler creates a new RPUSH handler
func NewRPushHandler(store ListStore) *RPushHandler {
	return &RPushHandler{store: store}
}

// Handle processes the RPUSH command
func (h *RPushHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) < 3 {
		return h.writer.WriteError("ERR wrong number of arguments for 'rpush' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	values := make([]string, 0, len(parts)-2)
	for i := 2; i < len(parts); i++ {
		if val, ok := parts[i].Value.(string); ok {
			values = append(values, val)
		} else {
			return h.writer.WriteError("ERR invalid value type")
		}
	}

	length, err := h.store.RPush(key, values...)
	if err != nil {
		return h.writer.WriteError("ERR " + err.Error())
	}

	return h.writer.WriteInteger(length)
}

// SetWriter sets the response writer for this handler
func (h *RPushHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// LPopHandler handles LPOP commands
type LPopHandler struct {
	writer *resp.ResponseWriter
	store  ListStore
}

// NewLPopHandler creates a new LPOP handler
func NewLPopHandler(store ListStore) *LPopHandler {
	return &LPopHandler{store: store}
}

// Handle processes the LPOP command
func (h *LPopHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) < 2 || len(parts) > 3 {
		return h.writer.WriteError("ERR wrong number of arguments for 'lpop' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	count := 1
	if len(parts) == 3 {
		if countStr, ok := parts[2].Value.(string); ok {
			var err error
			count, err = strconv.Atoi(countStr)
			if err != nil || count < 0 {
				return h.writer.WriteError("ERR value is not an integer or out of range")
			}
		} else {
			return h.writer.WriteError("ERR invalid count type")
		}
	}

	values, exists := h.store.LPop(key, count)
	if !exists {
		return h.writer.WriteNullBulkString()
	}

	if len(parts) == 2 && len(values) > 0 {
		return h.writer.WriteBulkString(values[0])
	}

	return h.writer.WriteArray(values)
}

// SetWriter sets the response writer for this handler
func (h *LPopHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// LRangeHandler handles LRANGE commands
type LRangeHandler struct {
	writer *resp.ResponseWriter
	store  ListStore
}

// NewLRangeHandler creates a new LRANGE handler
func NewLRangeHandler(store ListStore) *LRangeHandler {
	return &LRangeHandler{store: store}
}

// Handle processes the LRANGE command
func (h *LRangeHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 4 {
		return h.writer.WriteError("ERR wrong number of arguments for 'lrange' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	startStr, ok := parts[2].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid start index type")
	}

	endStr, ok := parts[3].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid end index type")
	}

	start, err := strconv.Atoi(startStr)
	if err != nil {
		return h.writer.WriteError("ERR value is not an integer or out of range")
	}

	end, err := strconv.Atoi(endStr)
	if err != nil {
		return h.writer.WriteError("ERR value is not an integer or out of range")
	}

	values, exists := h.store.LRange(key, start, end)
	if !exists {
		return h.writer.WriteArray([]string{})
	}

	return h.writer.WriteArray(values)
}

// SetWriter sets the response writer for this handler
func (h *LRangeHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// LLenHandler handles LLEN commands
type LLenHandler struct {
	writer *resp.ResponseWriter
	store  ListStore
}

// NewLLenHandler creates a new LLEN handler
func NewLLenHandler(store ListStore) *LLenHandler {
	return &LLenHandler{store: store}
}

// Handle processes the LLEN command
func (h *LLenHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 2 {
		return h.writer.WriteError("ERR wrong number of arguments for 'llen' command")
	}

	key, ok := parts[1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR invalid key type")
	}

	length, exists := h.store.LLen(key)
	if !exists {
		return h.writer.WriteInteger(0)
	}

	return h.writer.WriteInteger(length)
}

// SetWriter sets the response writer for this handler
func (h *LLenHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// BLPopHandler handles BLPOP commands
type BLPopHandler struct {
	writer *resp.ResponseWriter
	store  ListStore
}

// NewBLPopHandler creates a new BLPOP handler
func NewBLPopHandler(store ListStore) *BLPopHandler {
	return &BLPopHandler{store: store}
}

// Handle processes the BLPOP command
func (h *BLPopHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) < 3 {
		return h.writer.WriteError("ERR wrong number of arguments for 'blpop' command")
	}

	// Last argument is timeout
	timeoutStr, ok := parts[len(parts)-1].Value.(string)
	if !ok {
		return h.writer.WriteError("ERR timeout is not a float or out of range")
	}

	timeoutSeconds, err := strconv.ParseFloat(timeoutStr, 64)
	if err != nil || timeoutSeconds < 0 {
		return h.writer.WriteError("ERR timeout is not a float or out of range")
	}

	// Extract keys
	keys := make([]string, 0, len(parts)-2)
	for i := 1; i < len(parts)-1; i++ {
		if key, ok := parts[i].Value.(string); ok {
			keys = append(keys, key)
		} else {
			return h.writer.WriteError("ERR wrong number of arguments for 'blpop' command")
		}
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

// SetWriter sets the response writer for this handler
func (h *BLPopHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// Common interfaces and types
type ListStore interface {
	LPush(key string, values ...string) (int, error)
	RPush(key string, values ...string) (int, error)
	LPop(key string, count ...int) ([]string, bool)
	LRange(key string, start, end int) ([]string, bool)
	LLen(key string) (int, bool)
}
