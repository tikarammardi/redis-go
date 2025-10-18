package transaction

import (
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"net"
)

// MultiHandler handles MULTI commands
type MultiHandler struct {
	writer *resp.ResponseWriter
}

// NewMultiHandler creates a new MULTI handler
func NewMultiHandler() *MultiHandler {
	return &MultiHandler{}
}

// Handle processes the MULTI command
func (h *MultiHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 1 {
		return h.writer.WriteError("ERR wrong number of arguments for 'multi' command")
	}
	return h.writer.WriteSimpleString("OK")
}

// SetWriter sets the response writer for this handler
func (h *MultiHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// ExecHandler handles EXEC commands
type ExecHandler struct {
	writer *resp.ResponseWriter
}

// NewExecHandler creates a new EXEC handler
func NewExecHandler() *ExecHandler {
	return &ExecHandler{}
}

// Handle processes the EXEC command (actual logic is in command processor)
func (h *ExecHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 1 {
		return h.writer.WriteError("ERR wrong number of arguments for 'exec' command")
	}
	// This should not be reached as EXEC is handled specially in the processor
	return h.writer.WriteError("ERR EXEC without MULTI")
}

// SetWriter sets the response writer for this handler
func (h *ExecHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// DiscardHandler handles DISCARD commands
type DiscardHandler struct {
	writer *resp.ResponseWriter
}

// NewDiscardHandler creates a new DISCARD handler
func NewDiscardHandler() *DiscardHandler {
	return &DiscardHandler{}
}

// Handle processes the DISCARD command (actual logic is in command processor)
func (h *DiscardHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 1 {
		return h.writer.WriteError("ERR wrong number of arguments for 'discard' command")
	}
	// This should not be reached as DISCARD is handled specially in the processor
	return h.writer.WriteError("ERR DISCARD without MULTI")
}

// SetWriter sets the response writer for this handler
func (h *DiscardHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}
