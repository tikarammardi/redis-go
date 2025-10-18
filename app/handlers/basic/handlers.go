package basic

import (
	"net"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

// PingHandler handles PING commands
type PingHandler struct {
	writer *resp.ResponseWriter
}

// NewPingHandler creates a new PING handler
func NewPingHandler() *PingHandler {
	return &PingHandler{}
}

// Handle processes the PING command
func (h *PingHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) == 1 {
		return h.writer.WriteSimpleString("PONG")
	}
	if len(parts) == 2 {
		if msg, ok := parts[1].Value.(string); ok {
			return h.writer.WriteSimpleString(msg)
		}
	}
	return h.writer.WriteError("ERR wrong number of arguments for 'ping' command")
}

// SetWriter sets the response writer for this handler
func (h *PingHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// EchoHandler handles ECHO commands
type EchoHandler struct {
	writer *resp.ResponseWriter
}

// NewEchoHandler creates a new ECHO handler
func NewEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// Handle processes the ECHO command
func (h *EchoHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	if len(parts) != 2 {
		return h.writer.WriteError("ERR wrong number of arguments for 'echo' command")
	}

	if msg, ok := parts[1].Value.(string); ok {
		return h.writer.WriteBulkString(msg)
	}

	return h.writer.WriteError("ERR invalid argument type")
}

// SetWriter sets the response writer for this handler
func (h *EchoHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}

// InfoHandler handles INFO commands
type InfoHandler struct {
	writer *resp.ResponseWriter
	config ServerConfig
}

// ServerConfig interface for server configuration
type ServerConfig interface {
	GetServerInfo() map[string]string
}

// NewInfoHandler creates a new INFO handler
func NewInfoHandler(config ServerConfig) *InfoHandler {
	return &InfoHandler{
		config: config,
	}
}

// Handle processes the INFO command
func (h *InfoHandler) Handle(parts []resp.RespValue, conn net.Conn) error {
	info := h.config.GetServerInfo()

	var infoString string
	for key, value := range info {
		infoString += key + ":" + value + "\r\n"
	}

	return h.writer.WriteBulkString(infoString)
}

// SetWriter sets the response writer for this handler
func (h *InfoHandler) SetWriter(writer *resp.ResponseWriter) {
	h.writer = writer
}
