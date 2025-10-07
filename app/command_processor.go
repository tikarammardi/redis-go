package main

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// RedisCommandProcessor implements CommandProcessor interface
type RedisCommandProcessor struct {
	handlers map[string]CommandHandler
}

// NewRedisCommandProcessor creates a new command processor with dependency injection
func NewRedisCommandProcessor(kvStore KeyValueStore, listStore ListStore) *RedisCommandProcessor {
	processor := &RedisCommandProcessor{
		handlers: make(map[string]CommandHandler),
	}

	// Register command handlers - following Open-Closed Principle
	// New commands can be added without modifying existing code
	processor.registerHandlers(kvStore, listStore)

	return processor
}

func (p *RedisCommandProcessor) registerHandlers(kvStore KeyValueStore, listStore ListStore) {
	// Create a dummy connection to get the writer - this will be replaced per connection
	// This is a temporary solution; in a real implementation, we'd pass the writer differently
	dummyConn := &dummyConnection{}
	writer := NewRespResponseWriter(dummyConn)

	p.handlers["PING"] = NewPingHandler(writer)
	p.handlers["ECHO"] = NewEchoHandler(writer)
	p.handlers["SET"] = NewSetHandler(kvStore, writer)
	p.handlers["GET"] = NewGetHandler(kvStore, writer)
	p.handlers["LPUSH"] = NewLPushHandler(listStore, writer)
	p.handlers["RPUSH"] = NewRPushHandler(listStore, writer)
	p.handlers["LPOP"] = NewLPopHandler(listStore, writer)
	p.handlers["LRANGE"] = NewLRangeHandler(listStore, writer)
	p.handlers["LLEN"] = NewLLenHandler(listStore, writer)
	p.handlers["BLPOP"] = NewBLPopHandler(listStore, writer)
	p.handlers["TYPE"] = NewTypeHandler(kvStore, listStore, writer)
	p.handlers["XADD"] = NewXAddHandler(kvStore, writer)
	p.handlers["XRANGE"] = NewXRangeHandler(kvStore, writer)
}

func (p *RedisCommandProcessor) Process(command RespValue, conn net.Conn) error {
	if command.Type != ArrayType {
		writer := NewRespResponseWriter(conn)
		return writer.WriteError("ERR unknown command")
	}

	parts, ok := command.Value.([]RespValue)
	if !ok || len(parts) == 0 {
		writer := NewRespResponseWriter(conn)
		return writer.WriteError("ERR unknown command")
	}

	cmd, ok := parts[0].Value.(string)
	if !ok {
		writer := NewRespResponseWriter(conn)
		return writer.WriteError("ERR unknown command")
	}

	cmdUpper := strings.ToUpper(cmd)

	// Create a new writer for this connection
	writer := NewRespResponseWriter(conn)

	// Get the handler and update its writer
	handler, exists := p.handlers[cmdUpper]
	if !exists {
		return writer.WriteError("ERR unknown command")
	}

	// Update the handler's writer for this connection
	p.updateHandlerWriter(handler, writer)

	fmt.Printf("Processing command: %s with %d parts\n", cmdUpper, len(parts))
	return handler.Handle(parts, conn)
}

// updateHandlerWriter updates the response writer for the handler
func (p *RedisCommandProcessor) updateHandlerWriter(handler CommandHandler, writer ResponseWriter) {
	switch h := handler.(type) {
	case *PingHandler:
		h.writer = writer
	case *EchoHandler:
		h.writer = writer
	case *SetHandler:
		h.writer = writer
	case *GetHandler:
		h.writer = writer
	case *LPushHandler:
		h.writer = writer
	case *RPushHandler:
		h.writer = writer
	case *LPopHandler:
		h.writer = writer
	case *LRangeHandler:
		h.writer = writer
	case *LLenHandler:
		h.writer = writer
	case *BLPopHandler:
		h.writer = writer
	case *TypeHandler:
		h.writer = writer
	case *XAddHandler:
		h.writer = writer
	case *XRangeHandler:
		h.writer = writer
	}
}

// dummyConnection is a placeholder for handler registration
type dummyConnection struct{}

func (d *dummyConnection) Read(b []byte) (n int, err error)   { return 0, nil }
func (d *dummyConnection) Write(b []byte) (n int, err error)  { return len(b), nil }
func (d *dummyConnection) Close() error                       { return nil }
func (d *dummyConnection) LocalAddr() net.Addr                { return nil }
func (d *dummyConnection) RemoteAddr() net.Addr               { return nil }
func (d *dummyConnection) SetDeadline(t time.Time) error      { return nil }
func (d *dummyConnection) SetReadDeadline(t time.Time) error  { return nil }
func (d *dummyConnection) SetWriteDeadline(t time.Time) error { return nil }
