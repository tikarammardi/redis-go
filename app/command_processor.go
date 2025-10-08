package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// QueuedCommand represents a command that has been queued during a transaction
type QueuedCommand struct {
	Parts   []RespValue
	Handler CommandHandler
}

// TransactionState tracks the transaction state for each connection
type TransactionState struct {
	InTransaction  bool
	QueuedCommands []QueuedCommand
	mu             sync.Mutex
}

// RedisCommandProcessor implements CommandProcessor interface
type RedisCommandProcessor struct {
	handlers map[string]CommandHandler
	// Track transaction state per connection
	transactionStates map[net.Conn]*TransactionState
	transactionMu     sync.RWMutex
}

// NewRedisCommandProcessor creates a new command processor with dependency injection
func NewRedisCommandProcessor(kvStore KeyValueStore, listStore ListStore) *RedisCommandProcessor {
	processor := &RedisCommandProcessor{
		handlers:          make(map[string]CommandHandler),
		transactionStates: make(map[net.Conn]*TransactionState),
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
	p.handlers["XREAD"] = NewXReadHandler(kvStore, writer)
	p.handlers["INCR"] = NewIncrHandler(kvStore, writer)
	p.handlers["MULTI"] = NewMultiHandler(kvStore, writer)
	p.handlers["EXEC"] = NewExecHandler(kvStore, writer)
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

	// Check if connection is in transaction mode
	p.transactionMu.RLock()
	transState, inTransaction := p.transactionStates[conn]
	p.transactionMu.RUnlock()

	// If in transaction mode and command is not EXEC or MULTI, queue the command
	if inTransaction && transState.InTransaction && cmdUpper != "EXEC" && cmdUpper != "MULTI" {
		transState.mu.Lock()
		transState.QueuedCommands = append(transState.QueuedCommands, QueuedCommand{
			Parts:   parts,
			Handler: handler,
		})
		transState.mu.Unlock()

		// Return QUEUED response
		return writer.WriteSimpleString("QUEUED")
	}

	fmt.Printf("Processing command: %s with %d parts\n", cmdUpper, len(parts))

	// Special handling for MULTI and EXEC commands
	if cmdUpper == "MULTI" {
		p.startTransaction(conn)
	} else if cmdUpper == "EXEC" {
		return p.executeTransaction(conn, writer)
	}

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
	case *XReadHandler:
		h.writer = writer
	case *IncrHandler:
		h.writer = writer
	case *MultiHandler:
		h.writer = writer
	case *ExecHandler:
		h.writer = writer
	}
}

// startTransaction begins a transaction for the given connection
func (p *RedisCommandProcessor) startTransaction(conn net.Conn) {
	p.transactionMu.Lock()
	defer p.transactionMu.Unlock()

	if p.transactionStates[conn] == nil {
		p.transactionStates[conn] = &TransactionState{}
	}

	p.transactionStates[conn].InTransaction = true
	p.transactionStates[conn].QueuedCommands = nil // Clear any existing commands
}

// executeTransaction executes all queued commands for the given connection
func (p *RedisCommandProcessor) executeTransaction(conn net.Conn, writer ResponseWriter) error {
	p.transactionMu.RLock()
	transState, exists := p.transactionStates[conn]
	p.transactionMu.RUnlock()

	if !exists || !transState.InTransaction {
		return writer.WriteError("ERR EXEC without MULTI")
	}

	transState.mu.Lock()
	commands := transState.QueuedCommands
	transState.InTransaction = false
	transState.QueuedCommands = nil
	transState.mu.Unlock()

	// If no commands were queued, return empty array
	if len(commands) == 0 {
		return writer.WriteEmptyArray()
	}

	// Execute all queued commands and collect results
	results := make([]string, 0, len(commands))

	for _, queuedCmd := range commands {
		// Update handler writer for this execution
		p.updateHandlerWriter(queuedCmd.Handler, writer)

		// Execute the command and capture the result
		// For now, we'll execute the command normally
		// In a real implementation, we'd capture the output
		err := queuedCmd.Handler.Handle(queuedCmd.Parts, conn)
		if err != nil {
			results = append(results, "ERR "+err.Error())
		} else {
			results = append(results, "OK") // Simplified result
		}
	}

	// Write array of results
	return writer.WriteArray(results)
}

// CleanupConnection removes transaction state when connection closes
func (p *RedisCommandProcessor) CleanupConnection(conn net.Conn) {
	p.transactionMu.Lock()
	defer p.transactionMu.Unlock()
	delete(p.transactionStates, conn)
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
