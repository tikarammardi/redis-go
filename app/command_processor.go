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
	results := make([]RespValue, 0, len(commands))

	for _, queuedCmd := range commands {
		// Create a response capture writer to capture the command output
		captureWriter := NewResponseCapture()

		// Update handler writer to capture response
		p.updateHandlerWriter(queuedCmd.Handler, captureWriter)

		// Execute the command
		err := queuedCmd.Handler.Handle(queuedCmd.Parts, conn)
		if err != nil {
			// If there's an error, add error response to results
			results = append(results, RespValue{
				Type:  ErrorType,
				Value: "ERR " + err.Error(),
			})
		} else {
			// Add the captured response to results
			results = append(results, captureWriter.GetResponse())
		}
	}

	// Write array of captured responses
	return writer.WriteTransactionResults(results)
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

// ResponseCapture captures command responses for transaction execution
type ResponseCapture struct {
	response RespValue
}

func NewResponseCapture() *ResponseCapture {
	return &ResponseCapture{}
}

func (r *ResponseCapture) WriteSimpleString(s string) error {
	r.response = RespValue{Type: SimpleString, Value: s}
	return nil
}

func (r *ResponseCapture) WriteBulkString(s string) error {
	r.response = RespValue{Type: BulkString, Value: s}
	return nil
}

func (r *ResponseCapture) WriteInteger(i int) error {
	r.response = RespValue{Type: IntegerType, Value: i}
	return nil
}

func (r *ResponseCapture) WriteError(err string) error {
	r.response = RespValue{Type: ErrorType, Value: err}
	return nil
}

func (r *ResponseCapture) WriteArray(items []string) error {
	arrayItems := make([]RespValue, len(items))
	for i, item := range items {
		arrayItems[i] = RespValue{Type: BulkString, Value: item}
	}
	r.response = RespValue{Type: ArrayType, Value: arrayItems}
	return nil
}

func (r *ResponseCapture) WriteNullBulkString() error {
	r.response = RespValue{Type: BulkString, Value: nil}
	return nil
}

func (r *ResponseCapture) WriteNullArray() error {
	r.response = RespValue{Type: ArrayType, Value: nil}
	return nil
}

func (r *ResponseCapture) WriteEmptyArray() error {
	r.response = RespValue{Type: ArrayType, Value: []RespValue{}}
	return nil
}

func (r *ResponseCapture) WriteStreamEntries(entries []StreamEntry) error {
	// Convert stream entries to RespValue format
	arrayItems := make([]RespValue, len(entries))
	for i, entry := range entries {
		// Each entry is [id, [field1, value1, field2, value2, ...]]
		entryArray := make([]RespValue, 2)
		entryArray[0] = RespValue{Type: BulkString, Value: entry.ID}

		// Create field-value pairs array
		fieldCount := len(entry.Fields) * 2
		fieldArray := make([]RespValue, fieldCount)
		j := 0
		for field, value := range entry.Fields {
			fieldArray[j] = RespValue{Type: BulkString, Value: field}
			fieldArray[j+1] = RespValue{Type: BulkString, Value: value}
			j += 2
		}
		entryArray[1] = RespValue{Type: ArrayType, Value: fieldArray}
		arrayItems[i] = RespValue{Type: ArrayType, Value: entryArray}
	}
	r.response = RespValue{Type: ArrayType, Value: arrayItems}
	return nil
}

func (r *ResponseCapture) WriteStreamReadResults(results []StreamReadResult) error {
	// Convert stream read results to RespValue format
	arrayItems := make([]RespValue, len(results))
	for i, result := range results {
		// Each result is [key, [[id1, [field1, value1, ...]], [id2, [field2, value2, ...]], ...]]
		resultArray := make([]RespValue, 2)
		resultArray[0] = RespValue{Type: BulkString, Value: result.Key}

		// Create entries array
		entriesArray := make([]RespValue, len(result.Entries))
		for j, entry := range result.Entries {
			entryArray := make([]RespValue, 2)
			entryArray[0] = RespValue{Type: BulkString, Value: entry.ID}

			fieldCount := len(entry.Fields) * 2
			fieldArray := make([]RespValue, fieldCount)
			k := 0
			for field, value := range entry.Fields {
				fieldArray[k] = RespValue{Type: BulkString, Value: field}
				fieldArray[k+1] = RespValue{Type: BulkString, Value: value}
				k += 2
			}
			entryArray[1] = RespValue{Type: ArrayType, Value: fieldArray}
			entriesArray[j] = RespValue{Type: ArrayType, Value: entryArray}
		}
		resultArray[1] = RespValue{Type: ArrayType, Value: entriesArray}
		arrayItems[i] = RespValue{Type: ArrayType, Value: resultArray}
	}
	r.response = RespValue{Type: ArrayType, Value: arrayItems}
	return nil
}

func (r *ResponseCapture) WriteTransactionResults(results []RespValue) error {
	// This method is not needed for ResponseCapture since it's used to capture individual responses
	// The transaction results are handled by the actual ResponseWriter
	return nil
}

func (r *ResponseCapture) GetResponse() RespValue {
	return r.response
}
