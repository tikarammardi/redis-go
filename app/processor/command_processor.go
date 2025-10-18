package processor

import (
	"net"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"github.com/codecrafters-io/redis-starter-go/app/store"
)

// CommandProcessor processes Redis commands with improved architecture
type CommandProcessor struct {
	handlers           map[string]CommandHandler
	transactionManager *TransactionManager
	handlerFactory     *HandlerFactory
}

// NewCommandProcessor creates a new command processor
func NewCommandProcessor(kvStore KeyValueStore, listStore ListStore) *CommandProcessor {
	cp := &CommandProcessor{
		handlers:           make(map[string]CommandHandler),
		transactionManager: NewTransactionManager(),
		handlerFactory:     NewHandlerFactory(kvStore, listStore),
	}
	return cp
}

// SetConfig sets the configuration for handlers that need it
func (cp *CommandProcessor) SetConfig(cfg *config.Config) {
	cp.handlerFactory.SetConfig(cfg)
}

// RegisterHandlers registers all command handlers
func (cp *CommandProcessor) RegisterHandlers() {
	handlers := cp.handlerFactory.CreateAllHandlers()
	for cmd, handler := range handlers {
		cp.handlers[cmd] = handler
	}
}

// Process processes a command with improved error handling and transaction support
func (cp *CommandProcessor) Process(command resp.RespValue, conn net.Conn) error {
	if command.Type != resp.ArrayType {
		writer := resp.NewResponseWriter(conn)
		return writer.WriteError("ERR unknown command")
	}

	parts, ok := command.Value.([]resp.RespValue)
	if !ok || len(parts) == 0 {
		writer := resp.NewResponseWriter(conn)
		return writer.WriteError("ERR unknown command")
	}

	cmd, ok := parts[0].Value.(string)
	if !ok {
		writer := resp.NewResponseWriter(conn)
		return writer.WriteError("ERR unknown command")
	}

	cmdUpper := strings.ToUpper(cmd)
	writer := resp.NewResponseWriter(conn)

	// Get handler
	handler, exists := cp.handlers[cmdUpper]
	if !exists {
		return writer.WriteError("ERR unknown command")
	}

	// Update handler writer for this connection
	handler.SetWriter(writer)

	// Handle transaction commands specially
	switch cmdUpper {
	case "MULTI":
		cp.transactionManager.StartTransaction(conn)
		return writer.WriteSimpleString("OK")
	case "EXEC":
		return cp.executeTransaction(conn, writer)
	case "DISCARD":
		return cp.discardTransaction(conn, writer)
	}

	// If in transaction, queue the command
	if cp.transactionManager.IsInTransaction(conn) {
		cp.transactionManager.QueueCommand(conn, parts, handler)
		return writer.WriteSimpleString("QUEUED")
	}

	// Execute command normally
	return handler.Handle(parts, conn)
}

// executeTransaction executes all queued commands in a transaction
func (cp *CommandProcessor) executeTransaction(conn net.Conn, writer *resp.ResponseWriter) error {
	commands, ok := cp.transactionManager.ExecuteTransaction(conn)
	if !ok {
		return writer.WriteError("ERR EXEC without MULTI")
	}

	if len(commands) == 0 {
		return writer.WriteEmptyArray()
	}

	// Execute commands and collect results
	results := make([]resp.RespValue, 0, len(commands))
	for _, queuedCmd := range commands {
		// Create a capturing writer to collect the command's response
		capturingWriter, capturingConn := resp.NewCapturingWriter()

		// Temporarily replace the handler's writer with the capturing writer
		queuedCmd.Handler.SetWriter(capturingWriter)

		// Execute the command with the capturing connection
		err := queuedCmd.Handler.Handle(queuedCmd.Parts, capturingConn)

		if err != nil {
			// If there was an error executing the command, capture it
			results = append(results, resp.RespValue{
				Type:  resp.ErrorType,
				Value: "ERR " + err.Error(),
			})
		} else {
			// Get the captured response
			result := capturingConn.GetCapturedResponse()
			results = append(results, result)
		}
	}

	return writer.WriteTransactionResults(results)
}

// discardTransaction discards the current transaction
func (cp *CommandProcessor) discardTransaction(conn net.Conn, writer *resp.ResponseWriter) error {
	if !cp.transactionManager.DiscardTransaction(conn) {
		return writer.WriteError("ERR DISCARD without MULTI")
	}
	return writer.WriteSimpleString("OK")
}

// CleanupConnection cleans up resources for a connection
func (cp *CommandProcessor) CleanupConnection(conn net.Conn) {
	cp.transactionManager.CleanupConnection(conn)
}

// Interfaces for dependencies - Updated to match existing store implementations
type KeyValueStore interface {
	Set(key, value string, expiry ...time.Duration) error
	Get(key string) (string, bool)
	Delete(key string) error
	GetStreamNotifier() *store.StreamNotifier
}

type ListStore interface {
	LPush(key string, values ...string) (int, error)
	RPush(key string, values ...string) (int, error)
	LPop(key string, count ...int) ([]string, bool)
	LRange(key string, start, end int) ([]string, bool)
	LLen(key string) (int, bool)
}
