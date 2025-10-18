package processor

import (
	"github.com/codecrafters-io/redis-starter-go/app/resp"
	"net"
	"sync"
)

// QueuedCommand represents a command queued during a transaction
type QueuedCommand struct {
	Parts   []resp.RespValue
	Handler CommandHandler
}

// TransactionState tracks the transaction state for a connection
type TransactionState struct {
	InTransaction  bool
	QueuedCommands []QueuedCommand
	mu             sync.Mutex
}

// TransactionManager manages transaction state for connections
type TransactionManager struct {
	states map[net.Conn]*TransactionState
	mu     sync.RWMutex
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		states: make(map[net.Conn]*TransactionState),
	}
}

// StartTransaction begins a transaction for the given connection
func (tm *TransactionManager) StartTransaction(conn net.Conn) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.states[conn] == nil {
		tm.states[conn] = &TransactionState{}
	}

	tm.states[conn].InTransaction = true
	tm.states[conn].QueuedCommands = nil // Clear any existing commands
}

// IsInTransaction checks if a connection is in a transaction
func (tm *TransactionManager) IsInTransaction(conn net.Conn) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	state, exists := tm.states[conn]
	return exists && state.InTransaction
}

// QueueCommand adds a command to the transaction queue
func (tm *TransactionManager) QueueCommand(conn net.Conn, parts []resp.RespValue, handler CommandHandler) {
	tm.mu.RLock()
	state, exists := tm.states[conn]
	tm.mu.RUnlock()

	if !exists || !state.InTransaction {
		return
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	state.QueuedCommands = append(state.QueuedCommands, QueuedCommand{
		Parts:   parts,
		Handler: handler,
	})
}

// ExecuteTransaction executes all queued commands and returns results
func (tm *TransactionManager) ExecuteTransaction(conn net.Conn) ([]QueuedCommand, bool) {
	tm.mu.RLock()
	state, exists := tm.states[conn]
	tm.mu.RUnlock()

	if !exists || !state.InTransaction {
		return nil, false
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	commands := make([]QueuedCommand, len(state.QueuedCommands))
	copy(commands, state.QueuedCommands)

	state.InTransaction = false
	state.QueuedCommands = nil

	return commands, true
}

// DiscardTransaction discards the current transaction
func (tm *TransactionManager) DiscardTransaction(conn net.Conn) bool {
	tm.mu.RLock()
	state, exists := tm.states[conn]
	tm.mu.RUnlock()

	if !exists || !state.InTransaction {
		return false
	}

	state.mu.Lock()
	defer state.mu.Unlock()

	state.InTransaction = false
	state.QueuedCommands = nil

	return true
}

// CleanupConnection removes transaction state for a connection
func (tm *TransactionManager) CleanupConnection(conn net.Conn) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.states, conn)
}

// CommandHandler interface for handling commands
type CommandHandler interface {
	Handle(parts []resp.RespValue, conn net.Conn) error
	SetWriter(writer *resp.ResponseWriter)
}
