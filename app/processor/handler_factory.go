package processor

import (
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/handlers/basic"
	"github.com/codecrafters-io/redis-starter-go/app/handlers/keyvalue"
	"github.com/codecrafters-io/redis-starter-go/app/handlers/list"
	"github.com/codecrafters-io/redis-starter-go/app/handlers/stream"
	"github.com/codecrafters-io/redis-starter-go/app/handlers/transaction"
)

// HandlerFactory creates command handlers with proper dependency injection
type HandlerFactory struct {
	kvStore   KeyValueStore
	listStore ListStore
	config    *config.Config
}

// NewHandlerFactory creates a new handler factory
func NewHandlerFactory(kvStore KeyValueStore, listStore ListStore) *HandlerFactory {
	return &HandlerFactory{
		kvStore:   kvStore,
		listStore: listStore,
	}
}

// SetConfig sets the configuration for handlers that need it
func (hf *HandlerFactory) SetConfig(cfg *config.Config) {
	hf.config = cfg
}

// CreateAllHandlers creates all command handlers
func (hf *HandlerFactory) CreateAllHandlers() map[string]CommandHandler {
	handlers := make(map[string]CommandHandler)

	// Basic commands
	handlers["PING"] = basic.NewPingHandler()
	handlers["ECHO"] = basic.NewEchoHandler()
	if hf.config != nil {
		handlers["INFO"] = basic.NewInfoHandler(hf.config)
	}

	// Key-value commands
	handlers["SET"] = keyvalue.NewSetHandler(hf.kvStore)
	handlers["GET"] = keyvalue.NewGetHandler(hf.kvStore)
	handlers["INCR"] = keyvalue.NewIncrHandler(hf.kvStore)
	handlers["TYPE"] = keyvalue.NewTypeHandler(hf.kvStore, hf.listStore)

	// List commands
	handlers["LPUSH"] = list.NewLPushHandler(hf.listStore)
	handlers["RPUSH"] = list.NewRPushHandler(hf.listStore)
	handlers["LPOP"] = list.NewLPopHandler(hf.listStore)
	handlers["LRANGE"] = list.NewLRangeHandler(hf.listStore)
	handlers["LLEN"] = list.NewLLenHandler(hf.listStore)
	handlers["BLPOP"] = list.NewBLPopHandler(hf.listStore)

	// Transaction commands (these are handled specially in the processor)
	handlers["MULTI"] = transaction.NewMultiHandler()
	handlers["EXEC"] = transaction.NewExecHandler()
	handlers["DISCARD"] = transaction.NewDiscardHandler()

	// Stream commands
	handlers["XADD"] = stream.NewXAddHandler(hf.kvStore)
	handlers["XRANGE"] = stream.NewXRangeHandler(hf.kvStore)
	handlers["XREAD"] = stream.NewXReadHandler(hf.kvStore)

	return handlers
}
