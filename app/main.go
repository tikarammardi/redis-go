package main

import (
	"fmt"
	"github.com/codecrafters-io/redis-starter-go/app/config"
	"github.com/codecrafters-io/redis-starter-go/app/processor"
	"github.com/codecrafters-io/redis-starter-go/app/server"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	// Load configuration
	cfg := config.NewConfig()

	// Create stores
	kvStore := NewInMemoryKeyValueStore()
	listStore := NewInMemoryListStore()

	// Create command processor with improved dependency injection
	commandProcessor := processor.NewCommandProcessor(kvStore, listStore)
	commandProcessor.SetConfig(cfg)
	commandProcessor.RegisterHandlers()

	// Create and start the server
	redisServer := server.NewServer(commandProcessor, cfg)

	err := redisServer.Start()
	if err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
}
