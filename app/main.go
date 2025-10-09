package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// RedisServer implements Server interface
type RedisServer struct {
	processor CommandProcessor
	listener  net.Listener
}

// NewRedisServer creates a new Redis server with dependency injection
func NewRedisServer(port int) *RedisServer {
	// Create stores
	kvStore := NewInMemoryKeyValueStore()
	listStore := NewInMemoryListStore()

	// Create command processor with dependency injection
	processor := NewRedisCommandProcessor(kvStore, listStore)

	// Configure the port for handlers that need it
	processor.SetPort(port)

	return &RedisServer{
		processor: processor,
	}
}

func (s *RedisServer) Start(address string) error {
	fmt.Println("Starting Redis server on", address)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to bind to %s: %w", address, err)
	}

	s.listener = listener

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		// Handle each connection in a separate goroutine
		go s.handleConnection(conn)
	}
}

func (s *RedisServer) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *RedisServer) handleConnection(conn net.Conn) {
	defer func() {
		// Clean up transaction state when connection closes
		if processor, ok := s.processor.(*RedisCommandProcessor); ok {
			processor.CleanupConnection(conn)
		}
		conn.Close()
	}()

	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("Error reading from connection: %v\n", err)
			return
		}

		request := string(buf[:n])
		r := bufio.NewReader(strings.NewReader(request))

		command, err := ParseRESP(r)
		if err != nil {
			writer := NewRespResponseWriter(conn)
			writer.WriteError("ERR unknown command")
			continue
		}

		fmt.Printf("CommandType: %v, Value: %v\n", command.Type, command.Value)

		// Process command using the command processor
		err = s.processor.Process(command, conn)
		if err != nil {
			fmt.Printf("Error processing command: %v\n", err)
		}
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	// Parse command line flags
	var port int
	flag.IntVar(&port, "port", 6379, "Port to bind the Redis server to")
	flag.Parse()

	// Create and start the server
	server := NewRedisServer(port)

	address := "0.0.0.0:" + strconv.Itoa(port)
	err := server.Start(address)
	if err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		os.Exit(1)
	}
}
