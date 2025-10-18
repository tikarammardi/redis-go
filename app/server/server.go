package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/resp"
)

// Server represents a Redis-compatible server
type Server struct {
	processor CommandProcessor
	listener  net.Listener
	config    Config
}

// Config interface for server configuration
type Config interface {
	GetAddress() string
	GetPort() int
}

// CommandProcessor interface for processing commands
type CommandProcessor interface {
	Process(command resp.RespValue, conn net.Conn) error
	CleanupConnection(conn net.Conn)
}

// NewServer creates a new Redis server
func NewServer(processor CommandProcessor, config Config) *Server {
	return &Server{
		processor: processor,
		config:    config,
	}
}

// Start starts the server on the configured address
func (s *Server) Start() error {
	address := s.config.GetAddress()
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

// Stop stops the server
func (s *Server) Stop() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		s.processor.CleanupConnection(conn)
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

		command, err := resp.ParseRESP(r)
		if err != nil {
			writer := resp.NewResponseWriter(conn)
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
