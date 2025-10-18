package config

import (
	"flag"
	"strconv"
)

// Config holds the application configuration
type Config struct {
	Port    int
	Address string
}

// NewConfig creates a new configuration from command line flags
func NewConfig() *Config {
	var port int
	flag.IntVar(&port, "port", 6379, "Port to bind the Redis server to")
	flag.Parse()

	return &Config{
		Port:    port,
		Address: "0.0.0.0:" + strconv.Itoa(port),
	}
}

// GetAddress returns the server address
func (c *Config) GetAddress() string {
	return c.Address
}

// GetPort returns the server port
func (c *Config) GetPort() int {
	return c.Port
}

// GetServerInfo returns server information for INFO command
func (c *Config) GetServerInfo() map[string]string {
	return map[string]string{
		"redis_version":    "7.0.0",
		"redis_mode":       "standalone",
		"tcp_port":         strconv.Itoa(c.Port),
		"role":             "master",
		"connected_slaves": "0",
	}
}
