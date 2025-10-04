package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}
		request := string(buf[:n])
		r := bufio.NewReader(strings.NewReader(request))
		command, err := ParseRESP(r)
		fmt.Println("CommandType:", command.Type, "Value:", command.Value)
		// CommandType: 4 Value: [{3 ECHO} {3 pear}]
		if err != nil {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			continue
		}

		if command.Type != ArrayType {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			continue
		}

		parts, ok := command.Value.([]RespValue)
		if !ok || len(parts) == 0 {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			continue
		}

		cmd, ok := parts[0].Value.(string)
		if !ok {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			continue
		}

		switch strings.ToUpper(cmd) {
		case "PING":
			if len(parts) == 1 {
				_, err := conn.Write([]byte("+PONG\r\n"))
				if err != nil {
					return
				}
			} else if len(parts) == 2 {
				msg, ok := parts[1].Value.(string)
				if !ok {
					_, err := conn.Write([]byte("-ERR unknown command\r\n"))
					if err != nil {
						return
					}
					continue
				}
				response := fmt.Sprintf("+%s\r\n", msg)
				_, err := conn.Write([]byte(response))
				if err != nil {
					return
				}
			} else {
				_, err := conn.Write([]byte("-ERR unknown command\r\n"))
				if err != nil {
					return
				}
			}
		case "ECHO":
			if len(parts) != 2 {
				_, err := conn.Write([]byte("-ERR unknown command\r\n"))
				if err != nil {
					return
				}
				continue
			}
			msg, ok := parts[1].Value.(string)
			if !ok {
				_, err := conn.Write([]byte("-ERR unknown command\r\n"))
				if err != nil {
					return
				}
				continue
			}
			response := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
			_, err := conn.Write([]byte(response))
			if err != nil {
				return
			}
		default:
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
		}

	}
}
