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
		/*
			CommandType: 4 Value: [{3 ECHO} {3 pear}]
			CommandType: 4
			This is the RESP Array type, where 4 is  internal enumeration for arrays.
			Value: [{3 ECHO} {3 pear}]
			This is a slice/array of RESP values. Each item is parsed as {3 ECHO} and {3 pear}:
			{3 ECHO}: Here, 3 maps to BulkString type (the prefix $ in RESP).
			Value is "ECHO" (BulkString).
			{3 pear}: Type 3 again means BulkString.
			Value is "pear".

		*/
		if err != nil {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			continue
		}
		handleCommand(command, conn)
	}
}

func handleCommand(command RespValue, conn net.Conn) {

	if command.Type != ArrayType {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}

	parts, ok := command.Value.([]RespValue)
	if !ok || len(parts) == 0 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}

	cmd, ok := parts[0].Value.(string)
	if !ok {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	fmt.Println("Parts Debug:", parts)
	switch strings.ToUpper(cmd) {
	case "PING":
		handlePing(parts, conn)
	case "ECHO":
		handleEcho(parts, conn)
	case "SET":
		handleSet(parts, conn)
	case "GET":
		handleGet(parts, conn)
	case "RPUSH":
		handleRPush(parts, conn)
	case "LRANGE":
		handleLRange(parts, conn)
	case "LPUSH":
		handleLPush(parts, conn)
	case "LLEN":
		handleLLen(parts, conn)
	case "LPOP":
		handleLPop(parts, conn)
	case "BLPOP":
		handleBLPop(parts, conn)

	default:
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
	}

}
