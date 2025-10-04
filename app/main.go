package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/app/store"
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

	default:
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
	}

}

func handleGet(parts []RespValue, conn net.Conn) bool {
	if len(parts) != 2 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return true
		}
		return true
	}
	key := parts[1].Value.(string)
	st := store.NewInMemoryStore()
	value, exists := st.Get(key)
	fmt.Println("Exists:", exists, "Value:", value)

	if !exists {

		_, err := conn.Write([]byte("$-1\r\n")) // Null bulk string - https://redis.io/docs/latest/develop/reference/protocol-spec/#null-bulk-strings
		if err != nil {
			return true
		}
		return true

		//value = "bar"
		//response := fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
		//_, err := conn.Write([]byte(response))
		//if err != nil {
		//	return
		//}
		//return
	}

	// if exist https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	_, err := conn.Write([]byte(response))
	if err != nil {
		return true
	}
	return false
}

func handleSet(parts []RespValue, conn net.Conn) bool {
	for i := range parts {
		fmt.Printf("Part %d: Type %d, Value %v\n", i, parts[i].Type, parts[i].Value)
	}
	//if len(parts) != 3 {
	//	_, err := conn.Write([]byte("-ERR unknown command\r\n"))
	//	if err != nil {
	//		return
	//	}
	//	return
	//}
	expiryCommands := []string{"EX", "PX"}
	expiryValue := 0
	for i := 3; i < len(parts); i += 2 {
		partValue, ok := parts[i].Value.(string)
		if !ok {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return true
			}
			return true
		}
		partValue = strings.ToUpper(partValue)
		if !contains(expiryCommands, partValue) {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return true
			}
			return true
		}
		// if ex time will be in seconds and  px time will be in milliseconds

		if i+1 >= len(parts) {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return true
			}
			return true
		}
		timeValue, ok := parts[i+1].Value.(string)
		if !ok {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return true
			}
			return true
		}
		var timeInt int
		_, err := fmt.Sscanf(timeValue, "%d", &timeInt)
		if err != nil || timeInt <= 0 {
			_, err := conn.Write([]byte("-ERR invalid expire time in set\r\n"))
			if err != nil {
				return true
			}
			return true
		}
		if partValue == "EX" {
			expiryValue = timeInt * 1000 // convert to milliseconds
		} else if partValue == "PX" {
			expiryValue = timeInt
		}
	}

	if len(parts) < 3 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return true
		}
		return true
	}
	if parts[1].Type != BulkString || parts[2].Type != BulkString {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return true
		}
		return true
	}
	expiry := expiryValue
	key := parts[1].Value.(string)
	value := parts[2].Value.(string)
	st := store.NewInMemoryStore()
	st.Set(key, value, expiry)
	fmt.Printf("Set key: %s to value: %s\n", key, value)
	_, err := conn.Write([]byte("+OK\r\n"))
	if err != nil {
		return true
	}
	return false
}

func handleEcho(parts []RespValue, conn net.Conn) bool {
	if len(parts) != 2 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return true
		}
		return true
	}
	msg, ok := parts[1].Value.(string)
	if !ok {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return true
		}
		return true
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
	_, err := conn.Write([]byte(response))
	if err != nil {
		return true
	}
	return false
}

func handlePing(parts []RespValue, conn net.Conn) bool {
	if len(parts) == 1 {
		_, err := conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			return true
		}
	} else if len(parts) == 2 {
		msg, ok := parts[1].Value.(string)
		if !ok {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return true
			}
			return true
		}
		response := fmt.Sprintf("+%s\r\n", msg)
		_, err := conn.Write([]byte(response))
		if err != nil {
			return true
		}
	} else {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return true
		}
	}
	return false
}

func contains(commands []string, value string) bool {
	for _, cmd := range commands {
		if cmd == value {
			return true
		}
	}
	return false
}
