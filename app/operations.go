package main

import (
	"fmt"
	"net"
	"strings"
	"time"
)

func handleLPop(parts []RespValue, conn net.Conn) {
	printArgs(parts)
	if parts[1].Type != BulkString {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}

	key := parts[1].Value.(string)
	numberOfElementsToPop := 1
	if len(parts) == 3 {
		if parts[2].Type != BulkString {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			return
		}
		_, err := fmt.Sscanf(parts[2].Value.(string), "%d", &numberOfElementsToPop)
		if err != nil || numberOfElementsToPop <= 0 {
			_, err := conn.Write([]byte("-ERR invalid count for LPOP\r\n"))
			if err != nil {
				return
			}
			return
		}
	} else if len(parts) > 3 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}

	if numberOfElementsToPop > 1 {
		values, exists := lpopMultipleValues(key, numberOfElementsToPop)
		if !exists || len(values) == 0 {
			_, err := conn.Write([]byte("*0\r\n")) // List does not exist or is empty, return empty array
			if err != nil {
				return
			}
			return
		}
		response := fmt.Sprintf("*%d\r\n", len(values))
		for _, value := range values {
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
		}
		_, err := conn.Write([]byte(response))
		if err != nil {
			return
		}
		return
	}
	value, exists := lpopValue(key)
	if !exists {
		_, err := conn.Write([]byte("$-1\r\n")) // List does not exist or is empty, return null bulk string
		if err != nil {
			return
		}
		return
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	_, err := conn.Write([]byte(response))
	if err != nil {
		return
	}
}

func handleLLen(parts []RespValue, conn net.Conn) {
	if len(parts) != 2 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	if parts[1].Type != BulkString {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	key := parts[1].Value.(string)
	length, exists := getListLength(key)
	if !exists {
		_, err := conn.Write([]byte(":0\r\n")) // List does not exist, return 0 length
		if err != nil {
			return
		}
		return
	}
	response := fmt.Sprintf(":%d\r\n", length)
	_, err := conn.Write([]byte(response))
	if err != nil {
		return
	}
}

func handleLPush(parts []RespValue, conn net.Conn) {
	if len(parts) < 3 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	if parts[1].Type != BulkString {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	key := parts[1].Value.(string)
	newLength := 0
	for i := 2; i < len(parts); i++ {
		if parts[i].Type != BulkString {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			return
		}
		value := parts[i].Value.(string)
		length, err := lpushValue(key, value)
		if err != nil {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			return
		}
		newLength = length
	}
	response := fmt.Sprintf(":%d\r\n", newLength)
	_, err := conn.Write([]byte(response))
	if err != nil {
		return
	}

}

func handleLRange(parts []RespValue, conn net.Conn) {
	if len(parts) != 4 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	if parts[1].Type != BulkString || parts[2].Type != BulkString || parts[3].Type != BulkString {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	key := parts[1].Value.(string)
	startStr := parts[2].Value.(string)
	endStr := parts[3].Value.(string)
	fmt.Println("LRange:", startStr, endStr)
	var start, end int
	_, err := fmt.Sscanf(startStr, "%d", &start)
	if err != nil {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	_, err = fmt.Sscanf(endStr, "%d", &end)
	if err != nil {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	items, exists := getListRange(key, start, end)
	if !exists {
		_, err := conn.Write([]byte("*0\r\n")) // Empty array
		if err != nil {
			return
		}
		return
	}
	response := fmt.Sprintf("*%d\r\n", len(items))
	for _, item := range items {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(item), item)
	}
	_, err = conn.Write([]byte(response))
	if err != nil {
		return
	}
}

func handleRPush(parts []RespValue, conn net.Conn) {
	if len(parts) < 3 {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	if parts[1].Type != BulkString {
		_, err := conn.Write([]byte("-ERR unknown command\r\n"))
		if err != nil {
			return
		}
		return
	}
	key := parts[1].Value.(string)
	newLength := 0
	for i := 2; i < len(parts); i++ {
		if parts[i].Type != BulkString {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			return
		}
		value := parts[i].Value.(string)
		length, err := rpushValue(key, value)
		if err != nil {
			_, err := conn.Write([]byte("-ERR unknown command\r\n"))
			if err != nil {
				return
			}
			return
		}
		newLength = length
	}
	response := fmt.Sprintf(":%d\r\n", newLength)
	_, err := conn.Write([]byte(response))
	if err != nil {
		return
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

	value, exists := getValue(key)
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
	setValue(key, value, expiry)
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

func handleBLPop(parts []RespValue, conn net.Conn) {
	printArgs(parts)

	// BLPOP requires at least 3 parts: command, key(s), timeout
	if len(parts) < 3 {
		_, err := conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
		if err != nil {
			return
		}
		return
	}

	// Last argument is always the timeout
	timeoutStr, ok := parts[len(parts)-1].Value.(string)
	if !ok || parts[len(parts)-1].Type != BulkString {
		_, err := conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
		if err != nil {
			return
		}
		return
	}

	var timeoutSeconds float64
	_, err := fmt.Sscanf(timeoutStr, "%f", &timeoutSeconds)
	if err != nil || timeoutSeconds < 0 {
		_, err := conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
		if err != nil {
			return
		}
		return
	}

	// Extract keys (all parts except command and timeout)
	keys := make([]string, 0, len(parts)-2)
	for i := 1; i < len(parts)-1; i++ {
		if parts[i].Type != BulkString {
			_, err := conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
			if err != nil {
				return
			}
			return
		}
		keys = append(keys, parts[i].Value.(string))
	}

	// Try to pop from any of the keys immediately
	for _, key := range keys {
		value, exists := lpopValue(key)
		if exists {
			// Found a value, return it immediately
			response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
			_, err := conn.Write([]byte(response))
			if err != nil {
				return
			}
			return
		}
	}

	// No values found, need to block
	if timeoutSeconds == 0 {
		// Block indefinitely
		for {
			for _, key := range keys {
				value, exists := lpopValue(key)
				if exists {
					response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
					_, err := conn.Write([]byte(response))
					if err != nil {
						return
					}
					return
				}
			}
			// Sleep briefly to avoid busy waiting
			time.Sleep(10 * time.Millisecond)
		}
	} else {
		// Block with timeout
		timeout := time.Duration(timeoutSeconds * float64(time.Second))
		deadline := time.Now().Add(timeout)

		for time.Now().Before(deadline) {
			for _, key := range keys {
				value, exists := lpopValue(key)
				if exists {
					response := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
					_, err := conn.Write([]byte(response))
					if err != nil {
						return
					}
					return
				}
			}
			// Sleep briefly to avoid busy waiting
			time.Sleep(10 * time.Millisecond)
		}

		// Timeout reached, return null
		_, err := conn.Write([]byte("*-1\r\n"))
		if err != nil {
			return
		}
	}
}

func contains(commands []string, value string) bool {
	for _, cmd := range commands {
		if cmd == value {
			return true
		}
	}
	return false
}

func printArgs(parts []RespValue) {
	for index, part := range parts {
		fmt.Printf("Arg %d: Type %d, Value %v\n", index, part.Type, part.Value)
	}
}
