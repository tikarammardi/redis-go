package resp

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

type RespType int

const (
	SimpleString RespType = iota
	ErrorType
	IntegerType
	BulkString
	ArrayType
)

// RespValue represents a RESP protocol value
type RespValue struct {
	Type  RespType
	Value interface{}
}

// ParseRESP reads the next RESP value from the stream
func ParseRESP(r *bufio.Reader) (RespValue, error) {
	prefix, err := r.ReadByte()
	if err != nil {
		return RespValue{}, err
	}
	switch prefix {
	case '+':
		line, _ := r.ReadString('\n')
		return RespValue{SimpleString, strings.TrimSuffix(line, "\r\n")}, nil
	case '-':
		line, _ := r.ReadString('\n')
		return RespValue{ErrorType, strings.TrimSuffix(line, "\r\n")}, nil
	case ':':
		line, _ := r.ReadString('\n')
		num, _ := strconv.ParseInt(strings.TrimSuffix(line, "\r\n"), 10, 64)
		return RespValue{IntegerType, num}, nil
	case '$':
		lenLine, _ := r.ReadString('\n')
		length, _ := strconv.Atoi(strings.TrimSuffix(lenLine, "\r\n"))
		if length == -1 {
			return RespValue{BulkString, nil}, nil // Null bulk string
		}
		buf := make([]byte, length+2) // +2 for \r\n
		io.ReadFull(r, buf)
		return RespValue{BulkString, string(buf[:length])}, nil
	case '*':
		lenLine, _ := r.ReadString('\n')
		count, _ := strconv.Atoi(strings.TrimSuffix(lenLine, "\r\n"))
		if count == -1 {
			return RespValue{ArrayType, nil}, nil // Null array
		}
		items := make([]RespValue, count)
		for i := 0; i < count; i++ {
			item, err := ParseRESP(r)
			if err != nil {
				return RespValue{}, err
			}
			items[i] = item
		}
		return RespValue{ArrayType, items}, nil
	default:
		return RespValue{}, errors.New("unknown RESP prefix")
	}
}
