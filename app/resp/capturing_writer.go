package resp

import (
	"bytes"
	"fmt"
	"net"
	"time"
)

// CapturingConn is a connection that captures written data
type CapturingConn struct {
	buffer     *bytes.Buffer
	lastOp     string
	lastVal    interface{}
	lastBuffer *bytes.Buffer
}

// NewCapturingConn creates a new capturing connection
func NewCapturingConn() *CapturingConn {
	return &CapturingConn{
		buffer:     &bytes.Buffer{},
		lastBuffer: &bytes.Buffer{},
	}
}

// Write captures the written data and parses it to determine what was written
func (c *CapturingConn) Write(b []byte) (n int, err error) {
	c.buffer.Write(b)
	c.lastBuffer.Reset()
	c.lastBuffer.Write(b)
	c.parseLastWrite(b)
	return len(b), nil
}

// parseLastWrite analyzes what was written to determine the response type
func (c *CapturingConn) parseLastWrite(b []byte) {
	if len(b) == 0 {
		return
	}

	data := string(b)

	// Parse based on RESP protocol
	switch b[0] {
	case '+': // Simple string
		c.lastOp = "simpleString"
		// Extract value between + and \r\n
		end := len(data)
		if idx := bytes.Index(b, []byte("\r\n")); idx != -1 {
			end = idx
		}
		c.lastVal = data[1:end]

	case '$': // Bulk string or null bulk string
		if len(data) > 3 && data[1] == '-' && data[2] == '1' {
			c.lastOp = "nullBulkString"
			c.lastVal = nil
		} else {
			c.lastOp = "bulkString"
			// Find the \r\n after length, then extract the string
			if idx := bytes.Index(b, []byte("\r\n")); idx != -1 {
				if idx+2 < len(b) {
					secondCRLF := bytes.Index(b[idx+2:], []byte("\r\n"))
					if secondCRLF != -1 {
						c.lastVal = string(b[idx+2 : idx+2+secondCRLF])
					}
				}
			}
		}

	case ':': // Integer
		c.lastOp = "integer"
		end := len(data)
		if idx := bytes.Index(b, []byte("\r\n")); idx != -1 {
			end = idx
		}
		var val int
		fmt.Sscanf(data[1:end], "%d", &val)
		c.lastVal = val

	case '-': // Error
		c.lastOp = "error"
		end := len(data)
		if idx := bytes.Index(b, []byte("\r\n")); idx != -1 {
			end = idx
		}
		c.lastVal = data[1:end]

	case '*': // Array
		if len(data) > 3 && data[1] == '-' && data[2] == '1' {
			c.lastOp = "nullArray"
			c.lastVal = nil
		} else if len(data) > 3 && data[1] == '0' {
			c.lastOp = "emptyArray"
			c.lastVal = []RespValue{}
		} else {
			c.lastOp = "array"
			// For arrays, we'll mark it as array type but keep the raw data
			c.lastVal = data
		}
	}
}

// GetCapturedResponse returns the captured response as a RespValue
func (c *CapturingConn) GetCapturedResponse() RespValue {
	switch c.lastOp {
	case "simpleString":
		return RespValue{Type: SimpleString, Value: c.lastVal}
	case "bulkString":
		return RespValue{Type: BulkString, Value: c.lastVal}
	case "integer":
		return RespValue{Type: IntegerType, Value: c.lastVal}
	case "error":
		return RespValue{Type: ErrorType, Value: c.lastVal}
	case "nullBulkString":
		return RespValue{Type: BulkString, Value: nil}
	case "nullArray":
		return RespValue{Type: ArrayType, Value: nil}
	case "emptyArray":
		return RespValue{Type: ArrayType, Value: []RespValue{}}
	case "array":
		return RespValue{Type: ArrayType, Value: c.lastVal}
	default:
		return RespValue{Type: BulkString, Value: nil}
	}
}

// Implement other net.Conn methods (not used but required by interface)
func (c *CapturingConn) Read(b []byte) (n int, err error)   { return 0, nil }
func (c *CapturingConn) Close() error                       { return nil }
func (c *CapturingConn) LocalAddr() net.Addr                { return nil }
func (c *CapturingConn) RemoteAddr() net.Addr               { return nil }
func (c *CapturingConn) SetDeadline(t time.Time) error      { return nil }
func (c *CapturingConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *CapturingConn) SetWriteDeadline(t time.Time) error { return nil }

// NewCapturingWriter creates a ResponseWriter that captures responses
func NewCapturingWriter() (*ResponseWriter, *CapturingConn) {
	conn := NewCapturingConn()
	writer := NewResponseWriter(conn)
	return writer, conn
}
