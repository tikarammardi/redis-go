package resp

import (
	"fmt"
	"net"
	"strings"
)

// ResponseWriter handles writing RESP responses to connections
type ResponseWriter struct {
	conn net.Conn
}

// NewResponseWriter creates a new RESP response writer
func NewResponseWriter(conn net.Conn) *ResponseWriter {
	return &ResponseWriter{conn: conn}
}

// writeResponse is a helper method to write the final response
func (w *ResponseWriter) writeResponse(response string) error {
	_, err := w.conn.Write([]byte(response))
	return err
}

// formatBulkString formats a bulk string with proper RESP format
func formatBulkString(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// formatNullBulkString formats a null bulk string
func formatNullBulkString() string {
	return "$-1\r\n"
}

// formatArrayHeader formats an array header
func formatArrayHeader(length int) string {
	return fmt.Sprintf("*%d\r\n", length)
}

func (w *ResponseWriter) WriteSimpleString(s string) error {
	response := fmt.Sprintf("+%s\r\n", s)
	return w.writeResponse(response)
}

func (w *ResponseWriter) WriteBulkString(s string) error {
	return w.writeResponse(formatBulkString(s))
}

func (w *ResponseWriter) WriteInteger(i int) error {
	response := fmt.Sprintf(":%d\r\n", i)
	return w.writeResponse(response)
}

func (w *ResponseWriter) WriteError(err string) error {
	response := fmt.Sprintf("-%s\r\n", err)
	return w.writeResponse(response)
}

func (w *ResponseWriter) WriteArray(items []string) error {
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(items)))

	for _, item := range items {
		response.WriteString(formatBulkString(item))
	}

	return w.writeResponse(response.String())
}

func (w *ResponseWriter) WriteNullBulkString() error {
	return w.writeResponse(formatNullBulkString())
}

func (w *ResponseWriter) WriteNullArray() error {
	return w.writeResponse("*-1\r\n")
}

func (w *ResponseWriter) WriteEmptyArray() error {
	return w.writeResponse("*0\r\n")
}

// WriteTransactionResults writes the results of a transaction
func (w *ResponseWriter) WriteTransactionResults(results []RespValue) error {
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(results)))

	for _, result := range results {
		response.WriteString(formatRespValue(result))
	}

	return w.writeResponse(response.String())
}

// formatRespValue formats a single RespValue for transaction results
func formatRespValue(value RespValue) string {
	switch value.Type {
	case SimpleString:
		return fmt.Sprintf("+%s\r\n", value.Value)
	case BulkString:
		if value.Value == nil {
			return formatNullBulkString()
		}
		return formatBulkString(value.Value.(string))
	case IntegerType:
		return fmt.Sprintf(":%d\r\n", value.Value)
	case ErrorType:
		return fmt.Sprintf("-%s\r\n", value.Value)
	default:
		return formatNullBulkString()
	}
}

// WriteStreamEntries writes stream entries in the correct RESP format
func (w *ResponseWriter) WriteStreamEntries(entries []StreamEntry) error {
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(entries)))

	for _, entry := range entries {
		// Each entry is an array of 2 elements: [id, [field1, value1, field2, value2, ...]]
		response.WriteString("*2\r\n")
		response.WriteString(formatBulkString(entry.ID))

		// Format field-value pairs as an array
		fieldCount := len(entry.Fields) * 2
		response.WriteString(formatArrayHeader(fieldCount))

		for field, value := range entry.Fields {
			response.WriteString(formatBulkString(field))
			response.WriteString(formatBulkString(value))
		}
	}

	return w.writeResponse(response.String())
}

// StreamEntry represents a single stream entry
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

// StreamResult represents a stream with its entries for XREAD responses
type StreamResult struct {
	Key     string
	Entries []StreamEntry
}

// WriteStreamResults writes stream results for XREAD command in the correct RESP format
func (w *ResponseWriter) WriteStreamResults(results []StreamResult) error {
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(results)))

	for _, result := range results {
		// Each result is an array of 2 elements: [stream_key, [entries...]]
		response.WriteString("*2\r\n")
		response.WriteString(formatBulkString(result.Key))

		// Write entries array
		response.WriteString(formatArrayHeader(len(result.Entries)))
		for _, entry := range result.Entries {
			// Each entry is an array of 2 elements: [id, [field1, value1, field2, value2, ...]]
			response.WriteString("*2\r\n")
			response.WriteString(formatBulkString(entry.ID))

			// Format field-value pairs as an array
			fieldCount := len(entry.Fields) * 2
			response.WriteString(formatArrayHeader(fieldCount))

			for field, value := range entry.Fields {
				response.WriteString(formatBulkString(field))
				response.WriteString(formatBulkString(value))
			}
		}
	}

	return w.writeResponse(response.String())
}
