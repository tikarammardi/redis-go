package main

import (
	"fmt"
	"net"
	"strings"
)

// RespResponseWriter implements ResponseWriter interface
type RespResponseWriter struct {
	conn net.Conn
}

// NewRespResponseWriter creates a new RESP response writer
func NewRespResponseWriter(conn net.Conn) *RespResponseWriter {
	return &RespResponseWriter{conn: conn}
}

// writeResponse is a helper method to write the final response
func (w *RespResponseWriter) writeResponse(response string) error {
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

func (w *RespResponseWriter) WriteSimpleString(s string) error {
	response := fmt.Sprintf("+%s\r\n", s)
	return w.writeResponse(response)
}

func (w *RespResponseWriter) WriteBulkString(s string) error {
	return w.writeResponse(formatBulkString(s))
}

func (w *RespResponseWriter) WriteInteger(i int) error {
	response := fmt.Sprintf(":%d\r\n", i)
	return w.writeResponse(response)
}

func (w *RespResponseWriter) WriteError(err string) error {
	response := fmt.Sprintf("-%s\r\n", err)
	return w.writeResponse(response)
}

func (w *RespResponseWriter) WriteArray(items []string) error {
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(items)))

	for _, item := range items {
		response.WriteString(formatBulkString(item))
	}

	return w.writeResponse(response.String())
}

func (w *RespResponseWriter) WriteNullBulkString() error {
	return w.writeResponse(formatNullBulkString())
}

func (w *RespResponseWriter) WriteNullArray() error {
	return w.writeResponse("*-1\r\n")
}

func (w *RespResponseWriter) WriteEmptyArray() error {
	return w.writeResponse("*0\r\n")
}

// formatStreamEntry formats a single stream entry
func formatStreamEntry(entry StreamEntry) string {
	var response strings.Builder

	// Each entry is an array of 2 elements: [id, [field1, value1, field2, value2, ...]]
	response.WriteString("*2\r\n")
	response.WriteString(formatBulkString(entry.ID))

	// Format field-value pairs
	fieldCount := len(entry.Fields) * 2
	response.WriteString(formatArrayHeader(fieldCount))

	for field, value := range entry.Fields {
		response.WriteString(formatBulkString(field))
		response.WriteString(formatBulkString(value))
	}

	return response.String()
}

func (w *RespResponseWriter) WriteStreamEntries(entries []StreamEntry) error {
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(entries)))

	for _, entry := range entries {
		response.WriteString(formatStreamEntry(entry))
	}

	return w.writeResponse(response.String())
}

// formatStreamReadResult formats a single stream read result
func formatStreamReadResult(result StreamReadResult) string {
	var response strings.Builder

	// Each result is an array of 2 elements: [stream_key, [entries...]]
	response.WriteString("*2\r\n")
	response.WriteString(formatBulkString(result.Key))
	response.WriteString(formatArrayHeader(len(result.Entries)))

	for _, entry := range result.Entries {
		response.WriteString(formatStreamEntry(entry))
	}

	return response.String()
}

func (w *RespResponseWriter) WriteStreamReadResults(results []StreamReadResult) error {
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(results)))

	for _, result := range results {
		response.WriteString(formatStreamReadResult(result))
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
	case ArrayType:
		return formatRespArray(value)
	default:
		return formatNullBulkString()
	}
}

// formatRespArray formats an array RespValue
func formatRespArray(value RespValue) string {
	if value.Value == nil {
		return "*-1\r\n"
	}

	arrayValues := value.Value.([]RespValue)
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(arrayValues)))

	for _, arrayItem := range arrayValues {
		response.WriteString(formatRespValue(arrayItem))
	}

	return response.String()
}

func (w *RespResponseWriter) WriteTransactionResults(results []RespValue) error {
	var response strings.Builder
	response.WriteString(formatArrayHeader(len(results)))

	for _, result := range results {
		response.WriteString(formatRespValue(result))
	}

	return w.writeResponse(response.String())
}
