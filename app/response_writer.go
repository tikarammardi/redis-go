package main

import (
	"fmt"
	"net"
)

// RespResponseWriter implements ResponseWriter interface
type RespResponseWriter struct {
	conn net.Conn
}

// NewRespResponseWriter creates a new RESP response writer
func NewRespResponseWriter(conn net.Conn) *RespResponseWriter {
	return &RespResponseWriter{conn: conn}
}

func (w *RespResponseWriter) WriteSimpleString(s string) error {
	response := fmt.Sprintf("+%s\r\n", s)
	_, err := w.conn.Write([]byte(response))
	return err
}

func (w *RespResponseWriter) WriteBulkString(s string) error {
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	_, err := w.conn.Write([]byte(response))
	return err
}

func (w *RespResponseWriter) WriteInteger(i int) error {
	response := fmt.Sprintf(":%d\r\n", i)
	_, err := w.conn.Write([]byte(response))
	return err
}

func (w *RespResponseWriter) WriteError(err string) error {
	response := fmt.Sprintf("-%s\r\n", err)
	_, err2 := w.conn.Write([]byte(response))
	return err2
}

func (w *RespResponseWriter) WriteArray(items []string) error {
	response := fmt.Sprintf("*%d\r\n", len(items))
	for _, item := range items {
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(item), item)
	}
	_, err := w.conn.Write([]byte(response))
	return err
}

func (w *RespResponseWriter) WriteNullBulkString() error {
	_, err := w.conn.Write([]byte("$-1\r\n"))
	return err
}

func (w *RespResponseWriter) WriteNullArray() error {
	_, err := w.conn.Write([]byte("*-1\r\n"))
	return err
}

func (w *RespResponseWriter) WriteEmptyArray() error {
	_, err := w.conn.Write([]byte("*0\r\n"))
	return err
}

func (w *RespResponseWriter) WriteStreamEntries(entries []StreamEntry) error {
	// Write array length
	response := fmt.Sprintf("*%d\r\n", len(entries))

	for _, entry := range entries {
		// Each entry is an array of 2 elements: [id, [field1, value1, field2, value2, ...]]
		response += "*2\r\n" // Array of 2 elements

		// First element: the ID as bulk string
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID)

		// Second element: array of field-value pairs
		fieldCount := len(entry.Fields) * 2 // Each field has name and value
		response += fmt.Sprintf("*%d\r\n", fieldCount)

		// Add field-value pairs in order
		for field, value := range entry.Fields {
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(field), field)
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
		}
	}

	_, err := w.conn.Write([]byte(response))
	return err
}
