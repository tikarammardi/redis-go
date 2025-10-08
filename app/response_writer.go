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

func (w *RespResponseWriter) WriteStreamReadResults(results []StreamReadResult) error {
	// Write array length for number of streams with results
	response := fmt.Sprintf("*%d\r\n", len(results))

	for _, result := range results {
		// Each result is an array of 2 elements: [stream_key, [entries...]]
		response += "*2\r\n" // Array of 2 elements

		// First element: the stream key as bulk string
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(result.Key), result.Key)

		// Second element: array of entries
		response += fmt.Sprintf("*%d\r\n", len(result.Entries))

		for _, entry := range result.Entries {
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
	}

	_, err := w.conn.Write([]byte(response))
	return err
}

func (w *RespResponseWriter) WriteTransactionResults(results []RespValue) error {
	// Write array header
	response := fmt.Sprintf("*%d\r\n", len(results))

	// Write each result in the array
	for _, result := range results {
		switch result.Type {
		case SimpleString:
			response += fmt.Sprintf("+%s\r\n", result.Value)
		case BulkString:
			if result.Value == nil {
				response += "$-1\r\n"
			} else {
				s := result.Value.(string)
				response += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
			}
		case IntegerType:
			response += fmt.Sprintf(":%d\r\n", result.Value)
		case ErrorType:
			response += fmt.Sprintf("-%s\r\n", result.Value)
		case ArrayType:
			if result.Value == nil {
				response += "*-1\r\n"
			} else {
				arrayValues := result.Value.([]RespValue)
				response += fmt.Sprintf("*%d\r\n", len(arrayValues))
				// For nested arrays, we need to recursively serialize
				for _, arrayItem := range arrayValues {
					switch arrayItem.Type {
					case BulkString:
						if arrayItem.Value == nil {
							response += "$-1\r\n"
						} else {
							s := arrayItem.Value.(string)
							response += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
						}
					case IntegerType:
						response += fmt.Sprintf(":%d\r\n", arrayItem.Value)
					case SimpleString:
						response += fmt.Sprintf("+%s\r\n", arrayItem.Value)
					case ErrorType:
						response += fmt.Sprintf("-%s\r\n", arrayItem.Value)
						// Add more cases as needed for complex nested structures
					}
				}
			}
		}
	}

	_, err := w.conn.Write([]byte(response))
	return err
}
