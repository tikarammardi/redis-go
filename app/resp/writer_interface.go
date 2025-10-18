package resp

// Writer is an interface for writing RESP responses
// This allows both normal ResponseWriter and CapturingWriter to be used
type Writer interface {
	WriteSimpleString(s string) error
	WriteBulkString(s string) error
	WriteInteger(i int) error
	WriteError(err string) error
	WriteArray(items []string) error
	WriteNullBulkString() error
	WriteNullArray() error
	WriteEmptyArray() error
	WriteTransactionResults(results []RespValue) error
	WriteStreamEntries(entries []StreamEntry) error
	WriteStreamResults(results []StreamResult) error
}
