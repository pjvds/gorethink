package types

import (
	"encoding/json"

	p "github.com/dancannon/gorethink/ql2"
)

// Response represents the raw response from a query, most of the time you
// should instead use a Cursor when reading from the database.
type Response struct {
	Token     int64
	Type      p.Response_ResponseType   `json:"t",gorethink:"t"`
	Notes     []p.Response_ResponseNote `json:"n",gorethink:"n"`
	Responses []json.RawMessage         `json:"r",gorethink:"r"`
	Backtrace []interface{}             `json:"b",gorethink:"b"`
	Profile   interface{}               `json:"p",gorethink:"p"`
}
