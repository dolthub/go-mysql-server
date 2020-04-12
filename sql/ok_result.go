package sql

// OkResult is a representation of the OK packet MySQL sends for non-select queries such as UPDATE, INSERT, etc. It
// can be returned as the only element in the row for a Node that doesn't select anything.
// See https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
type OkResult struct {
	RowsAffected uint64 // Number of rows affected by this operation
	InsertID     uint64 // Inserted ID, if any, or -1 if not
	Info         string // Human-readable status string for extra status info, echoed verbatim to clients.
}

// OkResultColumnName should be used as the schema column name for Nodes that return an OkResult
const OkResultColumnName = "__ok_result__"

// OkResultColumnType should be used as the schema column type for Nodes that return an OkResult
var OkResultColumnType = Int64

// OkResultSchema should be used as the schema of Nodes that return an OkResult
var OkResultSchema = Schema{
	{
		Name: OkResultColumnName,
		Type: OkResultColumnType,
	},
}

// NewOKResult returns a new OkResult with the given number of rows affected.
func NewOkResult(rowsAffected int) OkResult {
	return OkResult{RowsAffected: uint64(rowsAffected)}
}