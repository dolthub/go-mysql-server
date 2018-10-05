package plan

import (
	"fmt"
	"sort"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// ShowTableStatus returns the status of the tables in the databases.
type ShowTableStatus struct {
	Databases []string
	Catalog   *sql.Catalog
}

// NewShowTableStatus creates a new ShowTableStatus node.
func NewShowTableStatus(dbs ...string) *ShowTableStatus {
	return &ShowTableStatus{Databases: dbs}
}

var showTableStatusSchema = sql.Schema{
	{Name: "Name", Type: sql.Text},
	{Name: "Engine", Type: sql.Text},
	{Name: "Version", Type: sql.Text},
	{Name: "Row_format", Type: sql.Text},
	{Name: "Rows", Type: sql.Int64},
	{Name: "Avg_row_length", Type: sql.Int64},
	{Name: "Data_length", Type: sql.Int64},
	{Name: "Max_data_length", Type: sql.Int64},
	{Name: "Index_length", Type: sql.Int64},
	{Name: "Data_free", Type: sql.Int64},
	{Name: "Auto_increment", Type: sql.Int64},
	{Name: "Create_time", Type: sql.Timestamp, Nullable: true},
	{Name: "Update_time", Type: sql.Timestamp, Nullable: true},
	{Name: "Check_time", Type: sql.Timestamp, Nullable: true},
	{Name: "Collation", Type: sql.Text},
	{Name: "Checksum", Type: sql.Text, Nullable: true},
	{Name: "Create_options", Type: sql.Text, Nullable: true},
	{Name: "Comments", Type: sql.Text, Nullable: true},
}

// Children implements the sql.Node interface.
func (s *ShowTableStatus) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (s *ShowTableStatus) Resolved() bool { return true }

// Schema implements the sql.Node interface.
func (s *ShowTableStatus) Schema() sql.Schema { return showTableStatusSchema }

// RowIter implements the sql.Node interface.
func (s *ShowTableStatus) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	var rows []sql.Row
	var tables []string
	for _, db := range s.Catalog.AllDatabases() {
		if len(s.Databases) > 0 && !stringContains(s.Databases, db.Name()) {
			continue
		}

		for t := range db.Tables() {
			tables = append(tables, t)
		}
	}

	sort.Strings(tables)
	for _, t := range tables {
		rows = append(rows, tableToStatusRow(t))
	}

	return sql.RowsToRowIter(rows...), nil
}

func (s *ShowTableStatus) String() string {
	return fmt.Sprintf("ShowTableStatus(%s)", strings.Join(s.Databases, ", "))
}

// TransformUp implements the sql.Node interface.
func (s *ShowTableStatus) TransformUp(f sql.TransformNodeFunc) (sql.Node, error) {
	return f(s)
}

// TransformExpressionsUp implements the sql.Node interface.
func (s *ShowTableStatus) TransformExpressionsUp(f sql.TransformExprFunc) (sql.Node, error) {
	return s, nil
}

func stringContains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func tableToStatusRow(table string) sql.Row {
	return sql.NewRow(
		table,    // Name
		"InnoDB", // Engine
		// This column is unused. With the removal of .frm files in MySQL 8.0, this
		// column now reports a hardcoded value of 10, which is the last .frm file
		// version used in MySQL 5.7.
		"10",       // Version
		"Fixed",    // Row_format
		int64(0),   // Rows
		int64(0),   // Avg_row_length
		int64(0),   // Data_length
		int64(0),   // Max_data_length
		int64(0),   // Index_length
		int64(0),   // Data_free
		int64(0),   // Auto_increment
		nil,        // Create_time
		nil,        // Update_time
		nil,        // Check_time
		"utf8_bin", // Collation
		nil,        // Create_options
		nil,        // Comments
	)
}
