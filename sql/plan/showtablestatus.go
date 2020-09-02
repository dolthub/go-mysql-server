package plan

import (
	"fmt"
	"sort"
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
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
	{Name: "Name", Type: sql.LongText},
	{Name: "Engine", Type: sql.LongText},
	{Name: "Version", Type: sql.LongText},
	{Name: "Row_format", Type: sql.LongText},
	{Name: "Rows", Type: sql.Int64},
	{Name: "Avg_row_length", Type: sql.Int64},
	{Name: "Data_length", Type: sql.Int64},
	{Name: "Max_data_length", Type: sql.Int64},
	{Name: "Index_length", Type: sql.Int64},
	{Name: "Data_free", Type: sql.Int64},
	{Name: "Auto_increment", Type: sql.Int64},
	{Name: "Create_time", Type: sql.Datetime, Nullable: true},
	{Name: "Update_time", Type: sql.Datetime, Nullable: true},
	{Name: "Check_time", Type: sql.Datetime, Nullable: true},
	{Name: "Collation", Type: sql.LongText},
	{Name: "Checksum", Type: sql.LongText, Nullable: true},
	{Name: "Create_options", Type: sql.LongText, Nullable: true},
	{Name: "Comments", Type: sql.LongText, Nullable: true},
}

// Children implements the sql.Node interface.
func (s *ShowTableStatus) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (s *ShowTableStatus) Resolved() bool { return true }

// Schema implements the sql.Node interface.
func (s *ShowTableStatus) Schema() sql.Schema { return showTableStatusSchema }

// RowIter implements the sql.Node interface.
func (s *ShowTableStatus) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	var tables []string
	var err error
	if len(s.Databases) > 0 {
		for _, db := range s.Catalog.AllDatabases() {
			if !stringContains(s.Databases, db.Name()) {
				continue
			}

			tables, err = db.GetTableNames(ctx)

			if err != nil {
				return nil, err
			}
		}
	} else {
		db, err := s.Catalog.Database(ctx.GetCurrentDatabase())
		if err != nil {
			return nil, err
		}

		tables, err = db.GetTableNames(ctx)

		if err != nil {
			return nil, err
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

// WithChildren implements the Node interface.
func (s *ShowTableStatus) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

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
		"10",                           // Version
		"Fixed",                        // Row_format
		int64(0),                       // Rows
		int64(0),                       // Avg_row_length
		int64(0),                       // Data_length
		int64(0),                       // Max_data_length
		int64(0),                       // Index_length
		int64(0),                       // Data_free
		int64(0),                       // Auto_increment
		nil,                            // Create_time
		nil,                            // Update_time
		nil,                            // Check_time
		sql.Collation_Default.String(), // Collation
		nil,                            // Create_options
		nil,                            // Comments
	)
}
