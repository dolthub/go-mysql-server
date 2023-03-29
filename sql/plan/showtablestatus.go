// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ShowTableStatus returns the status of the tables in a database.
type ShowTableStatus struct {
	db      sql.Database
	Catalog sql.Catalog
}

var _ sql.Node = (*ShowTableStatus)(nil)
var _ sql.Databaser = (*ShowTableStatus)(nil)
var _ sql.CollationCoercible = (*ShowTableStatus)(nil)

// NewShowTableStatus creates a new ShowTableStatus node.
func NewShowTableStatus(db sql.Database) *ShowTableStatus {
	return &ShowTableStatus{db: db}
}

func (s *ShowTableStatus) Database() sql.Database {
	return s.db
}

func (s *ShowTableStatus) WithDatabase(db sql.Database) (sql.Node, error) {
	ns := *s
	ns.db = db
	return &ns, nil
}

var showTableStatusSchema = sql.Schema{
	{Name: "Name", Type: types.LongText},
	{Name: "Engine", Type: types.LongText},
	{Name: "Version", Type: types.LongText},
	{Name: "Row_format", Type: types.LongText},
	{Name: "Rows", Type: types.Uint64},
	{Name: "Avg_row_length", Type: types.Uint64},
	{Name: "Data_length", Type: types.Uint64},
	{Name: "Max_data_length", Type: types.Uint64},
	{Name: "Index_length", Type: types.Int64},
	{Name: "Data_free", Type: types.Int64},
	{Name: "Auto_increment", Type: types.Int64},
	{Name: "Create_time", Type: types.Datetime, Nullable: true},
	{Name: "Update_time", Type: types.Datetime, Nullable: true},
	{Name: "Check_time", Type: types.Datetime, Nullable: true},
	{Name: "Collation", Type: types.LongText},
	{Name: "Checksum", Type: types.LongText, Nullable: true},
	{Name: "Create_options", Type: types.LongText, Nullable: true},
	{Name: "Comments", Type: types.LongText, Nullable: true},
}

// Children implements the sql.Node interface.
func (s *ShowTableStatus) Children() []sql.Node { return nil }

// Resolved implements the sql.Node interface.
func (s *ShowTableStatus) Resolved() bool { return true }

// Schema implements the sql.Node interface.
func (s *ShowTableStatus) Schema() sql.Schema { return showTableStatusSchema }

// RowIter implements the sql.Node interface.
func (s *ShowTableStatus) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	tables, err := s.db.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}

	var rows = make([]sql.Row, len(tables))

	for i, tName := range tables {
		table, _, err := s.Catalog.Table(ctx, s.db.Name(), tName)
		if err != nil {
			return nil, err
		}

		var numRows uint64
		var dataLength uint64

		if st, ok := table.(sql.StatisticsTable); ok {
			numRows, err = st.RowCount(ctx)
			if err != nil {
				return nil, err
			}

			dataLength, err = st.DataLength(ctx)
			if err != nil {
				return nil, err
			}
		}

		rows[i] = tableToStatusRow(tName, numRows, dataLength, table.Collation())
	}

	return sql.RowsToRowIter(rows...), nil
}

func (s *ShowTableStatus) String() string {
	return "SHOW TABLE STATUS"
}

// WithChildren implements the Node interface.
func (s *ShowTableStatus) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), 0)
	}

	return s, nil
}

// CheckPrivileges implements the interface sql.Node.
func (s *ShowTableStatus) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	// Some tables won't be visible in RowIter if the user doesn't have the correct privileges
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*ShowTableStatus) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// cc here: https://dev.mysql.com/doc/refman/8.0/en/show-table-status.html
func tableToStatusRow(table string, numRows uint64, dataLength uint64, collation sql.CollationID) sql.Row {
	var avgLength float64 = 0
	if numRows > 0 {
		avgLength = float64(dataLength) / float64(numRows)
	}
	return sql.NewRow(
		table,    // Name
		"InnoDB", // Engine
		// This column is unused. With the removal of .frm files in MySQL 8.0, this
		// column now reports a hardcoded value of 10, which is the last .frm file
		// version used in MySQL 5.7.
		"10",               // Version
		"Fixed",            // Row_format
		numRows,            // Rows
		uint64(avgLength),  // Avg_row_length
		dataLength,         // Data_length
		uint64(0),          // Max_data_length (Unused for InnoDB)
		int64(0),           // Index_length
		int64(0),           // Data_free
		nil,                // Auto_increment (always null)
		nil,                // Create_time
		nil,                // Update_time
		nil,                // Check_time
		collation.String(), // Collation
		nil,                // Checksum
		nil,                // Create_options
		nil,                // Comments
	)
}
