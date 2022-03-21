// Copyright 2022 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

type ShowFunctionStatus struct {
	RoutinesTable sql.Node
}

var _ sql.Node = (*ShowFunctionStatus)(nil)

var showFunctionStatusSchema = sql.Schema{
	&sql.Column{Name: "Db", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Name", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Type", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Definer", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Modified", Type: sql.Datetime, Nullable: false},
	&sql.Column{Name: "Created", Type: sql.Datetime, Nullable: false},
	&sql.Column{Name: "Security_type", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Comment", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "character_set_client", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "collation_connection", Type: sql.LongText, Nullable: false},
	&sql.Column{Name: "Database Collation", Type: sql.LongText, Nullable: false},
}

// NewShowFunctionStatus creates a new *ShowFunctionStatus node.
func NewShowFunctionStatus() *ShowFunctionStatus {
	return &ShowFunctionStatus{}
}

// String implements the sql.Node interface.
func (s *ShowFunctionStatus) String() string {
	return "SHOW PROCEDURE STATUS"
}

// Resolved implements the sql.Node interface.
func (s *ShowFunctionStatus) Resolved() bool {
	return true
}

// Children implements the sql.Node interface.
func (s *ShowFunctionStatus) Children() []sql.Node {
	if s.RoutinesTable == nil {
		return nil
	}
	return []sql.Node{s.RoutinesTable}
}

// Schema implements the sql.Node interface.
func (s *ShowFunctionStatus) Schema() sql.Schema {
	return showFunctionStatusSchema
}

// RowIter implements the sql.Node interface.
func (s *ShowFunctionStatus) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	ri, err := s.RoutinesTable.RowIter(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &showFuncStatusIter{originalIter: ri}, nil
}

// WithChildren implements the sql.Node interface.
func (s *ShowFunctionStatus) WithChildren(children ...sql.Node) (sql.Node, error) {
	expected := len(s.Children())
	if len(children) != expected {
		return nil, sql.ErrInvalidChildrenNumber.New(s, len(children), expected)
	}

	return s, nil
}

// CheckPrivileges implements the interface sql.Node.
func (s *ShowFunctionStatus) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	//TODO: function visibility should be limited by privileges
	return true
}

type showFuncStatusIter struct {
	originalIter sql.RowIter
}

func (sfsi *showFuncStatusIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := sfsi.originalIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	if strings.ToLower(row[4].(string)) != "function" {
		return nil, nil
	}

	var qr = sql.Row{
		row[2],  // Db
		row[3],  // Name
		row[4],  // Type
		row[27], // Definer
		row[24], // Modified
		row[23], // Created
		row[22], // Security_type
		row[26], // Comment
		row[28], // character_set_client
		row[29], // collation_connection
		row[30], // Database Collation
	}

	return qr, nil
}

func (sfsi *showFuncStatusIter) Close(ctx *sql.Context) error {
	return sfsi.originalIter.Close(ctx)
}
