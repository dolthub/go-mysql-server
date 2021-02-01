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
)

// ShowWarnings is a node that shows the session warnings
type ShowWarnings []*sql.Warning

// Resolved implements sql.Node interface. The function always returns true.
func (ShowWarnings) Resolved() bool {
	return true
}

// WithChildren implements the Node interface.
func (sw ShowWarnings) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(sw, len(children), 0)
	}

	return sw, nil
}

// String implements the fmt.Stringer interface.
func (ShowWarnings) String() string {
	return "SHOW WARNINGS"
}

// Schema returns a new Schema reference for "SHOW VARIABLES" query.
func (ShowWarnings) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Level", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "Code", Type: sql.Int32, Nullable: true},
		&sql.Column{Name: "Message", Type: sql.LongText, Nullable: false},
	}
}

// Children implements sql.Node interface. The function always returns nil.
func (ShowWarnings) Children() []sql.Node { return nil }

// RowIter implements the sql.Node interface.
// The function returns an iterator for warnings (considering offset and counter)
func (sw ShowWarnings) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	for _, w := range sw {
		rows = append(rows, sql.NewRow(w.Level, w.Code, w.Message))
	}

	return sql.RowsToRowIter(rows...), nil
}
