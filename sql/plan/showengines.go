// Copyright 2021 Dolthub, Inc.
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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

type ShowEngines struct {
	pattern string
}

// NewShowEngines returns a new ShowEngines reference.
// like is a "like pattern". If like is an empty string it will return all variables.
func NewShowEngines(like string) *ShowEngines {
	return &ShowEngines{
		pattern: like,
	}
}

// Resolved implements sql.Node interface. The function always returns true.
func (se *ShowEngines) Resolved() bool {
	return true
}

// WithChildren implements the Node interface.
func (se *ShowEngines) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(se, len(children), 0)
	}

	return se, nil
}

func (se *ShowEngines) String() string {
	var like string
	if se.pattern != "" {
		like = fmt.Sprintf(" LIKE '%s'", se.pattern)
	}
	return fmt.Sprintf("SHOW CHARSET%s", like)
}

func (se *ShowEngines) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "ENGINE", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "SUPPORT", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "COMMENT", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "TRANSACTIONS", Type: sql.Uint8, Nullable: true},
		&sql.Column{Name: "XA", Type: sql.Uint8, Nullable: true},
		&sql.Column{Name: "SAVEPOINTS", Type: sql.Uint8, Nullable: true},
	}
}

func (se *ShowEngines) Children() []sql.Node { return nil }

func (se *ShowEngines) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var (
		rows []sql.Row
		//like sql.Expression
	)
	//if se.pattern != "" {
	//	like = expression.NewLike(
	//		expression.NewGetField(0, sql.LongText, "", false),
	//		expression.NewGetField(1, sql.LongText, se.pattern, false),
	//	)
	//}

	rows = append(rows, sql.Row{"ARCHIVE", "YES", "Archive storage engine", "NO", nil, nil})

	return sql.RowsToRowIter(rows...), nil
}
