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

type ShowCharset struct {
	pattern string
}

// NewShowCharset returns a new ShowCharset reference.
// like is a "like pattern". If like is an empty string it will return all variables.
func NewShowCharset(like string) *ShowCharset {
	return &ShowCharset{
		pattern: like,
	}
}

// Resolved implements sql.Node interface. The function always returns true.
func (sc *ShowCharset) Resolved() bool {
	return true
}

// WithChildren implements the Node interface.
func (sc *ShowCharset) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(sc, len(children), 0)
	}

	return sc, nil
}

func (sc *ShowCharset) String() string {
	var like string
	if sc.pattern != "" {
		like = fmt.Sprintf(" LIKE '%s'", sc.pattern)
	}
	return fmt.Sprintf("SHOW CHARSET%s", like)
}

func (sc *ShowCharset) Schema() sql.Schema {
	return sql.Schema{
		&sql.Column{Name: "Charset", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "Description", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "Default collation", Type: sql.LongText, Nullable: false},
		&sql.Column{Name: "Maxlen", Type: sql.Uint8, Nullable: false},
	}
}

func (sc *ShowCharset) Children() []sql.Node { return nil }

func (sc *ShowCharset) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var (
		rows []sql.Row
		//like sql.Expression
	)
	//if sc.pattern != "" {
	//	like = expression.NewLike(
	//		expression.NewGetField(0, sql.LongText, "", false),
	//		expression.NewGetField(1, sql.LongText, sc.pattern, false),
	//	)
	//}

	rows = append(rows, sql.Row{"utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci", 4})

	return sql.RowsToRowIter(rows...), nil
}




