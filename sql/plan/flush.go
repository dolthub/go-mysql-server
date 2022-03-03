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
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/grant_tables"
	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// Flush handles all flush statements. https://dev.mysql.com/doc/refman/8.0/en/flush.html
type Flush struct {
	option *sqlparser.FlushOption
	flushType string
	gt grant_tables.GrantTables
}

var _ sql.Node = (*Flush)(nil)

// NewFlush creates a new Flush node.
func NewFlush(opt *sqlparser.FlushOption, ft string) *Flush {
	return &Flush{
		option: opt,
		flushType: ft,
	}
}

// RowIter implements the sql.Node interface.
func (f *Flush) RowIter(ctx *sql.Context, _ sql.Row) (sql.RowIter, error) {
	switch f.option.Name {
	case "PRIVILEGES":
		err := f.gt.Persist(ctx)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("%s not supported", f.option.Name)
	}

	return sql.RowsToRowIter(sql.Row{sql.NewOkResult(0)}), nil
}

func (*Flush) String() string { return "FLUSH PRIVILEGES" }

// WithChildren implements the Node interface.
func (f *Flush) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}

	return f, nil
}

// CheckPrivileges implements the interface sql.Node.
func (f *Flush) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {

	if opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation("mysql", "", "", sql.PrivilegeType_Reload)) {
		// TODO: some options require additional privileges
		return true
	}
	return false
}

// Resolved implements the sql.Node interface.
func (f *Flush) Resolved() bool {
	return true
}

// Children implements the sql.Node interface.
func (*Flush) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*Flush) Schema() sql.Schema { return nil }
