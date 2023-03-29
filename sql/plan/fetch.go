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
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/go-mysql-server/sql"
)

// Fetch represents the FETCH statement, which handles value acquisition from cursors.
type Fetch struct {
	Name      string
	Variables []string
	innerSet  *Set
	pRef      *expression.ProcedureReference
	sch       sql.Schema
}

var _ sql.Node = (*Fetch)(nil)
var _ sql.CollationCoercible = (*Fetch)(nil)
var _ expression.ProcedureReferencable = (*Fetch)(nil)

// NewFetch returns a new *Fetch node.
func NewFetch(name string, variables []string) *Fetch {
	exprs := make([]sql.Expression, len(variables))
	for i := range variables {
		exprs[i] = expression.NewSetField(
			expression.NewUnresolvedColumn(variables[i]),
			expression.NewGetField(i, types.Null, "", true),
		)
	}
	return &Fetch{
		Name:      name,
		Variables: variables,
		innerSet:  NewSet(exprs),
	}
}

// Resolved implements the interface sql.Node.
func (f *Fetch) Resolved() bool {
	return f.innerSet.Resolved()
}

// String implements the interface sql.Node.
func (f *Fetch) String() string {
	return fmt.Sprintf("FETCH %s INTO %s", f.Name, strings.Join(f.Variables, ", "))
}

// Schema implements the interface sql.Node.
func (f *Fetch) Schema() sql.Schema {
	return nil
}

// Children implements the interface sql.Node.
func (f *Fetch) Children() []sql.Node {
	return []sql.Node{f.innerSet}
}

// WithChildren implements the interface sql.Node.
func (f *Fetch) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 1)
	}

	var ok bool
	nf := *f
	nf.innerSet, ok = children[0].(*Set)
	if !ok {
		return nil, fmt.Errorf("FETCH expected SET child")
	}
	return &nf, nil
}

// CheckPrivileges implements the interface sql.Node.
func (f *Fetch) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*Fetch) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// RowIter implements the interface sql.Node.
func (f *Fetch) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	row, sch, err := f.pRef.FetchCursor(ctx, f.Name)
	if err == io.EOF {
		return sql.RowsToRowIter(), f.pRef.HandleError(ctx, err)
	} else if err != nil {
		return nil, err
	}
	if len(row) != len(f.innerSet.Exprs) {
		return nil, sql.ErrFetchIncorrectCount.New()
	}
	if f.sch == nil {
		f.sch = sch
		for i, expr := range f.innerSet.Exprs {
			setExpr, ok := expr.(*expression.SetField)
			if !ok {
				return nil, fmt.Errorf("expected SetField expression in FETCH")
			}
			col := sch[i]
			setExpr.Right = expression.NewGetField(i, col.Type, col.Name, col.Nullable)
		}
	}
	return f.innerSet.RowIter(ctx, row)
}

// WithParamReference implements the interface expression.ProcedureReferencable.
func (f *Fetch) WithParamReference(pRef *expression.ProcedureReference) sql.Node {
	nf := *f
	nf.pRef = pRef
	return &nf
}

// fetchIter is the sql.RowIter of *Fetch.
type fetchIter struct {
	*Fetch
	rowIter sql.RowIter
}

var _ sql.RowIter = (*fetchIter)(nil)

// Next implements the interface sql.RowIter.
func (f *fetchIter) Next(ctx *sql.Context) (sql.Row, error) {
	row, err := f.rowIter.Next(ctx)
	if err != nil {
		return nil, err
	}
	return row, nil
}

// Close implements the interface sql.RowIter.
func (f *fetchIter) Close(ctx *sql.Context) error {
	return f.rowIter.Close(ctx)
}
