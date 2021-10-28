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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
)

// ShowStatus implements the SHOW STATUS MySQL command.
// TODO: This is just a stub implementation that returns an empty set. The actual functionality needs to be implemented
// in the future.
type ShowStatus struct {
	modifier ShowStatusModifier
}

var _ sql.Node = (*ShowStatus)(nil)

type ShowStatusModifier byte

const (
	ShowStatusModifier_Session ShowStatusModifier = iota
	ShowStatusModifier_Global
)

// NewShowStatus returns a new ShowStatus reference.
func NewShowStatus(modifier ShowStatusModifier) *ShowStatus {
	return &ShowStatus{modifier: modifier}
}

// Resolved implements sql.Node interface.
func (s *ShowStatus) Resolved() bool {
	return true
}

// String implements sql.Node interface.
func (s *ShowStatus) String() string {
	return "SHOW STATUS"
}

// Schema implements sql.Node interface.
func (s *ShowStatus) Schema() sql.Schema {
	return sql.Schema{
		{Name: "Variable_name", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 64), Default: nil, Nullable: false},
		{Name: "Value", Type: sql.MustCreateStringWithDefaults(sqltypes.VarChar, 2048), Default: nil, Nullable: false},
	}
}

// Children implements sql.Node interface.
func (s *ShowStatus) Children() []sql.Node {
	return nil
}

// RowIter implements sql.Node interface.
func (s *ShowStatus) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return sql.RowsToRowIter(), nil
}

// WithChildren implements sql.Node interface.
func (s *ShowStatus) WithChildren(node ...sql.Node) (sql.Node, error) {
	return NewShowStatus(s.modifier), nil
}
