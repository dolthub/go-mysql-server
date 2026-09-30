// Copyright 2026 Dolthub, Inc.
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

package sql

// UpdateExpressionApplier evaluates the assignments for a row in an UPDATE statement.
// It does not apply to procedural SET or INSERT ON DUPLICATE KEY UPDATE statements.
type UpdateExpressionApplier interface {
	// ApplyRowUpdate returns the updated row without modifying oldRow. The result must
	// preserve the input row's length and unassigned fields, including outer-scope fields.
	// updateExprs separates explicit assignments from derived column updates; the applier
	// is responsible for both phases, assignment conversions, and IGNORE handling.
	// tableSchema describes the table or joined tables, excluding any outer-scope prefix.
	ApplyRowUpdate(ctx *Context, updateExprs *UpdateExprs, tableSchema Schema, oldRow Row, ignore bool) (Row, error)
}

// UpdateExprs holds explicit assignments followed by derived column updates.
type UpdateExprs struct {
	exprs []Expression
	// numExplicitExprs is the number of explicit update expressions. Explicit updates are updates that are explicitly
	// part of a query, as opposed to derived updates, which are derived from the table's column definitions.
	// numExplicitExprs is used to index into exprs to separate explicit and derived expressions when needed
	numExplicitExprs int
}

func NewUpdateExprs(exprs []Expression, numExplicitExprs int) *UpdateExprs {
	return &UpdateExprs{
		exprs:            exprs,
		numExplicitExprs: numExplicitExprs,
	}
}

func (ue *UpdateExprs) AllExpressions() []Expression {
	if ue == nil {
		return nil
	}
	return ue.exprs
}

func (ue *UpdateExprs) WithExpressions(newExprs []Expression) (*UpdateExprs, error) {
	length := ue.Length()
	if len(newExprs) != length {
		return nil, ErrInvalidExpressionNumber.New(ue, length, 1)
	}
	if length == 0 {
		return ue, nil
	}
	ret := *ue
	ret.exprs = newExprs
	return &ret, nil
}

// ExplicitUpdateExprs returns update expressions that are explicitly part of a query.
func (ue *UpdateExprs) ExplicitUpdateExprs() []Expression {
	return ue.exprs[:ue.numExplicitExprs]
}

// DerivedUpdateExprs returns update expressions derived from a table's column definition. This includes
// updates on generated columns and ON UPDATE columns. Derived update expressions should only be applied when explicit
// updates actually yield a change in the row's values
func (ue *UpdateExprs) DerivedUpdateExprs() []Expression {
	return ue.exprs[ue.numExplicitExprs:]
}

func (ue *UpdateExprs) Resolved() bool {
	if ue == nil {
		return true
	}
	for _, expr := range ue.exprs {
		if !expr.Resolved() {
			return false
		}
	}
	return true
}

func (ue *UpdateExprs) Length() int {
	if ue == nil {
		return 0
	}
	return len(ue.exprs)
}

func (ue *UpdateExprs) HasUpdates() bool {
	return ue != nil && len(ue.exprs) > 0
}

func (ue *UpdateExprs) HasDerivedUpdates() bool {
	return ue != nil && len(ue.exprs) > ue.numExplicitExprs
}
