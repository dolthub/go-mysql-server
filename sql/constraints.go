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

package sql

import (
	"fmt"
	"strings"
)

// ForeignKeyConstraint declares a constraint between the columns of two tables.
type ForeignKeyConstraint struct {
	Name           string
	Database       string
	Table          string
	Columns        []string
	ParentDatabase string
	ParentTable    string
	ParentColumns  []string
	OnUpdate       ForeignKeyReferentialAction
	OnDelete       ForeignKeyReferentialAction
	IsResolved     bool
}

// ForeignKeyReferentialAction is the behavior for this foreign key with the relevant action is performed on the foreign
// table.
type ForeignKeyReferentialAction string

const (
	ForeignKeyReferentialAction_DefaultAction ForeignKeyReferentialAction = "DEFAULT" // No explicit action was specified
	ForeignKeyReferentialAction_Restrict      ForeignKeyReferentialAction = "RESTRICT"
	ForeignKeyReferentialAction_Cascade       ForeignKeyReferentialAction = "CASCADE"
	ForeignKeyReferentialAction_NoAction      ForeignKeyReferentialAction = "NO ACTION"
	ForeignKeyReferentialAction_SetNull       ForeignKeyReferentialAction = "SET NULL"
	ForeignKeyReferentialAction_SetDefault    ForeignKeyReferentialAction = "SET DEFAULT"
)

// IsSelfReferential returns whether this foreign key represents a self-referential foreign key.
func (f ForeignKeyConstraint) IsSelfReferential() bool {
	return strings.ToLower(f.Database) == strings.ToLower(f.ParentDatabase) &&
		strings.ToLower(f.Table) == strings.ToLower(f.ParentTable)
}

func (f *ForeignKeyConstraint) DebugString() string {
	return fmt.Sprintf(
		"FOREIGN KEY %s (%s) REFERENCES %s (%s)",
		f.Name,
		strings.Join(f.Columns, ","),
		f.ParentTable,
		strings.Join(f.ParentColumns, ","),
	)
}

// CheckDefinition defines a trigger. Integrators are not expected to parse or understand the trigger definitions,
// but must store and return them when asked.
type CheckDefinition struct {
	Name            string // The name of this check. Check names in a database are unique.
	CheckExpression string // String serialization of the check expression
	Enforced        bool   // Whether this constraint is enforced
}

// CheckConstraint declares a boolean-eval constraint.
type CheckConstraint struct {
	Name     string
	Expr     Expression
	Enforced bool
}

type CheckConstraints []*CheckConstraint

// ToExpressions returns the check expressions in these constraints as a slice of sql.Expression
func (checks CheckConstraints) ToExpressions() []Expression {
	exprs := make([]Expression, len(checks))
	for i := range checks {
		exprs[i] = checks[i].Expr
	}
	return exprs
}

// FromExpressions takes a slice of sql.Expression in the same order as these constraints, and returns a new slice of
// constraints with the expressions given, holding names and other properties constant.
func (checks CheckConstraints) FromExpressions(exprs []Expression) (CheckConstraints, error) {
	if len(checks) != len(exprs) {
		return nil, ErrInvalidChildrenNumber.New(checks, len(exprs), len(checks))
	}

	newChecks := make(CheckConstraints, len(checks))
	for i := range exprs {
		nc := *checks[i]
		newChecks[i] = &nc
		newChecks[i].Expr = exprs[i]
	}

	return newChecks, nil
}

func (c CheckConstraint) DebugString() string {
	name := c.Name
	if len(name) > 0 {
		name += " "
	}
	not := ""
	if !c.Enforced {
		not = "not "
	}
	return fmt.Sprintf("%sCHECK %s %sENFORCED", name, DebugString(c.Expr), not)
}
