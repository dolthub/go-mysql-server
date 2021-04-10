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

package analyzer

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// validateCreateCheck legal expressions for CREATE CHECK statements, including those embedded in CREATE TABLE
// statements
func validateCreateCheck(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	switch n := n.(type) {
	case *plan.CreateCheck:
		return validateCreateCheckNode(n)
	case *plan.CreateTable:
		return validateCreateTableChecks(n)
	}

	return n, nil
}

func validateCreateTableChecks(n *plan.CreateTable) (sql.Node, error) {
	// TODO: make sure all the columns in the CHECK statement are valid. resolve_columns doesn't do this for us because
	//  it special cases CreateTable nodes
	var err error
	plan.InspectExpressions(n, func(e sql.Expression) bool {
		switch e := e.(type) {
		case *expression.Wrapper:
			// column defaults, no need to inspect these
			return false
		default:
			// check expressions, must be validated
			// TODO: would be better to wrap these in something else to be able to identify them better
			err = checkExpressionValid(e)
			if err != nil {
				return false
			}
			return true
		}
	})

	if err != nil {
		return nil, err
	}

	return n, nil
}

func validateCreateCheckNode(ct *plan.CreateCheck) (sql.Node, error) {
	err := checkExpressionValid(ct.Check.Expr)
	if err != nil {
		return nil, err
	}

	return ct, nil
}

func checkExpressionValid(e sql.Expression) error {
	var err error
	sql.Inspect(e, func(e sql.Expression) bool {
		switch e := e.(type) {
		// TODO: deterministic functions are fine
		case sql.FunctionExpression:
			err = sql.ErrInvalidConstraintFunctionsNotSupported.New(e.String())
			return false
		case *plan.Subquery:
			err = sql.ErrInvalidConstraintSubqueryNotSupported.New(e.String())
			return false
		}
		return true
	})
	return err
}

// loadChecks loads any checks that are required for a plan node to operate properly (except for nodes dealing with
// check execution).
func loadChecks(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("loadChecks")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch node := n.(type) {
		case *plan.InsertInto:
			nc := *node

			rtable, ok := nc.Destination.(*plan.ResolvedTable)
			if !ok {
				return node, nil
			}

			table, ok := rtable.Table.(sql.CheckAlterableTable)
			if !ok {
				return node, nil
			}

			loadedChecks, err := loadChecksFromTable(ctx, table)
			if err != nil {
				return nil, err
			}

			if len(loadedChecks) != 0 {
				nc.Checks = loadedChecks
			} else {
				nc.Checks = make([]sql.Expression, 0)
			}

			return &nc, nil
		// TODO : reimplement modify column nodes and throw errors here to protect check columns
		//case *plan.DropColumn:
		//case *plan.RenameColumn:
		//case *plan.ModifyColumn:
		default:
			return node, nil
		}
	})
}

func loadChecksFromTable(ctx *sql.Context, table sql.Table) ([]sql.Expression, error) {
	var loadedChecks []sql.Expression
	if checkTable, ok := table.(sql.CheckTable); ok {
		checks, err := checkTable.GetChecks(ctx)
		if err != nil {
			return nil, err
		}
		for _, ch := range checks {
			constraint, err := convertCheckDefToConstraint(ctx, &ch)
			if err != nil {
				return nil, err
			}
			if constraint.Enforced {
				loadedChecks = append(loadedChecks, constraint.Expr)
			}
		}
	}
	return loadedChecks, nil
}

func convertCheckDefToConstraint(ctx *sql.Context, check *sql.CheckDefinition) (*sql.CheckConstraint, error) {
	parseStr := fmt.Sprintf("select %s", check.CheckExpression)
	parsed, err := sqlparser.Parse(parseStr)
	if err != nil {
		return nil, err
	}

	selectStmt, ok := parsed.(*sqlparser.Select)
	if !ok || len(selectStmt.SelectExprs) != 1 {
		return nil, parse.ErrInvalidCheckConstraint.New(check.CheckExpression)
	}

	expr := selectStmt.SelectExprs[0]
	ae, ok := expr.(*sqlparser.AliasedExpr)
	if !ok {
		return nil, parse.ErrInvalidCheckConstraint.New(check.CheckExpression)
	}

	c, err := parse.ExprToExpression(ctx, ae.Expr)
	if err != nil {
		return nil, err
	}

	return &sql.CheckConstraint{
		Name:     check.Name,
		Expr:     c,
		Enforced: check.Enforced,
	}, nil
}
