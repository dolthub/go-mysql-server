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
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// validateCreateTrigger handles CreateTrigger nodes, resolving references to "old" and "new" table references in
// the trigger body. Also validates that these old and new references are being used appropriately -- they are only
// valid for certain kinds of triggers and certain statements.
func validateCreateCheck(ctx *sql.Context, a *Analyzer, node sql.Node, scope *Scope) (sql.Node, error) {
	ct, ok := node.(*plan.CreateCheck)
	if !ok {
		return node, nil
	}

	chAlterable, ok := ct.UnaryNode.Child.(sql.Table)
	if !ok {
		return node, nil
	}
	checkCols := make(map[string]bool)
	for _, col := range chAlterable.Schema() {
		checkCols[col.Name] = true
	}

	var err error
	plan.InspectExpressionsWithNode(node, func(n sql.Node, e sql.Expression) bool {
		if _, ok := n.(*plan.CreateCheck); !ok {
			return true
		}

		// Make sure that all columns are valid, in the table, and there are no duplicates
		switch expr := e.(type) {
		case *expression.UnresolvedColumn:
			if _, ok := checkCols[expr.Name()]; !ok {
				err = sql.ErrTableColumnNotFound.New(expr.Name())
				return false
			}
		case *expression.UnresolvedFunction:
			err = sql.ErrInvalidConstraintFunctionsNotSupported.New(expr.String())
			return false
		case *plan.Subquery:
			err = sql.ErrInvalidConstraintSubqueryNotSupported.New(expr.String())
			return false
		}
		return true
	})

	if err != nil {
		return nil, err
	}

	return ct, nil
}

// loadChecks loads any triggers that are required for a plan node to operate properly (except for nodes dealing with
// trigger execution).
func loadChecks(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("loadChecks")
	defer span.Finish()

	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch node := n.(type) {
		case *plan.InsertInto:
			nc := *node
			table, err := plan.GetCheckTable(nc.Destination)

			if err != nil {
				return node, err
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
	parsed, err := sqlparser.ParseStrictDDL(check.AlterStatement)
	if err != nil {
		return nil, err
	}

	ddl, ok := parsed.(*sqlparser.DDL)
	if !ok || ddl.ConstraintAction == "" || len(ddl.TableSpec.Constraints) != 1 || strings.ToLower(ddl.ConstraintAction) != sqlparser.AddStr {
		return nil, parse.ErrInvalidCheckConstraint.New(check.AlterStatement)
	}

	parsedConstraint := ddl.TableSpec.Constraints[0]
	chConstraint, ok := parsedConstraint.Details.(*sqlparser.CheckConstraintDefinition)
	if !ok || chConstraint.Expr == nil {
		return nil, parse.ErrInvalidCheckConstraint.New(check.AlterStatement)
	}

	c, err := parse.ExprToExpression(ctx, chConstraint.Expr)
	if err != nil {
		return nil, err
	}

	return &sql.CheckConstraint{
		Name:     parsedConstraint.Name,
		Expr:     c,
		Enforced: chConstraint.Enforced,
	}, nil
}
