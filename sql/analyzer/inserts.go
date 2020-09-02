// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

func applyInsertRowSource(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	insert, ok := n.(*plan.InsertInto)
	if !ok {
		return n, nil
	}

	insertable, err := getInsertable(insert.Left)
	if err != nil {
		return nil, err
	}

	if insert.IsReplace {
		var ok bool
		_, ok = insertable.(sql.ReplaceableTable)
		if !ok {
			return nil, plan.ErrReplaceIntoNotSupported.New()
		}
	}

	dstSchema := insertable.Schema()

	// If no columns are given, use the full schema
	columnNames := insert.ColumnNames
	if len(columnNames) == 0 {
		columnNames = make([]string, len(dstSchema))
		for i, f := range dstSchema {
			columnNames[i] = f.Name
		}
	} else {
		err = validateColumns(columnNames, dstSchema)
		if err != nil {
			return nil, err
		}
	}

	err = validateValueCount(columnNames, insert.Right)
	if err != nil {
		return nil, err
	}

	projExprs := make([]sql.Expression, len(dstSchema))
	for i, f := range dstSchema {
		found := false
		for j, col := range columnNames {
			if f.Name == col {
				projExprs[i] = expression.NewGetField(j, f.Type, f.Name, f.Nullable)
				found = true
				break
			}
		}

		if !found {
			if !f.Nullable && f.Default == nil {
				return nil, plan.ErrInsertIntoNonNullableDefaultNullColumn.New(f.Name)
			}
			projExprs[i] = expression.NewLiteral(f.Default, f.Type)
		}
	}

	rowSource, err := rowSource(insert.Right, projExprs)
	if err != nil {
		return nil, err
	}

	return insert.WithChildren(insert.Left, rowSource)
}

func validateColumns(columnNames []string, dstSchema sql.Schema) error {
	dstColNames := make(map[string]struct{})
	for _, dstCol := range dstSchema {
		dstColNames[dstCol.Name] = struct{}{}
	}
	usedNames := make(map[string]struct{})
	for _, columnName := range columnNames {
		if _, exists := dstColNames[columnName]; !exists {
			return plan.ErrInsertIntoNonexistentColumn.New(columnName)
		}
		if _, exists := usedNames[columnName]; !exists {
			usedNames[columnName] = struct{}{}
		} else {
			return plan.ErrInsertIntoDuplicateColumn.New(columnName)
		}
	}
	return nil
}

func validateValueCount(columnNames []string, values sql.Node) error {
	if exchange, ok := values.(*plan.Exchange); ok {
		values = exchange.Child
	}

	switch node := values.(type) {
	case *plan.Values:
		for _, exprTuple := range node.ExpressionTuples {
			if len(exprTuple) != len(columnNames) {
				return plan.ErrInsertIntoMismatchValueCount.New()
			}
		}
	case *plan.ResolvedTable, *plan.Project, *plan.InnerJoin, *plan.Filter:
		if len(columnNames) != len(values.Schema()) {
			return plan.ErrInsertIntoMismatchValueCount.New()
		}
	default:
		return plan.ErrInsertIntoUnsupportedValues.New(node)
	}
	return nil
}

func getInsertable(node sql.Node) (sql.InsertableTable, error) {
	switch node := node.(type) {
	case *plan.Exchange:
		return getInsertable(node.Child)
	case sql.InsertableTable:
		return node, nil
	case *plan.ResolvedTable:
		return getInsertableTable(node.Table)
	default:
		return nil, plan.ErrInsertIntoNotSupported.New()
	}
}

func getInsertableTable(t sql.Table) (sql.InsertableTable, error) {
	switch t := t.(type) {
	case sql.InsertableTable:
		return t, nil
	case sql.TableWrapper:
		return getInsertableTable(t.Underlying())
	default:
		return nil, plan.ErrInsertIntoNotSupported.New()
	}
}

func assertCompatibleSchemas(projExprs []sql.Expression, schema sql.Schema) error {
	for _, expr := range projExprs {
		switch e := expr.(type) {
		case *expression.Literal:
			continue
		case *expression.GetField:
			otherCol := schema[e.Index()]
			_, err := otherCol.Type.Convert(expr.Type().Zero())
			if err != nil {
				return plan.ErrInsertIntoIncompatibleTypes.New(otherCol.Type.String(), expr.Type().String())
			}
		default:
			return plan.ErrInsertIntoUnsupportedValues.New(expr)
		}
	}
	return nil
}

func rowSource(values sql.Node, projExprs []sql.Expression) (sql.Node, error) {
	if exchange, ok := values.(*plan.Exchange); ok {
		values = exchange.Child
	}

	switch n := values.(type) {
	case *plan.Values:
		return plan.NewProject(projExprs, n), nil
	case *plan.ResolvedTable, *plan.Project, *plan.InnerJoin, *plan.Filter:
		if err := assertCompatibleSchemas(projExprs, n.Schema()); err != nil {
			return nil, err
		}
		return plan.NewProject(projExprs, n), nil
	default:
		return nil, plan.ErrInsertIntoUnsupportedValues.New(n)
	}
}
