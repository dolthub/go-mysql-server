// Copyright 2020-2021 Dolthub, Inc.
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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func resolveInsertRows(ctx *sql.Context, a *Analyzer, n sql.Node, scope *plan.Scope, sel RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	if _, ok := n.(*plan.TriggerExecutor); ok {
		return n, transform.SameTree, nil
	} else if _, ok := n.(*plan.CreateProcedure); ok {
		return n, transform.SameTree, nil
	}
	// We capture all INSERTs along the tree, such as those inside of block statements.
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		insert, ok := n.(*plan.InsertInto)
		if !ok {
			return n, transform.SameTree, nil
		}

		table := getResolvedTable(insert.Destination)

		insertable, err := plan.GetInsertable(table)
		if err != nil {
			return nil, transform.SameTree, err
		}

		if insert.IsReplace {
			var ok bool
			_, ok = insertable.(sql.ReplaceableTable)
			if !ok {
				return nil, transform.SameTree, plan.ErrReplaceIntoNotSupported.New()
			}
		}

		if len(insert.OnDupExprs) > 0 {
			var ok bool
			_, ok = insertable.(sql.UpdatableTable)
			if !ok {
				return nil, transform.SameTree, plan.ErrOnDuplicateKeyUpdateNotSupported.New()
			}
		}

		source := insert.Source
		// TriggerExecutor has already been analyzed
		if _, ok := insert.Source.(*plan.TriggerExecutor); !ok {
			// Analyze the source of the insert independently
			source, _, err = a.analyzeWithSelector(ctx, insert.Source, scope, SelectAllBatches, newInsertSourceSelector(sel))
			if err != nil {
				return nil, transform.SameTree, err
			}

			source = StripPassthroughNodes(source)
		}

		dstSchema := insertable.Schema()

		// normalize the column name
		columnNames := make([]string, len(insert.ColumnNames))
		for i, name := range insert.ColumnNames {
			columnNames[i] = strings.ToLower(name)
		}

		// If no columns are given and value tuples are not all empty, use the full schema
		if len(columnNames) == 0 && existsNonZeroValueCount(source) {
			columnNames = make([]string, len(dstSchema))
			for i, f := range dstSchema {
				columnNames[i] = f.Name
			}
		} else {
			err = validateColumns(table.Name(), columnNames, dstSchema)
			if err != nil {
				return nil, transform.SameTree, err
			}
		}

		err = validateValueCount(columnNames, source)
		if err != nil {
			return nil, transform.SameTree, err
		}

		// The schema of the destination node and the underlying table differ subtly in terms of defaults
		project, autoAutoIncrement, err := wrapRowSource(ctx, source, insertable, insert.Destination.Schema(), columnNames)
		if err != nil {
			return nil, transform.SameTree, err
		}

		return insert.WithSource(project).WithUnspecifiedAutoIncrement(autoAutoIncrement), transform.NewTree, nil
	})
}

// Ensures that the number of elements in each Value tuple is empty
func existsNonZeroValueCount(values sql.Node) bool {
	switch node := values.(type) {
	case *plan.Values:
		for _, exprTuple := range node.ExpressionTuples {
			if len(exprTuple) != 0 {
				return true
			}
		}
	default:
		return true
	}
	return false
}

// wrapRowSource returns a projection that wraps the original row source so that its schema matches the full schema of
// the underlying table, in the same order. Also returns a boolean value that indicates whether this row source will
// result in an automatically generated value for an auto_increment column.
func wrapRowSource(ctx *sql.Context, insertSource sql.Node, destTbl sql.Table, schema sql.Schema, columnNames []string) (sql.Node, bool, error) {
	projExprs := make([]sql.Expression, len(schema))
	autoAutoIncrement := false

	for i, f := range schema {
		columnExplicitlySpecified := false
		for j, col := range columnNames {
			if strings.EqualFold(f.Name, col) {
				projExprs[i] = expression.NewGetField(j, f.Type, f.Name, f.Nullable)
				columnExplicitlySpecified = true
				break
			}
		}

		if !columnExplicitlySpecified {
			defaultExpr := f.Default
			if defaultExpr == nil {
				defaultExpr = f.Generated
			}

			if !f.Nullable && defaultExpr == nil && !f.AutoIncrement {
				return nil, false, sql.ErrInsertIntoNonNullableDefaultNullColumn.New(f.Name)
			}
			var err error

			colIdx := make(map[string]int)
			for i, c := range schema {
				colIdx[fmt.Sprintf("%s.%s", strings.ToLower(c.Source), strings.ToLower(c.Name))] = i
			}
			def, _, err := transform.Expr(defaultExpr, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				switch e := e.(type) {
				case *expression.GetField:
					idx, ok := colIdx[strings.ToLower(e.WithTable(destTbl.Name()).String())]
					if !ok {
						return nil, transform.SameTree, fmt.Errorf("field not found: %s", e.String())
					}
					return e.WithIndex(idx), transform.NewTree, nil
				default:
					return e, transform.SameTree, nil
				}
			})
			if err != nil {
				return nil, false, err
			}
			projExprs[i] = def
		}

		if f.AutoIncrement {
			ai, err := expression.NewAutoIncrement(ctx, destTbl, projExprs[i])
			if err != nil {
				return nil, false, err
			}
			projExprs[i] = ai

			if !columnExplicitlySpecified {
				autoAutoIncrement = true
			}
		}
	}

	err := validateRowSource(insertSource, projExprs)
	if err != nil {
		return nil, false, err
	}

	return plan.NewProject(projExprs, insertSource), autoAutoIncrement, nil
}

func validateColumns(tableName string, columnNames []string, dstSchema sql.Schema) error {
	dstColNames := make(map[string]*sql.Column)
	for _, dstCol := range dstSchema {
		dstColNames[strings.ToLower(dstCol.Name)] = dstCol
	}
	usedNames := make(map[string]struct{})
	for _, columnName := range columnNames {
		dstCol, exists := dstColNames[columnName]
		if !exists {
			return plan.ErrInsertIntoNonexistentColumn.New(columnName)
		}
		if dstCol.Generated != nil {
			return sql.ErrGeneratedColumnValue.New(dstCol.Name, tableName)
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
				return sql.ErrInsertIntoMismatchValueCount.New()
			}
		}
	case *plan.LoadData:
		dataColLen := len(node.ColumnNames)
		if dataColLen == 0 {
			dataColLen = len(node.Schema())
		}
		if len(columnNames) != dataColLen {
			return sql.ErrInsertIntoMismatchValueCount.New()
		}
	default:
		// Parser assures us that this will be some form of SelectStatement, so no need to type check it
		if len(columnNames) != len(values.Schema()) {
			return sql.ErrInsertIntoMismatchValueCount.New()
		}
	}
	return nil
}

func assertCompatibleSchemas(projExprs []sql.Expression, schema sql.Schema) error {
	for _, expr := range projExprs {
		switch e := expr.(type) {
		case *expression.Literal,
			*expression.AutoIncrement,
			*sql.ColumnDefaultValue:
			continue
		case *expression.GetField:
			otherCol := schema[e.Index()]
			// special case: null field type, will get checked at execution time
			if otherCol.Type == types.Null {
				continue
			}
			exprType := expr.Type()
			_, _, err := exprType.Convert(otherCol.Type.Zero())
			if err != nil {
				// The zero value will fail when passing string values to ENUM, so we specially handle this case
				if _, ok := exprType.(sql.EnumType); ok && types.IsText(otherCol.Type) {
					continue
				}
				return plan.ErrInsertIntoIncompatibleTypes.New(otherCol.Type.String(), expr.Type().String())
			}
		default:
			return plan.ErrInsertIntoUnsupportedValues.New(expr)
		}
	}
	return nil
}

func validateRowSource(values sql.Node, projExprs []sql.Expression) error {
	if exchange, ok := values.(*plan.Exchange); ok {
		values = exchange.Child
	}

	switch n := values.(type) {
	case *plan.Values, *plan.LoadData:
		// already verified
		return nil
	default:
		// Parser assures us that this will be some form of SelectStatement, so no need to type check it
		return assertCompatibleSchemas(projExprs, n.Schema())
	}
}
