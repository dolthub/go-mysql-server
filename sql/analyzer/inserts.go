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

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function"
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

		source := insert.Source
		// TriggerExecutor has already been analyzed
		if _, ok := insert.Source.(*plan.TriggerExecutor); !ok {
			// Analyze the source of the insert independently
			if _, ok := insert.Source.(*plan.Values); ok {
				scope = scope.NewScope(plan.NewProject(
					expression.SchemaToGetFields(insert.Source.Schema()[:len(insert.ColumnNames)], sql.ColSet{}),
					plan.NewSubqueryAlias("dummy", "", insert.Source),
				))
			}
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

	// Handle auto UUID columns
	autoUuidCol, autoUuidColIdx := findAutoUuidColumn(ctx, schema)
	if autoUuidCol != nil {
		if columnDefaultValue, ok := projExprs[autoUuidColIdx].(*sql.ColumnDefaultValue); ok {
			// If the auto UUID column is being populated through the projection (i.e. it's projecting a
			// ColumnDefaultValue to create the UUID), then update the project to include the AutoUuid expression.
			// TODO: This should be extracted to a reusable function... make sure the AutoUUID expression is
			//       created directly on top of the UUID function. Probably good to add a runtime check in the
			//       constructor for that, too.
			// TODO: Add a test case to trigger this issue (default column is projected, but values are used for non-UUID cols)
			projExprs[autoUuidColIdx] = expression.NewAutoUuid(ctx, autoUuidCol, columnDefaultValue)
		} else {
			// Otherwise, if the auto UUID column is not getting populated through the projection, then we
			// need to look through the tuples to look for the first DEFAULT or UUID() expression and apply
			// the AutoUuid expression to it.
			err := wrapAutoUuidInValuesTuples(ctx, autoUuidCol, insertSource, columnNames)
			if err != nil {
				return nil, false, err
			}
		}
	}

	err := validateRowSource(insertSource, projExprs)
	if err != nil {
		return nil, false, err
	}

	return plan.NewProject(projExprs, insertSource), autoAutoIncrement, nil
}

func findAutoUuidColumn(ctx *sql.Context, schema sql.Schema) (autoUuidCol *sql.Column, autoUuidColIdx int) {
	for i, col := range schema {
		if columnIsAutoUuid(col) {
			return col, i
		}
	}

	return nil, -1
}

func wrapAutoUuidInValuesTuples(ctx *sql.Context, autoUuidCol *sql.Column, insertSource sql.Node, columnNames []string) error {
	// Else... if the source is a *plan.Values, then search the tuples for the first DEFAULT expression, or the
	// first UUID() expression, or the first UUID_TO_BIN(UUID()) expression tree and wrap it in an expression.AutoUuid
	// to capture the generated value
	values, ok := insertSource.(*plan.Values)
	if !ok {
		// If the insert source isn't value tuples, then we don't need to do anything
		return nil
	}

	// Search the column names in the Values tuples to find the right tuple index
	autoUuidColTupleIdx := -1
	for i, columnName := range columnNames {
		if strings.ToLower(autoUuidCol.Name) == strings.ToLower(columnName) {
			autoUuidColTupleIdx = i
		}
	}
	if autoUuidColTupleIdx == -1 {
		return nil
	}

	for _, tuple := range values.ExpressionTuples {
		expr := tuple[autoUuidColTupleIdx]
		if wrapper, ok := expr.(*expression.Wrapper); ok {
			expr = wrapper.Unwrap()
		}

		if columnDefaultValue, ok := expr.(*sql.ColumnDefaultValue); ok {
			switch t := columnDefaultValue.Expr.(type) {
			case *function.UUIDFunc:
				autoUuid := expression.NewAutoUuid(ctx, autoUuidCol, t)
				expr, err := columnDefaultValue.WithChildren(autoUuid)
				if err != nil {
					return err
				}
				tuple[autoUuidColTupleIdx] = expr
			case *function.UUIDToBin:
				children := t.Children()
				autoUuid := expression.NewAutoUuid(ctx, autoUuidCol, children[0])
				children[0] = autoUuid
				uuidToBin, err := t.WithChildren(children...)
				if err != nil {
					return err
				}
				expr, err := columnDefaultValue.WithChildren(uuidToBin)
				if err != nil {
					return err
				}
				tuple[autoUuidColTupleIdx] = expr
			default:
				return fmt.Errorf("unexpected column default expression for auto UUID column: %T", t)
			}
			break
		}

		if _, ok := expr.(*function.UUIDFunc); ok {
			tuple[autoUuidColTupleIdx] = expression.NewAutoUuid(ctx, autoUuidCol, tuple[autoUuidColTupleIdx])
			break
		}

		if uuidToBinFunc, ok := expr.(*function.UUIDToBin); ok {
			if uuidFunc, ok := uuidToBinFunc.Children()[0].(*function.UUIDFunc); ok {
				children := uuidToBinFunc.Children()
				autoUuid := expression.NewAutoUuid(ctx, autoUuidCol, uuidFunc)
				children[0] = autoUuid
				expr, err := uuidToBinFunc.WithChildren(children...)
				if err != nil {
					return err
				}
				tuple[autoUuidColTupleIdx] = expr
				break
			}
		}
	}

	return nil
}

// TODO: Move to AutoUuid file?
func columnIsAutoUuid(col *sql.Column) bool {
	if col.PrimaryKey == false {
		return false
	}

	switch col.Type.Type() {
	case sqltypes.Char, sqltypes.VarChar:
		stringType := col.Type.(sql.StringType)
		if stringType.MaxCharacterLength() != 36 || col.Default == nil {
			return false
		}
		if _, ok := col.Default.Expr.(*function.UUIDFunc); ok {
			return true
		}
	case sqltypes.Binary, sqltypes.VarBinary:
		stringType := col.Type.(sql.StringType)
		if stringType.MaxByteLength() != 16 || col.Default == nil {
			return false
		}
		if uuidToBinFunc, ok := col.Default.Expr.(*function.UUIDToBin); ok {
			if _, ok := uuidToBinFunc.Children()[0].(*function.UUIDFunc); ok {
				return true
			}
		}
	}

	return false
}

// validGeneratedColumnValue returns true if the column is a generated column and the source node is not a values node.
// Explicit default values (`DEFAULT`) are the only valid values to specify for a generated column
func validGeneratedColumnValue(idx int, source sql.Node) bool {
	switch source := source.(type) {
	case *plan.Values:
		for _, tuple := range source.ExpressionTuples {
			switch val := tuple[idx].(type) {
			case *sql.ColumnDefaultValue: // should be wrapped, but just in case
				return true
			case *expression.Wrapper:
				if _, ok := val.Unwrap().(*sql.ColumnDefaultValue); ok {
					return true
				}
				return false
			default:
				return false
			}
		}
		return false
	default:
		return false
	}
}

func assertCompatibleSchemas(projExprs []sql.Expression, schema sql.Schema) error {
	for _, expr := range projExprs {
		switch e := expr.(type) {
		case *expression.Literal,
			*expression.AutoIncrement,
			*expression.AutoUuid,
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
