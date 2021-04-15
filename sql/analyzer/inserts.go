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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func resolveInsertRows(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if _, ok := n.(*plan.TriggerExecutor); ok {
		return n, nil
	} else if _, ok := n.(*plan.CreateProcedure); ok {
		return n, nil
	}
	// We capture all INSERTs along the tree, such as those inside of block statements.
	return plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		insert, ok := n.(*plan.InsertInto)
		if !ok {
			return n, nil
		}

		insertable, err := plan.GetInsertable(insert.Destination)
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

		if len(insert.OnDupExprs) > 0 {
			var ok bool
			_, ok = insertable.(sql.UpdatableTable)
			if !ok {
				return nil, plan.ErrOnDuplicateKeyUpdateNotSupported.New()
			}
		}

		source := insert.Source
		// TriggerExecutor has already been analyzed
		if _, ok := insert.Source.(*plan.TriggerExecutor); !ok {
			// Analyze the source of the insert independently
			source, err = a.Analyze(ctx, insert.Source, scope)
			if err != nil {
				return nil, err
			}

			source = stripQueryProcess(source)
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

		err = validateValueCount(columnNames, source)
		if err != nil {
			return nil, err
		}

		project, err := wrapRowSource(ctx, source, insertable, columnNames)
		if err != nil {
			return nil, err
		}

		return insert.WithSource(project), nil
	})
}

// wrapRowSource wraps the original row source in a projection so that its schema matches the full schema of the
// underlying table, in the same order.
func wrapRowSource(ctx *sql.Context, insertSource sql.Node, destTbl sql.Table, columnNames []string) (sql.Node, error) {
	projExprs := make([]sql.Expression, len(destTbl.Schema()))
	for i, f := range destTbl.Schema() {
		found := false
		for j, col := range columnNames {
			if f.Name == col {
				projExprs[i] = expression.NewGetField(j, f.Type, f.Name, f.Nullable)
				found = true
				break
			}
		}

		if !found {
			if !f.Nullable && f.Default == nil && !f.AutoIncrement {
				return nil, sql.ErrInsertIntoNonNullableDefaultNullColumn.New(f.Name)
			}
			projExprs[i] = f.Default
		}

		if f.AutoIncrement {
			ai, err := expression.NewAutoIncrement(ctx, destTbl, projExprs[i])
			if err != nil {
				return nil, err
			}
			projExprs[i] = ai
		}
	}

	err := validateRowSource(insertSource, projExprs)
	if err != nil {
		return nil, err
	}

	return plan.NewProject(projExprs, insertSource), nil
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
	default:
		// Parser assures us that this will be some form of SelectStatement, so no need to type check it
		if len(columnNames) != len(values.Schema()) {
			return plan.ErrInsertIntoMismatchValueCount.New()
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
			if otherCol.Type == sql.Null {
				continue
			}
			_, err := expr.Type().Convert(otherCol.Type.Zero())
			if err != nil {
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
	case *plan.Values:
		// already verified
		return nil
	default:
		// Parser assures us that this will be some form of SelectStatement, so no need to type check it
		return assertCompatibleSchemas(projExprs, n.Schema())
	}
}
