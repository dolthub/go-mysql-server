// Copyright 2024 Dolthub, Inc.
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

package planbuilder

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) validateInsert(ins *plan.InsertInto) {
	table := getResolvedTable(ins.Destination)
	if table == nil {
		return
	}

	insertable, err := plan.GetInsertable(table)
	if err != nil {
		b.handleErr(err)
	}

	if ins.IsReplace {
		var ok bool
		_, ok = insertable.(sql.ReplaceableTable)
		if !ok {
			err := plan.ErrReplaceIntoNotSupported.New()
			b.handleErr(err)
		}
	}

	if len(ins.OnDupExprs) > 0 {
		var ok bool
		_, ok = insertable.(sql.UpdatableTable)
		if !ok {
			err := plan.ErrOnDuplicateKeyUpdateNotSupported.New()
			b.handleErr(err)
		}
	}

	// normalize the column name
	dstSchema := insertable.Schema()
	columnNames := make([]string, len(ins.ColumnNames))
	for i, name := range ins.ColumnNames {
		columnNames[i] = strings.ToLower(name)
	}

	// If no columns are given and value tuples are not all empty, use the full schema
	if len(columnNames) == 0 && existsNonZeroValueCount(ins.Source) {
		columnNames = make([]string, len(dstSchema))
		for i, f := range dstSchema {
			columnNames[i] = f.Name
		}
	}

	if len(ins.ColumnNames) > 0 {
		err := validateColumns(table.Name(), columnNames, dstSchema, ins.Source)
		if err != nil {
			b.handleErr(err)
		}
	}

	err = validateValueCount(columnNames, ins.Source)
	if err != nil {
		b.handleErr(err)
	}
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

func validateColumns(tableName string, columnNames []string, dstSchema sql.Schema, source sql.Node) error {
	dstColNames := make(map[string]*sql.Column)
	for _, dstCol := range dstSchema {
		dstColNames[strings.ToLower(dstCol.Name)] = dstCol
	}
	usedNames := make(map[string]struct{})
	for i, columnName := range columnNames {
		dstCol, exists := dstColNames[columnName]
		if !exists {
			return plan.ErrInsertIntoNonexistentColumn.New(columnName)
		}
		if dstCol.Generated != nil && !validGeneratedColumnValue(i, source) {
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
