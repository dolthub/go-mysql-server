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

package analyzer

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// defaultIndexPrefixLength is the index prefix length that this analyzer rule applies automatically to TEXT columns
// in secondary indexes.
const defaultIndexPrefixLength = 255

// AddImplicitPrefixLengths searches the |node| tree for any nodes creating an index, and plugs in a default index
// prefix length for any TEXT columns in those new indexes. This rule is intended to be used for Postgres compatibility,
// since Postgres does not require specifying prefix lengths for TEXT columns.
func AddImplicitPrefixLengths(_ *sql.Context, _ *Analyzer, node sql.Node, _ *plan.Scope, _ RuleSelector, _ *sql.QueryFlags) (sql.Node, transform.TreeIdentity, error) {
	var initialSchema, targetSchema sql.Schema
	transform.Inspect(node, func(node sql.Node) bool {
		if st, ok := node.(sql.SchemaTarget); ok {
			targetSchema = st.TargetSchema()
			initialSchema = targetSchema.Copy()
			return false
		}
		return true
	})

	// Recurse through the node tree to fill in prefix lengths. Note that some statements come in as Block nodes
	// that contain multiple nodes, so we need to recurse through and handle all of them.
	return transform.Node(node, func(node sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch node := node.(type) {
		case *plan.AddColumn:
			// For any AddColumn nodes, we need to update the target schema with the column being added, otherwise
			// we won't be able to find those columns if they are also being added to a secondary index.
			var err error
			targetSchema, err = validateAddColumn(initialSchema, targetSchema, node)
			if err != nil {
				return nil, transform.SameTree, err
			}

		case *plan.CreateTable:
			newIndexes := make([]*sql.IndexDef, len(node.Indexes()))
			for i := range node.Indexes() {
				copy := *node.Indexes()[i]
				newIndexes[i] = &copy
			}
			indexModified := false
			for _, index := range newIndexes {
				targetSchema := node.TargetSchema()
				colMap := schToColMap(targetSchema)

				for i, _ := range index.Columns {
					col, ok := colMap[index.Columns[i].Name]
					if !ok {
						return nil, false, fmt.Errorf("indexed column %s not found in schema", index.Columns[i].Name)
					}
					if types.IsText(col.Type) && index.Columns[i].Length == 0 {
						index.Columns[i].Length = defaultIndexPrefixLength
						indexModified = true
					}
				}
			}
			if indexModified {
				newNode, err := node.WithIndexDefs(newIndexes)
				return newNode, transform.NewTree, err
			}

		case *plan.AlterIndex:
			if node.Action == plan.IndexAction_Create {
				colMap := schToColMap(targetSchema)
				newColumns := make([]sql.IndexColumn, len(node.Columns))
				for i := range node.Columns {
					copy := node.Columns[i]
					newColumns[i] = copy
				}
				indexModified := false
				for i, _ := range newColumns {
					col, ok := colMap[newColumns[i].Name]
					if !ok {
						return nil, false, fmt.Errorf("indexed column %s not found in schema", newColumns[i].Name)
					}
					if types.IsText(col.Type) && newColumns[i].Length == 0 {
						newColumns[i].Length = defaultIndexPrefixLength
						indexModified = true
					}
				}
				if indexModified {
					newNode, err := node.WithColumns(newColumns)
					return newNode, transform.NewTree, err
				}
			}
		}
		return node, transform.SameTree, nil
	})
}
