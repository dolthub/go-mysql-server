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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func validateCreateTable(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	ct, ok := n.(*plan.CreateTable)
	if !ok {
		return n, nil
	}

	ct.

	return n, nil
}

func validateAutoIncrement(schema sql.Schema) error {
	seen := false
	for _, col := range schema {
		if col.AutoIncrement {
			if !col.PrimaryKey {
				// AUTO_INCREMENT col must be a pk
				return sql.ErrInvalidAutoIncCols.New()
			}
			if col.Default != nil {
				// AUTO_INCREMENT col cannot have default
				return sql.ErrInvalidAutoIncCols.New()
			}
			if seen {
				// there can be at most one AUTO_INCREMENT col
				return sql.ErrInvalidAutoIncCols.New()
			}
			seen = true
		}
	}
	return nil
}