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

func resolveNewAndOldReferences(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	switch n := n.(type) {
	case *plan.CreateTrigger:
		return plan.TransformExpressionsUp(n, func(e sql.Expression) (sql.Expression, error) {
			switch e := e.(type) {
			case *deferredColumn:
				// For create triggers, we just want to verify that the trigger is correctly defined before creating it.
				// So replace new and old with the table name the trigger is defined on.
				// TODO: this might work badly for databases with tables named new and old. Needs tests.
				if e.Table() == "new" || e.Table() == "old" {
					return expression.NewUnresolvedQualifiedColumn(n.Table.(sql.Nameable).Name(), e.Name()), nil
				}
			}
			return e, nil
		})
	}

	return n, nil
}
