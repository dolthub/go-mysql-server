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
	"github.com/liquidata-inc/go-mysql-server/sql/plan"
)

// applyUpdateAccumulators wraps any Insert, Update, or Delete nodes with RowUpdateAccumulators to tally the results
// for report to the client.
func applyUpdateAccumulators(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	// Scope will be non-null in the case of trigger execution analysis. We don't want to apply update accumulators in
	// that case.
	// TODO: probably better to just remove this rule from the analyzer in that specific case
	if scope != nil {
		return n, nil
	}

	switch n := n.(type) {
	case *plan.InsertInto:
		if n.IsReplace {
			return plan.NewRowUpdateAccumulator(n, plan.UpdateTypeReplace), nil
		} else if len(n.OnDupExprs) > 0 {
			return plan.NewRowUpdateAccumulator(n, plan.UpdateTypeDuplicateKeyUpdate), nil
		}
		return plan.NewRowUpdateAccumulator(n, plan.UpdateTypeInsert), nil
	case *plan.DeleteFrom:
		return plan.NewRowUpdateAccumulator(n, plan.UpdateTypeDelete), nil
	case *plan.Update:
		return plan.NewRowUpdateAccumulator(n, plan.UpdateTypeUpdate), nil
	default:
		return n, nil
	}
}
