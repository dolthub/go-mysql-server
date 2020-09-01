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
	// TODO: update, delete
	if insert, ok := n.(*plan.InsertInto); ok {
		return plan.NewRowUpdateAccumulator(insert), nil
	}

	return n, nil
}

