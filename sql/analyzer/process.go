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
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// trackProcess will wrap the query in a process node and add progress items
// to the already existing process.
func trackProcess(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	if !n.Resolved() {
		return n, nil
	}

	if _, ok := n.(*plan.QueryProcess); ok {
		return n, nil
	}

	processList := ctx.ProcessList

	var seen = make(map[string]struct{})
	n, err := plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		switch n := n.(type) {
		case *plan.ResolvedTable:
			switch n.Table.(type) {
			case *plan.ProcessTable, *plan.ProcessIndexableTable:
				return n, nil
			}

			name := n.Table.Name()
			if _, ok := seen[name]; ok {
				return n, nil
			}

			var total int64 = -1
			if counter, ok := n.Table.(sql.PartitionCounter); ok {
				count, err := counter.PartitionCount(ctx)
				if err != nil {
					return nil, err
				}
				total = count
			}
			processList.AddTableProgress(ctx.Pid(), name, total)

			seen[name] = struct{}{}

			onPartitionDone := func(partitionName string) {
				processList.UpdateTableProgress(ctx.Pid(), name, 1)
				processList.RemovePartitionProgress(ctx.Pid(), name, partitionName)
			}

			onPartitionStart := func(partitionName string) {
				processList.AddPartitionProgress(ctx.Pid(), name, partitionName, -1)
			}

			onRowNext := func(partitionName string) {
				processList.UpdatePartitionProgress(ctx.Pid(), name, partitionName, 1)
			}

			var t sql.Table
			switch table := n.Table.(type) {
			case sql.DriverIndexableTable:
				t = plan.NewProcessIndexableTable(table, onPartitionDone, onPartitionStart, onRowNext)
			default:
				t = plan.NewProcessTable(table, onPartitionDone, onPartitionStart, onRowNext)
			}

			return n.WithTable(t)
		default:
			return n, nil
		}
	})
	if err != nil {
		return nil, err
	}

	// Don't wrap CreateIndex in a QueryProcess, as it is a CreateIndexProcess.
	// CreateIndex will take care of marking the process as done on its own.
	if _, ok := n.(*plan.CreateIndex); ok {
		return n, nil
	}

	// Remove QueryProcess nodes from the subqueries and trigger bodies. Otherwise, the process
	// will be marked as done as soon as a subquery / trigger finishes.
	node, err := plan.TransformUp(n, func(n sql.Node) (sql.Node, error) {
		if sq, ok := n.(*plan.SubqueryAlias); ok {
			if qp, ok := sq.Child.(*plan.QueryProcess); ok {
				return sq.WithChildren(qp.Child)
			}
		}
		if t, ok := n.(*plan.TriggerExecutor); ok {
			if qp, ok := t.Right().(*plan.QueryProcess); ok {
				return t.WithChildren(t.Left(), qp.Child)
			}
		}
		return n, nil
	})
	if err != nil {
		return nil, err
	}

	return plan.NewQueryProcess(node, func() {
		processList.Done(ctx.Pid())
		if span := ctx.RootSpan(); span != nil {
			span.Finish()
		}
	}), nil
}
