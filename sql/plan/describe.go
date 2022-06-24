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

package plan

import (
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/expression"

	"github.com/dolthub/go-mysql-server/sql"
)

// Describe is a node that describes its children.
type Describe struct {
	UnaryNode
}

// NewDescribe creates a new Describe node.
func NewDescribe(child sql.Node) *Describe {
	return &Describe{UnaryNode{child}}
}

// Schema implements the Node interface.
func (d *Describe) Schema() sql.Schema {
	return sql.Schema{{
		Name: "name",
		Type: sql.LongText,
	}, {
		Name: "type",
		Type: sql.LongText,
	}}
}

// RowIter implements the Node interface.
func (d *Describe) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return &describeIter{schema: d.Child.Schema()}, nil
}

// WithChildren implements the Node interface.
func (d *Describe) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(children), 1)
	}

	return NewDescribe(children[0]), nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *Describe) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return d.Child.CheckPrivileges(ctx, opChecker)
}

func (d Describe) String() string {
	p := sql.NewTreePrinter()
	_ = p.WriteNode("Describe")
	_ = p.WriteChildren(d.Child.String())
	return p.String()
}

type describeIter struct {
	schema sql.Schema
	i      int
}

func (i *describeIter) Next(ctx *sql.Context) (sql.Row, error) {
	if i.i >= len(i.schema) {
		return nil, io.EOF
	}

	f := i.schema[i.i]
	i.i++
	return sql.NewRow(f.Name, f.Type.String()), nil
}

func (i *describeIter) Close(*sql.Context) error {
	return nil
}

// DescribeQuery returns the description of the query plan.
type DescribeQuery struct {
	child  sql.Node
	Format string
}

func (d *DescribeQuery) Resolved() bool {
	return d.child.Resolved()
}

func (d *DescribeQuery) Children() []sql.Node {
	return nil
}

func (d *DescribeQuery) WithChildren(node ...sql.Node) (sql.Node, error) {
	if len(node) > 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(d, len(node), 0)
	}
	return d, nil
}

// CheckPrivileges implements the interface sql.Node.
func (d *DescribeQuery) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return d.child.CheckPrivileges(ctx, opChecker)
}

// DescribeSchema is the schema returned by a DescribeQuery node.
var DescribeSchema = sql.Schema{
	{Name: "plan", Type: sql.LongText},
}

// NewDescribeQuery creates a new DescribeQuery node.
func NewDescribeQuery(format string, child sql.Node) *DescribeQuery {
	return &DescribeQuery{child, format}
}

// Schema implements the Node interface.
func (d *DescribeQuery) Schema() sql.Schema {
	return DescribeSchema
}

// TODO: where do I put this so that Describe and TestQueryPlan can both see it?
// TODO: this function might already exist somewhere
func FindTable(table sql.Table) sql.Table {
	switch tbl := table.(type) {
	case *IndexedTableAccess:
		return FindTable(tbl.ResolvedTable)
	case *ResolvedTable:
		return FindTable(tbl.Table)
	case *ProcessTable:
		return FindTable(tbl.Underlying())
	default:
		return table
	}
}

// TODO: where do I put this so that Describe and TestQueryPlan can both see it?
func EstimatePlanCost(ctx *sql.Context, node sql.Node) (float64, error) {
	switch n := node.(type) {
	case *TransactionCommittingNode:
		return EstimatePlanCost(ctx, n.Child())
	case *QueryProcess:
		return EstimatePlanCost(ctx, n.Child())
	case *Project:
		return EstimatePlanCost(ctx, n.Child)
	case *DecoratedNode:
		return EstimatePlanCost(ctx, n.Child)
	case *Exchange:
		return EstimatePlanCost(ctx, n.Child)
	case *Sort:
		return EstimatePlanCost(ctx, n.Child)
	case *IndexedJoin:
		lcost, err := EstimatePlanCost(ctx, n.left)
		if err != nil {
			return 0, err
		}
		rcost, err := EstimatePlanCost(ctx, n.right)
		if err != nil {
			return 0, err
		}
		return lcost * rcost, nil
	case *CrossJoin:
		lcost, err := EstimatePlanCost(ctx, n.left)
		if err != nil {
			return 0, err
		}
		rcost, err := EstimatePlanCost(ctx, n.right)
		if err != nil {
			return 0, err
		}
		return lcost * rcost, nil
	case *IndexedTableAccess:
		// TODO: extract filter and apply it to histograms
		// TODO: or figure out a way to get cost out of joinOrderNode
		// get column name
		gf, ok := n.keyExprs[0].(*expression.GetField)
		if !ok {
			return 0, nil
		}
		colName := gf.Name()

		ranges := n.lookup.Ranges()
		l := ranges[0][0].LowerBound
		u := ranges[0][0].UpperBound

		lk, err := sql.Float64.Convert(sql.GetRangeCutKey(l))
		uk, err := sql.Float64.Convert(sql.GetRangeCutKey(u))
		if err != nil {
			return 0, err
		}

		table := FindTable(n)
		statsTbl, ok := table.(sql.StatisticsTable)
		if !ok {
			return 0, nil
		}

		stats, err := statsTbl.Statistics(ctx)
		if err != nil {
			return 0, err
		}

		histMap := stats.HistogramMap()
		if len(histMap) == 0 {
			return float64(stats.RowCount()), nil
		}
		hist, err := stats.Histogram(colName)
		if err != nil {
			return 0, err
		}

		var freq float64
		for _, bucket := range hist.Buckets {
			// values are in larger bucket, move on
			if lk.(float64) > bucket.LowerBound {
				continue
			}

			// passed all buckets that should've contained the value
			if uk.(float64) < bucket.LowerBound {
				break
			}

			freq += bucket.Frequency
		}

		return freq * float64(hist.Count), nil
	case sql.Table:
		table := FindTable(n)
		if statsTbl, ok := table.(sql.StatisticsTable); ok {
			stats, err := statsTbl.Statistics(ctx)
			if err != nil {
				return 0, err
			}
			numRows := stats.RowCount()
			return float64(numRows), nil
		}
		return 100, nil

	default:
		return 0, fmt.Errorf("EstimatePlanCost unhandled node: %T", n)
	}
}

// RowIter implements the Node interface.
func (d *DescribeQuery) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	var rows []sql.Row
	var formatString string
	if d.Format == "debug" {
		formatString = sql.DebugString(d.child)
	} else {
		formatString = d.child.String()
	}

	// TODO: come up with some way to have costs next to each node
	planCost, err := EstimatePlanCost(ctx, d.child)
	if err != nil {
		return nil, err
	}
	rows = append(rows, sql.NewRow(fmt.Sprintf("Estimated Cost: %.2f", planCost)))

	for _, l := range strings.Split(formatString, "\n") {
		if strings.TrimSpace(l) != "" {
			rows = append(rows, sql.NewRow(l))
		}
	}

	return sql.RowsToRowIter(rows...), nil
}

func (d *DescribeQuery) String() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DescribeQuery(format=%s)", d.Format)
	if d.Format == "debug" {
		_ = pr.WriteChildren(sql.DebugString(d.child))
	} else {
		_ = pr.WriteChildren(d.child.String())
	}
	return pr.String()
}

func (d *DescribeQuery) DebugString() string {
	pr := sql.NewTreePrinter()
	_ = pr.WriteNode("DescribeQuery(format=%s)", d.Format)
	_ = pr.WriteChildren(sql.DebugString(d.child))
	return pr.String()
}

// Query returns the query node being described
func (d *DescribeQuery) Query() sql.Node {
	return d.child
}

// WithQuery returns a copy of this node with the query node given
func (d *DescribeQuery) WithQuery(child sql.Node) sql.Node {
	return NewDescribeQuery(d.Format, child)
}
