// Copyright 2023 Dolthub, Inc.
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

package memo

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// relProps are relational attributes shared by all plans in an expression
// group (see: ExprGroup).
type relProps struct {
	grp *ExprGroup

	fds          *sql.FuncDepSet
	outputCols   sql.Schema
	inputTables  sql.FastIntSet
	outputTables sql.FastIntSet

	card float64

	Distinct distinctOp
	limit    sql.Expression
}

func newRelProps(rel RelExpr) *relProps {
	p := &relProps{
		grp: rel.Group(),
	}
	if r, ok := rel.(SourceRel); ok {
		if r.Name() == "" {
			p.outputCols = []*sql.Column{
				{
					Name:   "1",
					Source: "",
				},
			}
		} else {
			p.outputCols = r.OutputCols()
		}
		// need to assign column ids
		// TODO name resolution should replace assignColumnIds, then Fds can stay lazy
		p.populateFds()
	}
	p.populateOutputTables()
	p.populateInputTables()
	return p
}

// denormIdxExprs replaces the native table name in index
// expression strings with the aliased name.
// TODO: this is unstable as long as periods in Index.Expressions()
// identifiers are ambiguous.
func denormIdxExprs(table string, idx sql.Index) []string {
	denormExpr := make([]string, len(idx.Expressions()))
	for i, e := range idx.Expressions() {
		parts := strings.Split(e, ".")
		denormExpr[i] = strings.ToLower(fmt.Sprintf("%s.%s", table, parts[1]))
	}
	return denormExpr
}

func (p *relProps) populateFds() {
	var fds *sql.FuncDepSet
	switch rel := p.grp.First.(type) {
	case JoinRel:
		jp := rel.JoinPrivate()
		switch {
		case jp.Op.IsDegenerate():
			fds = sql.NewCrossJoinFDs(jp.Left.RelProps.FuncDeps(), jp.Right.RelProps.FuncDeps())
		case jp.Op.IsLeftOuter():
			fds = sql.NewLeftJoinFDs(jp.Left.RelProps.FuncDeps(), jp.Right.RelProps.FuncDeps(), getEquivs(jp.Filter))
		default:
			fds = sql.NewInnerJoinFDs(jp.Left.RelProps.FuncDeps(), jp.Right.RelProps.FuncDeps(), getEquivs(jp.Filter))
		}
	case *Max1Row:
		start := len(rel.Group().m.Columns)
		rel.Group().m.assignColumnIds(rel)
		end := len(rel.Group().m.Columns)

		sch := allTableCols(rel)

		var all sql.ColSet
		var notNull sql.ColSet
		for i := start; i < end; i++ {
			all.Add(sql.ColumnId(i + 1))
			if !sch[i-start].Nullable {
				notNull.Add(sql.ColumnId(i + 1))
			}
		}
		fds = sql.NewMax1RowFDs(all, notNull)
	case SourceRel:
		start := len(rel.Group().m.Columns)
		rel.Group().m.assignColumnIds(rel)
		end := len(rel.Group().m.Columns)

		sch := allTableCols(rel)

		var all sql.ColSet
		var notNull sql.ColSet
		for i := start; i < end; i++ {
			all.Add(sql.ColumnId(i + 1))
			if !sch[i-start].Nullable {
				notNull.Add(sql.ColumnId(i + 1))
			}
		}

		var indexes []sql.Index
		switch n := rel.(type) {
		case *TableAlias:
			rt, ok := n.Table.Child.(*plan.ResolvedTable)
			if !ok {
				break
			}
			table := rt.UnderlyingTable()
			indexableTable, ok := table.(sql.IndexAddressableTable)
			if !ok {
				break
			}
			indexes, _ = indexableTable.GetIndexes(rel.Group().m.Ctx)
		case *TableScan:
			table := n.Table.UnderlyingTable()
			indexableTable, ok := table.(sql.IndexAddressableTable)
			if !ok {
				break
			}
			indexes, _ = indexableTable.GetIndexes(rel.Group().m.Ctx)
		default:
		}

		var strictKeys []sql.ColSet
		var laxKeys []sql.ColSet
		var indexesNorm []*Index
		for _, idx := range indexes {
			// strict if primary key or all nonNull and unique
			exprs := denormIdxExprs(rel.Name(), idx)
			strict := true
			normIdx := &Index{idx: idx, order: make([]sql.ColumnId, len(exprs))}
			for i, e := range exprs {
				colId := rel.Group().m.Columns[e]
				if colId == 0 {
					panic("unregistered column")
				}
				normIdx.set.Add(colId)
				normIdx.order[i] = colId
				if !notNull.Contains(colId) {
					strict = false
				}
			}
			if !idx.IsUnique() {
				// not an FD
			} else if strict {
				strictKeys = append(strictKeys, normIdx.set)
			} else {
				laxKeys = append(laxKeys, normIdx.set)
			}
			indexesNorm = append(indexesNorm, normIdx)
		}
		rel.SetIndexes(indexesNorm)
		fds = sql.NewTablescanFDs(all, strictKeys, laxKeys, notNull)
	case *Filter:
		var notNull sql.ColSet
		var constant sql.ColSet
		var equiv [][2]sql.ColumnId
		for _, f := range rel.Filters {
			switch f := f.Scalar.(type) {
			case *Equal:
				if l, ok := f.Left.Scalar.(*ColRef); ok {
					switch r := f.Right.Scalar.(type) {
					case *ColRef:
						equiv = append(equiv, [2]sql.ColumnId{l.Col, r.Col})
					case *Literal:
						constant.Add(l.Col)
						if r.Val != nil {
							notNull.Add(l.Col)
						}
					}
				}
				if r, ok := f.Right.Scalar.(*ColRef); ok {
					switch l := f.Left.Scalar.(type) {
					case *ColRef:
						equiv = append(equiv, [2]sql.ColumnId{l.Col, r.Col})
					case *Literal:
						constant.Add(r.Col)
						if l.Val != nil {
							notNull.Add(r.Col)
						}
					}
				}
			case *Not:
				child, ok := f.Child.Scalar.(*IsNull)
				if ok {
					col, ok := child.Child.Scalar.(*ColRef)
					if ok {
						notNull.Add(col.Col)
					}
				}
			}
		}
		fds = sql.NewFilterFDs(rel.Child.RelProps.FuncDeps(), notNull, constant, equiv)
	case *Project:
		var projCols sql.ColSet
		for _, e := range rel.Projections {
			projCols = projCols.Union(e.scalarProps.Cols)
		}
		fds = sql.NewProjectFDs(rel.Child.RelProps.FuncDeps(), projCols, false)
	case *Distinct:
		fds = sql.NewProjectFDs(rel.Child.RelProps.FuncDeps(), rel.Child.RelProps.FuncDeps().All(), true)
	default:
		panic(fmt.Sprintf("unsupported relProps type: %T", rel))
	}
	p.fds = fds
}

// allTableCols returns the full schema of a table ignoring
// declared projections.
func allTableCols(rel SourceRel) sql.Schema {
	var table sql.Table
	switch rel := rel.(type) {
	case *TableAlias:
		rt, ok := rel.Table.Child.(*plan.ResolvedTable)
		if !ok {
			break
		}
		table = rt.UnderlyingTable()
	case *TableScan:
		table = rel.Table.UnderlyingTable()
	default:
		return rel.OutputCols()
	}
	projTab, ok := table.(sql.PrimaryKeyTable)
	if !ok {
		return rel.OutputCols()
	}

	sch := projTab.PrimaryKeySchema().Schema
	ret := make(sql.Schema, len(sch))
	for i, c := range sch {
		ret[i] = &sql.Column{
			Name:           c.Name,
			Type:           c.Type,
			Default:        c.Default,
			AutoIncrement:  c.AutoIncrement,
			Nullable:       c.Nullable,
			Source:         rel.Name(),
			DatabaseSource: c.DatabaseSource,
			PrimaryKey:     c.PrimaryKey,
			Comment:        c.Comment,
			Extra:          c.Extra,
		}
	}
	return ret

}

// getEquivs collects column equivalencies in the format sql.EquivSet expects.
func getEquivs(filters []ScalarExpr) [][2]sql.ColumnId {
	var ret [][2]sql.ColumnId
	for _, f := range filters {
		var l, r *ColRef
		switch f := f.(type) {
		case *Equal:
			l, _ = f.Left.Scalar.(*ColRef)
			r, _ = f.Right.Scalar.(*ColRef)
		case *NullSafeEq:
			l, _ = f.Left.Scalar.(*ColRef)
			r, _ = f.Right.Scalar.(*ColRef)
		}
		if l != nil && r != nil {
			ret = append(ret, [2]sql.ColumnId{l.Col, r.Col})
		}
	}
	return ret
}

func (p *relProps) FuncDeps() *sql.FuncDepSet {
	if p.fds == nil {
		p.populateFds()
	}
	return p.fds
}

// populateOutputTables initializes the bitmap indicating which tables'
// attributes are available outputs from the ExprGroup
func (p *relProps) populateOutputTables() {
	switch n := p.grp.First.(type) {
	case SourceRel:
		p.outputTables = sql.NewFastIntSet(int(n.TableId()))
	case *AntiJoin:
		p.outputTables = n.Left.RelProps.OutputTables()
	case *SemiJoin:
		p.outputTables = n.Left.RelProps.OutputTables()
	case *Distinct:
		p.outputTables = n.Child.RelProps.OutputTables()
	case *Project:
		p.outputTables = n.Child.RelProps.OutputTables()
	case *Filter:
		p.outputTables = n.Child.RelProps.OutputTables()
	case JoinRel:
		p.outputTables = n.JoinPrivate().Left.RelProps.OutputTables().Union(n.JoinPrivate().Right.RelProps.OutputTables())
	default:
		panic(fmt.Sprintf("unhandled type: %T", n))
	}
}

// populateInputTables initializes the bitmap indicating which tables
// are input into this ExprGroup. This is used to enforce join order
// hinting for semi joins.
func (p *relProps) populateInputTables() {
	switch n := p.grp.First.(type) {
	case SourceRel:
		p.inputTables = sql.NewFastIntSet(int(n.TableId()))
	case *Distinct:
		p.inputTables = n.Child.RelProps.InputTables()
	case *Project:
		p.inputTables = n.Child.RelProps.InputTables()
	case *Filter:
		p.inputTables = n.Child.RelProps.InputTables()
	case JoinRel:
		p.inputTables = n.JoinPrivate().Left.RelProps.InputTables().Union(n.JoinPrivate().Right.RelProps.InputTables())
	default:
		panic(fmt.Sprintf("unhandled type: %T", n))
	}
}

func (p *relProps) populateOutputCols() {
	p.outputCols = p.outputColsForRel(p.grp.Best)
}

func (p *relProps) outputColsForRel(r RelExpr) sql.Schema {
	switch r := r.(type) {
	case *SemiJoin:
		return r.Left.RelProps.OutputCols()
	case *AntiJoin:
		return r.Left.RelProps.OutputCols()
	case *LookupJoin:
		if r.Op.IsPartial() {
			return r.Left.RelProps.OutputCols()
		} else {
			return append(r.JoinPrivate().Left.RelProps.OutputCols(), r.JoinPrivate().Right.RelProps.OutputCols()...)
		}
	case JoinRel:
		return append(r.JoinPrivate().Left.RelProps.OutputCols(), r.JoinPrivate().Right.RelProps.OutputCols()...)
	case *Distinct:
		return r.Child.RelProps.OutputCols()
	case *Project:
		return r.outputCols()
	case *Filter:
		return r.outputCols()
	case SourceRel:
		return r.OutputCols()
	default:
		panic("unknown type")
	}
	return nil
}

// OutputCols returns the output schema of a node
func (p *relProps) OutputCols() sql.Schema {
	if p.outputCols == nil {
		if p.grp.Best == nil {
			return p.outputColsForRel(p.grp.First)
		}
		p.populateOutputCols()
	}
	return p.outputCols
}

// OutputTables returns a bitmap of tables in the output schema of this node.
func (p *relProps) OutputTables() sql.FastIntSet {
	return p.outputTables
}

// InputTables returns a bitmap of tables input into this node.
func (p *relProps) InputTables() sql.FastIntSet {
	return p.inputTables
}

// sortedInputs returns true if a relation's inputs are sorted on the
// full output schema. The OrderedDistinct operator can be used in this
// case.
func sortedInputs(rel RelExpr) bool {
	switch r := rel.(type) {
	case *Max1Row:
		return true
	case *Project:
		if _, ok := r.Child.Best.(*Max1Row); ok {
			return true
		}
		inputs := sortedColsForRel(r.Child.Best)
		outputs := r.outputCols()
		i := 0
		j := 0
		for i < len(outputs) && j < len(inputs) {
			// i -> output idx (distinct)
			// j -> input idx
			// want to find matches for all i where j_i <= j_i+1
			if strings.EqualFold(outputs[i].Name, inputs[j].Name) &&
				strings.EqualFold(outputs[i].Source, inputs[j].Source) {
				i++
			} else {
				// identical projections satisfied by same input
				j++
			}
		}
		return i == len(outputs)
	default:
		return false
	}
}

func sortedColsForRel(rel RelExpr) sql.Schema {
	switch r := rel.(type) {
	case *TableScan:
		tab, ok := r.Table.UnderlyingTable().(sql.PrimaryKeyTable)
		if ok {
			ords := tab.PrimaryKeySchema().PkOrdinals
			var pks sql.Schema
			for _, i := range ords {
				pks = append(pks, tab.PrimaryKeySchema().Schema[i])
			}
			return pks
		}
	case *MergeJoin:
		var ret sql.Schema
		for _, e := range r.InnerScan.Idx.SqlIdx().Expressions() {
			// TODO columns can have "." characters, this will miss cases
			parts := strings.Split(e, ".")
			var name string
			if len(parts) == 2 {
				name = parts[1]
			} else {
				return nil
			}
			ret = append(ret, &sql.Column{
				Name:     strings.ToLower(name),
				Source:   strings.ToLower(r.InnerScan.Idx.SqlIdx().Table()),
				Nullable: true},
			)
		}
		return ret
	case JoinRel:
		return sortedColsForRel(r.JoinPrivate().Left.Best)
	case *Project:
		// TODO remove projections from sortedColsForRel(n.child.best)
		return nil
	case *TableAlias:
		rt, ok := r.Table.Child.(*plan.ResolvedTable)
		if !ok {
			return nil
		}
		tab, ok := rt.Table.(sql.PrimaryKeyTable)
		if ok {
			ords := tab.PrimaryKeySchema().PkOrdinals
			var pks sql.Schema
			for _, i := range ords {
				col := tab.PrimaryKeySchema().Schema[i].Copy()
				col.Source = r.Name()
				pks = append(pks, col)
			}
			return pks
		}
	default:
	}
	return nil
}
