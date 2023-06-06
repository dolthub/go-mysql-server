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
	}
	p.populateOutputTables()
	p.populateInputTables()

	return p
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
			if strings.EqualFold(outputs[i].Source, inputs[j].Source) &&
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
		tab, ok := r.Table.Table.(sql.PrimaryKeyTable)
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
		for _, e := range r.InnerScan.Idx.Expressions() {
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
				Source:   strings.ToLower(r.InnerScan.Idx.Table()),
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
