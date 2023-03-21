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

package analyzer

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"regexp"
	"strings"
)

//go:generate stringer -type=HintType -linecomment

type HintType uint8

const (
	HintTypeUnknown                  HintType = iota //
	HintTypeJoinOrder                                // JOIN_ORDER
	HintTypeJoinFixedOrder                           // JOIN_FIXED_ORDER
	HintTypeMergeJoin                                // MERGE_JOIN
	HintTypeLookupJoin                               // LOOKUP_JOIN
	HintTypeHashJoin                                 // HASH_JOIN
	HintTypeSemiJoin                                 // SEMI_JOIN
	HintTypeAntiJoin                                 // ANTI_JOIN
	HintTypeInnerJoin                                // INNER_JOIN
	HintTypeNoIndexConditionPushDown                 // NO_ICP
	HintTypeMaxExecutionTime                         // MAX_EXECUTION_TIME
	HintTypeSetVar                                   // SET_VAR
)

type Hint struct {
	Typ  HintType
	Args []string
}

func (h Hint) String() string {
	if len(h.Args) > 0 {
		return fmt.Sprintf("%s(%s)", h.Typ, strings.Join(h.Args, ","))
	} else {
		return h.Typ.String()
	}
}

func newHint(joinTyp string, args []string) Hint {
	var typ HintType
	switch joinTyp {
	case "join_order":
		typ = HintTypeJoinOrder
	case "join_fixed_order":
		typ = HintTypeJoinFixedOrder
	case "merge_join":
		typ = HintTypeMergeJoin
	case "lookup_join":
		typ = HintTypeLookupJoin
	case "hash_join":
		typ = HintTypeHashJoin
	case "inner_join":
		typ = HintTypeInnerJoin
	case "semi_join":
		typ = HintTypeSemiJoin
	case "anti_join":
		typ = HintTypeAntiJoin
	case "no_icp":
		typ = HintTypeNoIndexConditionPushDown
	case "max_execution_time":
		typ = HintTypeMaxExecutionTime
	case "set_var":
		typ = HintTypeSetVar
	default:
		typ = HintTypeUnknown
	}
	return Hint{Typ: typ, Args: args}
}

func (h Hint) valid() bool {
	switch h.Typ {
	case HintTypeJoinOrder:
		return len(h.Args) > 0
	case HintTypeJoinFixedOrder:
		return len(h.Args) == 0
	case HintTypeMergeJoin:
		return len(h.Args) == 2
	case HintTypeLookupJoin:
		return len(h.Args) == 2
	case HintTypeHashJoin:
		return len(h.Args) == 2
	case HintTypeInnerJoin:
		return len(h.Args) == 2
	case HintTypeSemiJoin:
		return len(h.Args) == 2
	case HintTypeAntiJoin:
		return len(h.Args) == 2
	case HintTypeNoIndexConditionPushDown:
		return len(h.Args) == 0
	case HintTypeMaxExecutionTime:
		return len(h.Args) == 0
	case HintTypeSetVar:
		return len(h.Args) == 0
	case HintTypeUnknown:
	default:
	}
	return true
}

var hintRegex = regexp.MustCompile("(\\s*[a-z_]+(\\([^\\(]+\\))?\\s*)")

type hintState uint8

const (
	hsUnknown hintState = iota
	hsNextHint
	hsGetType
	hsGetArgs
	hsFinalizeHint
)

func extractJoinHint(n *plan.JoinNode) []Hint {
	if n.Comment() != "" {
		return parseJoinHints(n.Comment())
	}
	return nil
}

// TODO: this is pretty nasty. Should be done in the parser instead.
func parseJoinHints(comment string) []Hint {
	comment = strings.TrimPrefix(comment, "/*+")
	comment = strings.TrimSuffix(comment, "*/")
	comment = strings.ToLower(strings.TrimSpace(comment))
	comments := hintRegex.FindAll([]byte(comment), -1)
	var hints []Hint
	state := hsNextHint
	var hintStr string
	var hintType string
	var args []string
	for {
		switch state {
		case hsNextHint:
			if len(comments) == 0 {
				return hints
			}
			hintStr = strings.TrimSpace(string(comments[0]))
			comments = comments[1:]
			state = hsGetType
		case hsGetType:
			i := 0
			for hintType == "" {
				if i >= len(hintStr) {
					hintType = hintStr[:i]
					break
				}
				switch hintStr[i] {
				case '(', ' ':
					hintType = hintStr[:i]
				default:
				}
				i++
			}
			hintStr = hintStr[i:]
			state = hsGetArgs
		case hsGetArgs:
			if hintStr == "" {
				state = hsFinalizeHint
				continue
			}
			var arg strings.Builder
			for _, b := range hintStr {
				switch b {
				case ',', ')':
					args = append(args, strings.TrimSpace(arg.String()))
					arg = strings.Builder{}
				case ' ':
				default:
					arg.WriteRune(b)
				}
			}
			state = hsFinalizeHint
		case hsFinalizeHint:
			h := newHint(hintType, args)
			if h.valid() {
				hints = append(hints, h)
			}
			args = nil
			hintType = ""
			state = hsNextHint
		case hsUnknown:
		default:
		}
	}
}

// joinOrderHint encodes a groups relational dependencies in a bitset.
// This is equivalent to an expression group's base table inputs but
// reordered by the join hint table order.
type joinOrderHint struct {
	groups map[GroupId]vertexSet
	cache  map[uint64]bool
	order  map[GroupId]uint64
}

func newJoinOrderHint(order map[GroupId]uint64) *joinOrderHint {
	return &joinOrderHint{
		groups: make(map[GroupId]vertexSet),
		cache:  make(map[uint64]bool),
		order:  order,
	}
}

func (o joinOrderHint) build(grp *exprGroup) {
	s := vertexSet(0)
	// convert global table order to hint order
	inputs := grp.relProps.InputTables()
	for idx, ok := inputs.Next(0); ok; idx, ok = inputs.Next(idx + 1) {
		if i, ok := o.order[GroupId(idx+1)]; ok {
			// If group |idx+1| is a dependency of this table, record the
			// ordinal position of that group given by the hint order.
			s = s.add(i)
		}
	}
	o.groups[grp.id] = s

	for _, g := range grp.children() {
		if _, ok := o.groups[g.id]; !ok {
			// avoid duplicate work
			o.build(g)
		}
	}
}

// isValid returns true if the hint parsed correctly
func (o joinOrderHint) isValid() bool {
	for _, v := range o.groups {
		if v == vertexSet(0) {
			// invalid hint table name, fallback
			return false
		}
	}
	return true
}

func (o joinOrderHint) obeysOrder(n relExpr) bool {
	key := relKey(n)
	if v, ok := o.cache[key]; ok {
		return v
	}
	switch n := n.(type) {
	case joinRel:
		base := n.joinPrivate()
		if !base.left.hintOk || !base.right.hintOk {
			return false
		}
		l := o.groups[base.left.id]
		r := o.groups[base.right.id]
		valid := o.isOrdered(l, r) && o.isCompact(l, r)
		o.cache[key] = valid
		return valid
	case *project:
		return o.obeysOrder(n.child.best)
	case *distinct:
		return o.obeysOrder(n.child.best)
	case sourceRel:
		return true
	default:
		panic(fmt.Sprintf("missed type: %T", n))
	}
}

// isOrdered returns true if the vertex sets obey the table
// order requested by the hint.
//
// Ex: JOIN_ORDER(a,b,c) is ordered on [b]x[c], and not on
// on [c]x[b].
func (o joinOrderHint) isOrdered(s1, s2 vertexSet) bool {
	return s1 < s2
}

// isCompact returns true if the tables in the joined result
// set are a continuous subsection of the order hint.
//
// Ex: JOIN_ORDER(a,b,c) is compact on [b]x[c], and not
// on [a]x[c].
func (o joinOrderHint) isCompact(s1, s2 vertexSet) bool {
	if s1 == 0 || s2 == 0 {
		panic("unexpected nil vertex set")
	}
	union := s1.union(s2)
	last, _ := union.next(0)
	next, ok := union.next(last + 1)
	for ok {
		if last+1 != next {
			return false
		}
		last = next
		next, ok = union.next(next + 1)
	}

	// sets are compact, s1 higher than s2
	return true
}

type joinOpHint struct {
	op   HintType
	l, r sql.FastIntSet
}

func newjoinOpHint(op HintType, left, right GroupId) joinOpHint {
	return joinOpHint{
		op: op,
		l:  sql.NewFastIntSet(int(tableIdForSource(left))),
		r:  sql.NewFastIntSet(int(tableIdForSource(right))),
	}
}

// isValid returns true if the hint parsed correctly
func (o joinOpHint) isValid() bool {
	return !o.l.Empty() && !o.r.Empty()
}

// depsMatch returns whether this relExpr is a join with left/right inputs
// that match the join hint.
func (o joinOpHint) depsMatch(n relExpr) bool {
	switch n := n.(type) {
	case joinRel:
		base := n.joinPrivate()
		if o.l.Intersects(base.left.relProps.InputTables()) &&
			o.r.Intersects(base.right.relProps.InputTables()) ||
			o.l.Intersects(base.right.relProps.InputTables()) &&
				o.r.Intersects(base.left.relProps.InputTables()) {
			// one of the sides of the join is missing the table dependency
			return true
		}
	default:
		return true
	}
	return false
}

// opMatch returns false when |n| is a join whose left/right
// relations do not match the operator pattern.
//
// Ex: LOOKUP_JOIN(a,b) will match [a] x [b], [ac] x [b],
// but not [ab] x [c].
func (o joinOpHint) opMatch(n relExpr) (plan.JoinType, bool) {
	switch n := n.(type) {
	case joinRel:
		base := n.joinPrivate()
		var match bool
		switch o.op {
		case HintTypeLookupJoin:
			_, match = n.(*lookupJoin)
		case HintTypeMergeJoin:
			_, match = n.(*mergeJoin)
		case HintTypeInnerJoin:
			_, match = n.(*innerJoin)
		case HintTypeHashJoin:
			_, match = n.(*hashJoin)
		case HintTypeSemiJoin:
			_, match = n.(*semiJoin)
		case HintTypeAntiJoin:
			_, match = n.(*antiJoin)
		default:
		}
		if match {
			return base.op, true
		}
	default:
	}
	return plan.JoinTypeUnknown, false
}

// typeMatches returns whether a relExpr implements
// the physical join operator indicated by the hint.
//
// Ex: MERGE_JOIN(a,b) will match merge and left-merge joins.
func (o joinOpHint) typeMatches(n relExpr) bool {
	switch n := n.(type) {
	case joinRel:
		base := n.joinPrivate()
		switch o.op {
		case HintTypeLookupJoin:
			return base.op.IsLookup()
		case HintTypeMergeJoin:
			return base.op.IsMerge()
		case HintTypeInnerJoin:
			return !base.op.IsPhysical()
		case HintTypeHashJoin:
			return base.op.IsHash()
		case HintTypeSemiJoin:
			return base.op.IsSemi()
		case HintTypeAntiJoin:
			return base.op.IsAnti()
		default:
			return false
		}
	default:
	}
	return true
}

// joinHints wraps a collection of join hints, the memo
// interfaces with this object during costing.
type joinHints struct {
	ops   []joinOpHint
	order *joinOrderHint
}

// satisfiedBy returns whether a relExpr satisfies all of the join
// hints. This is binary, an expr that satisfies most of the join
// hints but fails one returns |false| and is subject to genpop costing.
func (h joinHints) satisfiedBy(n relExpr) bool {
	if h.order != nil && !h.order.obeysOrder(n) {
		return false
	}

	if h.ops == nil {
		return true
	}

	for _, op := range h.ops {
		if op.depsMatch(n) && op.typeMatches(n) {
			return true
		}
	}
	return false
}
