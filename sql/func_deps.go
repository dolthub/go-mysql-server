package sql

import (
	"fmt"
	"strings"
)

// EquivSets maintains equivalency sets
type EquivSets struct {
	sets []ColSet
}

func (e *EquivSets) Add(cols ColSet) {
	for i := range e.sets {
		set := e.sets[i]
		if cols.SubsetOf(set) {
			return
		} else if set.Intersects(cols) {
			set = set.Union(cols)
			e.sets[i] = set
			e.simplifyFromEditedSet(i)
			return
		}
	}
	e.sets = append(e.sets, cols)
}

func (e *EquivSets) Len() int {
	if e == nil {
		return 0
	}
	return len(e.sets)
}

func (e *EquivSets) Sets() []ColSet {
	if e == nil {
		return nil
	}
	return e.sets
}

func (e *EquivSets) String() string {
	if e == nil {
		return "equiv()"
	}
	b := strings.Builder{}
	sep := ""
	for i, set := range e.sets {
		b.WriteString(fmt.Sprintf("%sequiv%s", sep, set))
		if i == 0 {
			sep = "; "
		}
	}
	return b.String()
}

func (e *EquivSets) simplifyFromEditedSet(j int) {
	target := e.sets[j]
	for i := j + 1; i < len(e.sets); i++ {
		set := e.sets[i]
		if set.Intersects(target) {
			target = target.Union(set)
			e.sets[j] = target
			e.sets[i] = e.sets[len(e.sets)-1]
			e.sets = e.sets[:len(e.sets)-1]
		}
	}
}

// Key maintains a strict or lax dependency
type Key struct {
	strict bool
	cols   ColSet
	// nullability is implicit, pass in SetNotNullCols
}

func (k *Key) Empty() bool {
	return k.cols.Len() == 0
}

// FuncDepsSet encodes functional dependencies for a relational
// expression. Common uses for functional dependencies:
// - Do a set of equality columns comprise a strict key? (lookup joins)
// - Is there a strict key for a relation? (decorrelate scopes)
// - What are the set of equivalent filters? (join planning)
//
// This object expects fields to be set in the following order:
// - notNull: what columns are non-nullable?
// - consts: what columns are constant?
// - equivs: transitive closure of column equivalence
// - keys: primary and secondary keys, simplified
type FuncDepsSet struct {
	all ColSet
	// non-null columns for relation
	notNull ColSet
	// tracks in-scope constants
	consts ColSet
	// tracks in-scope equivalent closure
	equivs *EquivSets
	// first key is the lead key
	keys []Key
}

func (f *FuncDepsSet) StrictKey() (ColSet, bool) {
	if len(f.keys) == 0 || !f.keys[0].strict {
		return ColSet{}, false
	}
	return f.keys[0].cols, true
}

func (f *FuncDepsSet) LaxKey() (ColSet, bool) {
	if len(f.keys) == 0 || f.keys[0].strict {
		return ColSet{}, false
	}
	return f.keys[0].cols, true
}

func (f *FuncDepsSet) CopyKeys() []Key {
	ret := make([]Key, len(f.keys))
	copy(ret, f.keys)
	return ret
}

func (f *FuncDepsSet) HasMax1Row() bool {
	if len(f.keys) == 0 {
		return false
	}
	key := f.keys[0]
	return key.strict && key.Empty()
}

func (f *FuncDepsSet) String() string {
	b := strings.Builder{}
	if len(f.keys) > 0 {
		key := f.keys[0]
		lax := ""
		if !key.strict {
			lax = "lax-"
		}
		b.WriteString(fmt.Sprintf("%skey%s", lax, key.cols))
	}
	if !f.consts.Empty() {
		b.WriteString(fmt.Sprintf("; constant%s", f.consts))
	}
	if f.equivs.Len() > 0 {
		b.WriteString(fmt.Sprintf("; %s", f.equivs))
	}
	if len(f.keys) < 2 {
		return b.String()
	}
	for _, k := range f.keys[1:] {
		if k.strict {
			b.WriteString(fmt.Sprintf("; fd%s", k.cols))
		} else {
			b.WriteString(fmt.Sprintf("; lax-fd%s", k.cols))
		}
	}
	return b.String()
}

func (f *FuncDepsSet) Constants() ColSet {
	return f.consts
}

func (f *FuncDepsSet) EquivalenceClosure(cols ColSet) ColSet {
	for _, set := range f.equivs.Sets() {
		if set.Intersects(cols) {
			cols = cols.Union(set)
		}
	}
	return cols
}

// Questions to answer:
// - what columns are equivalent (building memo and spot filter)
// - are filters redundant
// - do the set of columns comprise a key? (do eq cols comprise index lookup)
// - what is a strict key for the relation (subq decorrelation)

// how to use constants for index lookup?

// tagging expressions in join tree
// - tables add colIds
// - filters represented by colIds and converted back w/ indexing?

// not null cols -- relProps collect non-null, set in FDs as one of last steps
// would be really nice to be given nullability beforehand

//todo
// - Add strict key, lax key from table
// - Add equivs from filter
//   - strict key, lax key
//   - fixup keys
// - Add constant from column
//   - fixup keys

func (f *FuncDepsSet) AddNotNullable(cols ColSet) {
	// must be called first
	cols = f.simplifyCols(cols)
	f.notNull = f.notNull.Union(cols)
}

func (f *FuncDepsSet) AddConstants(cols ColSet) {
	// compute closure of all constants
	// must be called after
	f.consts = f.consts.Union(cols)
}

func (f *FuncDepsSet) AddEquiv(i, j ColumnId) {
	cols := NewColSet(i, j)
	if f.equivs == nil {
		f.equivs = &EquivSets{}
	}
	f.AddEquivSet(cols)
}

func (f *FuncDepsSet) AddEquivSet(cols ColSet) {
	if f.equivs == nil {
		f.equivs = &EquivSets{}
	}
	f.equivs.Add(cols)
	for _, set := range f.equivs.Sets() {
		// if one col in equiv set is constant, rest are too
		if set.Intersects(f.consts) {
			f.AddConstants(set)
		}
	}
}

func (f *FuncDepsSet) AddKey(k Key) {
	switch k.strict {
	case true:
		f.AddStrictKey(k.cols)
	case false:
		f.AddLaxKey(k.cols)
	}
}

func (f *FuncDepsSet) AddStrictKey(cols ColSet) {
	// simplify colSet
	// add the key
	// try to simplify the current best key
	// update the best key (strict > lax, shorter better)
	cols = f.simplifyCols(cols)
	f.keys = append(f.keys, Key{cols: cols, strict: true})

	if len(f.keys) > 1 {
		lead := f.keys[0]
		lead.cols = f.simplifyCols(lead.cols)
		if !lead.strict || lead.strict && lead.cols.Len() > cols.Len() {
			// strict > lax
			// short > long
			f.keys[0], f.keys[len(f.keys)-1] = f.keys[len(f.keys)-1], lead
		}
	}
}

func (f *FuncDepsSet) AddLaxKey(cols ColSet) {
	// if all cols are not null, make strict
	// simplifyColset
	// add key
	// would need not nulls to convert to strict key
	// update best key
	nullableCols := f.notNull.Difference(cols)
	if nullableCols.Empty() {
		f.AddStrictKey(cols)
	}

	cols = f.simplifyCols(cols)
	f.keys = append(f.keys, Key{cols: cols, strict: false})
	if len(f.keys) > 1 && !f.keys[0].strict {
		// only try to improve if lax key
		lead := f.keys[0]
		lead.cols = f.simplifyCols(lead.cols)
		if lead.cols.Len() > cols.Len() {
			f.keys[0], f.keys[len(f.keys)-1] = f.keys[len(f.keys)-1], lead
		}
	}
}

// simplifyCols uses equivalence and constant sets to minimize
// a key set
func (f *FuncDepsSet) simplifyCols(key ColSet) ColSet {
	// for each column, attempt to remove and verify
	// the remaining set does not determine it
	// i.e. check if removedCol is in closure of rest of set
	ret := key.Copy()
	var plucked ColSet
	for i, ok := key.Next(1); ok; i, ok = key.Next(i + 1) {
		ret.Remove(i)
		plucked.Add(i)
		notConst := f.consts.Intersection(plucked).Empty()
		if notConst && !f.inClosureOf(plucked, ret) {
			// plucked is novel
			ret.Add(i)
		}
		plucked.Remove(i)
	}
	return ret
}

func (f *FuncDepsSet) inClosureOf(cols1, cols2 ColSet) bool {
	if cols1.SubsetOf(cols2) {
		return true
	}
	for _, set := range f.equivs.Sets() {
		if set.Intersects(cols2) {
			cols2 = cols2.Union(set)
		}
	}
	if cols1.SubsetOf(cols2) {
		return true
	}
	return false
}

func (f *FuncDepsSet) ColsAreStrictKey(cols ColSet) bool {
	return f.inClosureOf(f.keys[0].cols, cols)
}

// NewCrossJoinFDs makes functional dependencies for a cross join
// between two relations.
func NewCrossJoinFDs(left, right *FuncDepsSet) *FuncDepsSet {
	ret := &FuncDepsSet{}
	ret.AddNotNullable(left.notNull)
	ret.AddNotNullable(right.notNull)
	ret.AddConstants(left.consts)
	ret.AddConstants(right.consts)
	// no equiv in cross join
	// concatenate lead key, append others
	var lKey, rKey Key
	if len(left.keys) > 0 {
		lKey = left.keys[0]
		left.keys = left.keys[1:]
	}
	if len(right.keys) > 0 {
		rKey = right.keys[0]
		right.keys = right.keys[1:]
	}
	var jKey Key
	if lKey.Empty() && rKey.Empty() {
		return ret
	} else if lKey.Empty() {
		jKey = rKey
	} else if rKey.Empty() {
		jKey = lKey
	} else {
		jKey.cols = lKey.cols.Union(rKey.cols)
		jKey.strict = lKey.strict && rKey.strict
	}
	ret.keys = append(ret.keys, jKey)
	ret.keys = append(ret.keys, left.keys...)
	ret.keys = append(ret.keys, right.keys...)
	return ret
}

// NewInnerJoinFDs makes functional dependencies for an inner join
// between two relations.
func NewInnerJoinFDs(left, right *FuncDepsSet, filters [][2]ColumnId) *FuncDepsSet {
	ret := &FuncDepsSet{}
	ret.AddNotNullable(left.notNull)
	ret.AddNotNullable(right.notNull)
	ret.AddConstants(left.consts)
	ret.AddConstants(right.consts)
	for _, f := range filters {
		ret.AddEquiv(f[0], f[1])
	}
	leftKeys := left.CopyKeys()
	rightKeys := right.CopyKeys()
	// concatenate lead key, append others
	var lKey, rKey Key
	if len(leftKeys) > 0 {
		lKey = leftKeys[0]
		leftKeys = leftKeys[1:]
	}
	if len(rightKeys) > 0 {
		rKey = rightKeys[0]
		rightKeys = rightKeys[1:]
	}
	var jKey Key
	if lKey.Empty() && rKey.Empty() {
		return ret
	} else if lKey.Empty() {
		jKey = rKey
	} else if rKey.Empty() {
		jKey = lKey
	} else {
		jKey.cols = lKey.cols.Union(rKey.cols)
		jKey.strict = lKey.strict && rKey.strict
	}
	ret.AddKey(jKey)
	for _, k := range leftKeys {
		k.cols = ret.simplifyCols(k.cols)
		ret.keys = append(ret.keys, k)
	}
	for _, k := range rightKeys {
		k.cols = ret.simplifyCols(k.cols)
		ret.keys = append(ret.keys, k)
	}
	return ret
}

// NewProjectFDs returns a new functional dependency set projecting
// a subset of cols.
func NewProjectFDs(fds *FuncDepsSet, cols ColSet, distinct bool) *FuncDepsSet {
	ret := &FuncDepsSet{}
	ret.AddNotNullable(fds.notNull)

	if keptConst := fds.consts.Intersection(cols); !keptConst.Empty() {
		ret.AddConstants(keptConst)
	}

	if distinct {
		ret.AddStrictKey(cols)
	}

	// mapping deleted->equiv helps us keep keys whose removed cols
	// have projected equivalents
	equivMapping := make(map[ColumnId]ColumnId)
	for _, set := range fds.equivs.Sets() {
		if set.SubsetOf(cols) {
			if ret.equivs == nil {
				ret.equivs = &EquivSets{}
			}
			ret.AddEquivSet(set)
		} else {
			toKeep := set.Intersection(cols)
			if toKeep.Empty() {
				continue
			}
			if toRemove := set.Difference(cols); !toRemove.Empty() {
				for i, ok := toRemove.Next(1); ok; i, ok = toRemove.Next(i + 1) {
					equivMapping[i], _ = toKeep.Next(1)
				}
			}
		}
	}

	for _, key := range fds.keys {
		if key.cols.SubsetOf(cols) {
			continue
		}
		toRemove := key.cols.Difference(cols)
		newKey := key.cols.Intersection(cols)
		allOk := true
		var replace ColumnId
		for i, ok := toRemove.Next(1); ok; i, ok = toRemove.Next(i + 1) {
			replace, allOk = equivMapping[i]
			if !allOk {
				break
			}
			newKey.Add(replace)
		}
		if allOk {
			ret.AddKey(Key{strict: key.strict, cols: newKey})
		}
	}

	return ret
}

func NewLeftJoinFDs(left, right *FuncDepsSet, filters [][2]ColumnId) *FuncDepsSet {
	// get strict key from left
	// maybe need concat key from cross join
	// make cross product + filters FDs-> see if left is a strict key for it
	//  - inline inClosureOf, don't actually make?
	// null extend right rows
	// if max1Row, left cols in filter will be constant
	leftKey, leftStrict := left.StrictKey()
	leftColsAreInnerJoinKey := false
	if leftStrict {
		// leftcols are strict key
		j := NewInnerJoinFDs(left, right, filters)
		leftColsAreInnerJoinKey = j.inClosureOf(j.keys[0].cols, left.all)
	}

	leftKeys := left.CopyKeys()
	rightKeys := right.CopyKeys()
	var lKey, rKey Key
	if len(leftKeys) > 0 {
		lKey = leftKeys[0]
		left.keys = leftKeys[1:]
	}
	if len(rightKeys) > 0 {
		rKey = rightKeys[0]
		rightKeys = rightKeys[1:]
	}
	var jKey Key
	if lKey.Empty() && rKey.Empty() {
		// ?
	} else if lKey.Empty() {
		jKey = rKey
	} else if rKey.Empty() {
		jKey = lKey
	} else {
		jKey.cols = lKey.cols.Union(rKey.cols)
		jKey.strict = lKey.strict && rKey.strict
	}

	ret := &FuncDepsSet{}
	// left constants and equiv are safe
	ret.AddNotNullable(left.notNull)
	ret.AddConstants(left.consts)
	if left.HasMax1Row() {
		var leftConst ColSet
		// leftCols in filter are constant
		for i := range filters {
			col := filters[i][0]
			leftConst.Add(col)
		}
		ret.AddConstants(leftConst)
	}
	// only left equiv holds
	for _, equiv := range left.equivs.Sets() {
		ret.AddEquivSet(equiv)
	}

	if leftStrict && leftColsAreInnerJoinKey {
		ret.keys = append(ret.keys, Key{true, leftKey})
		ret.keys = append(ret.keys, rKey)
	} else {
		ret.keys = append(ret.keys, jKey)
		ret.keys = append(ret.keys, lKey)
		ret.keys = append(ret.keys, rKey)
	}

	// no filter equivs are valid
	// technically we could do (r)~~>(l), but is this useful?
	// right-side keys become lax unless all non-nullable in original
	for _, key := range rightKeys {
		if !key.cols.SubsetOf(right.notNull) {
			key.strict = false
		}
		ret.keys = append(ret.keys, key)
	}
	for _, key := range leftKeys {
		ret.keys = append(ret.keys, key)
	}
	// key w cols from both sides discarded unless strict key for whole rel
	return ret
}
