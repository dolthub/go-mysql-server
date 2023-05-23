package sql

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
	// non-null columns for relation
	notNull ColSet
	// tracks in-scope constants
	consts ColSet
	// tracks in-scope equivalent closure
	equivs *EquivSets
	// first key is the lead key
	keys []Key
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

func (f *FuncDepsSet) AddNullable(cols ColSet) {
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
	f.equivs.Add(cols)

	for _, set := range f.equivs.sets {
		// if one col in equiv set is constant, rest are too
		if set.Intersects(f.consts) {
			f.AddConstants(set)
		}
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
	for i, ok := key.Next(0); ok; i, ok = key.Next(i) {
		ret.Remove(i)
		plucked.Add(i)
		if !f.inClosureOf(plucked, ret) {
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
	for _, set := range f.equivs.sets {
		if set.Intersects(cols2) {
			cols2 = cols2.Union(set)
		}
	}
	if cols1.SubsetOf(cols2) {
		return true
	}
	return false
}
