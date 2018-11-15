package pilosa

import (
	"bytes"
	"encoding/gob"
	"io"
	"strings"
	"time"

	"github.com/pilosa/pilosa"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var (
	errUnknownType     = errors.NewKind("unknown type %T received as value")
	errTypeMismatch    = errors.NewKind("cannot compare type %T with type %T")
	errUnmergeableType = errors.NewKind("unmergeable type %T")

	// operation functors
	// r1 AND r2
	intersect = func(r1, r2 *pilosa.Row) *pilosa.Row {
		if r1 == nil {
			return r2
		}
		if r2 == nil {
			return nil
		}
		return r1.Intersect(r2)
	}
	// r1 OR r2
	union = func(r1, r2 *pilosa.Row) *pilosa.Row {
		if r1 == nil {
			return r2
		}
		if r2 == nil {
			return r1
		}

		return r1.Union(r2)
	}
	// r1 AND NOT r2
	difference = func(r1, r2 *pilosa.Row) *pilosa.Row {
		if r1 == nil {
			return r2
		}
		if r2 == nil {
			return r1
		}

		return r1.Difference(r2)
	}
)

type (

	// indexLookup implement following interfaces:
	// sql.IndexLookup, sql.Mergeable, sql.SetOperations
	indexLookup struct {
		id          string
		index       *concurrentPilosaIndex
		mapping     *mapping
		keys        []interface{}
		expressions []string
		operations  []*lookupOperation
	}

	lookupOperation struct {
		lookup    sql.IndexLookup
		operation func(*pilosa.Row, *pilosa.Row) *pilosa.Row
	}

	pilosaLookup interface {
		indexName() string
		values(sql.Partition) (*pilosa.Row, error)
	}
)

func (l *indexLookup) indexName() string {
	return l.index.Name()
}

func (l *indexLookup) intersectExpressions(p sql.Partition) (*pilosa.Row, error) {
	var row *pilosa.Row
	for i, expr := range l.expressions {
		field := l.index.Field(fieldName(l.id, expr, p))
		rowID, err := l.mapping.rowID(field.Name(), l.keys[i])
		if err == io.EOF {
			continue
		}
		if err != nil {
			return nil, err
		}

		r, err := field.Row(rowID)
		if err != nil {
			return nil, err
		}

		row = intersect(row, r)
	}
	return row, nil
}

func (l *indexLookup) values(p sql.Partition) (*pilosa.Row, error) {
	if err := l.mapping.open(); err != nil {
		return nil, err
	}
	defer l.mapping.close()

	if err := l.index.Open(); err != nil {
		return nil, err
	}
	row, err := l.intersectExpressions(p)
	if e := l.index.Close(); e != nil {
		if err == nil {
			err = e
		}
	}
	if err != nil {
		return nil, err
	}

	// evaluate composition of operations
	for _, op := range l.operations {
		var (
			r *pilosa.Row
			e error
		)

		il, ok := op.lookup.(pilosaLookup)
		if !ok {
			return nil, errUnmergeableType.New(op.lookup)
		}

		r, e = il.values(p)
		if e != nil {
			return nil, e
		}

		row = op.operation(row, r)
	}

	return row, nil
}

// Values implements sql.IndexLookup.Values
func (l *indexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	row, err := l.values(p)
	if err != nil {
		return nil, err
	}

	if row == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	bits := row.Columns()
	return &indexValueIter{
		total:     uint64(len(bits)),
		bits:      bits,
		mapping:   l.mapping,
		indexName: l.index.Name(),
	}, nil
}

func (l *indexLookup) Indexes() []string {
	return []string{l.id}
}

// IsMergeable implements sql.Mergeable interface.
func (l *indexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	if il, ok := lookup.(pilosaLookup); ok {
		return il.indexName() == l.indexName()
	}

	return false
}

// Intersection implements sql.SetOperations interface
func (l *indexLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, intersect})
	}

	return &lookup
}

// Union implements sql.SetOperations interface
func (l *indexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, union})
	}

	return &lookup
}

// Difference implements sql.SetOperations interface
func (l *indexLookup) Difference(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, difference})
	}

	return &lookup
}

type filteredLookup struct {
	id          string
	index       *concurrentPilosaIndex
	mapping     *mapping
	keys        []interface{}
	expressions []string
	operations  []*lookupOperation

	reverse bool
	filter  func(int, []byte) (bool, error)
}

func (l *filteredLookup) indexName() string {
	return l.index.Name()
}

// evaluate Intersection of bitmaps
func (l *filteredLookup) intersectExpressions(p sql.Partition) (*pilosa.Row, error) {
	var row *pilosa.Row

	for i, expr := range l.expressions {
		field := l.index.Field(fieldName(l.id, expr, p))
		rows, err := l.mapping.filter(field.Name(), func(b []byte) (bool, error) {
			return l.filter(i, b)
		})
		if err != nil {
			return nil, err
		}

		var r *pilosa.Row
		for _, ri := range rows {
			rr, err := field.Row(ri)
			if err != nil {
				return nil, err
			}
			r = union(r, rr)
		}

		row = intersect(row, r)
	}

	return row, nil
}

func (l *filteredLookup) values(p sql.Partition) (*pilosa.Row, error) {
	if err := l.mapping.open(); err != nil {
		return nil, err
	}
	defer l.mapping.close()

	if err := l.index.Open(); err != nil {
		return nil, err
	}
	row, err := l.intersectExpressions(p)
	if e := l.index.Close(); e != nil {
		if err == nil {
			err = e
		}
	}
	if err != nil {
		return nil, err
	}

	// evaluate composition of operations
	for _, op := range l.operations {
		var (
			r *pilosa.Row
			e error
		)

		il, ok := op.lookup.(pilosaLookup)
		if !ok {
			return nil, errUnmergeableType.New(op.lookup)
		}

		r, e = il.values(p)
		if e != nil {
			return nil, e
		}
		if r == nil {
			continue
		}

		row = op.operation(row, r)
	}

	return row, nil
}

func (l *filteredLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	row, err := l.values(p)
	if err != nil {
		return nil, err
	}

	if row == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	bits := row.Columns()
	if err := l.mapping.open(); err != nil {
		return nil, err
	}
	defer l.mapping.close()
	locations, err := l.mapping.sortedLocations(l.index.Name(), bits, l.reverse)
	if err != nil {
		return nil, err
	}

	return &locationValueIter{locations: locations}, nil
}

func (l *filteredLookup) Indexes() []string {
	return []string{l.id}
}

// IsMergeable implements sql.Mergeable interface.
func (l *filteredLookup) IsMergeable(lookup sql.IndexLookup) bool {
	if il, ok := lookup.(pilosaLookup); ok {
		return il.indexName() == l.indexName()
	}
	return false
}

// Intersection implements sql.SetOperations interface
func (l *filteredLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, intersect})
	}

	return &lookup
}

// Union implements sql.SetOperations interface
func (l *filteredLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, union})
	}

	return &lookup
}

// Difference implements sql.SetOperations interface
func (l *filteredLookup) Difference(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, difference})
	}

	return &lookup
}

type ascendLookup struct {
	*filteredLookup
	gte []interface{}
	lt  []interface{}
}

type descendLookup struct {
	*filteredLookup
	gt  []interface{}
	lte []interface{}
}

type negateLookup struct {
	id          string
	index       *concurrentPilosaIndex
	mapping     *mapping
	keys        []interface{}
	expressions []string
	operations  []*lookupOperation
}

func (l *negateLookup) indexName() string { return l.index.Name() }

func (l *negateLookup) intersectExpressions(p sql.Partition) (*pilosa.Row, error) {
	var row *pilosa.Row
	for i, expr := range l.expressions {
		field := l.index.Field(fieldName(l.id, expr, p))

		maxRowID, err := l.mapping.getMaxRowID(field.Name())
		if err != nil {
			return nil, err
		}

		// Since Pilosa does not have a negation in PQL (see:
		// https://github.com/pilosa/pilosa/issues/807), we have to get all the
		// ones in all the rows and join them, and then make difference between
		// them and the ones in the row of the given value.
		var r *pilosa.Row
		// rowIDs start with 1
		for ri := uint64(1); ri <= maxRowID; ri++ {
			rr, err := field.Row(ri)
			if err != nil {
				return nil, err
			}
			r = union(r, rr)
		}

		rowID, err := l.mapping.rowID(field.Name(), l.keys[i])
		if err != nil && err != io.EOF {
			return nil, err
		}

		rr, err := field.Row(rowID)
		if err != nil {
			return nil, err
		}
		r = difference(r, rr)

		row = intersect(row, r)
	}
	return row, nil
}

func (l *negateLookup) values(p sql.Partition) (*pilosa.Row, error) {
	if err := l.mapping.open(); err != nil {
		return nil, err
	}
	defer l.mapping.close()

	if err := l.index.Open(); err != nil {
		return nil, err
	}
	row, err := l.intersectExpressions(p)
	if e := l.index.Close(); e != nil {
		if err == nil {
			err = e
		}
	}
	if err != nil {
		return nil, err
	}

	// evaluate composition of operations
	for _, op := range l.operations {
		var (
			r *pilosa.Row
			e error
		)

		il, ok := op.lookup.(pilosaLookup)
		if !ok {
			return nil, errUnmergeableType.New(op.lookup)
		}

		r, e = il.values(p)
		if e != nil {
			return nil, e
		}

		if r == nil {
			continue
		}

		row = op.operation(row, r)
	}

	return row, nil
}

// Values implements sql.IndexLookup.Values
func (l *negateLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	row, err := l.values(p)
	if err != nil {
		return nil, err
	}

	if row == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	bits := row.Columns()
	return &indexValueIter{
		total:     uint64(len(bits)),
		bits:      bits,
		mapping:   l.mapping,
		indexName: l.index.Name(),
	}, nil
}

func (l *negateLookup) Indexes() []string {
	return []string{l.id}
}

// IsMergeable implements sql.Mergeable interface.
func (l *negateLookup) IsMergeable(lookup sql.IndexLookup) bool {
	if il, ok := lookup.(pilosaLookup); ok {
		return il.indexName() == l.indexName()
	}

	return false
}

// Intersection implements sql.SetOperations interface
func (l *negateLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, intersect})
	}

	return &lookup
}

// Union implements sql.SetOperations interface
func (l *negateLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, union})
	}

	return &lookup
}

// Difference implements sql.SetOperations interface
func (l *negateLookup) Difference(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := *l
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, difference})
	}

	return &lookup
}

func decodeGob(k []byte, value interface{}) (interface{}, error) {
	decoder := gob.NewDecoder(bytes.NewBuffer(k))

	switch value.(type) {
	case string:
		var v string
		err := decoder.Decode(&v)
		return v, err
	case int32:
		var v int32
		err := decoder.Decode(&v)
		return v, err
	case int64:
		var v int64
		err := decoder.Decode(&v)
		return v, err
	case uint32:
		var v uint32
		err := decoder.Decode(&v)
		return v, err
	case uint64:
		var v uint64
		err := decoder.Decode(&v)
		return v, err
	case float64:
		var v float64
		err := decoder.Decode(&v)
		return v, err
	case time.Time:
		var v time.Time
		err := decoder.Decode(&v)
		return v, err
	case []byte:
		var v []byte
		err := decoder.Decode(&v)
		return v, err
	case bool:
		var v bool
		err := decoder.Decode(&v)
		return v, err
	case []interface{}:
		var v []interface{}
		err := decoder.Decode(&v)
		return v, err
	default:
		return nil, errUnknownType.New(value)
	}
}

// compare two values of the same underlying type. The values MUST be of the
// same type.
func compare(a, b interface{}) (int, error) {
	switch a := a.(type) {
	case bool:
		v, ok := b.(bool)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a == false {
			return -1, nil
		}

		return 1, nil
	case string:
		v, ok := b.(string)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		return strings.Compare(a, v), nil
	case int32:
		v, ok := b.(int32)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case int64:
		v, ok := b.(int64)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case uint32:
		v, ok := b.(uint32)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case uint64:
		v, ok := b.(uint64)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case float64:
		v, ok := b.(float64)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a == v {
			return 0, nil
		}

		if a < v {
			return -1, nil
		}

		return 1, nil
	case []byte:
		v, ok := b.([]byte)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}
		return bytes.Compare(a, v), nil
	case []interface{}:
		v, ok := b.([]interface{})
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if len(a) < len(v) {
			return -1, nil
		}

		if len(a) > len(v) {
			return 1, nil
		}

		for i := range a {
			cmp, err := compare(a[i], v[i])
			if err != nil {
				return 0, err
			}

			if cmp != 0 {
				return cmp, nil
			}
		}

		return 0, nil
	case time.Time:
		v, ok := b.(time.Time)
		if !ok {
			return 0, errTypeMismatch.New(a, b)
		}

		if a.Equal(v) {
			return 0, nil
		}

		if a.Before(v) {
			return -1, nil
		}

		return 1, nil
	default:
		return 0, errUnknownType.New(a)
	}
}
