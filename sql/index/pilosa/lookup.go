package pilosa

import (
	"bytes"
	"encoding/gob"
	"io"
	"math"
	"strings"
	"time"

	pilosa "github.com/pilosa/go-pilosa"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

var (
	errUnknownType     = errors.NewKind("unknown type %T received as value")
	errTypeMismatch    = errors.NewKind("cannot compare type %T with type %T")
	errUnmergeableType = errors.NewKind("unmergeable type %T")

	// operation functors
	// r1 AND r2
	intersect = func(b1, b2 *pilosa.PQLRowQuery) *pilosa.PQLRowQuery {
		if b1 == nil {
			return nil
		}
		if b2 == nil {
			return nil
		}
		return b1.Index().Intersect(b1, b2)
	}
	// r1 OR r2
	union = func(b1, b2 *pilosa.PQLRowQuery) *pilosa.PQLRowQuery {
		if b1 == nil {
			return b2
		}
		if b2 == nil {
			return b1
		}

		return b1.Index().Union(b1, b2)
	}
	// r1 AND NOT r2
	difference = func(b1, b2 *pilosa.PQLRowQuery) *pilosa.PQLRowQuery {
		if b1 == nil {
			return nil
		}
		if b2 == nil {
			return b1
		}

		return b1.Index().Difference(b1, b2)
	}
)

type lookupOperation struct {
	lookup    sql.IndexLookup
	operation func(*pilosa.PQLRowQuery, *pilosa.PQLRowQuery) *pilosa.PQLRowQuery
}

type pilosaLookup interface {
	bitmapQuery(sql.Partition) (*pilosa.PQLRowQuery, error)
	indexName() string
}

// indexLookup implement following interfaces:
// sql.IndexLookup, sql.Mergeable, sql.SetOperations
type indexLookup struct {
	id          string
	client      *pilosa.Client
	index       *pilosa.Index
	mapping     *mapping
	keys        []interface{}
	expressions []string
	operations  []*lookupOperation
}

func (l *indexLookup) indexName() string { return l.index.Name() }

func (l *indexLookup) bitmapQuery(p sql.Partition) (*pilosa.PQLRowQuery, error) {
	l.mapping.open()
	defer l.mapping.close()

	var (
		bmp     *pilosa.PQLRowQuery
		bitmaps []*pilosa.PQLRowQuery
	)

	for i, expr := range l.expressions {
		f := l.index.Field(fieldName(l.id, expr, p))
		if err := l.client.EnsureField(f); err != nil {
			return nil, err
		}

		rowID, err := l.mapping.rowID(f.Name(), l.keys[i])
		if err == io.EOF {
			continue
		}

		if err != nil {
			return nil, err
		}

		bitmaps = append(bitmaps, f.Row(rowID))
	}
	if len(bitmaps) > 0 {
		// Compute Intersection of expression bitmaps
		bmp = l.index.Intersect(bitmaps...)
	}

	// Compute composition operations
	for _, op := range l.operations {
		il, ok := op.lookup.(pilosaLookup)
		if !ok {
			return nil, errUnmergeableType.New(op.lookup)
		}

		b, err := il.bitmapQuery(p)
		if err != nil {
			return nil, err
		}

		bmp = op.operation(bmp, b)
	}

	return bmp, nil
}

// Values implements sql.IndexLookup.Values
func (l *indexLookup) Values(partition sql.Partition) (sql.IndexValueIter, error) {
	bmp, err := l.bitmapQuery(partition)
	if err != nil {
		return nil, err
	}

	l.mapping.open()
	if bmp == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	resp, err := l.client.Query(bmp)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, errPilosaQuery.New(resp.ErrorMessage)
	}

	if resp.Result() == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	bits := resp.Result().Row().Columns
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
	client      *pilosa.Client
	index       *pilosa.Index
	mapping     *mapping
	keys        []interface{}
	expressions []string
	operations  []*lookupOperation

	reverse bool
	filter  func(int, []byte) (bool, error)
}

func (l *filteredLookup) indexName() string { return l.index.Name() }

func (l *filteredLookup) bitmapQuery(partition sql.Partition) (*pilosa.PQLRowQuery, error) {
	l.mapping.open()
	defer l.mapping.close()

	// Compute Intersection of bitmaps
	var (
		bmp     *pilosa.PQLRowQuery
		bitmaps []*pilosa.PQLRowQuery
	)
	for i, expr := range l.expressions {
		f := l.index.Field(fieldName(l.id, expr, partition))
		if err := l.client.EnsureField(f); err != nil {
			return nil, err
		}

		rows, err := l.mapping.filter(f.Name(), func(b []byte) (bool, error) {
			return l.filter(i, b)
		})

		if err != nil {
			return nil, err
		}

		var bs []*pilosa.PQLRowQuery
		for _, row := range rows {
			bs = append(bs, f.Row(row))
		}

		bitmaps = append(bitmaps, l.index.Union(bs...))
	}
	if len(bitmaps) > 0 {
		// Compute Intersection of expression bitmaps
		bmp = l.index.Intersect(bitmaps...)
	}

	// Compute composition operations
	for _, op := range l.operations {
		il, ok := op.lookup.(pilosaLookup)
		if !ok {
			return nil, errUnmergeableType.New(op.lookup)
		}

		b, err := il.bitmapQuery(partition)
		if err != nil {
			return nil, err
		}

		bmp = op.operation(bmp, b)
	}

	return bmp, nil
}

func (l *filteredLookup) values(p sql.Partition) (sql.IndexValueIter, error) {
	bmp, err := l.bitmapQuery(p)
	if err != nil {
		return nil, err
	}

	l.mapping.open()
	if bmp == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	resp, err := l.client.Query(bmp)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, errPilosaQuery.New(resp.ErrorMessage)
	}

	if resp.Result() == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	bits := resp.Result().Row().Columns
	locations, err := l.mapping.sortedLocations(l.index.Name(), bits, l.reverse)
	if err != nil {
		return nil, err
	}

	return &locationValueIter{locations: locations}, nil
}

type ascendLookup struct {
	*filteredLookup
	gte []interface{}
	lt  []interface{}
}

func (l *ascendLookup) initFilter() {
	l.filter = func(i int, value []byte) (bool, error) {
		var v interface{}
		var err error
		if len(l.gte) > 0 {
			v, err = decodeGob(value, l.gte[i])
			if err != nil {
				return false, err
			}

			cmp, err := compare(v, l.gte[i])
			if err != nil {
				return false, err
			}

			if cmp < 0 {
				return false, nil
			}
		}

		if len(l.lt) > 0 {
			if v == nil {
				v, err = decodeGob(value, l.lt[i])
				if err != nil {
					return false, err
				}
			}

			cmp, err := compare(v, l.lt[i])
			if err != nil {
				return false, err
			}

			if cmp >= 0 {
				return false, nil
			}
		}

		return true, nil
	}
}

func (l *ascendLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return l.values(p)
}

func (l *ascendLookup) Indexes() []string {
	return []string{l.id}
}

// IsMergeable implements sql.Mergeable interface.
func (l *ascendLookup) IsMergeable(lookup sql.IndexLookup) bool {
	if il, ok := lookup.(pilosaLookup); ok {
		return il.indexName() == l.indexName()
	}

	return false
}

// Intersection implements sql.SetOperations interface
func (l *ascendLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	filteredLookup := *l.filteredLookup
	lookup := &ascendLookup{
		filteredLookup: &filteredLookup,
		gte:            l.gte,
		lt:             l.lt,
	}

	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, intersect})
	}

	return lookup
}

// Union implements sql.SetOperations interface
func (l *ascendLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	filteredLookup := *l.filteredLookup
	lookup := &ascendLookup{
		filteredLookup: &filteredLookup,
		gte:            l.gte,
		lt:             l.lt,
	}

	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, union})
	}

	return lookup
}

// Difference implements sql.SetOperations interface
func (l *ascendLookup) Difference(lookups ...sql.IndexLookup) sql.IndexLookup {
	filteredLookup := *l.filteredLookup
	lookup := &ascendLookup{
		filteredLookup: &filteredLookup,
		gte:            l.gte,
		lt:             l.lt,
	}

	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, difference})
	}

	return lookup
}

type descendLookup struct {
	*filteredLookup
	gt  []interface{}
	lte []interface{}
}

func (l *descendLookup) initFilter() {
	l.filter = func(i int, value []byte) (bool, error) {
		var v interface{}
		var err error
		if len(l.gt) > 0 {
			v, err = decodeGob(value, l.gt[i])
			if err != nil {
				return false, err
			}

			cmp, err := compare(v, l.gt[i])
			if err != nil {
				return false, err
			}

			if cmp <= 0 {
				return false, nil
			}
		}

		if len(l.lte) > 0 {
			if v == nil {
				v, err = decodeGob(value, l.lte[i])
				if err != nil {
					return false, err
				}
			}

			cmp, err := compare(v, l.lte[i])
			if err != nil {
				return false, err
			}

			if cmp > 0 {
				return false, nil
			}
		}

		return true, nil
	}
}

func (l *descendLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	return l.values(p)
}

func (l *descendLookup) Indexes() []string {
	return []string{l.id}
}

// IsMergeable implements sql.Mergeable interface.
func (l *descendLookup) IsMergeable(lookup sql.IndexLookup) bool {
	if il, ok := lookup.(pilosaLookup); ok {
		return il.indexName() == l.indexName()
	}

	return false
}

// Intersection implements sql.SetOperations interface
func (l *descendLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	filteredLookup := *l.filteredLookup
	lookup := &descendLookup{
		filteredLookup: &filteredLookup,
		gt:             l.gt,
		lte:            l.lte,
	}

	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, intersect})
	}

	return lookup
}

// Union implements sql.SetOperations interface
func (l *descendLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	filteredLookup := *l.filteredLookup
	lookup := &descendLookup{
		filteredLookup: &filteredLookup,
		gt:             l.gt,
		lte:            l.lte,
	}

	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, union})
	}

	return lookup
}

// Difference implements sql.SetOperations interface
func (l *descendLookup) Difference(lookups ...sql.IndexLookup) sql.IndexLookup {
	filteredLookup := *l.filteredLookup
	lookup := &descendLookup{
		filteredLookup: &filteredLookup,
		gt:             l.gt,
		lte:            l.lte,
	}

	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, difference})
	}

	return lookup
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

type negateLookup struct {
	id          string
	client      *pilosa.Client
	index       *pilosa.Index
	mapping     *mapping
	keys        []interface{}
	expressions []string
	operations  []*lookupOperation
}

var (
	zeroTime time.Time
	maxTime  = time.Unix(math.MaxInt64, math.MaxInt64)
)

func (l *negateLookup) indexName() string { return l.index.Name() }

func (l *negateLookup) bitmapQuery(p sql.Partition) (*pilosa.PQLRowQuery, error) {
	l.mapping.open()
	defer l.mapping.close()

	var (
		bmp     *pilosa.PQLRowQuery
		bitmaps []*pilosa.PQLRowQuery
	)
	for i, expr := range l.expressions {
		f := l.index.Field(fieldName(l.id, expr, p))
		if err := l.client.EnsureField(f); err != nil {
			return nil, err
		}

		maxRowID, err := l.mapping.getMaxRowID(f.Name())
		if err != nil {
			return nil, err
		}

		// Since Pilosa does not have a negation in PQL (see:
		// https://github.com/pilosa/pilosa/issues/807), we have to get all the
		// ones in all the rows and join them, and then make difference between
		// them and the ones in the row of the given value.
		var rows []*pilosa.PQLRowQuery
		// rowIDs start with 1
		for i := uint64(1); i <= maxRowID; i++ {
			rows = append(rows, f.Row(i))
		}
		all := l.index.Union(rows...)

		rowID, err := l.mapping.rowID(f.Name(), l.keys[i])
		if err != nil && err != io.EOF {
			return nil, err
		}

		bitmaps = append(
			bitmaps,
			l.index.Difference(all, f.Row(rowID)),
		)
	}
	if len(bitmaps) > 0 {
		// Compute Intersection of expression bitmaps
		bmp = l.index.Intersect(bitmaps...)
	}

	// Compute composition operations
	for _, op := range l.operations {
		il, ok := op.lookup.(pilosaLookup)
		if !ok {
			return nil, errUnmergeableType.New(op.lookup)
		}

		b, err := il.bitmapQuery(p)
		if err != nil {
			return nil, err
		}

		bmp = op.operation(bmp, b)
	}

	return bmp, nil
}

// Values implements sql.IndexLookup.Values
func (l *negateLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	bmp, err := l.bitmapQuery(p)
	if err != nil {
		return nil, err
	}

	l.mapping.open()
	if bmp == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	resp, err := l.client.Query(bmp)
	if err != nil {
		return nil, err
	}

	if !resp.Success {
		return nil, errPilosaQuery.New(resp.ErrorMessage)
	}

	if resp.Result() == nil {
		return &indexValueIter{mapping: l.mapping, indexName: l.index.Name()}, nil
	}

	bits := resp.Result().Row().Columns
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
