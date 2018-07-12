package pilosa

import (
	"bytes"
	"encoding/gob"
	"io"
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
)

type (

	// indexLookup implement following interfaces:
	// sql.IndexLookup, sql.Mergeable, sql.SetOperations
	indexLookup struct {
		id          string
		client      *pilosa.Client
		index       *pilosa.Index
		mapping     *mapping
		keys        []interface{}
		expressions []sql.ExpressionHash
		operations  []*lookupOperation
	}

	lookupOperation struct {
		lookup    sql.IndexLookup
		operation func(...*pilosa.PQLBitmapQuery) *pilosa.PQLBitmapQuery
	}
)

func (l *indexLookup) bitmapQuery() (*pilosa.PQLBitmapQuery, error) {
	l.mapping.open()
	defer l.mapping.close()

	var bitmaps []*pilosa.PQLBitmapQuery
	for i, expr := range l.expressions {
		frm, err := l.index.Frame(frameName(l.id, expr))
		if err != nil {
			return nil, err
		}

		rowID, err := l.mapping.rowID(frm.Name(), l.keys[i])
		if err == io.EOF {
			continue
		}

		if err != nil {
			return nil, err
		}

		bitmaps = append(bitmaps, frm.Bitmap(rowID))
	}
	if len(bitmaps) == 0 {
		return nil, nil
	}

	// Compute Intersection of expression bitmaps
	bmp := l.index.Intersect(bitmaps...)

	// Compute composition operations
	for _, op := range l.operations {
		var (
			b *pilosa.PQLBitmapQuery
			e error
		)

		switch il := op.lookup.(type) {
		case *indexLookup:
			b, e = il.bitmapQuery()
		case *ascendLookup:
			b, e = il.bitmapQuery()
		case *descendLookup:
			b, e = il.bitmapQuery()
		default:
			return nil, errUnmergeableType.New(op.lookup)
		}

		if e != nil {
			return nil, e
		}
		if e = b.Error(); e != nil {
			return nil, e
		}

		bmp = op.operation(bmp, b)
	}

	return bmp, nil
}

// Values implements sql.IndexLookup.Values
func (l *indexLookup) Values() (sql.IndexValueIter, error) {
	bmp, err := l.bitmapQuery()
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

	bits := resp.Result().Bitmap().Bits
	return &indexValueIter{
		total:     uint64(len(bits)),
		bits:      bits,
		mapping:   l.mapping,
		indexName: l.index.Name(),
	}, nil
}

// IsMergeable implements sql.Mergeable interface.
func (l *indexLookup) IsMergeable(lookup sql.IndexLookup) bool {
	switch il := lookup.(type) {
	case *indexLookup:
		return il.index.Name() == l.index.Name()
	case *ascendLookup:
		return il.index.Name() == l.index.Name()
	case *descendLookup:
		return il.index.Name() == l.index.Name()

	}

	return false
}

// Intersection implements sql.SetOperations interface
func (l *indexLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &indexLookup{
		id:          l.id,
		client:      l.client,
		index:       l.index,
		mapping:     l.mapping,
		keys:        l.keys,
		expressions: l.expressions,
		operations:  l.operations,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Intersect})
	}

	return lookup
}

// Union implements sql.SetOperations interface
func (l *indexLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &indexLookup{
		id:          l.id,
		client:      l.client,
		index:       l.index,
		mapping:     l.mapping,
		keys:        l.keys,
		expressions: l.expressions,
		operations:  l.operations,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Union})
	}

	return lookup
}

// Difference implements sql.SetOperations interface
func (l *indexLookup) Difference(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &indexLookup{
		id:          l.id,
		client:      l.client,
		index:       l.index,
		mapping:     l.mapping,
		keys:        l.keys,
		expressions: l.expressions,
		operations:  l.operations,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Difference})
	}

	return lookup
}

type filteredLookup struct {
	id          string
	client      *pilosa.Client
	index       *pilosa.Index
	mapping     *mapping
	keys        []interface{}
	expressions []sql.ExpressionHash
	operations  []*lookupOperation

	reverse bool
	filter  func(int, []byte) (bool, error)
}

func (l *filteredLookup) bitmapQuery() (*pilosa.PQLBitmapQuery, error) {
	l.mapping.open()
	defer l.mapping.close()

	// Compute Intersection of bitmaps
	var bitmaps []*pilosa.PQLBitmapQuery
	for i, expr := range l.expressions {
		frm, err := l.index.Frame(frameName(l.id, expr))
		if err != nil {
			return nil, err
		}

		rows, err := l.mapping.filter(frm.Name(), func(b []byte) (bool, error) {
			return l.filter(i, b)
		})

		if err != nil {
			return nil, err
		}

		var bs []*pilosa.PQLBitmapQuery
		for _, row := range rows {
			bs = append(bs, frm.Bitmap(row))
		}

		bitmaps = append(bitmaps, l.index.Union(bs...))
	}
	if len(bitmaps) == 0 {
		return nil, nil
	}

	bmp := l.index.Intersect(bitmaps...)
	// Compute composition operations
	for _, op := range l.operations {
		var (
			b *pilosa.PQLBitmapQuery
			e error
		)

		switch il := op.lookup.(type) {
		case *indexLookup:
			b, e = il.bitmapQuery()
		case *ascendLookup:
			b, e = il.bitmapQuery()
		case *descendLookup:
			b, e = il.bitmapQuery()
		default:
			return nil, errUnmergeableType.New(op.lookup)
		}

		if e != nil {
			return nil, e
		}
		if e = b.Error(); e != nil {
			return nil, e
		}

		bmp = op.operation(bmp, b)
	}

	return bmp, nil
}

func (l *filteredLookup) values() (sql.IndexValueIter, error) {
	bmp, err := l.bitmapQuery()
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

	bits := resp.Result().Bitmap().Bits
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

func (l *ascendLookup) Values() (sql.IndexValueIter, error) {
	return l.values()
}

// IsMergeable implements sql.Mergeable interface.
func (l *ascendLookup) IsMergeable(lookup sql.IndexLookup) bool {
	switch il := lookup.(type) {
	case *indexLookup:
		return il.index.Name() == l.index.Name()
	case *ascendLookup:
		return il.index.Name() == l.index.Name()
	case *descendLookup:
		return il.index.Name() == l.index.Name()
	}

	return false
}

// Intersection implements sql.SetOperations interface
func (l *ascendLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &ascendLookup{
		filteredLookup: &filteredLookup{
			id:          l.id,
			client:      l.client,
			index:       l.index,
			mapping:     l.mapping,
			keys:        l.keys,
			expressions: l.expressions,
			operations:  l.operations,
			reverse:     l.reverse,
			filter:      l.filter,
		},
		gte: l.gte,
		lt:  l.lt,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Intersect})
	}

	return lookup
}

// Union implements sql.SetOperations interface
func (l *ascendLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &ascendLookup{
		filteredLookup: &filteredLookup{
			id:          l.id,
			client:      l.client,
			index:       l.index,
			mapping:     l.mapping,
			keys:        l.keys,
			expressions: l.expressions,
			operations:  l.operations,
			reverse:     l.reverse,
			filter:      l.filter,
		},
		gte: l.gte,
		lt:  l.lt,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Union})
	}

	return lookup
}

// Difference implements sql.SetOperations interface
func (l *ascendLookup) Difference(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &ascendLookup{
		filteredLookup: &filteredLookup{
			id:          l.id,
			client:      l.client,
			index:       l.index,
			mapping:     l.mapping,
			keys:        l.keys,
			expressions: l.expressions,
			operations:  l.operations,
			reverse:     l.reverse,
			filter:      l.filter,
		},
		gte: l.gte,
		lt:  l.lt,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Difference})
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

func (l *descendLookup) Values() (sql.IndexValueIter, error) {
	return l.values()
}

// IsMergeable implements sql.Mergeable interface.
func (l *descendLookup) IsMergeable(lookup sql.IndexLookup) bool {
	switch il := lookup.(type) {
	case *indexLookup:
		return il.index.Name() == l.index.Name()
	case *ascendLookup:
		return il.index.Name() == l.index.Name()
	case *descendLookup:
		return il.index.Name() == l.index.Name()
	}

	return false
}

// Intersection implements sql.SetOperations interface
func (l *descendLookup) Intersection(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &descendLookup{
		filteredLookup: &filteredLookup{
			id:          l.id,
			client:      l.client,
			index:       l.index,
			mapping:     l.mapping,
			keys:        l.keys,
			expressions: l.expressions,
			operations:  l.operations,
			reverse:     l.reverse,
			filter:      l.filter,
		},
		gt:  l.gt,
		lte: l.lte,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Intersect})
	}

	return lookup
}

// Union implements sql.SetOperations interface
func (l *descendLookup) Union(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &descendLookup{
		filteredLookup: &filteredLookup{
			id:          l.id,
			client:      l.client,
			index:       l.index,
			mapping:     l.mapping,
			keys:        l.keys,
			expressions: l.expressions,
			operations:  l.operations,
			reverse:     l.reverse,
			filter:      l.filter,
		},
		gt:  l.gt,
		lte: l.lte,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Union})
	}

	return lookup
}

// Difference implements sql.SetOperations interface
func (l *descendLookup) Difference(lookups ...sql.IndexLookup) sql.IndexLookup {
	lookup := &descendLookup{
		filteredLookup: &filteredLookup{
			id:          l.id,
			client:      l.client,
			index:       l.index,
			mapping:     l.mapping,
			keys:        l.keys,
			expressions: l.expressions,
			operations:  l.operations,
			reverse:     l.reverse,
			filter:      l.filter,
		},
		gt:  l.gt,
		lte: l.lte,
	}
	for _, li := range lookups {
		lookup.operations = append(lookup.operations, &lookupOperation{li, l.index.Difference})
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
