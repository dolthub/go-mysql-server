package aggregation

import (
	"fmt"
	"reflect"

	"github.com/mitchellh/hashstructure"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

type sumBuffer struct {
	isnil bool
	sum   float64
	expr  sql.Expression
}

func NewSumBuffer(child sql.Expression) *sumBuffer {
	return &sumBuffer{true, float64(0), child}
}

// Update implements the AggregationBuffer interface.
func (m *sumBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := m.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	val, err := sql.Float64.Convert(v)
	if err != nil {
		val = float64(0)
	}

	if m.isnil {
		m.sum = 0
		m.isnil = false
	}

	m.sum += val.(float64)

	return nil
}

// Eval implements the AggregationBuffer interface.
func (m *sumBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	if m.isnil {
		return nil, nil
	}
	return m.sum, nil
}

// Dispose implements the Disposable interface.
func (m *sumBuffer) Dispose() {
	expression.Dispose(m.expr)
}

type lastBuffer struct {
	val  interface{}
	expr sql.Expression
}

func NewLastBuffer(child sql.Expression) *lastBuffer {
	const (
		sum  = float64(0)
		rows = int64(0)
	)

	return &lastBuffer{nil, child}
}

// Update implements the AggregationBuffer interface.
func (l *lastBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := l.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	l.val = v

	return nil
}

// Eval implements the AggregationBuffer interface.
func (l *lastBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return l.val, nil
}

// Dispose implements the Disposable interface.
func (l *lastBuffer) Dispose() {
	expression.Dispose(l.expr)
}

type avgBuffer struct {
	sum  float64
	rows int64
	expr sql.Expression
}

func NewAvgBuffer(child sql.Expression) *avgBuffer {
	const (
		sum  = float64(0)
		rows = int64(0)
	)

	return &avgBuffer{sum, rows, child}
}

// Update implements the AggregationBuffer interface.
func (a *avgBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := a.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	v, err = sql.Float64.Convert(v)
	if err != nil {
		v = float64(0)
	}

	a.sum += v.(float64)
	a.rows += 1

	return nil
}

// Eval implements the AggregationBuffer interface.
func (a *avgBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	// This case is triggered when no rows exist.
	if a.sum == 0 && a.rows == 0 {
		return nil, nil
	}

	if a.rows == 0 {
		return float64(0), nil
	}

	return a.sum / float64(a.rows), nil
}

// Dispose implements the Disposable interface.
func (a *avgBuffer) Dispose() {
	expression.Dispose(a.expr)
}

type countDistinctBuffer struct {
	seen map[uint64]struct{}
	expr sql.Expression
}

func NewCountDistinctBuffer(child sql.Expression) *countDistinctBuffer {
	return &countDistinctBuffer{make(map[uint64]struct{}), child}
}

// Update implements the AggregationBuffer interface.
func (c *countDistinctBuffer) Update(ctx *sql.Context, row sql.Row) error {
	var value interface{}
	if _, ok := c.expr.(*expression.Star); ok {
		value = row
	} else {
		v, err := c.expr.Eval(ctx, row)
		if v == nil {
			return nil
		}

		if err != nil {
			return err
		}

		value = v
	}

	hash, err := hashstructure.Hash(value, nil)
	if err != nil {
		return fmt.Errorf("count distinct unable to hash value: %s", err)
	}

	c.seen[hash] = struct{}{}

	return nil
}

// Eval implements the AggregationBuffer interface.
func (c *countDistinctBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return int64(len(c.seen)), nil
}

func (c *countDistinctBuffer) Dispose() {
	expression.Dispose(c.expr)
}

type countBuffer struct {
	cnt  int64
	expr sql.Expression
}

func NewCountBuffer(child sql.Expression) *countBuffer {
	return &countBuffer{0, child}
}

// Update implements the AggregationBuffer interface.
func (c *countBuffer) Update(ctx *sql.Context, row sql.Row) error {
	var inc bool
	if _, ok := c.expr.(*expression.Star); ok {
		inc = true
	} else {
		v, err := c.expr.Eval(ctx, row)
		if v != nil {
			inc = true
		}

		if err != nil {
			return err
		}
	}

	if inc {
		c.cnt += 1
	}

	return nil
}

// Eval implements the AggregationBuffer interface.
func (c *countBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return c.cnt, nil
}

// Dispose implements the Disposable interface.
func (c *countBuffer) Dispose() {
	expression.Dispose(c.expr)
}

type firstBuffer struct {
	val  interface{}
	expr sql.Expression
}

func NewFirstBuffer(child sql.Expression) *firstBuffer {
	return &firstBuffer{nil, child}
}

// Update implements the AggregationBuffer interface.
func (f *firstBuffer) Update(ctx *sql.Context, row sql.Row) error {
	if f.val != nil {
		return nil
	}

	v, err := f.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	f.val = v

	return nil
}

// Eval implements the AggregationBuffer interface.
func (f *firstBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return f.val, nil
}

// Dispose implements the Disposable interface.
func (f *firstBuffer) Dispose() {
	expression.Dispose(f.expr)
}

type maxBuffer struct {
	val  interface{}
	expr sql.Expression
}

func NewMaxBuffer(child sql.Expression) *maxBuffer {
	return &maxBuffer{nil, child}
}

// Update implements the AggregationBuffer interface.
func (m *maxBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := m.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if m.val == nil {
		m.val = v
		return nil
	}

	cmp, err := m.expr.Type().Compare(v, m.val)
	if err != nil {
		return err
	}
	if cmp == 1 {
		m.val = v
	}

	return nil
}

// Eval implements the AggregationBuffer interface.
func (m *maxBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return m.val, nil
}

// Dispose implements the Disposable interface.
func (m *maxBuffer) Dispose() {
	expression.Dispose(m.expr)
}

type minBuffer struct {
	val  interface{}
	expr sql.Expression
}

func NewMinBuffer(child sql.Expression) *minBuffer {
	return &minBuffer{nil, child}
}

// Update implements the AggregationBuffer interface.
func (m *minBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := m.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if m.val == nil {
		m.val = v
		return nil
	}

	cmp, err := m.expr.Type().Compare(v, m.val)
	if err != nil {
		return err
	}
	if cmp == -1 {
		m.val = v
	}

	return nil
}

// Eval implements the AggregationBuffer interface.
func (m *minBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return m.val, nil
}

// Dispose implements the Disposable interface.
func (m *minBuffer) Dispose() {
	expression.Dispose(m.expr)
}

type jsonArrayBuffer struct {
	vals []interface{}
	expr sql.Expression
}

func NewJsonArrayBuffer(child sql.Expression) *jsonArrayBuffer {
	return &jsonArrayBuffer{nil, child}
}

// Update implements the AggregationBuffer interface.
func (j *jsonArrayBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := j.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	// unwrap JSON values
	if js, ok := v.(sql.JSONValue); ok {
		doc, err := js.Unmarshall(ctx)
		if err != nil {
			return err
		}
		v = doc.Val
	}

	j.vals = append(j.vals, v)

	return nil
}

// Eval implements the AggregationBuffer interface.
func (j *jsonArrayBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return sql.JSONDocument{Val: j.vals}, nil
}

// Dispose implements the Disposable interface.
func (j *jsonArrayBuffer) Dispose() {
}
