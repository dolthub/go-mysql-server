package aggregation

import (
	"fmt"
	"reflect"

	"github.com/mitchellh/hashstructure"
	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type anyValueBuffer struct {
	res  interface{}
	expr sql.Expression
}

func NewAnyValueBuffer(child sql.Expression) *anyValueBuffer {
	return &anyValueBuffer{nil, child}
}

// Update implements the AggregationBuffer interface.
func (a *anyValueBuffer) Update(ctx *sql.Context, row sql.Row) error {
	if a.res != nil {
		return nil
	}

	v, err := a.expr.Eval(ctx, row)
	if err != nil {
		return err
	}
	if v == nil {
		return nil
	}

	a.res = v

	return nil
}

// Eval implements the AggregationBuffer interface.
func (a *anyValueBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return a.res, nil
}

// Dispose implements the Disposable interface.
func (a *anyValueBuffer) Dispose() {
	expression.Dispose(a.expr)
}

type sumBuffer struct {
	isnil bool
	sum   interface{} // sum is either decimal.Decimal or float64
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

	m.PerformSum(v)

	return nil
}

func (m *sumBuffer) PerformSum(v interface{}) {
	// decimal.Decimal values are evaluated to string value even though the Literal expr type is Decimal type,
	// so convert it to appropriate Decimal type
	if s, isStr := v.(string); isStr && types.IsDecimal(m.expr.Type()) {
		val, _, err := m.expr.Type().Convert(s)
		if err == nil {
			v = val
		}
	}

	switch n := v.(type) {
	case decimal.Decimal:
		if m.isnil {
			m.sum = decimal.NewFromInt(0)
			m.isnil = false
		}
		if sum, ok := m.sum.(decimal.Decimal); ok {
			m.sum = sum.Add(n)
		} else {
			m.sum = decimal.NewFromFloat(m.sum.(float64)).Add(n)
		}
	default:
		val, _, err := types.Float64.Convert(n)
		if err != nil {
			val = float64(0)
		}
		if m.isnil {
			m.sum = float64(0)
			m.isnil = false
		}
		sum, _, err := types.Float64.Convert(m.sum)
		if err != nil {
			sum = float64(0)
		}
		m.sum = sum.(float64) + val.(float64)
	}
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
	sum  *sumBuffer // sum is either decimal.Decimal or float64
	rows int64
	expr sql.Expression
}

func NewAvgBuffer(child sql.Expression) *avgBuffer {
	const (
		rows = int64(0)
	)

	return &avgBuffer{NewSumBuffer(child), rows, child}
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

	a.sum.PerformSum(v)
	a.rows += 1

	return nil
}

// Eval implements the AggregationBuffer interface.
func (a *avgBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	sum, err := a.sum.Eval(ctx)
	if err != nil {
		return nil, err
	}
	// This case is triggered when no rows exist.
	switch s := sum.(type) {
	case float64:
		if s == 0 && a.rows == 0 {
			return nil, nil
		}

		if a.rows == 0 {
			return float64(0), nil
		}

		return s / float64(a.rows), nil
	case decimal.Decimal:
		if s.IsZero() && a.rows == 0 {
			return nil, nil
		}
		if a.rows == 0 {
			return decimal.NewFromInt(0), nil
		}
		scale := (s.Exponent() * -1) + 4
		return s.DivRound(decimal.NewFromInt(a.rows), scale), nil
	}
	return nil, nil
}

// Dispose implements the Disposable interface.
func (a *avgBuffer) Dispose() {
	expression.Dispose(a.expr)
}

type bitAndBuffer struct {
	res  uint64
	rows uint64
	expr sql.Expression
}

func NewBitAndBuffer(child sql.Expression) *bitAndBuffer {
	const (
		res  = ^uint64(0) // bitwise not xor, so 0xffff...
		rows = uint64(0)
	)

	return &bitAndBuffer{res, rows, child}
}

// Update implements the AggregationBuffer interface.
func (b *bitAndBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := b.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	v, _, err = types.Uint64.Convert(v)
	if err != nil {
		v = uint64(0)
	}

	b.res &= v.(uint64)
	b.rows += 1

	return nil
}

// Eval implements the AggregationBuffer interface.
func (b *bitAndBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return b.res, nil
}

// Dispose implements the Disposable interface.
func (b *bitAndBuffer) Dispose() {
	expression.Dispose(b.expr)
}

type bitOrBuffer struct {
	res  uint64
	rows uint64
	expr sql.Expression
}

func NewBitOrBuffer(child sql.Expression) *bitOrBuffer {
	const (
		res  = uint64(0)
		rows = uint64(0)
	)

	return &bitOrBuffer{res, rows, child}
}

// Update implements the AggregationBuffer interface.
func (b *bitOrBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := b.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	v, _, err = types.Uint64.Convert(v)
	if err != nil {
		v = uint64(0)
	}

	b.res |= v.(uint64)
	b.rows += 1

	return nil
}

// Eval implements the AggregationBuffer interface.
func (b *bitOrBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return b.res, nil
}

// Dispose implements the Disposable interface.
func (b *bitOrBuffer) Dispose() {
	expression.Dispose(b.expr)
}

type bitXorBuffer struct {
	res  uint64
	rows uint64
	expr sql.Expression
}

func NewBitXorBuffer(child sql.Expression) *bitXorBuffer {
	const (
		res  = uint64(0)
		rows = uint64(0)
	)

	return &bitXorBuffer{res, rows, child}
}

// Update implements the AggregationBuffer interface.
func (b *bitXorBuffer) Update(ctx *sql.Context, row sql.Row) error {
	v, err := b.expr.Eval(ctx, row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	v, _, err = types.Uint64.Convert(v)
	if err != nil {
		v = uint64(0)
	}

	b.res ^= v.(uint64)
	b.rows += 1

	return nil
}

// Eval implements the AggregationBuffer interface.
func (b *bitXorBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	// This case is triggered when no rows exist.
	if b.res == 0 && b.rows == 0 {
		return uint64(0), nil
	}

	if b.rows == 0 {
		return uint64(0), nil
	}

	return b.res, nil
}

// Dispose implements the Disposable interface.
func (b *bitXorBuffer) Dispose() {
	expression.Dispose(b.expr)
}

type countDistinctBuffer struct {
	seen  map[uint64]struct{}
	exprs []sql.Expression
}

func NewCountDistinctBuffer(children []sql.Expression) *countDistinctBuffer {
	return &countDistinctBuffer{make(map[uint64]struct{}), children}
}

// Update implements the AggregationBuffer interface.
func (c *countDistinctBuffer) Update(ctx *sql.Context, row sql.Row) error {
	var value interface{}
	if len(c.exprs) == 0 {
		return fmt.Errorf("no expressions")
	}
	if _, ok := c.exprs[0].(*expression.Star); ok {
		value = row
	} else {
		val := make([]interface{}, len(c.exprs))
		for i, expr := range c.exprs {
			v, err := expr.Eval(ctx, row)
			if err != nil {
				return err
			}
			// skip nil values
			if v == nil {
				return nil
			}
			val[i] = v
		}
		value = val
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
	for _, e := range c.exprs {
		expression.Dispose(e)
	}
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
	if js, ok := v.(sql.JSONWrapper); ok {
		v = js.ToInterface()
	}

	j.vals = append(j.vals, v)

	return nil
}

// Eval implements the AggregationBuffer interface.
func (j *jsonArrayBuffer) Eval(ctx *sql.Context) (interface{}, error) {
	return types.JSONDocument{Val: j.vals}, nil
}

// Dispose implements the Disposable interface.
func (j *jsonArrayBuffer) Dispose() {
}
