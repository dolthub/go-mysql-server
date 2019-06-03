package sql

import (
	"io"

	"gopkg.in/src-d/go-errors.v1"
)

// Generator will generate a set of values for a given row.
type Generator interface {
	// Next value in the generator.
	Next() (interface{}, error)
	// Close the generator and dispose resources.
	Close() error
}

// ErrNotGenerator is returned when the value cannot be converted to a
// generator.
var ErrNotGenerator = errors.NewKind("cannot convert value of type %T to a generator")

// ToGenerator converts a value to a generator if possible.
func ToGenerator(v interface{}) (Generator, error) {
	switch v := v.(type) {
	case Generator:
		return v, nil
	case []interface{}:
		return NewArrayGenerator(v), nil
	case nil:
		return NewArrayGenerator(nil), nil
	default:
		return nil, ErrNotGenerator.New(v)
	}
}

// NewArrayGenerator creates a generator for a given array.
func NewArrayGenerator(array []interface{}) Generator {
	return &arrayGenerator{array, 0}
}

type arrayGenerator struct {
	array []interface{}
	pos   int
}

func (g *arrayGenerator) Next() (interface{}, error) {
	if g.pos >= len(g.array) {
		return nil, io.EOF
	}

	g.pos++
	return g.array[g.pos-1], nil
}

func (g *arrayGenerator) Close() error {
	g.pos = len(g.array)
	return nil
}
