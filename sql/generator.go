// Copyright 2020-2021 Dolthub, Inc.
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
