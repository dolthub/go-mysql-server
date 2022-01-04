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

package function

import (
	"encoding/binary"
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"math"

	"github.com/dolthub/go-mysql-server/sql"
)

// PointFromWKB is a function that returns a point type from a WKB byte array
type PointFromWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*PointFromWKB)(nil)

// NewPointFromWKB creates a new point expression.
func NewPointFromWKB(e sql.Expression) sql.Expression {
	return &PointFromWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *PointFromWKB) FunctionName() string {
	return "st_pointfromwkb"
}

// Description implements sql.FunctionExpression
func (p *PointFromWKB) Description() string {
	return "returns a new point from a WKB string."
}

// IsNullable implements the sql.Expression interface.
func (p *PointFromWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *PointFromWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *PointFromWKB) String() string {
	return fmt.Sprintf("ST_POINTFROMWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *PointFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewPointFromWKB(children[0]), nil
}


// WKBToPoint parses the data portion of a byte array in WKB format to a point object
func WKBToPoint(buf []byte, isBig bool) (sql.Point, error) {
	var x, y float64
	if isBig {
		x = math.Float64frombits(binary.BigEndian.Uint64(buf[:8]))
		y = math.Float64frombits(binary.BigEndian.Uint64(buf[8:]))
	} else {
		x = math.Float64frombits(binary.LittleEndian.Uint64(buf[:8]))
		y = math.Float64frombits(binary.LittleEndian.Uint64(buf[8:]))
	}
	return sql.Point{X: x, Y: y}, nil
}

// Eval implements the sql.Expression interface.
func (p *PointFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Must be of type byte array
	v, ok := val.([]byte)
	if !ok {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromWKB1")
	}

	// Must be 21 bytes long
	if len(v) != 21 {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromWKB2")
	}

	// Read Endianness
	isBig := v[0] == 0

	// Get type
	var geomType uint32
	if isBig {
		geomType = binary.BigEndian.Uint32(v[1:5])
	} else {
		geomType = binary.LittleEndian.Uint32(v[1:5])
	}

	// Not a point, throw error
	if geomType != 1 {
		return nil, sql.ErrInvalidGISData.New("ST_PointFromWKB3")
	}

	// Read data
	return WKBToPoint(v[5:], isBig)
}
