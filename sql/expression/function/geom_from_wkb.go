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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// GeomFromWKB is a function that returns a geometry type from a WKB byte array
type GeomFromWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*GeomFromWKB)(nil)

// NewGeomFromWKB creates a new geometry expression.
func NewGeomFromWKB(e sql.Expression) sql.Expression {
	return &GeomFromWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *GeomFromWKB) FunctionName() string {
	return "st_geomfromwkb"
}

// Description implements sql.FunctionExpression
func (p *GeomFromWKB) Description() string {
	return "returns a new geometry from a WKB string."
}

// IsNullable implements the sql.Expression interface.
func (p *GeomFromWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *GeomFromWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *GeomFromWKB) String() string {
	return fmt.Sprintf("ST_GEOMFROMWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *GeomFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewGeomFromWKB(children[0]), nil
}

// Header contains endianness (1 byte) and geometry type (4 bytes)
const WKBHeaderLength = 5

// TODO: Enums instead?
// Type IDs
const WKBPointID = 1
const WKBLineID = 2
const WKBPolyID = 3

// ParseWKBHeader parses the header portion of a byte array in WKB format to extract endianness and type
func ParseWKBHeader(buf []byte) (bool, uint32, error) {
	// Header length
	if len(buf) < WKBHeaderLength {
		return false, 0, sql.ErrInvalidGISData.New("ST_GeomFromWKB3")
	}

	// Get Endianness
	isBig := buf[0] == 0

	// Get Geometry Type
	var geomType uint32
	if isBig {
		geomType = binary.BigEndian.Uint32(buf[1:5])
	} else {
		geomType = binary.LittleEndian.Uint32(buf[1:5])
	}

	return isBig, geomType, nil
}

// Eval implements the sql.Expression interface.
func (p *GeomFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromWKB")
	}

	// Parse Header
	isBig, geomType, err := ParseWKBHeader(v)
	if err != nil {
		return nil, err
	}

	// Parse accordingly
	switch geomType {
	case WKBPointID:
		return WKBToPoint(v[WKBHeaderLength:], isBig)
	case WKBLineID:
		return WKBToLine(v[WKBHeaderLength:], isBig)
	case WKBPolyID:
		return WKBToPoly(v[WKBHeaderLength:], isBig)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_GeomFromWKB")
	}
}
