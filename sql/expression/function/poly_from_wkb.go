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

// PolyFromWKB is a function that returns a polygon type from a WKB byte array
type PolyFromWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*PolyFromWKB)(nil)

// NewPolyFromWKB creates a new point expression.
func NewPolyFromWKB(e sql.Expression) sql.Expression {
	return &PolyFromWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *PolyFromWKB) FunctionName() string {
	return "st_polyfromwkb"
}

// Description implements sql.FunctionExpression
func (p *PolyFromWKB) Description() string {
	return "returns a new polygon from WKB format."
}

// IsNullable implements the sql.Expression interface.
func (p *PolyFromWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *PolyFromWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *PolyFromWKB) String() string {
	return fmt.Sprintf("ST_POLYFROMWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *PolyFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewPolyFromWKB(children[0]), nil
}

// WKBToPoly parses the data portion of a byte array in WKB format to a point object
func WKBToPoly(buf []byte, isBig bool) (sql.Polygon, error) {
	// Must be at least 4 bytes (length of polygon)
	if len(buf) < 4 {
		return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB1")
	}

	// Get number of lines in polygon
	var numLines uint32
	if isBig {
		numLines = binary.BigEndian.Uint32(buf[:4])
	} else {
		numLines = binary.LittleEndian.Uint32(buf[:4])
	}

	// Extract poly data
	polyData := buf[4:]

	// Parse lines
	s := 0
	lines := make([]sql.Linestring, numLines)
	for i := uint32(0); i < numLines; i++ {
		if line, err := WKBToLine(polyData[s:], isBig); err == nil {
			if isLinearRing(line) {
				lines[i] = line
				s += 4 + 16*len(line.Points) // shift parsing location over
			} else {
				return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB2")
			}
		} else {
			return sql.Polygon{}, sql.ErrInvalidGISData.New("ST_PolyFromWKB3")
		}
	}

	return sql.Polygon{Lines: lines}, nil
}

// Eval implements the sql.Expression interface.
func (p *PolyFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
	}

	// Parse Header
	isBig, geomType, err := ParseWKBHeader(v)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
	}

	// Not a point, throw error
	if geomType != WKBPolyID {
		return nil, sql.ErrInvalidGISData.New("ST_PolyFromWKB")
	}

	// Read data
	return WKBToPoly(v[WKBHeaderLength:], isBig)
}
