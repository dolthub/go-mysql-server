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

// LineFromWKB is a function that returns a linestring type from a WKB byte array
type LineFromWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*LineFromWKB)(nil)

// NewLineFromWKB creates a new point expression.
func NewLineFromWKB(e sql.Expression) sql.Expression {
	return &LineFromWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *LineFromWKB) FunctionName() string {
	return "st_linefromwkb"
}

// Description implements sql.FunctionExpression
func (p *LineFromWKB) Description() string {
	return "returns a new linestring from WKB format."
}

// IsNullable implements the sql.Expression interface.
func (p *LineFromWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *LineFromWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *LineFromWKB) String() string {
	return fmt.Sprintf("ST_LINEFROMWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *LineFromWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewLineFromWKB(children[0]), nil
}

// WKBToLine parses the data portion of a byte array in WKB format to a point object
func WKBToLine(buf []byte, isBig bool) (sql.Linestring, error) {
	// Must be at least 4 bytes (length of linestring)
	if len(buf) < 4 {
		return sql.Linestring{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Read length of line string
	var numPoints uint32
	if isBig {
		numPoints = binary.BigEndian.Uint32(buf[:4])
	} else {
		numPoints = binary.LittleEndian.Uint32(buf[:4])
	}

	// Extract line data
	lineData := buf[4:]

	// Check length
	if uint32(len(lineData)) < 16*numPoints {
		return sql.Linestring{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Parse points
	points := make([]sql.Point, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		if point, err := WKBToPoint(lineData[16*i:16*(i+1)], isBig); err == nil {
			points[i] = point
		} else {
			return sql.Linestring{}, sql.ErrInvalidGISData.New("ST_LineFromWKB")
		}
	}

	return sql.Linestring{Points: points}, nil
}

// Eval implements the sql.Expression interface.
func (p *LineFromWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
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
		return nil, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Parse Header
	isBig, geomType, err := ParseWKBHeader(v)
	if err != nil {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Not a point, throw error
	if geomType != WKBLineID {
		return nil, sql.ErrInvalidGISData.New("ST_LineFromWKB")
	}

	// Read data
	return WKBToLine(v[WKBHeaderLength:], isBig)
}
