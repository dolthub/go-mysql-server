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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// AsWKB is a function that converts a spatial type into WKB format (alias for AsBinary)
type AsWKB struct {
	expression.UnaryExpression
}

var _ sql.FunctionExpression = (*AsWKB)(nil)

// NewAsWKB creates a new point expression.
func NewAsWKB(e sql.Expression) sql.Expression {
	return &AsWKB{expression.UnaryExpression{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (p *AsWKB) FunctionName() string {
	return "st_aswkb"
}

// Description implements sql.FunctionExpression
func (p *AsWKB) Description() string {
	return "returns binary representation of given spatial type."
}

// IsNullable implements the sql.Expression interface.
func (p *AsWKB) IsNullable() bool {
	return p.Child.IsNullable()
}

// Type implements the sql.Expression interface.
func (p *AsWKB) Type() sql.Type {
	return p.Child.Type()
}

func (p *AsWKB) String() string {
	return fmt.Sprintf("ST_ASWKB(%s)", p.Child.String())
}

// WithChildren implements the Expression interface.
func (p *AsWKB) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 1)
	}
	return NewAsWKB(children[0]), nil
}

// PointToBytes converts a sql.Point to a byte array
func PointToBytes(p sql.Point) []byte {
	// Initialize point buffer
	pointBuf := make([]byte, 16)
	// Write x and y
	binary.LittleEndian.PutUint64(pointBuf[0:8], math.Float64bits(p.X))
	binary.LittleEndian.PutUint64(pointBuf[8:16], math.Float64bits(p.Y))
	return pointBuf
}

// LineToBytes converts a sql.Linestring to a byte array
func LineToBytes(l sql.Linestring) []byte {
	// Initialize line buffer
	lineBuf := make([]byte, 4)
	// Write number of points
	binary.LittleEndian.PutUint32(lineBuf[0:4], uint32(len(l.Points)))
	// Append each point
	for _, p := range l.Points {
		pointBuf := PointToBytes(p)
		lineBuf = append(lineBuf, pointBuf...)
	}
	return lineBuf
}

// PolygonToBytes converts a sql.Polygon to a byte array
func PolygonToBytes(p sql.Polygon) []byte {
	// Initialize polygon buffer
	polygonBuf := make([]byte, 4)
	// Write number of lines
	binary.LittleEndian.PutUint32(polygonBuf[0:4], uint32(len(p.Lines)))
	// Append each point
	for _, l := range p.Lines {
		pointBuf := LineToBytes(l)
		polygonBuf = append(polygonBuf, pointBuf...)
	}
	return polygonBuf
}

// TODO: Could combine PointToBytes, LineToBytes and PolygonToBytes into recursive GeometryToBytes function?

// Eval implements the sql.Expression interface.
func (p *AsWKB) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := p.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Initialize buf with space for endianness (1 byte) and type (4 bytes)
	buf := make([]byte, 5)
	// MySQL seems to always use Little Endian
	buf[0] = 1
	var data []byte

	// Expect one of the geometry types
	switch v := val.(type) {
	case sql.Point:
		// Mark as point type
		binary.LittleEndian.PutUint32(buf[1:5], 1)
		data = PointToBytes(v)
	case sql.Linestring:
		// Mark as linestring type
		binary.LittleEndian.PutUint32(buf[1:5], 2)
		data = LineToBytes(v)
	case sql.Polygon:
		// Mark as Polygon type
		binary.LittleEndian.PutUint32(buf[1:5], 3)
		data = PolygonToBytes(v)
	default:
		return nil, sql.ErrInvalidGISData.New("ST_AsWKB")
	}

	// Append to header
	buf = append(buf, data...)

	return buf, nil
}
