// Copyright 2025 Dolthub, Inc.
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

package spatial

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

const base32 = "0123456789bcdefghjkmnpqrstuvwxyz"

// GeoHash is a function that returns a geohash string for a geometry value.
// ST_GeoHash(longitude, latitude, max_length) or ST_GeoHash(point, max_length).
type GeoHash struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*GeoHash)(nil)
var _ sql.CollationCoercible = (*GeoHash)(nil)

// NewGeoHash creates a new GeoHash expression.
func NewGeoHash(ctx *sql.Context, args ...sql.Expression) (sql.Expression, error) {
	if len(args) != 2 && len(args) != 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_GEOHASH", "2 or 3", len(args))
	}
	return &GeoHash{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (g *GeoHash) FunctionName() string {
	return "st_geohash"
}

// Description implements sql.FunctionExpression
func (g *GeoHash) Description() string {
	return "returns a geohash string for the given geometry or longitude/latitude."
}

// IsNullable implements the sql.Expression interface.
func (g *GeoHash) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (g *GeoHash) Type(ctx *sql.Context) sql.Type {
	return types.LongText
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*GeoHash) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (g *GeoHash) String() string {
	var args = make([]string, len(g.ChildExpressions))
	for i, arg := range g.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("%s(%s)", g.FunctionName(), strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (g *GeoHash) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	return NewGeoHash(ctx, children...)
}

// encodeGeoHash encodes longitude/latitude to a geohash string of the given length.
func encodeGeoHash(lon, lat float64, length int) string {
	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0

	var bits int
	var ch int
	var result []byte
	isLon := true

	for len(result) < length {
		if isLon {
			mid := (minLon + maxLon) / 2
			if lon >= mid {
				ch = ch*2 + 1
				minLon = mid
			} else {
				ch = ch * 2
				maxLon = mid
			}
		} else {
			mid := (minLat + maxLat) / 2
			if lat >= mid {
				ch = ch*2 + 1
				minLat = mid
			} else {
				ch = ch * 2
				maxLat = mid
			}
		}
		isLon = !isLon
		bits++

		if bits == 5 {
			result = append(result, base32[ch])
			bits = 0
			ch = 0
		}
	}

	return string(result)
}

// Eval implements the sql.Expression interface.
func (g *GeoHash) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	var lon, lat float64
	var maxLen int

	if len(g.ChildExpressions) == 2 {
		// ST_GeoHash(point, max_length)
		val, err := g.ChildExpressions[0].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if val == nil {
			return nil, nil
		}
		gv, err := types.UnwrapGeometry(ctx, val)
		if err != nil {
			return nil, sql.ErrInvalidGISData.New(g.FunctionName())
		}
		p, ok := gv.(types.Point)
		if !ok {
			return nil, sql.ErrInvalidArgument.New(g.FunctionName())
		}
		// For SRID 4326, X is longitude, Y is latitude
		lon = p.X
		lat = p.Y

		lenVal, err := g.ChildExpressions[1].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if lenVal == nil {
			return nil, nil
		}
		l, _, err := types.Int64.Convert(ctx, lenVal)
		if err != nil {
			return nil, err
		}
		maxLen = int(l.(int64))
	} else {
		// ST_GeoHash(longitude, latitude, max_length)
		lonVal, err := g.ChildExpressions[0].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if lonVal == nil {
			return nil, nil
		}

		latVal, err := g.ChildExpressions[1].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if latVal == nil {
			return nil, nil
		}

		lenVal, err := g.ChildExpressions[2].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if lenVal == nil {
			return nil, nil
		}

		lonF, _, err := types.Float64.Convert(ctx, lonVal)
		if err != nil {
			return nil, err
		}
		latF, _, err := types.Float64.Convert(ctx, latVal)
		if err != nil {
			return nil, err
		}
		l, _, err := types.Int64.Convert(ctx, lenVal)
		if err != nil {
			return nil, err
		}

		lon = lonF.(float64)
		lat = latF.(float64)
		maxLen = int(l.(int64))
	}

	if maxLen < 1 {
		maxLen = 1
	}
	if maxLen > 100 {
		maxLen = 100
	}

	if lat < -90 || lat > 90 {
		return nil, fmt.Errorf("latitude %v is out of range in function %s. It must be within [-90, 90]", lat, g.FunctionName())
	}
	if lon < -180 || lon > 180 {
		return nil, fmt.Errorf("longitude %v is out of range in function %s. It must be within [-180, 180]", lon, g.FunctionName())
	}

	return encodeGeoHash(lon, lat, maxLen), nil
}

// PointFromGeoHash is a function that returns a Point from a geohash string.
type PointFromGeoHash struct {
	expression.BinaryExpressionStub
}

var _ sql.FunctionExpression = (*PointFromGeoHash)(nil)
var _ sql.CollationCoercible = (*PointFromGeoHash)(nil)

// NewPointFromGeoHash creates a new PointFromGeoHash expression.
func NewPointFromGeoHash(ctx *sql.Context, hash, srid sql.Expression) sql.Expression {
	return &PointFromGeoHash{
		expression.BinaryExpressionStub{
			LeftChild:  hash,
			RightChild: srid,
		},
	}
}

// FunctionName implements sql.FunctionExpression
func (p *PointFromGeoHash) FunctionName() string {
	return "st_pointfromgeohash"
}

// Description implements sql.FunctionExpression
func (p *PointFromGeoHash) Description() string {
	return "returns a Point from a geohash string."
}

// IsNullable implements the sql.Expression interface.
func (p *PointFromGeoHash) IsNullable(ctx *sql.Context) bool {
	return true
}

// Type implements the sql.Expression interface.
func (p *PointFromGeoHash) Type(ctx *sql.Context) sql.Type {
	return types.PointType{}
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*PointFromGeoHash) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 4
}

func (p *PointFromGeoHash) String() string {
	return fmt.Sprintf("%s(%s,%s)", p.FunctionName(), p.LeftChild.String(), p.RightChild.String())
}

// WithChildren implements the Expression interface.
func (p *PointFromGeoHash) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 2 {
		return nil, sql.ErrInvalidChildrenNumber.New(p, len(children), 2)
	}
	return NewPointFromGeoHash(ctx, children[0], children[1]), nil
}

// decodeGeoHash decodes a geohash string to longitude, latitude.
func decodeGeoHash(hash string) (lon, lat float64, err error) {
	minLat, maxLat := -90.0, 90.0
	minLon, maxLon := -180.0, 180.0
	isLon := true

	for _, c := range hash {
		idx := strings.IndexRune(base32, c)
		if idx < 0 {
			return 0, 0, fmt.Errorf("invalid geohash character: %c", c)
		}
		for bit := 4; bit >= 0; bit-- {
			if isLon {
				mid := (minLon + maxLon) / 2
				if idx&(1<<uint(bit)) != 0 {
					minLon = mid
				} else {
					maxLon = mid
				}
			} else {
				mid := (minLat + maxLat) / 2
				if idx&(1<<uint(bit)) != 0 {
					minLat = mid
				} else {
					maxLat = mid
				}
			}
			isLon = !isLon
		}
	}

	lon = (minLon + maxLon) / 2
	lat = (minLat + maxLat) / 2
	return lon, lat, nil
}

// Eval implements the sql.Expression interface.
func (p *PointFromGeoHash) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	hashVal, err := p.LeftChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if hashVal == nil {
		return nil, nil
	}

	sridVal, err := p.RightChild.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if sridVal == nil {
		return nil, nil
	}

	hashStr, ok := hashVal.(string)
	if !ok {
		return nil, sql.ErrInvalidArgument.New(p.FunctionName())
	}

	s, _, err := types.Int64.Convert(ctx, sridVal)
	if err != nil {
		return nil, err
	}
	srid := uint32(s.(int64))

	lon, lat, err := decodeGeoHash(hashStr)
	if err != nil {
		return nil, err
	}

	return types.Point{SRID: srid, X: lon, Y: lat}, nil
}

// LatFromGeoHash is a function that returns the latitude from a geohash string.
type LatFromGeoHash struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*LatFromGeoHash)(nil)
var _ sql.CollationCoercible = (*LatFromGeoHash)(nil)

// NewLatFromGeoHash creates a new LatFromGeoHash expression.
func NewLatFromGeoHash(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &LatFromGeoHash{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (l *LatFromGeoHash) FunctionName() string {
	return "st_latfromgeohash"
}

// Description implements sql.FunctionExpression
func (l *LatFromGeoHash) Description() string {
	return "returns the latitude from a geohash string."
}

// IsNullable implements the sql.Expression interface.
func (l *LatFromGeoHash) IsNullable(ctx *sql.Context) bool {
	return l.Child.IsNullable(ctx)
}

// Type implements the sql.Expression interface.
func (l *LatFromGeoHash) Type(ctx *sql.Context) sql.Type {
	return types.Float64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*LatFromGeoHash) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (l *LatFromGeoHash) String() string {
	return fmt.Sprintf("%s(%s)", l.FunctionName(), l.Child.String())
}

// WithChildren implements the Expression interface.
func (l *LatFromGeoHash) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLatFromGeoHash(ctx, children[0]), nil
}

// Eval implements the sql.Expression interface.
func (l *LatFromGeoHash) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := l.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	hashStr, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidArgument.New(l.FunctionName())
	}

	_, lat, err := decodeGeoHash(hashStr)
	if err != nil {
		return nil, err
	}
	return lat, nil
}

// LongFromGeoHash is a function that returns the longitude from a geohash string.
type LongFromGeoHash struct {
	expression.UnaryExpressionStub
}

var _ sql.FunctionExpression = (*LongFromGeoHash)(nil)
var _ sql.CollationCoercible = (*LongFromGeoHash)(nil)

// NewLongFromGeoHash creates a new LongFromGeoHash expression.
func NewLongFromGeoHash(ctx *sql.Context, e sql.Expression) sql.Expression {
	return &LongFromGeoHash{expression.UnaryExpressionStub{Child: e}}
}

// FunctionName implements sql.FunctionExpression
func (l *LongFromGeoHash) FunctionName() string {
	return "st_longfromgeohash"
}

// Description implements sql.FunctionExpression
func (l *LongFromGeoHash) Description() string {
	return "returns the longitude from a geohash string."
}

// IsNullable implements the sql.Expression interface.
func (l *LongFromGeoHash) IsNullable(ctx *sql.Context) bool {
	return l.Child.IsNullable(ctx)
}

// Type implements the sql.Expression interface.
func (l *LongFromGeoHash) Type(ctx *sql.Context) sql.Type {
	return types.Float64
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*LongFromGeoHash) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 5
}

func (l *LongFromGeoHash) String() string {
	return fmt.Sprintf("%s(%s)", l.FunctionName(), l.Child.String())
}

// WithChildren implements the Expression interface.
func (l *LongFromGeoHash) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(l, len(children), 1)
	}
	return NewLongFromGeoHash(ctx, children[0]), nil
}

// Eval implements the sql.Expression interface.
func (l *LongFromGeoHash) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	val, err := l.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, nil
	}

	hashStr, ok := val.(string)
	if !ok {
		return nil, sql.ErrInvalidArgument.New(l.FunctionName())
	}

	lon, _, err := decodeGeoHash(hashStr)
	if err != nil {
		return nil, err
	}
	return lon, nil
}
