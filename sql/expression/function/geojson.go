package function

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/shopspring/decimal"
	"math"
	"strings"
)

// AsGeoJSON is a function that returns a point type from a WKT string
type AsGeoJSON struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*AsGeoJSON)(nil)

// NewAsGeoJSON creates a new point expression.
func NewAsGeoJSON(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_ASGEOJSON", "1, 2, or 3", len(args))
	}
	return &AsGeoJSON{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (g *AsGeoJSON) FunctionName() string {
	return "st_asgeojson"
}

// Description implements sql.FunctionExpression
func (g *AsGeoJSON) Description() string {
	return "returns a GeoJSON object from the geometry."
}

// Type implements the sql.Expression interface.
func (g *AsGeoJSON) Type() sql.Type {
	return sql.JSON
}

func (g *AsGeoJSON) String() string {
	var args = make([]string, len(g.ChildExpressions))
	for i, arg := range g.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_GEOMFROMWKT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (g *AsGeoJSON) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewAsGeoJSON(children...)
}

func PointToSlice(p sql.Point, prec int32) [2]decimal.Decimal {
	return [2]decimal.Decimal{decimal.NewFromFloat(p.X).Round(prec), decimal.NewFromFloat(p.Y).Round(prec)}
}

func LineToSlice(l sql.Linestring, prec int32) [][2]decimal.Decimal {
	arr := make([][2]decimal.Decimal, len(l.Points))
	for i, p := range l.Points {
		arr[i] = PointToSlice(p, prec)
	}
	return arr
}

func PolyToSlice(p sql.Polygon, prec int32) [][][2]decimal.Decimal {
	arr := make([][][2]decimal.Decimal, len(p.Lines))
	for i, l := range p.Lines {
		arr[i] = LineToSlice(l, prec)
	}
	return arr
}

func FindBBox(v interface{}) [4]float64 {
	var res [4]float64
	switch v := v.(type) {
	case sql.Point:
		res = [4]float64{v.X, v.Y, v.X, v.Y}
	case sql.Linestring:
		res = [4]float64{math.MaxFloat64, math.MaxFloat64, math.SmallestNonzeroFloat64, math.SmallestNonzeroFloat64}
		for _, p := range v.Points {
			tmp := FindBBox(p)
			res[0] = math.Min(res[0], tmp[0])
			res[1] = math.Min(res[1], tmp[1])
			res[2] = math.Max(res[2], tmp[2])
			res[3] = math.Max(res[3], tmp[3])
		}
	case sql.Polygon:
		res = [4]float64{math.MaxFloat64, math.MaxFloat64, math.SmallestNonzeroFloat64, math.SmallestNonzeroFloat64}
		for _, l := range v.Lines {
			tmp := FindBBox(l)
			res[0] = math.Min(res[0], tmp[0])
			res[1] = math.Min(res[1], tmp[1])
			res[2] = math.Max(res[2], tmp[2])
			res[3] = math.Max(res[3], tmp[3])
		}
	}
	return res
}

// Eval implements the sql.Expression interface.
func (g *AsGeoJSON) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := g.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// Return nil when geometry is nil
	if val == nil {
		return nil, nil
	}

	// Evaluate precision
	prec := int32(math.MaxInt32) // TODO: MySQL claims to be able to handle 2^32-1, but I can't get it past 2^31-1
	if len(g.ChildExpressions) >= 2 {
		p, err := g.ChildExpressions[1].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		p, err = sql.Int32.Convert(p)
		if err != nil {
			return nil, err
		}
		prec = p.(int32)
	}
	// TODO: takes too long to deal with high precisions, 17 is about the most MySQL prints anyway
	if prec > 17 {
		prec = 17
	}

	// Create map object to hold values
	obj := make(map[string]interface{}, 3) // TODO: needs to be 3 when including bounding box
	switch v := val.(type) {
	case sql.Point:
		obj["type"] = "Point"
		obj["coordinates"] = PointToSlice(v, prec)
	case sql.Linestring:
		obj["type"] = "LineString"
		obj["coordinates"] = LineToSlice(v, prec)
	case sql.Polygon:
		obj["type"] = "Polygon"
		obj["coordinates"] = PolyToSlice(v, prec)
	default:
		return nil, ErrInvalidArgumentType.New(g.FunctionName())
	}

	// Calculate bounding box
	// TODO: This can be done while we are converting to slice
	if len(g.ChildExpressions) == 3 {
		tmp := FindBBox(val)
		res := [4]decimal.Decimal{}
		for i, t := range tmp {
			res[i] = decimal.NewFromFloat(t).Round(prec)
		}
		obj["bbox"] = res
	}

	// TODO: GeoJSON object always assume srid is 4326
	return sql.JSONDocument{Val: obj}, nil
}