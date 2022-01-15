package function

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/shopspring/decimal"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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
	// TODO: any better way to handle precision? math.Round(x * math.Pow10(prec)) / math.Pow10(prec)
	prec := int32(math.MaxInt32) // TODO: MySQL claims to be able to handle 2^32-1, but I can't get it past 2^31-1
	if len(g.ChildExpressions) >= 2 {
		p, err := g.ChildExpressions[1].Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if p == nil {
			return nil, nil
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

	// No flag argument, just return object
	if len(g.ChildExpressions) < 3 {
		return sql.JSONDocument{Val: obj}, nil
	}

	// Evaluate flag argument
	flag, err := g.ChildExpressions[2].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if flag == nil {
		return nil, nil
	}
	flag, err = sql.Int32.Convert(flag)
	if err != nil {
		return nil, err
	}
	_flag := flag.(int32)
	// Only flags 0-7 are valid
	if _flag < 0 || _flag > 7 {
		return nil, ErrInvalidArgument.New(g.FunctionName(), _flag)
	}
	// TODO: figure out exactly what flags are; only 1,3,5 have bbox
	if _flag%2 == 1 {
		// Calculate bounding box
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

// GeomFromGeoJSON is a function returns a geometry based on a string
type GeomFromGeoJSON struct {
	expression.NaryExpression
}

var _ sql.FunctionExpression = (*GeomFromGeoJSON)(nil)

// NewGeomFromGeoJSON creates a new point expression.
func NewGeomFromGeoJSON(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 1 || len(args) > 3 {
		return nil, sql.ErrInvalidArgumentNumber.New("ST_ASGEOJSON", "1, 2, or 3", len(args))
	}
	return &GeomFromGeoJSON{expression.NaryExpression{ChildExpressions: args}}, nil
}

// FunctionName implements sql.FunctionExpression
func (g *GeomFromGeoJSON) FunctionName() string {
	return "st_geomfromgeojson"
}

// Description implements sql.FunctionExpression
func (g *GeomFromGeoJSON) Description() string {
	return "returns a GeoJSON object from the geometry."
}

// Type implements the sql.Expression interface.
func (g *GeomFromGeoJSON) Type() sql.Type {
	return sql.PointType{}
}

func (g *GeomFromGeoJSON) String() string {
	var args = make([]string, len(g.ChildExpressions))
	for i, arg := range g.ChildExpressions {
		args[i] = arg.String()
	}
	return fmt.Sprintf("ST_GEOMFROMWKT(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (g *GeomFromGeoJSON) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewAsGeoJSON(children...)
}

// Eval implements the sql.Expression interface.
func (g *GeomFromGeoJSON) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := g.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	// Return nil when geometry is nil
	if val == nil {
		return nil, nil
	}
	// Convert to string
	val, err = sql.LongBlob.Convert(val)
	if err != nil {
		return nil, err
	}
	// Parse string as JSON
	var obj map[string]interface{}
	err = json.Unmarshal(val.([]byte), obj)
	if err != nil {
		return nil, err
	}
	// Check for type and coordinates
	geomType, ok := obj["type"]
	if !ok {
		return nil, errors.New("missing required member 'type'")
	}
	coords, ok := obj["coordinates"]
	if !ok {
		return nil, errors.New("missing required member 'coordinates'")
	}
	// Create type accordingly
	switch geomType {
	case "Point":
		c, ok := coords.([]float64)
		if !ok {
			return nil, errors.New("coordinates wrong type")
		}
		return sql.Point{SRID: 4326, X: c[1], Y: c[0]}, nil
	case "LineString":
	case "Polygon":
	default:
		return nil, errors.New("Bad type")
	}
	return nil, nil
}
