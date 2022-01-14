package function

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
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

func PointToSlice(p sql.Point) [2]float64 {
	return [2]float64{p.X, p.Y}
}

func LineToSlice(l sql.Linestring) [][2]float64 {
	arr := make([][2]float64, len(l.Points))
	for i, p := range l.Points {
		arr[i] = PointToSlice(p)
	}
	return arr
}

func PolyToSlice(p sql.Polygon) [][][2]float64 {
	arr := make([][][2]float64, len(p.Lines))
	for i, l := range p.Lines {
		arr[i] = LineToSlice(l)
	}
	return arr
}

// Eval implements the sql.Expression interface.
func (g *AsGeoJSON) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	// Evaluate child
	val, err := g.ChildExpressions[0].Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	// Create map object to hold values
	obj := make(map[string]interface{}, 2) // TODO: needs to be 3 when including bounding box

	switch v := val.(type) {
	case sql.Point:
		obj["type"] = "Point"
		obj["coordinates"] = PointToSlice(v)
	case sql.Linestring:
		obj["type"] = "LineString"
		obj["coordinates"] = LineToSlice(v)
	case sql.Polygon:
		obj["type"] = "Polygon"
		obj["coordinates"] = PolyToSlice(v)
	default:
		return nil, ErrInvalidArgumentType.New(g.FunctionName())
	}

	// TODO: GeoJSON object always assume srid is 4326
	return sql.JSONDocument{Val: obj}, nil
}