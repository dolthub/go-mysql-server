package function

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

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
	return fmt.Sprintf("ST_ASGEOJSON(%s)", strings.Join(args, ","))
}

// WithChildren implements the Expression interface.
func (g *AsGeoJSON) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewAsGeoJSON(children...)
}

func PointToSlice(p sql.Point) [2]float64 {
	return [2]float64{p.X, p.Y}
}

func LineToSlice(l sql.LineString) [][2]float64 {
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

func FindBBox(v interface{}) [4]float64 {
	var res [4]float64
	switch v := v.(type) {
	case sql.Point:
		res = [4]float64{v.X, v.Y, v.X, v.Y}
	case sql.LineString:
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

func RoundFloatSlices(v interface{}, p float64) interface{} {
	switch v := v.(type) {
	case [2]float64:
		return [2]float64{math.Round(v[0]*p) / p, math.Round(v[1]*p) / p}
	case [][2]float64:
		res := make([][2]float64, len(v))
		for i, c := range v {
			res[i] = RoundFloatSlices(c, p).([2]float64)
		}
		return res
	case [][][2]float64:
		res := make([][][2]float64, len(v))
		for i, c := range v {
			res[i] = RoundFloatSlices(c, p).([][2]float64)
		}
		return res
	}
	return nil
}

// GetSRID returns the SRID given a Geometry type, will return -1 otherwise
func GetSRID(val interface{}) int {
	switch v := val.(type) {
	case sql.Point:
		return int(v.SRID)
	case sql.LineString:
		return int(v.SRID)
	case sql.Polygon:
		return int(v.SRID)
	default:
		return -1
	}
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

	// Create map object to hold values
	obj := make(map[string]interface{})
	switch v := val.(type) {
	case sql.Point:
		obj["type"] = "Point"
		obj["coordinates"] = PointToSlice(v)
	case sql.LineString:
		obj["type"] = "LineString"
		obj["coordinates"] = LineToSlice(v)
	case sql.Polygon:
		obj["type"] = "Polygon"
		obj["coordinates"] = PolyToSlice(v)
	default:
		return nil, ErrInvalidArgumentType.New(g.FunctionName())
	}

	// No precision argument, just return object
	if len(g.ChildExpressions) == 1 {
		return sql.JSONDocument{Val: obj}, nil
	}

	// Evaluate precision
	p, err := g.ChildExpressions[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	// Return null if precision is null
	if p == nil {
		return nil, nil
	}
	// Must be an int type
	_p := 0
	switch p := p.(type) {
	case int8:
		_p = int(p)
	case int16:
		_p = int(p)
	case int32:
		_p = int(p)
	case int64:
		_p = int(p)
	case int:
		_p = p
	default:
		return nil, errors.New("incorrect precision value")
	}
	// Must be >= 0
	if _p < 0 {
		return nil, errors.New("incorrect precision value")
	}

	// TODO: lose accuracy with high precisions, 17 is about the most MySQL prints anyway
	if _p > 17 {
		_p = 17
	}

	// Round floats
	prec := math.Pow10(_p)
	obj["coordinates"] = RoundFloatSlices(obj["coordinates"], prec)

	// No flag argument, just return object
	if len(g.ChildExpressions) == 2 {
		return sql.JSONDocument{Val: obj}, nil
	}

	// Evaluate flag argument
	flag, err := g.ChildExpressions[2].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	// Return null if flag is null
	if flag == nil {
		return nil, nil
	}
	// Must be an int type
	_flag := 0
	switch flag := flag.(type) {
	case int8:
		_flag = int(flag)
	case int16:
		_flag = int(flag)
	case int32:
		_flag = int(flag)
	case int64:
		_flag = int(flag)
	case int:
		_flag = flag
	default:
		return nil, errors.New("incorrect flag value")
	}
	// Only flags 0-7 are valid
	if _flag < 0 || _flag > 7 {
		return nil, sql.ErrInvalidArgumentDetails.New(g.FunctionName(), _flag)
	}

	switch _flag {
	// Flags 1,3,5 have bounding box
	case 1, 3, 5:
		// Calculate bounding box
		res := FindBBox(val)
		for i, r := range res {
			res[i] = math.Round(r*prec) / prec
		}
		obj["bbox"] = res
	// Flag 2 and 4 add CRS URN (EPSG: <srid>); only shows up if SRID != 0
	case 2, 4:
		// CRS obj only shows up if srid != 0
		srid := GetSRID(val)
		if srid != 0 {
			// Create CRS URN Object
			crs := make(map[string]interface{})
			crs["type"] = "name"

			// Create properties
			props := make(map[string]interface{})
			// Flag 2 is short format CRS URN, while 4 is long format
			sridStr := strconv.Itoa(srid)
			if _flag == 2 {
				props["name"] = "EPSG:" + sridStr
			} else {
				props["name"] = "urn:ogc:def:crs:EPSG::" + sridStr
			}
			// Add properties to crs
			crs["properties"] = props

			// Add CRS to main object
			obj["crs"] = crs
		}
	}

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
	return NewGeomFromGeoJSON(children...)
}

func SliceToPoint(coords interface{}) (interface{}, error) {
	// coords must be a slice of 2 float64
	c, ok := coords.([]interface{})
	if !ok {
		return nil, errors.New("member 'coordinates' must be of type 'array'")
	}
	if len(c) < 2 {
		return nil, errors.New("unsupported number of coordinate dimensions")
	}
	x, ok := c[1].(float64)
	if !ok {
		return nil, errors.New("coordinate must be of type number")
	}
	y, ok := c[0].(float64)
	if !ok {
		return nil, errors.New("coordinate must be of type number")
	}
	return sql.Point{SRID: sql.GeoSpatialSRID, X: x, Y: y}, nil
}

func SliceToLine(coords interface{}) (interface{}, error) {
	// coords must be a slice of at least 2 slices of 2 float64
	cs, ok := coords.([]interface{})
	if !ok {
		return nil, errors.New("member 'coordinates' must be of type 'array'")
	}
	if len(cs) < 2 {
		return nil, errors.New("invalid GeoJSON data provided")
	}
	points := make([]sql.Point, len(cs))
	for i, c := range cs {
		p, err := SliceToPoint(c)
		if err != nil {
			return nil, err
		}
		points[i] = p.(sql.Point)
	}
	return sql.LineString{SRID: sql.GeoSpatialSRID, Points: points}, nil
}

func SliceToPoly(coords interface{}) (interface{}, error) {
	// coords must be a slice of slices of at least 2 slices of 2 float64
	cs, ok := coords.([]interface{})
	if !ok {
		return nil, errors.New("member 'coordinates' must be of type 'array'")
	}
	if len(cs) == 0 {
		return nil, errors.New("not enough lines")
	}
	lines := make([]sql.LineString, len(cs))
	for i, c := range cs {
		l, err := SliceToLine(c)
		if err != nil {
			return nil, err
		}
		if !isLinearRing(l.(sql.LineString)) {
			return nil, errors.New("invalid GeoJSON data provided")
		}
		lines[i] = l.(sql.LineString)
	}
	return sql.Polygon{SRID: sql.GeoSpatialSRID, Lines: lines}, nil
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
	err = json.Unmarshal([]byte(val.(string)), &obj)
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
	var res interface{}
	switch geomType {
	case "Point":
		res, err = SliceToPoint(coords)
	case "LineString":
		res, err = SliceToLine(coords)
	case "Polygon":
		res, err = SliceToPoly(coords)
	default:
		return nil, errors.New("member 'type' is wrong")
	}
	// Handle error
	if err != nil {
		return nil, err
	}
	// if only 1 argument, return
	if len(g.ChildExpressions) == 1 {
		return res, nil
	}
	// Evaluate flag argument
	flag, err := g.ChildExpressions[1].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if flag == nil {
		return nil, nil
	}
	// Must be an int type
	_flag := 0
	switch flag := flag.(type) {
	case int8:
		_flag = int(flag)
	case int16:
		_flag = int(flag)
	case int32:
		_flag = int(flag)
	case int64:
		_flag = int(flag)
	case int:
		_flag = flag
	default:
		return nil, errors.New("incorrect flag value")
	}
	// Only flags 1-4 are valid
	if _flag < 1 || _flag > 4 {
		return nil, sql.ErrInvalidArgumentDetails.New(g.FunctionName(), _flag)
	}
	// If flag is 1 and dimension of coordinates is greater than 2, throw error
	if _flag == 1 {
		// Swap coordinates with SRID 0
		switch geomType {
		case "Point":
			if len(obj["coordinates"].([]interface{})) > 2 {
				return nil, errors.New("unsupported number of coordinate dimensions")
			}
		case "LineString":
			for _, a := range obj["coordinates"].([]interface{}) {
				if len(a.([]interface{})) > 2 {
					return nil, errors.New("unsupported number of coordinate dimensions")
				}
			}
		case "Polygon":
			for _, a := range obj["coordinates"].([]interface{}) {
				for _, b := range a.([]interface{}) {
					if len(b.([]interface{})) > 2 {
						return nil, errors.New("unsupported number of coordinate dimensions")
					}
				}
			}
		}
	}
	// If no SRID provided, return answer
	if len(g.ChildExpressions) == 2 {
		return res, nil
	}
	// Evaluate SRID
	srid, err := g.ChildExpressions[2].Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if srid == nil {
		return nil, nil
	}
	// Must be an uint32 type
	_srid := uint32(0)
	switch srid := srid.(type) {
	case int8:
		_srid = uint32(srid)
	case int16:
		_srid = uint32(srid)
	case int32:
		_srid = uint32(srid)
	case int64:
		_srid = uint32(srid)
	case int:
		_srid = uint32(srid)
	default:
		return nil, errors.New("incorrect srid value")
	}
	if err = ValidateSRID(_srid); err != nil {
		return nil, err
	}
	// If SRID is GeoSpatialSRID (4326), do nothing
	if _srid == sql.GeoSpatialSRID {
		return res, nil
	}
	// Swap coordinates with SRID 0
	switch geomType {
	case "Point":
		_res := res.(sql.Point)
		_res.SRID = _srid
		_res.X, _res.Y = _res.Y, _res.X
		return _res, nil
	case "LineString":
		_res := res.(sql.LineString)
		_res.SRID = _srid
		for i, p := range _res.Points {
			_res.Points[i].SRID = _srid
			_res.Points[i].X, _res.Points[i].Y = p.Y, p.X
		}
		return _res, nil
	case "Polygon":
		_res := res.(sql.Polygon)
		_res.SRID = _srid
		for i, l := range _res.Lines {
			_res.Lines[i].SRID = _srid
			for j, p := range l.Points {
				_res.Lines[i].Points[j].SRID = _srid
				_res.Lines[i].Points[j].X, _res.Lines[i].Points[j].Y = p.Y, p.X
			}
		}
		return _res, nil
	}
	return nil, nil
}
