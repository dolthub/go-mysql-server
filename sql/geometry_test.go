// Copyright 2022 Dolthub, Inc.
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

package sql

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"
)

func TestSpatialTypeMatchSRID(t *testing.T) {
	var (
		// SRID 0 points
		Cx1y2 = Point{CartesianSRID, 1, 2}
		Cx2y3 = Point{CartesianSRID, 2, 3}
		Cx0y0 = Point{CartesianSRID, 0, 0}

		// SRID 4326 points
		Gx1y2 = Point{GeoSpatialSRID, 1, 2}
		Gx2y3 = Point{GeoSpatialSRID, 2, 3}
		Gx0y0 = Point{GeoSpatialSRID, 0, 0}
		Gx0y1 = Point{GeoSpatialSRID, 0, 1}
		Gx1y0 = Point{GeoSpatialSRID, 1, 0}
	)
	tests := []struct {
		typeVal  SpatialColumnType
		objVal   interface{}
		expected *errors.Kind
	}{
		{PointType{CartesianSRID, false}, Cx1y2, nil},
		{PointType{CartesianSRID, false}, Gx1y2, nil},
		{PointType{GeoSpatialSRID, true}, Cx1y2, ErrNotMatchingSRID},
		{PointType{GeoSpatialSRID, true}, LineString{GeoSpatialSRID, []Point{Cx1y2, Cx2y3}}, ErrNotPoint},
		{PointType{GeoSpatialSRID, true}, LineString{GeoSpatialSRID, []Point{Cx1y2, Cx2y3}}, ErrNotPoint},

		{LineStringType{GeoSpatialSRID, true}, LineString{GeoSpatialSRID, []Point{Cx1y2, Cx2y3}}, nil},
		// MySQL checks only the container's SRID value, so the objects inside can have any SRID value.
		// For example, LineStringType column with 4326 allows LineString object with 4326 containing Points with 0 and 4326 SRID values.
		{LineStringType{CartesianSRID, false}, LineString{GeoSpatialSRID, []Point{Cx1y2, Gx2y3}}, nil},
		{LineStringType{CartesianSRID, true}, LineString{GeoSpatialSRID, []Point{Cx1y2, Cx2y3}}, ErrNotMatchingSRID},
		{LineStringType{GeoSpatialSRID, true}, Gx2y3, ErrNotLineString},
		{LineStringType{GeoSpatialSRID, true}, Polygon{GeoSpatialSRID, []LineString{{GeoSpatialSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Gx0y0}}}}, ErrNotLineString},

		{PolygonType{CartesianSRID, true}, Polygon{CartesianSRID, []LineString{{GeoSpatialSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Gx0y0}}}}, nil},
		{PolygonType{CartesianSRID, false}, Polygon{GeoSpatialSRID, []LineString{{GeoSpatialSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Gx0y0}}}}, nil},
		{PolygonType{CartesianSRID, true}, Polygon{GeoSpatialSRID, []LineString{{GeoSpatialSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Gx0y0}}}}, ErrNotMatchingSRID},
		{PolygonType{GeoSpatialSRID, true}, Gx2y3, ErrNotPolygon},
		{PolygonType{GeoSpatialSRID, true}, LineString{GeoSpatialSRID, []Point{Cx1y2, Cx2y3}}, ErrNotPolygon},

		{GeometryType{CartesianSRID, false}, Cx1y2, nil},
		{GeometryType{CartesianSRID, false}, Gx1y2, nil},
		{GeometryType{GeoSpatialSRID, true}, Gx1y2, nil},
		{GeometryType{GeoSpatialSRID, true}, Cx1y2, ErrNotMatchingSRID},
		{GeometryType{GeoSpatialSRID, true}, LineString{GeoSpatialSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Gx0y0}}, nil},
		{GeometryType{GeoSpatialSRID, true}, LineString{CartesianSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Gx0y0}}, ErrNotMatchingSRID},
		{GeometryType{GeoSpatialSRID, true}, LineString{GeoSpatialSRID, []Point{Cx1y2, Cx2y3}}, nil},
		{GeometryType{CartesianSRID, true}, LineString{GeoSpatialSRID, []Point{Cx1y2, Cx2y3}}, ErrNotMatchingSRID},
		{GeometryType{GeoSpatialSRID, true}, Polygon{GeoSpatialSRID, []LineString{{GeoSpatialSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Gx0y0}}}}, nil},
		{GeometryType{GeoSpatialSRID, true}, Polygon{GeoSpatialSRID, []LineString{{CartesianSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Cx0y0}}}}, nil},
		{GeometryType{CartesianSRID, true}, Polygon{GeoSpatialSRID, []LineString{{CartesianSRID, []Point{Gx0y0, Gx0y1, Gx1y0, Cx0y0}}}}, ErrNotMatchingSRID},
	}

	for _, test := range tests {
		s, d := test.typeVal.GetSpatialTypeSRID()
		g, _ := test.typeVal.(Type)
		t.Run(fmt.Sprintf("%s %v %v match %v", g.String(), s, d, test.objVal), func(t *testing.T) {
			err := test.typeVal.MatchSRID(test.objVal)
			if test.expected == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.True(t, test.expected.Is(err), "Expected error of type %s but got %s", test.expected, err)
			}
		})
	}
}
