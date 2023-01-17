// Copyright 2023 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

type SpatialRef struct {
	Name          string
	ID            uint32
	Organization  interface{}
	OrgCoordsysId interface{}
	Definition    string
	Description   interface{}
}

var SupportedSRIDs = map[uint32]SpatialRef{
	/*0*/ sql.CartesianSRID: {"", sql.CartesianSRID, nil, nil, "", nil},
	/*4326*/ sql.GeoSpatialSRID: {"WGS 84", sql.GeoSpatialSRID, "EPSG", sql.GeoSpatialSRID, "GEOGCS[\"WGS 84\",DATUM[\"World Geodetic System 1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],AUTHORITY[\"EPSG\",\"4326\"]]", nil},
}
