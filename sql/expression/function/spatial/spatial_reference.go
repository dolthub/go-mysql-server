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

import "github.com/dolthub/go-mysql-server/sql/types"

type SpatialRef struct {
	Name          string
	ID            uint32
	Organization  interface{}
	OrgCoordsysId interface{}
	Definition    string
	Description   interface{}
}

// TODO: need to keep track of new supported SRIDs instead of making changes to add them in multiple places

var SupportedSRIDs = map[uint32]SpatialRef{
	/*0*/ types.CartesianSRID: {"", types.CartesianSRID, nil, nil, "", nil},
	/*3857*/ uint32(3857): {"WGS 84 / Pseudo-Mercator", uint32(3857), "EPSG", uint32(3857), "PROJCS[\"WGS 84 / Pseudo-Mercator\",GEOGCS[\"WGS 84\",DATUM[\"World Geodetic System 1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],AUTHORITY[\"EPSG\",\"4326\"]],PROJECTION[\"Popular Visualisation Pseudo Mercator\",AUTHORITY[\"EPSG\",\"1024\"]],PARAMETER[\"Latitude of natural origin\",0,AUTHORITY[\"EPSG\",\"8801\"]],PARAMETER[\"Longitude of natural origin\",0,AUTHORITY[\"EPSG\",\"8802\"]],PARAMETER[\"False easting\",0,AUTHORITY[\"EPSG\",\"8806\"]],PARAMETER[\"False northing\",0,AUTHORITY[\"EPSG\",\"8807\"]],UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],AXIS[\"X\",EAST],AXIS[\"Y\",NORTH],AUTHORITY[\"EPSG\",\"3857\"]]", nil},
	/*4326*/ types.GeoSpatialSRID: {"WGS 84", types.GeoSpatialSRID, "EPSG", types.GeoSpatialSRID, "GEOGCS[\"WGS 84\",DATUM[\"World Geodetic System 1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.017453292519943278,AUTHORITY[\"EPSG\",\"9122\"]],AXIS[\"Lat\",NORTH],AXIS[\"Lon\",EAST],AUTHORITY[\"EPSG\",\"4326\"]]", nil},
}
