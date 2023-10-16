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
	"math"

	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/internal/similartext"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation/window"
	"github.com/dolthub/go-mysql-server/sql/expression/function/json"
	"github.com/dolthub/go-mysql-server/sql/expression/function/spatial"
)

// ErrFunctionAlreadyRegistered is thrown when a function is already registered
var ErrFunctionAlreadyRegistered = errors.NewKind("function '%s' is already registered")

// BuiltIns is the set of built-in functions any integrator can use
var BuiltIns = []sql.Function{
	// elt, find_in_set, insert, load_file, locate
	sql.Function1{Name: "abs", Fn: NewAbsVal},
	sql.Function1{Name: "acos", Fn: NewAcos},
	sql.Function1{Name: "any_value", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewAnyValue(e) }},
	sql.Function1{Name: "ascii", Fn: NewAscii},
	sql.Function1{Name: "asin", Fn: NewAsin},
	sql.Function1{Name: "atan", Fn: NewAtan},
	sql.Function1{Name: "avg", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewAvg(e) }},
	sql.Function1{Name: "bin", Fn: NewBin},
	sql.FunctionN{Name: "bin_to_uuid", Fn: NewBinToUUID},
	sql.Function1{Name: "bit_and", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewBitAnd(e) }},
	sql.Function1{Name: "bit_length", Fn: NewBitlength},
	sql.Function1{Name: "bit_or", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewBitOr(e) }},
	sql.Function1{Name: "bit_xor", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewBitXor(e) }},
	sql.Function1{Name: "ceil", Fn: NewCeil},
	sql.Function1{Name: "ceiling", Fn: NewCeil},
	sql.Function1{Name: "char_length", Fn: NewCharLength},
	sql.Function1{Name: "character_length", Fn: NewCharLength},
	sql.FunctionN{Name: "coalesce", Fn: NewCoalesce},
	sql.Function1{Name: "coercibility", Fn: NewCoercibility},
	sql.Function1{Name: "collation", Fn: NewCollation},
	sql.FunctionN{Name: "concat", Fn: NewConcat},
	sql.FunctionN{Name: "concat_ws", Fn: NewConcatWithSeparator},
	sql.NewFunction0("connection_id", NewConnectionID),
	sql.Function3{Name: "conv", Fn: NewConv},
	sql.Function1{Name: "cos", Fn: NewCos},
	sql.Function1{Name: "cot", Fn: NewCot},
	sql.Function3{Name: "convert_tz", Fn: NewConvertTz},
	sql.Function1{Name: "count", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewCount(e) }},
	sql.Function1{Name: "crc32", Fn: NewCrc32},
	sql.NewFunction0("curdate", NewCurrDate),
	sql.NewFunction0("current_date", NewCurrentDate),
	sql.NewFunction0("current_time", NewCurrentTime),
	sql.FunctionN{Name: "current_timestamp", Fn: NewCurrTimestamp},
	sql.NewFunction0("current_user", NewCurrentUser),
	sql.NewFunction0("curtime", NewCurrTime),
	sql.Function0{Name: "database", Fn: NewDatabase},
	sql.Function1{Name: "date", Fn: NewDate},
	sql.FunctionN{Name: "datetime", Fn: NewDatetime},
	sql.Function2{Name: "datediff", Fn: NewDateDiff},
	sql.FunctionN{Name: "date_add", Fn: NewDateAdd},
	sql.Function2{Name: "date_format", Fn: NewDateFormat},
	sql.FunctionN{Name: "date_sub", Fn: NewDateSub},
	sql.Function1{Name: "day", Fn: NewDay},
	sql.Function1{Name: "dayname", Fn: NewDayName},
	sql.Function1{Name: "dayofmonth", Fn: NewDay},
	sql.Function1{Name: "dayofweek", Fn: NewDayOfWeek},
	sql.Function1{Name: "dayofyear", Fn: NewDayOfYear},
	sql.Function1{Name: "degrees", Fn: NewDegrees},
	sql.Function2{Name: "extract", Fn: NewExtract},
	sql.Function2{Name: "find_in_set", Fn: NewFindInSet},
	sql.Function1{Name: "first", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewFirst(e) }},
	sql.Function1{Name: "floor", Fn: NewFloor},
	sql.Function0{Name: "found_rows", Fn: NewFoundRows},
	sql.FunctionN{Name: "format", Fn: NewFormat},
	sql.Function1{Name: "from_base64", Fn: NewFromBase64},
	sql.Function1{Name: "from_unixtime", Fn: NewFromUnixtime},
	sql.FunctionN{Name: "greatest", Fn: NewGreatest},
	sql.Function0{Name: "group_concat", Fn: aggregation.NewEmptyGroupConcat},
	sql.Function1{Name: "hex", Fn: NewHex},
	sql.Function1{Name: "hour", Fn: NewHour},
	sql.Function3{Name: "if", Fn: NewIf},
	sql.Function2{Name: "ifnull", Fn: NewIfNull},
	sql.Function1{Name: "inet_aton", Fn: NewInetAton},
	sql.Function1{Name: "inet_ntoa", Fn: NewInetNtoa},
	sql.Function1{Name: "inet6_aton", Fn: NewInet6Aton},
	sql.Function1{Name: "inet6_ntoa", Fn: NewInet6Ntoa},
	sql.Function2{Name: "instr", Fn: NewInstr},
	sql.Function1{Name: "is_binary", Fn: NewIsBinary},
	sql.Function1{Name: "is_ipv4", Fn: NewIsIPv4},
	sql.Function1{Name: "is_ipv4_compat", Fn: NewIsIPv4Compat},
	sql.Function1{Name: "is_ipv4_mapped", Fn: NewIsIPv4Mapped},
	sql.Function1{Name: "is_ipv6", Fn: NewIsIPv6},
	sql.Function1{Name: "is_uuid", Fn: NewIsUUID},
	sql.Function1{Name: "isnull", Fn: NewIsNull},
	sql.FunctionN{Name: "json_array", Fn: json.NewJSONArray},
	sql.Function1{Name: "json_arrayagg", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewJsonArray(e) }},
	sql.Function2{Name: "json_objectagg", Fn: aggregation.NewJSONObjectAgg},
	sql.FunctionN{Name: "json_array_append", Fn: json.NewJSONArrayAppend},
	sql.FunctionN{Name: "json_array_insert", Fn: json.NewJSONArrayInsert},
	sql.FunctionN{Name: "json_contains", Fn: json.NewJSONContains},
	sql.FunctionN{Name: "json_contains_path", Fn: json.NewJSONContainsPath},
	sql.FunctionN{Name: "json_depth", Fn: json.NewJSONDepth},
	sql.FunctionN{Name: "json_extract", Fn: json.NewJSONExtract},
	sql.FunctionN{Name: "json_insert", Fn: json.NewJSONInsert},
	sql.FunctionN{Name: "json_keys", Fn: json.NewJSONKeys},
	sql.FunctionN{Name: "json_length", Fn: json.NewJsonLength},
	sql.FunctionN{Name: "json_merge_patch", Fn: json.NewJSONMergePatch},
	sql.FunctionN{Name: "json_merge_preserve", Fn: json.NewJSONMergePreserve},
	sql.FunctionN{Name: "json_object", Fn: json.NewJSONObject},
	sql.FunctionN{Name: "json_overlaps", Fn: json.NewJSONOverlaps},
	sql.FunctionN{Name: "json_pretty", Fn: json.NewJSONPretty},
	sql.FunctionN{Name: "json_quote", Fn: json.NewJSONQuote},
	sql.FunctionN{Name: "json_remove", Fn: json.NewJSONRemove},
	sql.FunctionN{Name: "json_replace", Fn: json.NewJSONReplace},
	sql.FunctionN{Name: "json_schema_valid", Fn: json.NewJSONSchemaValid},
	sql.FunctionN{Name: "json_schema_validation_report", Fn: json.NewJSONSchemaValidationReport},
	sql.FunctionN{Name: "json_search", Fn: json.NewJSONSearch},
	sql.FunctionN{Name: "json_set", Fn: json.NewJSONSet},
	sql.FunctionN{Name: "json_storage_free", Fn: json.NewJSONStorageFree},
	sql.FunctionN{Name: "json_storage_size", Fn: json.NewJSONStorageSize},
	sql.FunctionN{Name: "json_table", Fn: json.NewJSONTable},
	sql.FunctionN{Name: "json_type", Fn: json.NewJSONType},
	sql.Function1{Name: "json_unquote", Fn: json.NewJSONUnquote},
	sql.FunctionN{Name: "json_valid", Fn: json.NewJSONValid},
	sql.FunctionN{Name: "json_value", Fn: json.NewJsonValue},
	sql.FunctionN{Name: "lag", Fn: func(e ...sql.Expression) (sql.Expression, error) { return window.NewLag(e...) }},
	sql.Function1{Name: "last", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewLast(e) }},
	sql.FunctionN{Name: "last_insert_id", Fn: NewLastInsertId},
	sql.Function1{Name: "lcase", Fn: NewLower},
	sql.FunctionN{Name: "lead", Fn: func(e ...sql.Expression) (sql.Expression, error) { return window.NewLead(e...) }},
	sql.FunctionN{Name: "least", Fn: NewLeast},
	sql.Function2{Name: "left", Fn: NewLeft},
	sql.Function1{Name: "length", Fn: NewLength},
	sql.Function1{Name: "ln", Fn: NewLogBaseFunc(float64(math.E))},
	sql.Function1{Name: "load_file", Fn: NewLoadFile},
	sql.FunctionN{Name: "locate", Fn: NewLocate},
	sql.FunctionN{Name: "log", Fn: NewLog},
	sql.Function1{Name: "log10", Fn: NewLogBaseFunc(float64(10))},
	sql.Function1{Name: "log2", Fn: NewLogBaseFunc(float64(2))},
	sql.Function1{Name: "lower", Fn: NewLower},
	sql.FunctionN{Name: "lpad", Fn: NewLeftPad},
	sql.Function1{Name: "ltrim", Fn: NewLeftTrim},
	sql.Function1{Name: "max", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewMax(e) }},
	sql.Function1{Name: "md5", Fn: NewMD5},
	sql.Function1{Name: "microsecond", Fn: NewMicrosecond},
	sql.FunctionN{Name: "mid", Fn: NewSubstring},
	sql.Function1{Name: "min", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewMin(e) }},
	sql.Function1{Name: "minute", Fn: NewMinute},
	sql.FunctionN{Name: "mod", Fn: NewMod},
	sql.Function1{Name: "month", Fn: NewMonth},
	sql.Function1{Name: "monthname", Fn: NewMonthName},
	sql.FunctionN{Name: "now", Fn: NewNow},
	sql.Function2{Name: "nullif", Fn: NewNullIf},
	sql.Function2{Name: "pow", Fn: NewPower},
	sql.Function2{Name: "power", Fn: NewPower},
	sql.Function1{Name: "radians", Fn: NewRadians},
	sql.FunctionN{Name: "rand", Fn: NewRand},
	sql.FunctionN{Name: "regexp_like", Fn: NewRegexpLike},
	sql.FunctionN{Name: "regexp_replace", Fn: NewRegexpReplace},
	sql.Function2{Name: "repeat", Fn: NewRepeat},
	sql.Function3{Name: "replace", Fn: NewReplace},
	sql.Function1{Name: "reverse", Fn: NewReverse},
	sql.Function2{Name: "right", Fn: NewRight},
	sql.FunctionN{Name: "round", Fn: NewRound},
	sql.Function0{Name: "row_count", Fn: NewRowCount},
	sql.Function0{Name: "row_number", Fn: window.NewRowNumber},
	sql.Function0{Name: "percent_rank", Fn: window.NewPercentRank},
	sql.Function0{Name: "rank", Fn: window.NewRank},
	sql.Function0{Name: "dense_rank", Fn: window.NewDenseRank},
	sql.Function1{Name: "first_value", Fn: window.NewFirstValue},
	sql.Function1{Name: "last_value", Fn: window.NewLastValue},
	sql.FunctionN{Name: "rpad", Fn: NewRightPad},
	sql.Function1{Name: "rtrim", Fn: NewRightTrim},
	sql.Function0{Name: "schema", Fn: NewDatabase},
	sql.Function1{Name: "second", Fn: NewSecond},
	sql.Function1{Name: "sha", Fn: NewSHA1},
	sql.Function1{Name: "sha1", Fn: NewSHA1},
	sql.Function2{Name: "sha2", Fn: NewSHA2},
	sql.Function1{Name: "sign", Fn: NewSign},
	sql.Function1{Name: "sin", Fn: NewSin},
	sql.Function1{Name: "sleep", Fn: NewSleep},
	sql.Function1{Name: "soundex", Fn: NewSoundex},
	sql.Function1{Name: "sqrt", Fn: NewSqrt},
	sql.FunctionN{Name: "str_to_date", Fn: NewStrToDate},
	sql.Function2{Name: "point", Fn: spatial.NewPoint},
	sql.FunctionN{Name: "linestring", Fn: spatial.NewLineString},
	sql.FunctionN{Name: "polygon", Fn: spatial.NewPolygon},
	sql.FunctionN{Name: "multipoint", Fn: spatial.NewMultiPoint},
	sql.FunctionN{Name: "multilinestring", Fn: spatial.NewMultiLineString},
	sql.FunctionN{Name: "multipolygon", Fn: spatial.NewMultiPolygon},
	sql.FunctionN{Name: "geometrycollection", Fn: spatial.NewGeomColl},
	sql.FunctionN{Name: "geomcollection", Fn: spatial.NewGeomColl},
	sql.Function1{Name: "st_area", Fn: spatial.NewArea},
	sql.Function1{Name: "st_asbinary", Fn: spatial.NewAsWKB},
	sql.FunctionN{Name: "st_asgeojson", Fn: spatial.NewAsGeoJSON},
	sql.Function1{Name: "st_aswkb", Fn: spatial.NewAsWKB},
	sql.Function1{Name: "st_aswkt", Fn: spatial.NewAsWKT},
	sql.Function1{Name: "st_astext", Fn: spatial.NewAsWKT},
	sql.FunctionN{Name: "st_distance", Fn: spatial.NewDistance},
	sql.Function1{Name: "st_dimension", Fn: spatial.NewDimension},
	sql.Function2{Name: "st_equal", Fn: spatial.NewSTEquals},
	sql.Function1{Name: "st_endpoint", Fn: spatial.NewEndPoint},
	sql.FunctionN{Name: "st_geomcollfromtext", Fn: spatial.NewGeomCollFromText},
	sql.FunctionN{Name: "st_geomcollfromtxt", Fn: spatial.NewGeomCollFromText},
	sql.FunctionN{Name: "st_geomcollfromwkb", Fn: spatial.NewGeomCollFromWKB},
	sql.FunctionN{Name: "st_geometrycollectionfromwkb", Fn: spatial.NewGeomCollFromWKB},
	sql.FunctionN{Name: "st_geometrycollectionfromtext", Fn: spatial.NewGeomCollFromText},
	sql.FunctionN{Name: "st_geomfromgeojson", Fn: spatial.NewGeomFromGeoJSON},
	sql.FunctionN{Name: "st_geometryfromtext", Fn: spatial.NewGeomFromText},
	sql.FunctionN{Name: "st_geomfromtext", Fn: spatial.NewGeomFromText},
	sql.FunctionN{Name: "st_geometryfromwkb", Fn: spatial.NewGeomFromWKB},
	sql.FunctionN{Name: "st_geomfromwkb", Fn: spatial.NewGeomFromWKB},
	sql.Function1{Name: "st_isclosed", Fn: spatial.NewIsClosed},
	sql.Function2{Name: "st_intersects", Fn: spatial.NewIntersects},
	sql.FunctionN{Name: "st_length", Fn: spatial.NewSTLength},
	sql.FunctionN{Name: "st_longitude", Fn: spatial.NewLongitude},
	sql.FunctionN{Name: "st_linefromtext", Fn: spatial.NewLineFromText},
	sql.FunctionN{Name: "st_linefromwkb", Fn: spatial.NewLineFromWKB},
	sql.FunctionN{Name: "st_linestringfromtext", Fn: spatial.NewLineFromText},
	sql.FunctionN{Name: "st_linestringfromwkb", Fn: spatial.NewLineFromWKB},
	sql.FunctionN{Name: "st_mlinefromtext", Fn: spatial.NewMLineFromText},
	sql.FunctionN{Name: "st_mlinefromwkb", Fn: spatial.NewMLineFromWKB},
	sql.FunctionN{Name: "st_multilinestringfromtext", Fn: spatial.NewLineFromText},
	sql.FunctionN{Name: "st_multilinestringfromwkb", Fn: spatial.NewMLineFromWKB},
	sql.FunctionN{Name: "st_mpointfromtext", Fn: spatial.NewMPointFromText},
	sql.FunctionN{Name: "st_mpointfromwkb", Fn: spatial.NewMPointFromWKB},
	sql.FunctionN{Name: "st_multipointfromtext", Fn: spatial.NewMPointFromText},
	sql.FunctionN{Name: "st_multipointfromwkb", Fn: spatial.NewMPointFromWKB},
	sql.FunctionN{Name: "st_mpolyfromwkb", Fn: spatial.NewMPolyFromWKB},
	sql.FunctionN{Name: "st_mpolyfromtext", Fn: spatial.NewMPolyFromText},
	sql.FunctionN{Name: "st_multipolygonfromwkb", Fn: spatial.NewMPolyFromWKB},
	sql.FunctionN{Name: "st_multipolygonfromtext", Fn: spatial.NewMPolyFromText},
	sql.FunctionN{Name: "st_perimeter", Fn: spatial.NewPerimeter},
	sql.FunctionN{Name: "st_pointfromtext", Fn: spatial.NewPointFromText},
	sql.FunctionN{Name: "st_pointfromwkb", Fn: spatial.NewPointFromWKB},
	sql.FunctionN{Name: "st_polyfromtext", Fn: spatial.NewPolyFromText},
	sql.FunctionN{Name: "st_polyfromwkb", Fn: spatial.NewPolyFromWKB},
	sql.FunctionN{Name: "st_polygonfromtext", Fn: spatial.NewPolyFromText},
	sql.FunctionN{Name: "st_polygonfromwkb", Fn: spatial.NewPolyFromWKB},
	sql.FunctionN{Name: "st_srid", Fn: spatial.NewSRID},
	sql.Function1{Name: "st_startpoint", Fn: spatial.NewStartPoint},
	sql.Function1{Name: "st_swapxy", Fn: spatial.NewSwapXY},
	sql.Function2{Name: "st_within", Fn: spatial.NewWithin},
	sql.FunctionN{Name: "st_x", Fn: spatial.NewSTX},
	sql.FunctionN{Name: "st_y", Fn: spatial.NewSTY},
	sql.Function2{Name: "strcmp", Fn: NewStrCmp},
	sql.FunctionN{Name: "substr", Fn: NewSubstring},
	sql.FunctionN{Name: "substring", Fn: NewSubstring},
	sql.Function3{Name: "substring_index", Fn: NewSubstringIndex},
	sql.Function1{Name: "sum", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewSum(e) }},
	sql.Function1{Name: "tan", Fn: NewTan},
	sql.Function1{Name: "time", Fn: NewTime},
	sql.Function2{Name: "time_format", Fn: NewTimeFormat},
	sql.Function1{Name: "time_to_sec", Fn: NewTimeToSec},
	sql.Function2{Name: "timediff", Fn: NewTimeDiff},
	sql.FunctionN{Name: "timestamp", Fn: NewTimestamp},
	sql.Function3{Name: "timestampdiff", Fn: NewTimestampDiff},
	sql.Function1{Name: "to_base64", Fn: NewToBase64},
	sql.Function1{Name: "ucase", Fn: NewUpper},
	sql.Function1{Name: "unhex", Fn: NewUnhex},
	sql.FunctionN{Name: "unix_timestamp", Fn: NewUnixTimestamp},
	sql.Function1{Name: "upper", Fn: NewUpper},
	sql.NewFunction0("user", NewUser),
	sql.FunctionN{Name: "utc_timestamp", Fn: NewUTCTimestamp},
	sql.Function0{Name: "uuid", Fn: NewUUIDFunc},
	sql.FunctionN{Name: "uuid_to_bin", Fn: NewUUIDToBin},
	sql.FunctionN{Name: "week", Fn: NewWeek},
	sql.Function1{Name: "values", Fn: NewValues},
	sql.Function1{Name: "weekday", Fn: NewWeekday},
	sql.Function1{Name: "weekofyear", Fn: NewWeekOfYear},
	sql.Function1{Name: "year", Fn: NewYear},
	sql.FunctionN{Name: "yearweek", Fn: NewYearWeek},
}

func GetLockingFuncs(ls *sql.LockSubsystem) []sql.Function {
	return []sql.Function{
		sql.Function2{Name: "get_lock", Fn: CreateNewGetLock(ls)},
		sql.Function1{Name: "is_free_lock", Fn: NewIsFreeLock(ls)},
		sql.Function1{Name: "is_used_lock", Fn: NewIsUsedLock(ls)},
		sql.NewFunction0("release_all_locks", NewReleaseAllLocks(ls)),
		sql.Function1{Name: "release_lock", Fn: NewReleaseLock(ls)},
	}
}

// Registry is used to register functions
type Registry map[string]sql.Function

var _ sql.FunctionProvider = Registry{}

// NewRegistry creates a new Registry.
func NewRegistry() Registry {
	fr := make(Registry)
	fr.mustRegister(BuiltIns...)
	return fr
}

// Register registers functions, returning an error if it's already registered
func (r Registry) Register(fn ...sql.Function) error {
	for _, f := range fn {
		if _, ok := r[f.FunctionName()]; ok {
			return ErrFunctionAlreadyRegistered.New(f.FunctionName())
		}
		r[f.FunctionName()] = f
	}
	return nil
}

// Function implements sql.FunctionProvider
func (r Registry) Function(ctx *sql.Context, name string) (sql.Function, error) {
	if fn, ok := r[name]; ok {
		return fn, nil
	}
	similar := similartext.FindFromMap(r, name)
	return nil, sql.ErrFunctionNotFound.New(name + similar)
}

func (r Registry) mustRegister(fn ...sql.Function) {
	if err := r.Register(fn...); err != nil {
		panic(err)
	}
}
