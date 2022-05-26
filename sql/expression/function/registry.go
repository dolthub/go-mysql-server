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
)

// ErrFunctionAlreadyRegistered is thrown when a function is already registered
var ErrFunctionAlreadyRegistered = errors.NewKind("function '%s' is already registered")

// BuiltIns is the set of built-in functions any integrator can use
var BuiltIns = []sql.Function{
	// elt, find_in_set, insert, load_file, locate
	sql.Function1{Name: "abs", Fn: NewAbsVal},
	sql.Function1{Name: "acos", Fn: NewAcos},
	sql.Function1{Name: "array_length", Fn: NewArrayLength},
	sql.Function1{Name: "ascii", Fn: NewAscii},
	sql.Function1{Name: "asin", Fn: NewAsin},
	sql.Function1{Name: "atan", Fn: NewAtan},
	sql.Function1{Name: "avg", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewAvg(e) }},
	sql.Function1{Name: "bin", Fn: NewBin},
	sql.FunctionN{Name: "bin_to_uuid", Fn: NewBinToUUID},
	sql.Function1{Name: "bit_length", Fn: NewBitlength},
	sql.Function1{Name: "ceil", Fn: NewCeil},
	sql.Function1{Name: "ceiling", Fn: NewCeil},
	sql.Function1{Name: "char_length", Fn: NewCharLength},
	sql.Function1{Name: "character_length", Fn: NewCharLength},
	sql.FunctionN{Name: "coalesce", Fn: NewCoalesce},
	sql.FunctionN{Name: "concat", Fn: NewConcat},
	sql.FunctionN{Name: "concat_ws", Fn: NewConcatWithSeparator},
	sql.NewFunction0("connection_id", NewConnectionID),
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
	sql.Function1{Name: "explode", Fn: NewExplode},
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
	sql.FunctionN{Name: "json_array", Fn: NewJSONArray},
	sql.Function1{Name: "json_arrayagg", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewJsonArray(e) }},
	sql.Function2{Name: "json_objectagg", Fn: aggregation.NewJSONObjectAgg},
	sql.FunctionN{Name: "json_array_append", Fn: NewJSONArrayAppend},
	sql.FunctionN{Name: "json_array_insert", Fn: NewJSONArrayInsert},
	sql.FunctionN{Name: "json_contains", Fn: NewJSONContains},
	sql.FunctionN{Name: "json_contains_path", Fn: NewJSONContainsPath},
	sql.FunctionN{Name: "json_depth", Fn: NewJSONDepth},
	sql.FunctionN{Name: "json_extract", Fn: NewJSONExtract},
	sql.FunctionN{Name: "json_insert", Fn: NewJSONInsert},
	sql.FunctionN{Name: "json_keys", Fn: NewJSONKeys},
	sql.FunctionN{Name: "json_length", Fn: NewJSONLength},
	sql.FunctionN{Name: "json_merge_patch", Fn: NewJSONMergePatch},
	sql.FunctionN{Name: "json_merge_preserve", Fn: NewJSONMergePreserve},
	sql.FunctionN{Name: "json_object", Fn: NewJSONObject},
	sql.FunctionN{Name: "json_overlaps", Fn: NewJSONOverlaps},
	sql.FunctionN{Name: "json_pretty", Fn: NewJSONPretty},
	sql.FunctionN{Name: "json_quote", Fn: NewJSONQuote},
	sql.FunctionN{Name: "json_remove", Fn: NewJSONRemove},
	sql.FunctionN{Name: "json_replace", Fn: NewJSONReplace},
	sql.FunctionN{Name: "json_schema_valid", Fn: NewJSONSchemaValid},
	sql.FunctionN{Name: "json_schema_validation_report", Fn: NewJSONSchemaValidationReport},
	sql.FunctionN{Name: "json_search", Fn: NewJSONSearch},
	sql.FunctionN{Name: "json_set", Fn: NewJSONSet},
	sql.FunctionN{Name: "json_storage_free", Fn: NewJSONStorageFree},
	sql.FunctionN{Name: "json_storage_size", Fn: NewJSONStorageSize},
	sql.FunctionN{Name: "json_table", Fn: NewJSONTable},
	sql.FunctionN{Name: "json_type", Fn: NewJSONType},
	sql.Function1{Name: "json_unquote", Fn: NewJSONUnquote},
	sql.FunctionN{Name: "json_valid", Fn: NewJSONValid},
	sql.FunctionN{Name: "json_value", Fn: NewJSONValue},
	sql.FunctionN{Name: "lag", Fn: func(e ...sql.Expression) (sql.Expression, error) { return window.NewLag(e...) }},
	sql.Function1{Name: "last", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewLast(e) }},
	sql.Function0{Name: "last_insert_id", Fn: NewLastInsertId},
	sql.Function1{Name: "lcase", Fn: NewLower},
	sql.FunctionN{Name: "least", Fn: NewLeast},
	sql.Function2{Name: "left", Fn: NewLeft},
	sql.Function1{Name: "length", Fn: NewLength},
	sql.FunctionN{Name: "linestring", Fn: NewLineString},
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
	sql.Function2{Name: "point", Fn: NewPoint},
	sql.FunctionN{Name: "polygon", Fn: NewPolygon},
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
	sql.Function1{Name: "first_value", Fn: window.NewFirstValue},
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
	sql.Function2{Name: "split", Fn: NewSplit},
	sql.Function1{Name: "sqrt", Fn: NewSqrt},
	sql.FunctionN{Name: "str_to_date", Fn: NewStrToDate},
	sql.Function1{Name: "st_asbinary", Fn: NewAsWKB},
	sql.FunctionN{Name: "st_asgeojson", Fn: NewAsGeoJSON},
	sql.Function1{Name: "st_aswkb", Fn: NewAsWKB},
	sql.Function1{Name: "st_aswkt", Fn: NewAsWKT},
	sql.Function1{Name: "st_astext", Fn: NewAsWKT},
	sql.Function1{Name: "st_dimension", Fn: NewDimension},
	sql.FunctionN{Name: "st_geomfromgeojson", Fn: NewGeomFromGeoJSON},
	sql.FunctionN{Name: "st_geomfromtext", Fn: NewGeomFromWKT},
	sql.FunctionN{Name: "st_geomfromwkb", Fn: NewGeomFromWKB},
	sql.FunctionN{Name: "st_longitude", Fn: NewLongitude},
	sql.FunctionN{Name: "st_linefromwkb", Fn: NewLineFromWKB},
	sql.FunctionN{Name: "st_pointfromwkb", Fn: NewPointFromWKB},
	sql.FunctionN{Name: "st_polyfromwkb", Fn: NewPolyFromWKB},
	sql.FunctionN{Name: "st_geomfromwkt", Fn: NewGeomFromWKT},
	sql.FunctionN{Name: "st_linefromwkt", Fn: NewLineFromWKT},
	sql.FunctionN{Name: "st_pointfromwkt", Fn: NewPointFromWKT},
	sql.FunctionN{Name: "st_polyfromwkt", Fn: NewPolyFromWKT},
	sql.FunctionN{Name: "st_srid", Fn: NewSRID},
	sql.Function1{Name: "st_swapxy", Fn: NewSwapXY},
	sql.FunctionN{Name: "st_x", Fn: NewSTX},
	sql.FunctionN{Name: "st_y", Fn: NewSTY},
	sql.FunctionN{Name: "substr", Fn: NewSubstring},
	sql.FunctionN{Name: "substring", Fn: NewSubstring},
	sql.Function3{Name: "substring_index", Fn: NewSubstringIndex},
	sql.Function1{Name: "sum", Fn: func(e sql.Expression) sql.Expression { return aggregation.NewSum(e) }},
	sql.Function1{Name: "tan", Fn: NewTan},
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
