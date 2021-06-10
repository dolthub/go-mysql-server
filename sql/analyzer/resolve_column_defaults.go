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

package analyzer

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

var validColumnDefaultFuncs = map[string]struct{}{
	"abs":                                {},
	"acos":                               {},
	"adddate":                            {},
	"addtime":                            {},
	"aes_decrypt":                        {},
	"aes_encrypt":                        {},
	"any_value":                          {},
	"ascii":                              {},
	"asin":                               {},
	"atan2":                              {},
	"atan":                               {},
	"avg":                                {},
	"benchmark":                          {},
	"bin":                                {},
	"bin_to_uuid":                        {},
	"bit_and":                            {},
	"bit_length":                         {},
	"bit_count":                          {},
	"bit_or":                             {},
	"bit_xor":                            {},
	"can_access_column":                  {},
	"can_access_database":                {},
	"can_access_table":                   {},
	"can_access_view":                    {},
	"cast":                               {},
	"ceil":                               {},
	"ceiling":                            {},
	"char":                               {},
	"char_length":                        {},
	"character_length":                   {},
	"charset":                            {},
	"coalesce":                           {},
	"coercibility":                       {},
	"collation":                          {},
	"compress":                           {},
	"concat":                             {},
	"concat_ws":                          {},
	"connection_id":                      {},
	"conv":                               {},
	"convert":                            {},
	"convert_tz":                         {},
	"cos":                                {},
	"cot":                                {},
	"count":                              {},
	"crc32":                              {},
	"cume_dist":                          {},
	"curdate":                            {},
	"current_role":                       {},
	"current_timestamp":                  {},
	"curtime":                            {},
	"database":                           {},
	"date":                               {},
	"date_add":                           {},
	"date_format":                        {},
	"date_sub":                           {},
	"datediff":                           {},
	"day":                                {},
	"dayname":                            {},
	"dayofmonth":                         {},
	"dayofweek":                          {},
	"dayofyear":                          {},
	"default":                            {},
	"degrees":                            {},
	"dense_rank":                         {},
	"elt":                                {},
	"exp":                                {},
	"export_set":                         {},
	"extract":                            {},
	"extractvalue":                       {},
	"field":                              {},
	"find_in_set":                        {},
	"first_value":                        {},
	"floor":                              {},
	"format":                             {},
	"format_bytes":                       {},
	"format_pico_time":                   {},
	"found_rows":                         {},
	"from_base64":                        {},
	"from_days":                          {},
	"from_unixtime":                      {},
	"geomcollection":                     {},
	"geometrycollection":                 {},
	"get_dd_column_privileges":           {},
	"get_dd_create_options":              {},
	"get_dd_index_sub_part_length":       {},
	"get_format":                         {},
	"get_lock":                           {},
	"greatest":                           {},
	"group_concat":                       {},
	"grouping":                           {},
	"gtid_subset":                        {},
	"gtid_subtract":                      {},
	"hex":                                {},
	"hour":                               {},
	"icu_version":                        {},
	"if":                                 {},
	"ifnull":                             {},
	"in":                                 {},
	"inet_aton":                          {},
	"inet_ntoa":                          {},
	"inet6_aton":                         {},
	"inet6_ntoa":                         {},
	"insert":                             {},
	"instr":                              {},
	"internal_auto_increment":            {},
	"internal_avg_row_length":            {},
	"internal_check_time":                {},
	"internal_checksum":                  {},
	"internal_data_free":                 {},
	"internal_data_length":               {},
	"internal_dd_char_length":            {},
	"internal_get_comment_or_error":      {},
	"internal_get_enabled_role_json":     {},
	"internal_get_hostname":              {},
	"internal_get_username":              {},
	"internal_get_view_warning_or_error": {},
	"internal_index_column_cardinality":  {},
	"internal_index_length":              {},
	"internal_is_enabled_role":           {},
	"internal_is_mandatory_role":         {},
	"internal_keys_disabled":             {},
	"internal_max_data_length":           {},
	"internal_table_rows":                {},
	"internal_update_time":               {},
	"interval":                           {},
	"is_free_lock":                       {},
	"is_ipv4":                            {},
	"is_ipv4_compat":                     {},
	"is_ipv4_mapped":                     {},
	"is_ipv6":                            {},
	"is_used_lock":                       {},
	"is_uuid":                            {},
	"isnull":                             {},
	"json_array":                         {},
	"json_array_append":                  {},
	"json_array_insert":                  {},
	"json_arrayagg":                      {},
	"json_contains":                      {},
	"json_contains_path":                 {},
	"json_depth":                         {},
	"json_extract":                       {},
	"json_insert":                        {},
	"json_keys":                          {},
	"json_length":                        {},
	"json_merge":                         {},
	"json_merge_patch":                   {},
	"json_merge_preserve":                {},
	"json_object":                        {},
	"json_objectagg":                     {},
	"json_overlaps":                      {},
	"json_pretty":                        {},
	"json_quote":                         {},
	"json_remove":                        {},
	"json_replace":                       {},
	"json_schema_valid":                  {},
	"json_schema_validation_report":      {},
	"json_search":                        {},
	"json_set":                           {},
	"json_storage_free":                  {},
	"json_storage_size":                  {},
	"json_table":                         {},
	"json_type":                          {},
	"json_unquote":                       {},
	"json_valid":                         {},
	"json_value":                         {},
	"lag":                                {},
	"last_insert_id":                     {},
	"last_value":                         {},
	"lcase":                              {},
	"lead":                               {},
	"least":                              {},
	"left":                               {},
	"length":                             {},
	"linestring":                         {},
	"ln":                                 {},
	"load_file":                          {},
	"localtimestamp":                     {},
	"locate":                             {},
	"log":                                {},
	"log10":                              {},
	"log2":                               {},
	"lower":                              {},
	"lpad":                               {},
	"ltrim":                              {},
	"make_set":                           {},
	"makedate":                           {},
	"maketime":                           {},
	"master_pos_wait":                    {},
	"max":                                {},
	"mbrcontains":                        {},
	"mbrcoveredby":                       {},
	"mbrcovers":                          {},
	"mbrdisjoint":                        {},
	"mbrequals":                          {},
	"mbrintersects":                      {},
	"mbroverlaps":                        {},
	"mbrtouches":                         {},
	"mbrwithin":                          {},
	"md5":                                {},
	"microsecond":                        {},
	"mid":                                {},
	"min":                                {},
	"minute":                             {},
	"mod":                                {},
	"month":                              {},
	"monthname":                          {},
	"multilinestring":                    {},
	"multipoint":                         {},
	"multipolygon":                       {},
	"name_const":                         {},
	"now":                                {},
	"nth_value":                          {},
	"ntile":                              {},
	"nullif":                             {},
	"oct":                                {},
	"octet_length":                       {},
	"ord":                                {},
	"percent_rank":                       {},
	"period_add":                         {},
	"period_diff":                        {},
	"pi":                                 {},
	"point":                              {},
	"polygon":                            {},
	"position":                           {},
	"pow":                                {},
	"power":                              {},
	"ps_current_thread_id":               {},
	"ps_thread_id":                       {},
	"quarter":                            {},
	"quote":                              {},
	"radians":                            {},
	"rand":                               {},
	"random_bytes":                       {},
	"rank":                               {},
	"regexp_instr":                       {},
	"regexp_like":                        {},
	"regexp_replace":                     {},
	"regexp_substr":                      {},
	"release_all_locks":                  {},
	"release_lock":                       {},
	"repeat":                             {},
	"replace":                            {},
	"reverse":                            {},
	"right":                              {},
	"roles_graphml":                      {},
	"round":                              {},
	"row_count":                          {},
	"row_number":                         {},
	"rpad":                               {},
	"rtrim":                              {},
	"schema":                             {},
	"sec_to_time":                        {},
	"second":                             {},
	"session_user":                       {},
	"sha1":                               {},
	"sha":                                {},
	"sha2":                               {},
	"sign":                               {},
	"sin":                                {},
	"sleep":                              {},
	"soundex":                            {},
	"space":                              {},
	"sqrt":                               {},
	"st_area":                            {},
	"st_asbinary":                        {},
	"st_aswkb":                           {},
	"st_asgeojson":                       {},
	"st_astext":                          {},
	"st_aswkt":                           {},
	"st_buffer":                          {},
	"st_buffer_strategy":                 {},
	"st_centroid":                        {},
	"st_contains":                        {},
	"st_convexhull":                      {},
	"st_crosses":                         {},
	"st_difference":                      {},
	"st_dimension":                       {},
	"st_disjoint":                        {},
	"st_distance":                        {},
	"st_distance_sphere":                 {},
	"st_endpoint":                        {},
	"st_envelope":                        {},
	"st_equals":                          {},
	"st_exteriorring":                    {},
	"st_geohash":                         {},
	"st_geomcollfromtext":                {},
	"st_geomcollfromtxt":                 {},
	"st_geomcollfromwkb":                 {},
	"st_geometrycollectionfromtext":      {},
	"st_geometrycollectionfromwkb":       {},
	"st_geometryn":                       {},
	"st_geometrytype":                    {},
	"st_geomfromgeojson":                 {},
	"st_geomfromtext":                    {},
	"st_geometryfromtext":                {},
	"st_geomfromwkb":                     {},
	"st_geometryfromwkb":                 {},
	"st_interiorringn":                   {},
	"st_intersection":                    {},
	"st_intersects":                      {},
	"st_isclosed":                        {},
	"st_isempty":                         {},
	"st_issimple":                        {},
	"st_isvalid":                         {},
	"st_latfromgeohash":                  {},
	"st_latitude":                        {},
	"st_length":                          {},
	"st_linefromtext":                    {},
	"st_linestringfromtext":              {},
	"st_linefromwkb":                     {},
	"st_linestringfromwkb":               {},
	"st_longfromgeohash":                 {},
	"st_longitude":                       {},
	"st_makeenvelope":                    {},
	"st_mlinefromtext":                   {},
	"st_multilinestringfromtext":         {},
	"st_mlinefromwkb":                    {},
	"st_multilinestringfromwkb":          {},
	"st_mpointfromtext":                  {},
	"st_multipointfromtext":              {},
	"st_mpointfromwkb":                   {},
	"st_multipointfromwkb":               {},
	"st_mpolyfromtext":                   {},
	"st_multipolygonfromtext":            {},
	"st_mpolyfromwkb":                    {},
	"st_multipolygonfromwkb":             {},
	"st_numgeometries":                   {},
	"st_numinteriorring":                 {},
	"st_numinteriorrings":                {},
	"st_numpoints":                       {},
	"st_overlaps":                        {},
	"st_pointfromgeohash":                {},
	"st_pointfromtext":                   {},
	"st_pointfromwkb":                    {},
	"st_pointn":                          {},
	"st_polyfromtext":                    {},
	"st_polygonfromtext":                 {},
	"st_polyfromwkb":                     {},
	"st_polygonfromwkb":                  {},
	"st_simplify":                        {},
	"st_srid":                            {},
	"st_startpoint":                      {},
	"st_swapxy":                          {},
	"st_symdifference":                   {},
	"st_touches":                         {},
	"st_transform":                       {},
	"st_union":                           {},
	"st_validate":                        {},
	"st_within":                          {},
	"st_x":                               {},
	"st_y":                               {},
	"statement_digest":                   {},
	"statement_digest_text":              {},
	"std":                                {},
	"stddev":                             {},
	"stddev_pop":                         {},
	"stddev_samp":                        {},
	"str_to_date":                        {},
	"strcmp":                             {},
	"subdate":                            {},
	"substr":                             {},
	"substring":                          {},
	"substring_index":                    {},
	"subtime":                            {},
	"sum":                                {},
	"sysdate":                            {},
	"system_user":                        {},
	"tan":                                {},
	"time":                               {},
	"time_format":                        {},
	"time_to_sec":                        {},
	"timediff":                           {},
	"timestamp":                          {},
	"timestampadd":                       {},
	"timestampdiff":                      {},
	"to_base64":                          {},
	"to_days":                            {},
	"to_seconds":                         {},
	"trim":                               {},
	"truncate":                           {},
	"ucase":                              {},
	"uncompress":                         {},
	"uncompressed_length":                {},
	"unhex":                              {},
	"unix_timestamp":                     {},
	"updatexml":                          {},
	"upper":                              {},
	"user":                               {},
	"utc_date":                           {},
	"utc_time":                           {},
	"utc_timestamp":                      {},
	"uuid":                               {},
	"uuid_short":                         {},
	"uuid_to_bin":                        {},
	"validate_password_strength":         {},
	"values":                             {},
	"var_pop":                            {},
	"var_samp":                           {},
	"variance":                           {},
	"version":                            {},
	"wait_for_executed_gtid_set":         {},
	"wait_until_sql_thread_after_gtids":  {},
	"week":                               {},
	"weekday":                            {},
	"weekofyear":                         {},
	"weight_string":                      {},
	"year":                               {},
	"yearweek":                           {},
}

func resolveColumnDefaults(ctx *sql.Context, a *Analyzer, n sql.Node, scope *Scope) (sql.Node, error) {
	span, _ := ctx.Span("resolveColumnDefaults")
	defer span.Finish()

	// This is kind of hacky: we rely on the fact that we know that CreateTable returns the default for every
	// column in the table, and they get evaluated in order below
	colIndex := 0
	return plan.TransformExpressionsUpWithNode(ctx, n, func(n sql.Node, e sql.Expression) (sql.Expression, error) {
		eWrapper, ok := e.(*expression.Wrapper)
		if !ok {
			return e, nil
		}
		switch node := n.(type) {
		case *plan.CreateTable:
			sch := node.Schema()
			col := sch[colIndex]
			colIndex++
			return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
		case *plan.AddColumn:
			return resolveColumnDefaultsOnWrapper(ctx, node.Column(), eWrapper)
		case *plan.ModifyColumn:
			return resolveColumnDefaultsOnWrapper(ctx, node.NewColumn(), eWrapper)
		case *plan.AlterDefaultSet:
			loweredColName := strings.ToLower(node.ColumnName)
			var col *sql.Column
			for _, schCol := range node.Schema() {
				if strings.ToLower(schCol.Name) == loweredColName {
					col = schCol
					break
				}
			}
			if col == nil {
				return nil, sql.ErrTableColumnNotFound.New(node.Child.String(), node.ColumnName)
			}
			return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
		default:
			return e, nil
		}
	})
}

func resolveColumnDefaultsOnWrapper(ctx *sql.Context, col *sql.Column, e *expression.Wrapper) (sql.Expression, error) {
	newDefault, ok := e.Unwrap().(*sql.ColumnDefaultValue)
	if !ok {
		return e, nil
	}

	if newDefault.Resolved() {
		return e, nil
	}

	if sql.IsTextBlob(col.Type) && newDefault.IsLiteral() {
		return nil, sql.ErrInvalidTextBlobColumnDefault.New()
	}

	var err error
	newDefault.Expression, err = expression.TransformUp(ctx, newDefault.Expression, func(e sql.Expression) (sql.Expression, error) {
		if expr, ok := e.(*expression.GetField); ok {
			// Default values can only reference their host table, so we can remove the table name, removing
			// the necessity to update default values on table renames.
			return expr.WithTable(""), nil
		}
		return e, nil
	})

	if err != nil {
		return nil, err
	}

	sql.Inspect(newDefault.Expression, func(e sql.Expression) bool {
		switch expr := e.(type) {
		case sql.FunctionExpression:
			funcName := expr.FunctionName()
			if _, isValid := validColumnDefaultFuncs[funcName]; !isValid {
				err = sql.ErrInvalidColumnDefaultFunction.New(funcName, col.Name)
				return false
			}
			if (funcName == "now" || funcName == "current_timestamp") &&
				newDefault.IsLiteral() &&
				(!sql.IsTime(col.Type) || sql.Date == col.Type) {
				err = sql.ErrColumnDefaultDatetimeOnlyFunc.New()
				return false
			}
			return true
		case *plan.Subquery:
			err = sql.ErrColumnDefaultSubquery.New(col.Name)
			return false
		default:
			return true
		}
	})
	if err != nil {
		return nil, err
	}
	//TODO: fix the vitess parser so that it parses negative numbers as numbers and not negation of an expression
	isLiteral := newDefault.IsLiteral()
	if unaryMinusExpr, ok := newDefault.Expression.(*expression.UnaryMinus); ok {
		if literalExpr, ok := unaryMinusExpr.Child.(*expression.Literal); ok {
			switch val := literalExpr.Value().(type) {
			case float32:
				newDefault.Expression = expression.NewLiteral(-val, sql.Float32)
				isLiteral = true
			case float64:
				newDefault.Expression = expression.NewLiteral(-val, sql.Float64)
				isLiteral = true
			}
		}
	}

	newDefault, err = sql.NewColumnDefaultValue(newDefault.Expression, col.Type, isLiteral, col.Nullable)
	if err != nil {
		return nil, err
	}

	return expression.WrapExpression(newDefault), nil
}
