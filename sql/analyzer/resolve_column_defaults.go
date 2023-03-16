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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// validColumnDefaultFuncs is the set of functions that are legal in a column default value
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

// Resolving column defaults is a multi-phase process, with different analyzer rules for each phase.
//
// * parseColumnDefaults: Some integrators (dolt but not GMS) store their column defaults as strings, which we need to
// 	parse into expressions before we can analyze them any further.
// * resolveColumnDefaults: Once we have an expression for a default value, it may contain expressions that need
// 	simplification before further phases of processing can take place.
//
// After this stage, expressions in column default values are handled by the normal analyzer machinery responsible for
// resolving expressions, including things like columns and functions. Every node that needs to do this for its default
// values implements `sql.Expressioner` to expose such expressions. There is custom logic in `resolveColumns` to help
// identify the correct indexes for column references, which can vary based on the node type.
//
// Finally there are cleanup phases:
// * validateColumnDefaults: ensures that newly created column defaults from a DDL statement are legal for the type of
// 	column, various other business logic checks to match MySQL's logic.
// * stripTableNamesFromDefault: column defaults headed for storage or serialization in a query result need the table
// 	names in any GetField expressions stripped out so that they serialize to strings without such table names. Table
// 	names in GetField expressions are expected in much of the rest of the analyzer, so we do this after the bulk of
// 	analyzer work.
//
// The `information_schema.columns` table also needs access to the default values of every column in the database, and
// because it's a table it can't implement `sql.Expressioner` like other node types. Instead it has special handling
// here, as well as in the `resolve_functions` rule.

func validateColumnDefaults(ctx *sql.Context, _ *Analyzer, n sql.Node, _ *Scope, _ RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("validateColumnDefaults")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch node := n.(type) {
		case *plan.AlterDefaultSet:
			table := getResolvedTable(node)
			sch := table.Schema()
			index := sch.IndexOfColName(node.ColumnName)
			col := sch[index]

			eWrapper := expression.WrapExpression(node.Default)
			err := validateColumnDefault(ctx, col, eWrapper)
			if err != nil {
				return node, transform.SameTree, err
			}
			return node, transform.SameTree, nil
		case sql.SchemaTarget:
			switch node.(type) {
			case *plan.AlterPK, *plan.AddColumn, *plan.ModifyColumn, *plan.AlterDefaultDrop, *plan.CreateTable, *plan.DropColumn:
				// DDL nodes must validate any new column defaults, continue to logic below
			default:
				// other node types are not altering the schema and therefore don't need validation of column defaults
				return n, transform.SameTree, nil
			}

			// There may be multiple DDL nodes in the plan (ALTER TABLE statements can have many clauses), and for each of them
			// we need to count the column indexes in the very hacky way outlined above.
			colIndex := 0
			return transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				col, err := lookupColumnForTargetSchema(ctx, node, colIndex)
				if err != nil {
					return nil, transform.SameTree, err
				}
				colIndex++

				err = validateColumnDefault(ctx, col, eWrapper)
				if err != nil {
					return nil, transform.SameTree, err
				}

				return e, transform.SameTree, nil
			})
		default:
			return node, transform.SameTree, nil
		}
	})
}

func resolveColumnDefaults(ctx *sql.Context, _ *Analyzer, n sql.Node, _ *Scope, _ RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("resolveColumnDefaults")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		// There may be multiple DDL nodes in the plan (ALTER TABLE statements can have many clauses), and for each of them
		// we need to count the column indexes as we transform expressions
		colIndex := 0

		switch node := n.(type) {
		case *plan.InsertDestination:
			return transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				table := getResolvedTable(node)
				sch := table.Schema()
				if colIndex >= len(sch) {
					return e, transform.SameTree, nil
				}

				col := sch[colIndex]
				colIndex++
				return resolveColumnDefault(ctx, col, eWrapper)
			})
		case *plan.InsertInto:
			// node.Source needs to be explicitly handled here because it's not a
			// registered child of InsertInto
			newSource, identity, err := transform.NodeExprs(node.Source, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				// Instead of grabbing the schema from TargetSchema(), use the Destination node
				sch := node.Destination.Schema()

				// InsertInto.Source can contain multiple rows, so loop over the included columns
				colIndex = colIndex % len(node.ColumnNames)

				// Columns can be specified in any order, so use the order from the InsertInto statement
				schemaIndex := sch.IndexOfColName(node.ColumnNames[colIndex])
				if schemaIndex == -1 {
					return nil, transform.SameTree, sql.ErrColumnNotFound.New(node.ColumnNames[colIndex])
				}
				col := sch[schemaIndex]
				colIndex++
				return resolveColumnDefault(ctx, col, eWrapper)
			})

			if identity == transform.NewTree {
				node.Source = newSource
			}
			return node, identity, err
		case *plan.AlterDefaultSet:
			table := getResolvedTable(node)
			sch := table.Schema()
			index := sch.IndexOfColName(node.ColumnName)
			col := sch[index]

			eWrapper := expression.WrapExpression(node.Default)
			newExpr, same, err := resolveColumnDefault(ctx, col, eWrapper)
			if err != nil {
				return node, transform.SameTree, err
			}
			if same {
				return node, transform.SameTree, nil
			}

			newNode, err := node.WithDefault(newExpr)
			if err != nil {
				return node, transform.SameTree, err
			}
			return newNode, transform.NewTree, nil
		case sql.SchemaTarget:
			return transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				col, err := lookupColumnForTargetSchema(ctx, node, colIndex)
				if err != nil {
					return nil, transform.SameTree, err
				}
				colIndex++

				return resolveColumnDefault(ctx, col, eWrapper)
			})
		case *plan.ResolvedTable:
			ct, ok := node.Table.(*information_schema.ColumnsTable)
			if !ok {
				return node, transform.SameTree, nil
			}

			allColumns, err := ct.AllColumns(ctx)
			if err != nil {
				return nil, transform.SameTree, err
			}

			allDefaults, same, err := transform.Exprs(transform.WrappedColumnDefaults(allColumns), func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				colIdx := colIndex
				colIndex++
				return resolveColumnDefault(ctx, allColumns[colIdx], eWrapper)
			})

			if err != nil {
				return nil, transform.SameTree, err
			}

			if !same {
				node.Table, err = ct.WithColumnDefaults(allDefaults)
				if err != nil {
					return nil, transform.SameTree, err
				}
			}

			return node, transform.NewTree, err
		default:
			return node, transform.SameTree, nil
		}
	})
}

// stripTableNamesFromColumnDefaults removes the table name from any GetField expressions in column default expressions.
// Default values can only reference their host table, and since we serialize the GetField expression for storage, it's
// important that we remove the table name before passing it off for storage. Otherwise we end up with serialized
// defaults like `tableName.field + 1` instead of just `field + 1`.
func stripTableNamesFromColumnDefaults(ctx *sql.Context, _ *Analyzer, n sql.Node, _ *Scope, _ RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("stripTableNamesFromColumnDefaults")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch node := n.(type) {
		case *plan.AlterDefaultSet:
			eWrapper := expression.WrapExpression(node.Default)
			newExpr, same, err := stripTableNamesFromDefault(eWrapper)
			if err != nil {
				return node, transform.SameTree, err
			}
			if same {
				return node, transform.SameTree, nil
			}

			newNode, err := node.WithDefault(newExpr)
			if err != nil {
				return node, transform.SameTree, err
			}
			return newNode, transform.NewTree, nil
		case sql.SchemaTarget:
			return transform.NodeExprs(n, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				return stripTableNamesFromDefault(eWrapper)
			})
		case *plan.ResolvedTable:
			ct, ok := node.Table.(*information_schema.ColumnsTable)
			if !ok {
				return node, transform.SameTree, nil
			}

			allColumns, err := ct.AllColumns(ctx)
			if err != nil {
				return nil, transform.SameTree, err
			}

			allDefaults, same, err := transform.Exprs(transform.WrappedColumnDefaults(allColumns), func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				return stripTableNamesFromDefault(eWrapper)
			})

			if err != nil {
				return nil, transform.SameTree, err
			}

			if !same {
				node.Table, err = ct.WithColumnDefaults(allDefaults)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return node, transform.NewTree, err
			}

			return node, transform.SameTree, err
		default:
			return node, transform.SameTree, nil
		}
	})
}

// lookupColumnForTargetSchema looks at the target schema for the specified SchemaTarget node and returns
// the column based on the specified index. For most node types, this is simply indexing into the target
// schema but a few types require special handling.
func lookupColumnForTargetSchema(_ *sql.Context, node sql.SchemaTarget, colIndex int) (*sql.Column, error) {
	schema := node.TargetSchema()

	switch n2 := node.(type) {
	case *plan.ModifyColumn:
		if colIndex < len(schema) {
			return schema[colIndex], nil
		} else {
			return n2.NewColumn(), nil
		}
	case *plan.AddColumn:
		if colIndex < len(schema) {
			return schema[colIndex], nil
		} else {
			return n2.Column(), nil
		}
	case *plan.AlterDefaultSet:
		index := schema.IndexOfColName(n2.ColumnName)
		if index == -1 {
			return nil, sql.ErrTableColumnNotFound.New(n2.Table, n2.ColumnName)
		}
		return n2.Schema()[index], nil
	default:
		if colIndex < len(schema) {
			return schema[colIndex], nil
		} else {
			return nil, sql.ErrColumnNotFound.New(colIndex)
		}
	}
}

// parseColumnDefaults transforms UnresolvedColumnDefault expressions into ColumnDefaultValue expressions, which
// amounts to parsing the string representation into an actual expression. We only require an actual column default
// value for some node types, where the value will be used.
func parseColumnDefaults(ctx *sql.Context, _ *Analyzer, n sql.Node, _ *Scope, _ RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, ctx := ctx.Span("parse_column_defaults")
	defer span.End()

	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		switch nn := n.(type) {
		case *plan.InsertInto:
			if !nn.Destination.Resolved() {
				return nn, transform.SameTree, nil
			}
			var err error
			n, err = nn.WithChildren(plan.NewInsertDestination(nn.Destination.Schema(), nn.Destination))
			nn = n.(*plan.InsertInto)
			if err != nil {
				return nil, transform.SameTree, err
			}

			err = fillInColumnDefaults(ctx, nn)
			if err != nil {
				return nil, transform.SameTree, err
			}

			// InsertInto.Source needs special handling, since it is not modeled as a Child
			newNode, _, err := parseDefaultsForNode(ctx, nn.Source)
			if err != nil {
				return nil, transform.SameTree, err
			}

			nn.Source = newNode
			return parseDefaultsForNode(ctx, nn)
		case *plan.ResolvedTable:
			ct, ok := nn.Table.(*information_schema.ColumnsTable)
			if !ok {
				return nn, transform.SameTree, nil
			}

			allColumns, err := ct.AllColumns(ctx)
			if err != nil {
				return nil, transform.SameTree, err
			}

			allDefaults, same, err := transform.Exprs(transform.WrappedColumnDefaults(allColumns), func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				newWr, parseSame, err := parseColumnDefault(ctx, eWrapper)
				if err != nil {
					return nil, transform.SameTree, err
				}

				newWr, stripSame, err := stripTableNamesFromDefault(newWr.(*expression.Wrapper))
				if err != nil {
					return nil, false, err
				}

				return newWr, parseSame && stripSame, nil
			})

			if err != nil {
				return nil, transform.SameTree, err
			}

			if !same {
				nn.Table, err = ct.WithColumnDefaults(allDefaults)
				if err != nil {
					return nil, transform.SameTree, err
				}
				return nn, transform.NewTree, err
			}

			return nn, transform.SameTree, err
		case *plan.AlterIndex:
			newSchema := make([]*sql.Column, len(nn.TargetSchema()))
			for i, col := range nn.TargetSchema() {
				newSchema[i] = col

				if col.Default != nil {
					if ucd, ok := col.Default.Expression.(sql.UnresolvedColumnDefault); ok {
						resolvedDefault, err := parse.StringToColumnDefaultValue(ctx, ucd.String())
						if err != nil {
							return nil, transform.SameTree, err
						}
						col.Default = resolvedDefault
						newSchema[i] = col
					}
				}
			}
			newNode, err := nn.WithTargetSchema(newSchema)
			if err != nil {
				return nil, transform.SameTree, err
			}

			return newNode, transform.NewTree, nil
		default:
			return parseDefaultsForNode(ctx, nn)
		}
	})
}

// parseDefaultsForNode walks over the expressions contained in the specified node and transforms all
// UnresolvedColumnDefaultValues into ColumnDefaultValues.
func parseDefaultsForNode(ctx *sql.Context, input sql.Node) (sql.Node, transform.TreeIdentity, error) {
	return transform.NodeExprsWithNode(input, func(node sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		eWrapper, ok := e.(*expression.Wrapper)
		if !ok {
			return e, transform.SameTree, nil
		}
		switch node.(type) {
		case *plan.Values, *plan.InsertDestination, sql.SchemaTarget:
			return parseColumnDefault(ctx, eWrapper)
		default:
			return e, transform.SameTree, nil
		}
	})
}

// fillInColumnDefaults fills in column default expressions for any source data that explicitly used
// the 'DEFAULT' keyword as input. This requires that the InsertInto Destination has been fully resolved
// before the column default values can be used.
func fillInColumnDefaults(_ *sql.Context, insertInto *plan.InsertInto) error {
	schema := insertInto.Destination.Schema()

	// If the source values are not specified, fill in column default placeholders
	if values, ok := insertInto.Source.(*plan.Values); ok {
		for i, tuple := range values.ExpressionTuples {
			if len(tuple) == 0 {
				tuple = make([]sql.Expression, len(insertInto.ColumnNames))
				for j, _ := range tuple {
					tuple[j] = expression.NewDefaultColumn(insertInto.ColumnNames[j])
				}
				values.ExpressionTuples[i] = tuple
			}
		}
	}

	// Pull the column default values out into the same order the columns were specified
	columnDefaultValues := make([]*sql.ColumnDefaultValue, len(insertInto.ColumnNames))
	for i, columnName := range insertInto.ColumnNames {
		index := schema.IndexOfColName(columnName)
		if index == -1 {
			return plan.ErrInsertIntoNonexistentColumn.New(columnName)
		}
		columnDefaultValues[i] = schema[index].Default
	}

	// Walk through the expression tuples looking for any column defaults to fill in
	if values, ok := insertInto.Source.(*plan.Values); ok {
		for _, exprTuple := range values.ExpressionTuples {
			for i, value := range exprTuple {
				newExpression, _, err := transform.Expr(value, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
					if _, ok := e.(*expression.DefaultColumn); ok {
						return columnDefaultValues[i], transform.NewTree, nil
					}
					return e, transform.SameTree, nil
				})
				if err != nil {
					return err
				}
				exprTuple[i] = expression.WrapExpression(newExpression)
			}
		}
	}

	return nil
}

// parseColumnDefault transforms an UnresolvedColumnDefault expression into a ColumnDefaultValue expression
func parseColumnDefault(ctx *sql.Context, e *expression.Wrapper) (sql.Expression, transform.TreeIdentity, error) {
	newDefault, ok := e.Unwrap().(*sql.ColumnDefaultValue)
	if !ok {
		return e, transform.SameTree, nil
	}

	if newDefault.Resolved() {
		return e, transform.SameTree, nil
	}

	if ucd, ok := newDefault.Expression.(sql.UnresolvedColumnDefault); ok {
		var err error
		newDefault, err = parse.StringToColumnDefaultValue(ctx, ucd.String())
		if err != nil {
			return nil, transform.SameTree, err
		}
	}

	return expression.WrapExpression(newDefault), transform.NewTree, nil
}

func resolveColumnDefault(ctx *sql.Context, col *sql.Column, e *expression.Wrapper) (sql.Expression, transform.TreeIdentity, error) {
	newDefault, ok := e.Unwrap().(*sql.ColumnDefaultValue)
	if !ok {
		return e, transform.SameTree, nil
	}

	if newDefault == nil {
		return e, transform.SameTree, nil
	}

	// TODO: fix the vitess parser so that it parses negative numbers as numbers and not negation of an expression
	isLiteral := newDefault.IsLiteral()
	if unaryMinusExpr, ok := newDefault.Expression.(*expression.UnaryMinus); ok {
		if literalExpr, ok := unaryMinusExpr.Child.(*expression.Literal); ok {
			switch val := literalExpr.Value().(type) {
			case float32:
				newDefault.Expression = expression.NewLiteral(-val, types.Float32)
				isLiteral = true
			case float64:
				newDefault.Expression = expression.NewLiteral(-val, types.Float64)
				isLiteral = true
			}
		}
	}

	var err error
	newDefault, err = sql.NewColumnDefaultValue(newDefault.Expression, col.Type, isLiteral, newDefault.IsParenthesized(), col.Nullable)
	if err != nil {
		return nil, transform.SameTree, err
	}

	return expression.WrapExpression(newDefault), transform.NewTree, nil
}

// validateColumnDefault validates that the column default expression is valid for the column type and returns an error
// if not
func validateColumnDefault(ctx *sql.Context, col *sql.Column, e *expression.Wrapper) error {
	newDefault, ok := e.Unwrap().(*sql.ColumnDefaultValue)
	if !ok {
		return nil
	}

	if newDefault == nil {
		return nil
	}

	// Some column types can only have a NULL for a literal default, must be an expression otherwise
	isLiteralRestrictedType := types.IsTextBlob(col.Type) || types.IsJSON(col.Type) || types.IsGeometry(col.Type)
	if isLiteralRestrictedType && newDefault.IsLiteral() {
		lit, err := newDefault.Expression.Eval(ctx, nil)
		if err != nil {
			return err
		}
		if lit != nil {
			return sql.ErrInvalidTextBlobColumnDefault.New()
		}
	}

	var err error
	sql.Inspect(newDefault.Expression, func(e sql.Expression) bool {
		switch e.(type) {
		case sql.FunctionExpression, *expression.UnresolvedFunction:
			var funcName string
			switch expr := e.(type) {
			case sql.FunctionExpression:
				funcName = expr.FunctionName()
			case *expression.UnresolvedFunction:
				funcName = expr.Name()
			}

			if _, isValid := validColumnDefaultFuncs[funcName]; !isValid {
				err = sql.ErrInvalidColumnDefaultFunction.New(funcName, col.Name)
				return false
			}
			if !newDefault.IsParenthesized() {
				if funcName == "now" || funcName == "current_timestamp" {
					// now and current_timestamps are the only functions that don't have to be enclosed in
					// parens when used as a column default value, but ONLY when they are used with a
					// datetime or timestamp column, otherwise it's invalid.
					if col.Type.Type() == sqltypes.Datetime || col.Type.Type() == sqltypes.Timestamp {
						return true
					} else {
						err = sql.ErrColumnDefaultDatetimeOnlyFunc.New()
						return false
					}
				}
			}
			return true
		case *plan.Subquery:
			err = sql.ErrColumnDefaultSubquery.New(col.Name)
			return false
		case *deferredColumn:
			err = sql.ErrInvalidColumnDefaultValue.New(col.Name)
			return false
		case *expression.GetField:
			if newDefault.IsParenthesized() == false {
				err = sql.ErrInvalidColumnDefaultValue.New(col.Name)
				return false
			} else {
				return true
			}
		default:
			return true
		}
	})

	if err != nil {
		return err
	}

	// validate type of default expression
	if err = newDefault.CheckType(ctx); err != nil {
		return err
	}

	return nil
}

func stripTableNamesFromDefault(e *expression.Wrapper) (sql.Expression, transform.TreeIdentity, error) {
	newDefault, ok := e.Unwrap().(*sql.ColumnDefaultValue)
	if !ok {
		return e, transform.SameTree, nil
	}

	if newDefault == nil {
		return e, transform.SameTree, nil
	}

	newExpr, same, err := transform.Expr(newDefault.Expression, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if expr, ok := e.(*expression.GetField); ok {
			return expr.WithTable(""), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})
	if err != nil {
		return nil, transform.SameTree, err
	}

	if same {
		return e, transform.SameTree, nil
	}

	nd := *newDefault
	nd.Expression = newExpr
	return expression.WrapExpression(&nd), transform.NewTree, nil
}
