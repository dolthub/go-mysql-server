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
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
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

type targetSchema interface {
	TargetSchema() sql.Schema
}

func resolveColumnDefaults(ctx *sql.Context, _ *Analyzer, n sql.Node, _ *Scope, _ RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("resolveColumnDefaults")
	defer span.Finish()

	// TODO: this is pretty hacky, many of the transformations below rely on a particular ordering of expressions
	//  returned by Expressions() for these nodes
	return transform.Node(n, func(n sql.Node) (sql.Node, transform.TreeIdentity, error) {
		if n.Resolved() {
			return n, transform.SameTree, nil
		}

		// There may be multiple DDL nodes in the plan (ALTER TABLE statements can have many clauses), and for each of them
		// we need to count the column indexes in the very hacky way outlined above.
		colIndex := 0

		switch node := n.(type) {
		case *plan.ShowColumns, *plan.ShowCreateTable:
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
				return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
			})
		case *plan.InsertInto:
			newSource, _, err := transform.NodeExprs(node.Source, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				sch := node.Destination.Schema()

				// TODO: Are these always going to be in the same order as the schema?
				if colIndex >= len(sch) {
					// TODO: This is hacky! :-(
					colIndex = colIndex % len(sch)
				}
				col := sch[colIndex]
				colIndex++
				return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
			})

			node.Source = newSource
			n = node
			return n, false, err
		case *plan.InsertDestination:
			return transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}
				sch := node.Sch
				col := sch[colIndex]
				colIndex++
				return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
			})
		case *plan.CreateTable:
			return transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}
				sch := node.CreateSchema.Schema
				col := sch[colIndex]
				colIndex++
				return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
			})
		case *plan.RenameColumn, *plan.DropColumn:
			return transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				// Not a public interface, should make part of sql.SchemaTarget?
				sch := node.(targetSchema).TargetSchema()

				var col *sql.Column
				if colIndex >= len(sch) {
					return e, transform.SameTree, nil
				}

				col = sch[colIndex]
				colIndex++

				return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
			})

		case *plan.ModifyColumn:
			return transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				sch := node.TargetSchema()

				var col *sql.Column
				if colIndex < len(sch) {
					col = sch[colIndex]
				} else {
					col = node.NewColumn()
				}

				colIndex++

				return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
			})
		case *plan.AddColumn:
			return transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				sch := node.TargetSchema()

				var col *sql.Column
				if colIndex < len(sch) {
					col = sch[colIndex]
				} else {
					col = node.Column()
				}

				colIndex++

				return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
			})
		case *plan.AlterDefaultSet:
			return transform.NodeExprs(node, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
				eWrapper, ok := e.(*expression.Wrapper)
				if !ok {
					return e, transform.SameTree, nil
				}

				loweredColName := strings.ToLower(node.ColumnName)
				var col *sql.Column
				for _, schCol := range node.Schema() {
					if strings.ToLower(schCol.Name) == loweredColName {
						col = schCol
						break
					}
				}
				if col == nil {
					return nil, transform.SameTree, sql.ErrTableColumnNotFound.New(node.Table, node.ColumnName)
				}
				return resolveColumnDefaultsOnWrapper(ctx, col, eWrapper)
			})
		default:
			return node, transform.SameTree, nil
		}
	})
}

// parseColumnDefaults transforms UnresolvedColumnDefault expressions into ColumnDefaultValue expressions, which
// amounts to parsing the string representation into an actual expression. We only require an actual column default
// value for some node types, where the value will be used.
func parseColumnDefaults(ctx *sql.Context, _ *Analyzer, n sql.Node, _ *Scope, _ RuleSelector) (sql.Node, transform.TreeIdentity, error) {
	span, _ := ctx.Span("parse_column_defaults")
	defer span.Finish()

	switch nn := n.(type) {
	case *plan.InsertInto:
		if !nn.Destination.Resolved() {
			return nn, transform.SameTree, nil
		}
		var err error
		n, err = nn.WithChildren(plan.NewInsertDestination(nn.Destination.Schema(), nn.Destination))
		if err != nil {
			return nil, transform.SameTree, err
		}

		// TODO: Hacky... can we clean this up? This is needed so we don't lose the changes above to
		//       set the InsertDestination as the Destination of this node.
		nn = n.(*plan.InsertInto)

		err = fillInColumnDefaults(ctx, nn)
		if err != nil {
			return nil, transform.SameTree, err
		}

		// TODO: Duplicated... refactor this to clean up
		newNode, _, err := transform.NodeExprsWithNode(nn.Source, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
			eWrapper, ok := e.(*expression.Wrapper)
			if !ok {
				return e, transform.SameTree, nil
			}
			switch n.(type) {
			case *plan.Values, *plan.InsertDestination, *plan.AddColumn, *plan.ShowColumns, *plan.ShowCreateTable, *plan.RenameColumn, *plan.ModifyColumn, *plan.DropColumn, *plan.CreateTable:
				n, same, err := parseColumnDefaultsForWrapper(ctx, eWrapper)
				return n, same, err
			default:
				return e, transform.SameTree, nil
			}
		})
		if err != nil {
			panic(err)
		}

		nn.Source = newNode
		n = nn
	}

	// TODO: Ignore identity since we change the node earlier and need that to show up...
	node, _, err := transform.NodeExprsWithNode(n, func(n sql.Node, e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		eWrapper, ok := e.(*expression.Wrapper)
		if !ok {
			return e, transform.SameTree, nil
		}
		switch n.(type) {
		case *plan.InsertDestination, *plan.AddColumn, *plan.ShowColumns, *plan.ShowCreateTable, *plan.RenameColumn, *plan.ModifyColumn, *plan.DropColumn, *plan.CreateTable:
			n, same, err := parseColumnDefaultsForWrapper(ctx, eWrapper)
			return n, same, err
		default:
			return e, transform.SameTree, nil
		}
	})

	return node, transform.NewTree, err
}

// fillInColumnDefaults fills in column default expressions for any source data that explicitly used
// the 'DEFAULT' keyword as input. This requires that the InsertInto Destination has been fully resolved
// before the column default values can be used.
func fillInColumnDefaults(_ *sql.Context, insertInto *plan.InsertInto) error {
	schema := insertInto.Destination.Schema()

	// If no column names were specified in the query, go ahead and fill
	// them all in now that the destination is resolved.
	if len(insertInto.ColumnNames) == 0 {
		insertInto.ColumnNames = make([]string, len(schema))
		for i, col := range schema {
			insertInto.ColumnNames[i] = col.Name
		}
	}

	// Pull the column default values out into the same order the columns were specified
	columnDefaultValues := make([]*sql.ColumnDefaultValue, len(insertInto.ColumnNames))
	for i, columnName := range insertInto.ColumnNames {
		found := false
		for _, col := range schema {
			if strings.ToLower(col.Name) == strings.ToLower(columnName) {
				columnDefaultValues[i] = col.Default
				found = true
				break
			}
		}
		if !found {
			return plan.ErrInsertIntoNonexistentColumn.New(columnName)
		}
	}

	if insertInto.Source == nil {
		return fmt.Errorf("no values specified for insert into statement")
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
					panic(err)
				}
				exprTuple[i] = expression.WrapExpression(newExpression)
			}
		}
	}

	return nil
}

func parseColumnDefaultsForWrapper(ctx *sql.Context, e *expression.Wrapper) (sql.Expression, transform.TreeIdentity, error) {
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

func resolveColumnDefaultsOnWrapper(ctx *sql.Context, col *sql.Column, e *expression.Wrapper) (sql.Expression, transform.TreeIdentity, error) {
	newDefault, ok := e.Unwrap().(*sql.ColumnDefaultValue)
	if !ok {
		return e, transform.SameTree, nil
	}

	if newDefault.Resolved() {
		return e, transform.SameTree, nil
	}

	if sql.IsTextBlob(col.Type) && newDefault.IsLiteral() && newDefault.Type() != sql.Null {
		return nil, transform.SameTree, sql.ErrInvalidTextBlobColumnDefault.New()
	}

	var err error
	newDefault.Expression, _, err = transform.Expr(newDefault.Expression, func(e sql.Expression) (sql.Expression, transform.TreeIdentity, error) {
		if expr, ok := e.(*expression.GetField); ok {
			// Default values can only reference their host table, so we can remove the table name, removing
			// the necessity to update default values on table renames.
			return expr.WithTable(""), transform.NewTree, nil
		}
		return e, transform.SameTree, nil
	})

	if err != nil {
		return nil, transform.SameTree, err
	}

	sql.Inspect(newDefault.Expression, func(e sql.Expression) bool {
		switch expr := e.(type) {
		case sql.FunctionExpression:
			funcName := expr.FunctionName()
			if _, isValid := validColumnDefaultFuncs[funcName]; !isValid {
				err = sql.ErrInvalidColumnDefaultFunction.New(funcName, col.Name)
				return false
			}
			if newDefault.IsLiteral() {
				if funcName == "now" || funcName == "current_timestamp" {
					if col.Type.Type() == sqltypes.Datetime || col.Type.Type() == sqltypes.Timestamp {
						return true
					} else {
						err = sql.ErrColumnDefaultDatetimeOnlyFunc.New()
						return false
					}
				}
				err = sql.ErrInvalidColumnDefaultValue.New(col.Name)
				return false
			}

			return true
		case *plan.Subquery:
			err = sql.ErrColumnDefaultSubquery.New(col.Name)
			return false
		case *deferredColumn:
			err = sql.ErrInvalidColumnDefaultValue.New(col.Name)
			return false
		case *expression.GetField:
			if newDefault.IsLiteral() {
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
		return nil, transform.SameTree, err
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
		return nil, transform.SameTree, err
	}

	// validate type of default expression
	if err = newDefault.CheckType(ctx); err != nil {
		return nil, transform.SameTree, err
	}

	return expression.WrapExpression(newDefault), transform.NewTree, nil
}
