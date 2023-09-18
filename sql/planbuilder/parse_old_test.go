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

package planbuilder

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var showCollationProjection = plan.NewProject([]sql.Expression{
	expression.NewAlias("collation", expression.NewUnresolvedColumn("collation_name")),
	expression.NewAlias("charset", expression.NewUnresolvedColumn("character_set_name")),
	expression.NewUnresolvedColumn("id"),
	expression.NewAlias("default", expression.NewUnresolvedColumn("is_default")),
	expression.NewAlias("compiled", expression.NewUnresolvedColumn("is_compiled")),
	expression.NewUnresolvedColumn("sortlen"),
	expression.NewUnresolvedColumn("pad_attribute"),
},
	plan.NewUnresolvedTable("collations", "information_schema"),
)

type parseTest struct {
	input string
	plan  sql.Node
}

func TestParse(t *testing.T) {
	t.Skip("todo use planbuilder")
	//	var fixtures = []parseTest{
	//		{
	//			input: `CREATE TABLE t1(a INTEGER, b TEXT, c DATE, d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, g DATETIME, h CHAR(40))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:     "a",
	//						Type:     types.Int32,
	//						Nullable: true,
	//					}, {
	//						Name:     "b",
	//						Type:     types.Text,
	//						Nullable: true,
	//					}, {
	//						Name:     "c",
	//						Type:     types.Date,
	//						Nullable: true,
	//					}, {
	//						Name:     "d",
	//						Type:     types.Timestamp,
	//						Nullable: true,
	//					}, {
	//						Name:     "e",
	//						Type:     types.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
	//						Nullable: true,
	//					}, {
	//						Name:     "f",
	//						Type:     types.Blob,
	//						Nullable: false,
	//					}, {
	//						Name:     "g",
	//						Type:     types.Datetime,
	//						Nullable: true,
	//					}, {
	//						Name:     "h",
	//						Type:     types.MustCreateStringWithDefaults(sqltypes.Char, 40),
	//						Nullable: true,
	//					}}),
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER NOT NULL PRIMARY KEY, b TEXT)`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Text,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER NOT NULL PRIMARY KEY COMMENT "hello", b TEXT COMMENT "goodbye")`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//						Comment:    "hello",
	//					}, {
	//						Name:       "b",
	//						Type:       types.Text,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//						Comment:    "goodbye",
	//					}}),
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER, b TEXT, PRIMARY KEY (a))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Text,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					IdxDefs: []*plan.IndexDefinition{
	//						{
	//							IndexName: "PRIMARY",
	//							Columns: []sql.IndexColumn{
	//								{Name: "a"},
	//							},
	//							Constraint: sql.IndexConstraint_Primary,
	//						},
	//					},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER, b TEXT, PRIMARY KEY (a, b))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(
	//						sql.Schema{{
	//							Name:       "a",
	//							Type:       types.Int32,
	//							Nullable:   false,
	//							PrimaryKey: true,
	//						}, {
	//							Name:       "b",
	//							Type:       types.Text,
	//							Nullable:   false,
	//							PrimaryKey: true,
	//						}}),
	//					IdxDefs: []*plan.IndexDefinition{
	//						{
	//							IndexName: "PRIMARY",
	//							Columns: []sql.IndexColumn{
	//								{Name: "a"},
	//								{Name: "b"},
	//							},
	//							Constraint: sql.IndexConstraint_Primary,
	//						},
	//					},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER, b TEXT, PRIMARY KEY (b, a))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(
	//						sql.Schema{{
	//							Name:       "a",
	//							Type:       types.Int32,
	//							Nullable:   false,
	//							PrimaryKey: true,
	//						}, {
	//							Name:       "b",
	//							Type:       types.Text,
	//							Nullable:   false,
	//							PrimaryKey: true,
	//						}}, 1, 0),
	//					IdxDefs: []*plan.IndexDefinition{
	//						{
	//							IndexName: "PRIMARY",
	//							Columns: []sql.IndexColumn{
	//								{Name: "b"},
	//								{Name: "a"},
	//							},
	//							Constraint: sql.IndexConstraint_Primary,
	//						},
	//					},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER, b int, CONSTRAINT pk PRIMARY KEY (b, a), CONSTRAINT UNIQUE KEY (a))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}}, 1, 0),
	//					IdxDefs: []*plan.IndexDefinition{
	//						{
	//							IndexName: "pk",
	//							Columns: []sql.IndexColumn{
	//								{Name: "b"},
	//								{Name: "a"},
	//							},
	//							Constraint: sql.IndexConstraint_Primary,
	//						},
	//						{
	//							Columns: []sql.IndexColumn{
	//								{Name: "a"},
	//							},
	//							Constraint: sql.IndexConstraint_Unique,
	//						},
	//					},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE IF NOT EXISTS t1(a INTEGER, b TEXT, PRIMARY KEY (a, b))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExists,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(
	//						sql.Schema{{
	//							Name:       "a",
	//							Type:       types.Int32,
	//							Nullable:   false,
	//							PrimaryKey: true,
	//						}, {
	//							Name:       "b",
	//							Type:       types.Text,
	//							Nullable:   false,
	//							PrimaryKey: true,
	//						}}),
	//					IdxDefs: []*plan.IndexDefinition{
	//						{
	//							IndexName:  "PRIMARY",
	//							Constraint: sql.IndexConstraint_Primary,
	//							Columns: []sql.IndexColumn{
	//								{Name: "a"},
	//								{Name: "b"},
	//							},
	//						},
	//					},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX (b))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					IdxDefs: []*plan.IndexDefinition{
	//						{
	//							IndexName:  "",
	//							Using:      sql.IndexUsing_Default,
	//							Constraint: sql.IndexConstraint_None,
	//							Columns:    []sql.IndexColumn{{"b", 0}},
	//							Comment:    "",
	//						},
	//					},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX idx_name (b))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					IdxDefs: []*plan.IndexDefinition{{
	//						IndexName:  "idx_name",
	//						Using:      sql.IndexUsing_Default,
	//						Constraint: sql.IndexConstraint_None,
	//						Columns:    []sql.IndexColumn{{"b", 0}},
	//						Comment:    "",
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX idx_name (b) COMMENT 'hi')`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					IdxDefs: []*plan.IndexDefinition{{
	//						IndexName:  "idx_name",
	//						Using:      sql.IndexUsing_Default,
	//						Constraint: sql.IndexConstraint_None,
	//						Columns:    []sql.IndexColumn{{"b", 0}},
	//						Comment:    "hi",
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, UNIQUE INDEX (b))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					IdxDefs: []*plan.IndexDefinition{{
	//						IndexName:  "",
	//						Using:      sql.IndexUsing_Default,
	//						Constraint: sql.IndexConstraint_Unique,
	//						Columns:    []sql.IndexColumn{{"b", 0}},
	//						Comment:    "",
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, UNIQUE (b))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					IdxDefs: []*plan.IndexDefinition{{
	//						IndexName:  "",
	//						Using:      sql.IndexUsing_Default,
	//						Constraint: sql.IndexConstraint_Unique,
	//						Columns:    []sql.IndexColumn{{"b", 0}},
	//						Comment:    "",
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX (b, a))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					IdxDefs: []*plan.IndexDefinition{{
	//						IndexName:  "",
	//						Using:      sql.IndexUsing_Default,
	//						Constraint: sql.IndexConstraint_None,
	//						Columns:    []sql.IndexColumn{{"b", 0}, {"a", 0}},
	//						Comment:    "",
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX (b), INDEX (b, a))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					IdxDefs: []*plan.IndexDefinition{{
	//						IndexName:  "",
	//						Using:      sql.IndexUsing_Default,
	//						Constraint: sql.IndexConstraint_None,
	//						Columns:    []sql.IndexColumn{{"b", 0}},
	//						Comment:    "",
	//					}, {
	//						IndexName:  "",
	//						Using:      sql.IndexUsing_Default,
	//						Constraint: sql.IndexConstraint_None,
	//						Columns:    []sql.IndexColumn{{"b", 0}, {"a", 0}},
	//						Comment:    "",
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES t0(b))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b_id",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					FkDefs: []*sql.ForeignKeyConstraint{{
	//						Name:           "",
	//						Database:       "",
	//						Table:          "t1",
	//						Columns:        []string{"b_id"},
	//						ParentDatabase: "",
	//						ParentTable:    "t0",
	//						ParentColumns:  []string{"b"},
	//						OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
	//						OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
	//						IsResolved:     false,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, CONSTRAINT fk_name FOREIGN KEY (b_id) REFERENCES t0(b))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b_id",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					FkDefs: []*sql.ForeignKeyConstraint{{
	//						Name:           "fk_name",
	//						Database:       "",
	//						Table:          "t1",
	//						Columns:        []string{"b_id"},
	//						ParentDatabase: "",
	//						ParentTable:    "t0",
	//						ParentColumns:  []string{"b"},
	//						OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
	//						OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
	//						IsResolved:     false,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES t0(b) ON UPDATE CASCADE)`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b_id",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					FkDefs: []*sql.ForeignKeyConstraint{{
	//						Name:           "",
	//						Database:       "",
	//						Table:          "t1",
	//						Columns:        []string{"b_id"},
	//						ParentDatabase: "",
	//						ParentTable:    "t0",
	//						ParentColumns:  []string{"b"},
	//						OnUpdate:       sql.ForeignKeyReferentialAction_Cascade,
	//						OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
	//						IsResolved:     false,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES t0(b) ON DELETE RESTRICT)`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b_id",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					FkDefs: []*sql.ForeignKeyConstraint{{
	//						Name:           "",
	//						Database:       "",
	//						Table:          "t1",
	//						Columns:        []string{"b_id"},
	//						ParentDatabase: "",
	//						ParentTable:    "t0",
	//						ParentColumns:  []string{"b"},
	//						OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
	//						OnDelete:       sql.ForeignKeyReferentialAction_Restrict,
	//						IsResolved:     false,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES t0(b) ON UPDATE SET NULL ON DELETE NO ACTION)`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b_id",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//
	//					FkDefs: []*sql.ForeignKeyConstraint{{
	//						Name:           "",
	//						Database:       "",
	//						Table:          "t1",
	//						Columns:        []string{"b_id"},
	//						ParentDatabase: "",
	//						ParentTable:    "t0",
	//						ParentColumns:  []string{"b"},
	//						OnUpdate:       sql.ForeignKeyReferentialAction_SetNull,
	//						OnDelete:       sql.ForeignKeyReferentialAction_NoAction,
	//						IsResolved:     false,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, c_id BIGINT, FOREIGN KEY (b_id, c_id) REFERENCES t0(b, c))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b_id",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}, {
	//						Name:       "c_id",
	//						Type:       types.Int64,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					FkDefs: []*sql.ForeignKeyConstraint{{
	//						Name:           "",
	//						Database:       "",
	//						Table:          "t1",
	//						Columns:        []string{"b_id", "c_id"},
	//						ParentDatabase: "",
	//						ParentTable:    "t0",
	//						ParentColumns:  []string{"b", "c"},
	//						OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
	//						OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
	//						IsResolved:     false,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, c_id BIGINT, CONSTRAINT fk_name FOREIGN KEY (b_id, c_id) REFERENCES t0(b, c) ON UPDATE RESTRICT ON DELETE CASCADE)`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}, {
	//						Name:       "b_id",
	//						Type:       types.Int32,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}, {
	//						Name:       "c_id",
	//						Type:       types.Int64,
	//						Nullable:   true,
	//						PrimaryKey: false,
	//					}}),
	//					FkDefs: []*sql.ForeignKeyConstraint{{
	//						Name:           "fk_name",
	//						Database:       "",
	//						Table:          "t1",
	//						Columns:        []string{"b_id", "c_id"},
	//						ParentDatabase: "",
	//						ParentTable:    "t0",
	//						ParentColumns:  []string{"b", "c"},
	//						OnUpdate:       sql.ForeignKeyReferentialAction_Restrict,
	//						OnDelete:       sql.ForeignKeyReferentialAction_Cascade,
	//						IsResolved:     false,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, CHECK (a > 0))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}}),
	//					ChDefs: []*sql.CheckConstraint{{
	//						Name: "",
	//						Expr: expression.NewGreaterThan(
	//							expression.NewUnresolvedColumn("a"),
	//							expression.NewLiteral(int8(0), types.Int8),
	//						),
	//						Enforced: true,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `
	//CREATE TABLE t4
	//(
	//  CHECK (c1 = c2),
	//  c1 INT CHECK (c1 > 10),
	//  c2 INT CONSTRAINT c2_positive CHECK (c2 > 0),
	//  CHECK (c1 > c3)
	//);`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t4",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{
	//						{
	//							Name:     "c1",
	//							Source:   "t4",
	//							Type:     types.Int32,
	//							Nullable: true,
	//						},
	//						{
	//							Name:     "c2",
	//							Source:   "t4",
	//							Type:     types.Int32,
	//							Nullable: true,
	//						},
	//					}),
	//					ChDefs: []*sql.CheckConstraint{
	//						{
	//							Expr: expression.NewEquals(
	//								expression.NewUnresolvedColumn("c1"),
	//								expression.NewUnresolvedColumn("c2"),
	//							),
	//							Enforced: true,
	//						},
	//						{
	//							Expr: expression.NewGreaterThan(
	//								expression.NewUnresolvedColumn("c1"),
	//								expression.NewLiteral(int8(10), types.Int8),
	//							),
	//							Enforced: true,
	//						},
	//						{
	//							Name: "c2_positive",
	//							Expr: expression.NewGreaterThan(
	//								expression.NewUnresolvedColumn("c2"),
	//								expression.NewLiteral(int8(0), types.Int8),
	//							),
	//							Enforced: true,
	//						},
	//						{
	//							Expr: expression.NewGreaterThan(
	//								expression.NewUnresolvedColumn("c1"),
	//								expression.NewUnresolvedColumn("c3"),
	//							),
	//							Enforced: true,
	//						},
	//					},
	//				},
	//			),
	//		},
	//		{
	//			input: `
	//CREATE TABLE t2
	//(
	//  CHECK (c1 = c2),
	//  c1 INT CHECK (c1 > 10),
	//  c2 INT CONSTRAINT c2_positive CHECK (c2 > 0),
	//  c3 INT CHECK (c3 < 100),
	//  CONSTRAINT c1_nonzero CHECK (c1 = 0),
	//  CHECK (c1 > c3)
	//);`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t2",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{
	//						{
	//							Name:     "c1",
	//							Source:   "t2",
	//							Type:     types.Int32,
	//							Nullable: true,
	//						},
	//						{
	//							Name:     "c2",
	//							Source:   "t2",
	//							Type:     types.Int32,
	//							Nullable: true,
	//						},
	//						{
	//							Name:     "c3",
	//							Source:   "t2",
	//							Type:     types.Int32,
	//							Nullable: true,
	//						},
	//					}),
	//					ChDefs: []*sql.CheckConstraint{
	//						{
	//							Expr: expression.NewEquals(
	//								expression.NewUnresolvedColumn("c1"),
	//								expression.NewUnresolvedColumn("c2"),
	//							),
	//							Enforced: true,
	//						},
	//						{
	//							Expr: expression.NewGreaterThan(
	//								expression.NewUnresolvedColumn("c1"),
	//								expression.NewLiteral(int8(10), types.Int8),
	//							),
	//							Enforced: true,
	//						},
	//						{
	//							Name: "c2_positive",
	//							Expr: expression.NewGreaterThan(
	//								expression.NewUnresolvedColumn("c2"),
	//								expression.NewLiteral(int8(0), types.Int8),
	//							),
	//							Enforced: true,
	//						},
	//						{
	//							Expr: expression.NewLessThan(
	//								expression.NewUnresolvedColumn("c3"),
	//								expression.NewLiteral(int8(100), types.Int8),
	//							),
	//							Enforced: true,
	//						},
	//						{
	//							Name: "c1_nonzero",
	//							Expr: expression.NewEquals(
	//								expression.NewUnresolvedColumn("c1"),
	//								expression.NewLiteral(int8(0), types.Int8),
	//							),
	//							Enforced: true,
	//						},
	//						{
	//							Expr: expression.NewGreaterThan(
	//								expression.NewUnresolvedColumn("c1"),
	//								expression.NewUnresolvedColumn("c3"),
	//							),
	//							Enforced: true,
	//						},
	//					},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY CHECK (a > 0))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}}),
	//					ChDefs: []*sql.CheckConstraint{{
	//						Name: "",
	//						Expr: expression.NewGreaterThan(
	//							expression.NewUnresolvedColumn("a"),
	//							expression.NewLiteral(int8(0), types.Int8),
	//						),
	//						Enforced: true,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY, CONSTRAINT ch1 CHECK (a > 0))`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}}),
	//					ChDefs: []*sql.CheckConstraint{{
	//						Name: "ch1",
	//						Expr: expression.NewGreaterThan(
	//							expression.NewUnresolvedColumn("a"),
	//							expression.NewLiteral(int8(0), types.Int8),
	//						),
	//						Enforced: true,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY CHECK (a > 0) ENFORCED)`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}}),
	//					ChDefs: []*sql.CheckConstraint{{
	//						Name: "",
	//						Expr: expression.NewGreaterThan(
	//							expression.NewUnresolvedColumn("a"),
	//							expression.NewLiteral(int8(0), types.Int8),
	//						),
	//						Enforced: true,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TABLE t1(a INTEGER PRIMARY KEY CHECK (a > 0) NOT ENFORCED)`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTableAbsent,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:       "a",
	//						Type:       types.Int32,
	//						Nullable:   false,
	//						PrimaryKey: true,
	//					}}),
	//					ChDefs: []*sql.CheckConstraint{{
	//						Name: "",
	//						Expr: expression.NewGreaterThan(
	//							expression.NewUnresolvedColumn("a"),
	//							expression.NewLiteral(int8(0), types.Int8),
	//						),
	//						Enforced: false,
	//					}},
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TEMPORARY TABLE t1(a INTEGER, b TEXT)`,
	//			plan: plan.NewCreateTable(
	//				sql.UnresolvedDatabase(""),
	//				"t1",
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTable,
	//				&plan.TableSpec{
	//					Schema: sql.NewPrimaryKeySchema(sql.Schema{{
	//						Name:     "a",
	//						Type:     types.Int32,
	//						Nullable: true,
	//					}, {
	//						Name:     "b",
	//						Type:     types.Text,
	//						Nullable: true,
	//					}}),
	//				},
	//			),
	//		},
	//		{
	//			input: `CREATE TEMPORARY TABLE mytable AS SELECT * from othertable`,
	//			plan: plan.NewCreateTableSelect(
	//				sql.UnresolvedDatabase(""),
	//				"mytable",
	//				plan.NewProject([]sql.Expression{expression.NewStar()}, plan.NewUnresolvedTable("othertable", "")),
	//				&plan.TableSpec{},
	//				plan.IfNotExistsAbsent,
	//				plan.IsTempTable),
	//		},
	//		{
	//			input: `DROP TABLE curdb.foo;`,
	//			plan: plan.NewDropTable(
	//				[]sql.Node{plan.NewUnresolvedTable("foo", "curdb")}, false,
	//			),
	//		},
	//		{
	//			input: `DROP TABLE t1, t2;`,
	//			plan: plan.NewDropTable(
	//				[]sql.Node{plan.NewUnresolvedTable("t1", ""), plan.NewUnresolvedTable("t2", "")}, false,
	//			),
	//		},
	//		{
	//			input: `DROP TABLE IF EXISTS curdb.foo;`,
	//			plan: plan.NewDropTable(
	//				[]sql.Node{plan.NewUnresolvedTable("foo", "curdb")}, true,
	//			),
	//		},
	//		{
	//			input: `DROP TABLE IF EXISTS curdb.foo, curdb.bar, curdb.baz;`,
	//			plan: plan.NewDropTable(
	//				[]sql.Node{plan.NewUnresolvedTable("foo", "curdb"), plan.NewUnresolvedTable("bar", "curdb"), plan.NewUnresolvedTable("baz", "curdb")}, true,
	//			),
	//		},
	//		{
	//			input: `RENAME TABLE foo TO bar`,
	//			plan:  plan.NewRenameTable(sql.UnresolvedDatabase(""), []string{"foo"}, []string{"bar"}, false),
	//		},
	//		{
	//			input: `RENAME TABLE foo TO bar, baz TO qux`,
	//			plan:  plan.NewRenameTable(sql.UnresolvedDatabase(""), []string{"foo", "baz"}, []string{"bar", "qux"}, false),
	//		},
	//		{
	//			input: `ALTER TABLE foo RENAME bar`,
	//			plan:  plan.NewRenameTable(sql.UnresolvedDatabase(""), []string{"foo"}, []string{"bar"}, true),
	//		},
	//		{
	//			input: `ALTER TABLE foo RENAME TO bar`,
	//			plan:  plan.NewRenameTable(sql.UnresolvedDatabase(""), []string{"foo"}, []string{"bar"}, true),
	//		},
	//		{
	//			input: `ALTER TABLE foo RENAME COLUMN bar TO baz`,
	//			plan: plan.NewRenameColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("foo", ""), "bar", "baz",
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE otherdb.mytable RENAME COLUMN i TO s`,
	//			plan: plan.NewRenameColumn(
	//				sql.UnresolvedDatabase("otherdb"),
	//				plan.NewUnresolvedTable("mytable", "otherdb"), "i", "s",
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable RENAME COLUMN bar TO baz, RENAME COLUMN abc TO xyz`,
	//			plan: plan.NewBlock(
	//				[]sql.Node{
	//					plan.NewRenameColumn(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("mytable", ""), "bar", "baz"),
	//					plan.NewRenameColumn(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("mytable", ""), "abc", "xyz"),
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable ADD COLUMN bar INT NOT NULL`,
	//			plan: plan.NewAddColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""), &sql.Column{
	//					Name:     "bar",
	//					Type:     types.Int32,
	//					Nullable: false,
	//				}, nil,
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable ADD COLUMN bar INT NOT NULL DEFAULT 42 COMMENT 'hello' AFTER baz`,
	//			plan: plan.NewAddColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""), &sql.Column{
	//					Name:     "bar",
	//					Type:     types.Int32,
	//					Nullable: false,
	//					Comment:  "hello",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "42", nil, true),
	//				}, &sql.ColumnOrder{AfterColumn: "baz"},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable ADD COLUMN bar INT NOT NULL DEFAULT -42.0 COMMENT 'hello' AFTER baz`,
	//			plan: plan.NewAddColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""), &sql.Column{
	//					Name:     "bar",
	//					Type:     types.Int32,
	//					Nullable: false,
	//					Comment:  "hello",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "-42.0", nil, true),
	//				}, &sql.ColumnOrder{AfterColumn: "baz"},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable ADD COLUMN bar INT NOT NULL DEFAULT ((2+2)/2) COMMENT 'hello' AFTER baz`,
	//			plan: plan.NewAddColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""), &sql.Column{
	//					Name:     "bar",
	//					Type:     types.Int32,
	//					Nullable: false,
	//					Comment:  "hello",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "((2+2)/2)", nil, true),
	//				}, &sql.ColumnOrder{AfterColumn: "baz"},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable ADD COLUMN bar VARCHAR(10) NULL DEFAULT 'string' COMMENT 'hello'`,
	//			plan: plan.NewAddColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""), &sql.Column{
	//					Name:     "bar",
	//					Type:     types.MustCreateString(sqltypes.VarChar, 10, sql.Collation_Unspecified),
	//					Nullable: true,
	//					Comment:  "hello",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), `"string"`, nil, true),
	//				}, nil,
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable ADD COLUMN bar FLOAT NULL DEFAULT 32.0 COMMENT 'hello'`,
	//			plan: plan.NewAddColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""), &sql.Column{
	//					Name:     "bar",
	//					Type:     types.Float32,
	//					Nullable: true,
	//					Comment:  "hello",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "32.0", nil, true),
	//				}, nil,
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable ADD COLUMN bar INT DEFAULT 1 FIRST`,
	//			plan: plan.NewAddColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""), &sql.Column{
	//					Name:     "bar",
	//					Type:     types.Int32,
	//					Nullable: true,
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "1", nil, true),
	//				}, &sql.ColumnOrder{First: true},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mydb.mytable ADD COLUMN bar INT DEFAULT 1 COMMENT 'otherdb'`,
	//			plan: plan.NewAddColumn(
	//				sql.UnresolvedDatabase("mydb"),
	//				plan.NewUnresolvedTable("mytable", "mydb"), &sql.Column{
	//					Name:     "bar",
	//					Type:     types.Int32,
	//					Nullable: true,
	//					Comment:  "otherdb",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "1", nil, true),
	//				}, nil,
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable ADD INDEX (v1)`,
	//			plan: plan.NewAlterCreateIndex(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""),
	//				"",
	//				sql.IndexUsing_BTree,
	//				sql.IndexConstraint_None,
	//				[]sql.IndexColumn{{"v1", 0}},
	//				"",
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mytable DROP COLUMN bar`,
	//			plan: plan.NewDropColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("mytable", ""), "bar",
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE otherdb.mytable DROP COLUMN bar`,
	//			plan: plan.NewDropColumn(
	//				sql.UnresolvedDatabase("otherdb"),
	//				plan.NewUnresolvedTable("mytable", "otherdb"), "bar",
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE tabletest MODIFY COLUMN bar VARCHAR(10) NULL DEFAULT 'string' COMMENT 'hello' FIRST`,
	//			plan: plan.NewModifyColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("tabletest", ""), "bar", &sql.Column{
	//					Name:     "bar",
	//					Type:     types.MustCreateString(sqltypes.VarChar, 10, sql.Collation_Unspecified),
	//					Nullable: true,
	//					Comment:  "hello",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), `"string"`, nil, true),
	//				}, &sql.ColumnOrder{First: true},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE tabletest CHANGE COLUMN bar baz VARCHAR(10) NULL DEFAULT 'string' COMMENT 'hello' FIRST`,
	//			plan: plan.NewModifyColumn(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("tabletest", ""), "bar", &sql.Column{
	//					Name:     "baz",
	//					Type:     types.MustCreateString(sqltypes.VarChar, 10, sql.Collation_Unspecified),
	//					Nullable: true,
	//					Comment:  "hello",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), `"string"`, nil, true),
	//				}, &sql.ColumnOrder{First: true},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE mydb.mytable MODIFY COLUMN col1 VARCHAR(20) NULL DEFAULT 'string' COMMENT 'changed'`,
	//			plan: plan.NewModifyColumn(
	//				sql.UnresolvedDatabase("mydb"),
	//				plan.NewUnresolvedTable("mytable", "mydb"), "col1", &sql.Column{
	//					Name:     "col1",
	//					Type:     types.MustCreateString(sqltypes.VarChar, 20, sql.Collation_Unspecified),
	//					Nullable: true,
	//					Comment:  "changed",
	//					Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), `"string"`, nil, true),
	//				}, nil,
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD FOREIGN KEY (b_id) REFERENCES t0(b)`,
	//			plan: plan.NewAlterAddForeignKey(
	//				&sql.ForeignKeyConstraint{
	//					Name:           "",
	//					Database:       "",
	//					Table:          "t1",
	//					Columns:        []string{"b_id"},
	//					ParentDatabase: "",
	//					ParentTable:    "t0",
	//					ParentColumns:  []string{"b"},
	//					OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
	//					OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
	//					IsResolved:     false,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD CONSTRAINT fk_name FOREIGN KEY (b_id) REFERENCES t0(b)`,
	//			plan: plan.NewAlterAddForeignKey(
	//				&sql.ForeignKeyConstraint{
	//					Name:           "fk_name",
	//					Database:       "",
	//					Table:          "t1",
	//					Columns:        []string{"b_id"},
	//					ParentDatabase: "",
	//					ParentTable:    "t0",
	//					ParentColumns:  []string{"b"},
	//					OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
	//					OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
	//					IsResolved:     false,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD FOREIGN KEY (b_id) REFERENCES t0(b) ON UPDATE CASCADE`,
	//			plan: plan.NewAlterAddForeignKey(
	//				&sql.ForeignKeyConstraint{
	//					Name:           "",
	//					Database:       "",
	//					Table:          "t1",
	//					Columns:        []string{"b_id"},
	//					ParentDatabase: "",
	//					ParentTable:    "t0",
	//					ParentColumns:  []string{"b"},
	//					OnUpdate:       sql.ForeignKeyReferentialAction_Cascade,
	//					OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
	//					IsResolved:     false,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD FOREIGN KEY (b_id) REFERENCES t0(b) ON DELETE RESTRICT`,
	//			plan: plan.NewAlterAddForeignKey(
	//				&sql.ForeignKeyConstraint{
	//					Name:           "",
	//					Database:       "",
	//					Table:          "t1",
	//					Columns:        []string{"b_id"},
	//					ParentDatabase: "",
	//					ParentTable:    "t0",
	//					ParentColumns:  []string{"b"},
	//					OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
	//					OnDelete:       sql.ForeignKeyReferentialAction_Restrict,
	//					IsResolved:     false,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD FOREIGN KEY (b_id) REFERENCES t0(b) ON UPDATE SET NULL ON DELETE NO ACTION`,
	//			plan: plan.NewAlterAddForeignKey(
	//				&sql.ForeignKeyConstraint{
	//					Name:           "",
	//					Database:       "",
	//					Table:          "t1",
	//					Columns:        []string{"b_id"},
	//					ParentDatabase: "",
	//					ParentTable:    "t0",
	//					ParentColumns:  []string{"b"},
	//					OnUpdate:       sql.ForeignKeyReferentialAction_SetNull,
	//					OnDelete:       sql.ForeignKeyReferentialAction_NoAction,
	//					IsResolved:     false,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD FOREIGN KEY (b_id, c_id) REFERENCES t0(b, c)`,
	//			plan: plan.NewAlterAddForeignKey(
	//				&sql.ForeignKeyConstraint{
	//					Name:           "",
	//					Database:       "",
	//					Table:          "t1",
	//					Columns:        []string{"b_id", "c_id"},
	//					ParentDatabase: "",
	//					ParentTable:    "t0",
	//					ParentColumns:  []string{"b", "c"},
	//					OnUpdate:       sql.ForeignKeyReferentialAction_DefaultAction,
	//					OnDelete:       sql.ForeignKeyReferentialAction_DefaultAction,
	//					IsResolved:     false,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD CONSTRAINT fk_name FOREIGN KEY (b_id, c_id) REFERENCES t0(b, c) ON UPDATE RESTRICT ON DELETE CASCADE`,
	//			plan: plan.NewAlterAddForeignKey(
	//				&sql.ForeignKeyConstraint{
	//					Name:           "fk_name",
	//					Database:       "",
	//					Table:          "t1",
	//					Columns:        []string{"b_id", "c_id"},
	//					ParentDatabase: "",
	//					ParentTable:    "t0",
	//					ParentColumns:  []string{"b", "c"},
	//					OnUpdate:       sql.ForeignKeyReferentialAction_Restrict,
	//					OnDelete:       sql.ForeignKeyReferentialAction_Cascade,
	//					IsResolved:     false,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD CHECK (a > 0)`,
	//			plan: plan.NewAlterAddCheck(
	//				plan.NewUnresolvedTable("t1", ""),
	//				&sql.CheckConstraint{
	//					Name: "",
	//					Expr: expression.NewGreaterThan(
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewLiteral(int8(0), types.Int8),
	//					),
	//					Enforced: true,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD CONSTRAINT ch1 CHECK (a > 0)`,
	//			plan: plan.NewAlterAddCheck(
	//				plan.NewUnresolvedTable("t1", ""),
	//				&sql.CheckConstraint{
	//					Name: "ch1",
	//					Expr: expression.NewGreaterThan(
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewLiteral(int8(0), types.Int8),
	//					),
	//					Enforced: true,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 ADD CONSTRAINT CHECK (a > 0)`,
	//			plan: plan.NewAlterAddCheck(
	//				plan.NewUnresolvedTable("t1", ""),
	//				&sql.CheckConstraint{
	//					Name: "",
	//					Expr: expression.NewGreaterThan(
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewLiteral(int8(0), types.Int8),
	//					),
	//					Enforced: true,
	//				},
	//			),
	//		},
	//		{
	//			input: `ALTER TABLE t1 DROP FOREIGN KEY fk_name`,
	//			plan:  plan.NewAlterDropForeignKey("", "t1", "fk_name"),
	//		},
	//		{
	//			input: `ALTER TABLE t1 DROP CONSTRAINT fk_name`,
	//			plan: plan.NewDropConstraint(
	//				plan.NewUnresolvedTable("t1", ""),
	//				"fk_name",
	//			),
	//		},
	//		{
	//			input: `DESCRIBE foo;`,
	//			plan: plan.NewShowColumns(false,
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `DESC foo;`,
	//			plan: plan.NewShowColumns(false,
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: "DESCRIBE FORMAT=tree SELECT * FROM foo",
	//			plan: plan.NewDescribeQuery(
	//				"tree", plan.NewProject(
	//					[]sql.Expression{expression.NewStar()},
	//					plan.NewUnresolvedTable("foo", ""),
	//				)),
	//		},
	//		{
	//			input: "DESC FORMAT=tree SELECT * FROM foo",
	//			plan: plan.NewDescribeQuery(
	//				"tree", plan.NewProject(
	//					[]sql.Expression{expression.NewStar()},
	//					plan.NewUnresolvedTable("foo", ""),
	//				)),
	//		},
	//		{
	//			input: "EXPLAIN FORMAT=tree SELECT * FROM foo",
	//			plan: plan.NewDescribeQuery(
	//				"tree", plan.NewProject(
	//					[]sql.Expression{expression.NewStar()},
	//					plan.NewUnresolvedTable("foo", "")),
	//			),
	//		},
	//		{
	//			input: "DESCRIBE SELECT * FROM foo",
	//			plan: plan.NewDescribeQuery(
	//				"tree", plan.NewProject(
	//					[]sql.Expression{expression.NewStar()},
	//					plan.NewUnresolvedTable("foo", ""),
	//				)),
	//		},
	//		{
	//			input: "DESC SELECT * FROM foo",
	//			plan: plan.NewDescribeQuery(
	//				"tree", plan.NewProject(
	//					[]sql.Expression{expression.NewStar()},
	//					plan.NewUnresolvedTable("foo", ""),
	//				)),
	//		},
	//		{
	//			input: "EXPLAIN SELECT * FROM foo",
	//			plan: plan.NewDescribeQuery(
	//				"tree", plan.NewProject(
	//					[]sql.Expression{expression.NewStar()},
	//					plan.NewUnresolvedTable("foo", "")),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT foo IS NULL, bar IS NOT NULL FROM foo;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
	//					expression.NewAlias("bar IS NOT NULL",
	//						expression.NewNot(expression.NewIsNull(expression.NewUnresolvedColumn("bar"))),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT foo IS TRUE, bar IS NOT FALSE FROM foo;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewIsTrue(expression.NewUnresolvedColumn("foo")),
	//					expression.NewAlias("bar IS NOT FALSE",
	//						expression.NewNot(expression.NewIsFalse(expression.NewUnresolvedColumn("bar"))),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT foo AS bar FROM foo;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("bar", expression.NewUnresolvedColumn("foo")),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT foo AS bAz FROM foo;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("bAz", expression.NewUnresolvedColumn("foo")),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT foo AS bar FROM foo AS OF '2019-01-01' AS baz;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("bar", expression.NewUnresolvedColumn("foo")),
	//				},
	//				plan.NewTableAlias("baz",
	//					plan.NewUnresolvedTableAsOf("foo", "",
	//						expression.NewLiteral("2019-01-01", types.LongText))),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo WHERE foo = bar;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewFilter(
	//					expression.NewEquals(
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewUnresolvedColumn("bar"),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo WHERE foo = 'bar';`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewFilter(
	//					expression.NewEquals(
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewLiteral("bar", types.LongText),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo WHERE foo = ?;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewFilter(
	//					expression.NewEquals(
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewBindVar("v1"),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM (SELECT * FROM foo WHERE bar = ?) a;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewSubqueryAlias(
	//					"a",
	//					"select * from foo where bar = :v1",
	//					plan.NewProject(
	//						[]sql.Expression{
	//							expression.NewStar(),
	//						},
	//						plan.NewFilter(
	//							expression.NewEquals(
	//								expression.NewUnresolvedColumn("bar"),
	//								expression.NewBindVar("v1"),
	//							),
	//							plan.NewUnresolvedTable("foo", ""),
	//						),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM (values row(1,2), row(3,4)) a;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewValueDerivedTable(
	//					plan.NewValues([][]sql.Expression{
	//						{
	//							expression.NewLiteral(int8(1), types.Int8),
	//							expression.NewLiteral(int8(2), types.Int8),
	//						},
	//						{
	//							expression.NewLiteral(int8(3), types.Int8),
	//							expression.NewLiteral(int8(4), types.Int8),
	//						},
	//					}),
	//					"a"),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM (values row(1+1,2+2), row(rand(),concat("a","b"))) a;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewValueDerivedTable(
	//					plan.NewValues([][]sql.Expression{
	//						{
	//							expression.NewArithmetic(
	//								expression.NewLiteral(int8(1), types.Int8),
	//								expression.NewLiteral(int8(1), types.Int8),
	//								"+",
	//							),
	//							expression.NewArithmetic(
	//								expression.NewLiteral(int8(2), types.Int8),
	//								expression.NewLiteral(int8(2), types.Int8),
	//								"+",
	//							),
	//						},
	//						{
	//							expression.NewUnresolvedFunction("rand", false, nil),
	//							expression.NewUnresolvedFunction("concat", false, nil, expression.NewLiteral("a", types.LongText), expression.NewLiteral("b", types.LongText)),
	//						},
	//					}),
	//					"a"),
	//			),
	//		},
	//		{
	//			input: `SELECT column_0 FROM (values row(1,2), row(3,4)) a limit 1`,
	//			plan: plan.NewLimit(expression.NewLiteral(int8(1), types.Int8),
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("column_0"),
	//					},
	//					plan.NewValueDerivedTable(
	//						plan.NewValues([][]sql.Expression{
	//							{
	//								expression.NewLiteral(int8(1), types.Int8),
	//								expression.NewLiteral(int8(2), types.Int8),
	//							},
	//							{
	//								expression.NewLiteral(int8(3), types.Int8),
	//								expression.NewLiteral(int8(4), types.Int8),
	//							},
	//						}),
	//						"a"),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo WHERE foo <=> bar;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewFilter(
	//					expression.NewNullSafeEquals(
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewUnresolvedColumn("bar"),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo WHERE foo = :var;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewFilter(
	//					expression.NewEquals(
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewBindVar("var"),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE foo != 'bar';`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewFilter(
	//					expression.NewNot(expression.NewEquals(
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewLiteral("bar", types.LongText),
	//					)),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo LIMIT 10;`,
	//			plan: plan.NewLimit(expression.NewLiteral(int8(10), types.Int8),
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewUnresolvedColumn("bar"),
	//					},
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo ORDER BY baz DESC;`,
	//			plan: plan.NewSort(
	//				[]sql.SortField{
	//					{
	//						Column:       expression.NewUnresolvedColumn("baz"),
	//						Column2:      expression.NewUnresolvedColumn("baz"),
	//						Order:        sql.Descending,
	//						NullOrdering: sql.NullsFirst,
	//					},
	//				},
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewUnresolvedColumn("bar"),
	//					},
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo WHERE foo = bar LIMIT 10;`,
	//			plan: plan.NewLimit(expression.NewLiteral(int8(10), types.Int8),
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewUnresolvedColumn("bar"),
	//					},
	//					plan.NewFilter(
	//						expression.NewEquals(
	//							expression.NewUnresolvedColumn("foo"),
	//							expression.NewUnresolvedColumn("bar"),
	//						),
	//						plan.NewUnresolvedTable("foo", ""),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo ORDER BY baz DESC LIMIT 1;`,
	//			plan: plan.NewLimit(expression.NewLiteral(int8(1), types.Int8),
	//				plan.NewSort(
	//					[]sql.SortField{
	//						{
	//							Column:       expression.NewUnresolvedColumn("baz"),
	//							Column2:      expression.NewUnresolvedColumn("baz"),
	//							Order:        sql.Descending,
	//							NullOrdering: sql.NullsFirst,
	//						},
	//					},
	//					plan.NewProject(
	//						[]sql.Expression{
	//							expression.NewUnresolvedColumn("foo"),
	//							expression.NewUnresolvedColumn("bar"),
	//						},
	//						plan.NewUnresolvedTable("foo", ""),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo WHERE qux = 1 ORDER BY baz DESC LIMIT 1;`,
	//			plan: plan.NewLimit(expression.NewLiteral(int8(1), types.Int8),
	//				plan.NewSort(
	//					[]sql.SortField{
	//						{
	//							Column:       expression.NewUnresolvedColumn("baz"),
	//							Column2:      expression.NewUnresolvedColumn("baz"),
	//							Order:        sql.Descending,
	//							NullOrdering: sql.NullsFirst,
	//						},
	//					},
	//					plan.NewProject(
	//						[]sql.Expression{
	//							expression.NewUnresolvedColumn("foo"),
	//							expression.NewUnresolvedColumn("bar"),
	//						},
	//						plan.NewFilter(
	//							expression.NewEquals(
	//								expression.NewUnresolvedColumn("qux"),
	//								expression.NewLiteral(int8(1), types.Int8),
	//							),
	//							plan.NewUnresolvedTable("foo", ""),
	//						),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM t1, t2;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewCrossJoin(
	//					plan.NewUnresolvedTable("t1", ""),
	//					plan.NewUnresolvedTable("t2", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM t1 JOIN t2;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewCrossJoin(
	//					plan.NewUnresolvedTable("t1", ""),
	//					plan.NewUnresolvedTable("t2", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM t1 GROUP BY foo, bar;`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewUnresolvedTable("t1", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM t1 GROUP BY 1, 2;`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("foo"),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewUnresolvedTable("t1", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT COUNT(*) FROM t1;`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("COUNT(*)",
	//						expression.NewUnresolvedFunction("count", true, nil,
	//							expression.NewStar()),
	//					),
	//				},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("t1", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a FROM t1 where a regexp '.*test.*';`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//				},
	//				plan.NewFilter(
	//					expression.NewRegexp(
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewLiteral(".*test.*", types.LongText),
	//					),
	//					plan.NewUnresolvedTable("t1", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT a FROM t1 where a regexp '*main.go';`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//				},
	//				plan.NewFilter(
	//					expression.NewRegexp(
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewLiteral("*main.go", types.LongText),
	//					),
	//					plan.NewUnresolvedTable("t1", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT a FROM t1 where a not regexp '.*test.*';`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//				},
	//				plan.NewFilter(
	//					expression.NewNot(
	//						expression.NewRegexp(
	//							expression.NewUnresolvedColumn("a"),
	//							expression.NewLiteral(".*test.*", types.LongText),
	//						),
	//					),
	//					plan.NewUnresolvedTable("t1", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `INSERT INTO t1 (col1, col2) VALUES ('a', 1)`,
	//			plan: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
	//				expression.NewLiteral("a", types.LongText),
	//				expression.NewLiteral(int8(1), types.Int8),
	//			}}), false, []string{"col1", "col2"}, []sql.Expression{}, false),
	//		},
	//		{
	//			input: `INSERT INTO mydb.t1 (col1, col2) VALUES ('a', 1)`,
	//			plan: plan.NewInsertInto(sql.UnresolvedDatabase("mydb"), plan.NewUnresolvedTable("t1", "mydb"), plan.NewValues([][]sql.Expression{{
	//				expression.NewLiteral("a", types.LongText),
	//				expression.NewLiteral(int8(1), types.Int8),
	//			}}), false, []string{"col1", "col2"}, []sql.Expression{}, false),
	//		},
	//		{
	//			input: `INSERT INTO t1 (col1, col2) VALUES (?, ?)`,
	//			plan: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
	//				expression.NewBindVar("v1"),
	//				expression.NewBindVar("v2"),
	//			}}), false, []string{"col1", "col2"}, []sql.Expression{}, false),
	//		},
	//		{
	//			input: `INSERT INTO t1 VALUES (b'0111')`,
	//			plan: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
	//				expression.NewLiteral(uint64(7), types.Uint64),
	//			}}), false, []string{}, []sql.Expression{}, false),
	//		},
	//		{
	//			input: `INSERT INTO t1 (col1, col2) VALUES ('a', DEFAULT)`,
	//			plan: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
	//				expression.NewLiteral("a", types.LongText),
	//				&expression.DefaultColumn{},
	//			}}), false, []string{"col1", "col2"}, []sql.Expression{}, false),
	//		},
	//		{
	//			input: `INSERT INTO test (decimal_col) VALUES (11981.5923291839784651)`,
	//			plan: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("test", ""), plan.NewValues([][]sql.Expression{{
	//				expression.NewLiteral(decimal.RequireFromString("11981.5923291839784651"), types.MustCreateDecimalType(21, 16)),
	//			}}), false, []string{"decimal_col"}, []sql.Expression{}, false),
	//		},
	//		{
	//			input: `INSERT INTO test (decimal_col) VALUES (119815923291839784651.11981592329183978465111981592329183978465144)`,
	//			plan: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("test", ""), plan.NewValues([][]sql.Expression{{
	//				expression.NewLiteral("119815923291839784651.11981592329183978465111981592329183978465144", types.LongText),
	//			}}), false, []string{"decimal_col"}, []sql.Expression{}, false),
	//		},
	//		{
	//			input: `UPDATE t1 SET col1 = ?, col2 = ? WHERE id = ?`,
	//			plan: plan.NewUpdate(plan.NewFilter(
	//				expression.NewEquals(expression.NewUnresolvedColumn("id"), expression.NewBindVar("v3")),
	//				plan.NewUnresolvedTable("t1", ""),
	//			), false, []sql.Expression{
	//				expression.NewSetField(expression.NewUnresolvedColumn("col1"), expression.NewBindVar("v1")),
	//				expression.NewSetField(expression.NewUnresolvedColumn("col2"), expression.NewBindVar("v2")),
	//			}),
	//		},
	//		{
	//			input: `REPLACE INTO t1 (col1, col2) VALUES ('a', 1)`,
	//			plan: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
	//				expression.NewLiteral("a", types.LongText),
	//				expression.NewLiteral(int8(1), types.Int8),
	//			}}), true, []string{"col1", "col2"}, []sql.Expression{}, false),
	//		},
	//		{
	//			input: `SHOW TABLES`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase(""), false, nil),
	//		},
	//		{
	//			input: `SHOW FULL TABLES`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase(""), true, nil),
	//		},
	//		{
	//			input: `SHOW TABLES FROM foo`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase("foo"), false, nil),
	//		},
	//		{
	//			input: `SHOW TABLES IN foo`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase("foo"), false, nil),
	//		},
	//		{
	//			input: `SHOW FULL TABLES FROM foo`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase("foo"), true, nil),
	//		},
	//		{
	//			input: `SHOW FULL TABLES IN foo`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase("foo"), true, nil),
	//		},
	//		{
	//			input: `SHOW TABLES AS OF 'abc'`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase(""), false, expression.NewLiteral("abc", types.LongText)),
	//		},
	//		{
	//			input: `SHOW FULL TABLES AS OF 'abc'`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase(""), true, expression.NewLiteral("abc", types.LongText)),
	//		},
	//		{
	//			input: `SHOW TABLES FROM foo AS OF 'abc'`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase("foo"), false, expression.NewLiteral("abc", types.LongText)),
	//		},
	//		{
	//			input: `SHOW FULL TABLES FROM foo AS OF 'abc'`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase("foo"), true, expression.NewLiteral("abc", types.LongText)),
	//		},
	//		{
	//			input: `SHOW FULL TABLES IN foo AS OF 'abc'`,
	//			plan:  plan.NewShowTables(sql.UnresolvedDatabase("foo"), true, expression.NewLiteral("abc", types.LongText)),
	//		},
	//		{
	//			input: `SHOW TABLES FROM mydb LIKE 'foo'`,
	//			plan: plan.NewFilter(
	//				expression.NewLike(
	//					expression.NewUnresolvedColumn("Tables_in_mydb"),
	//					expression.NewLiteral("foo", types.LongText),
	//					nil,
	//				),
	//				plan.NewShowTables(sql.UnresolvedDatabase("mydb"), false, nil),
	//			),
	//		},
	//		{
	//			input: `SHOW TABLES FROM mydb AS OF 'abc' LIKE 'foo'`,
	//			plan: plan.NewFilter(
	//				expression.NewLike(
	//					expression.NewUnresolvedColumn("Tables_in_mydb"),
	//					expression.NewLiteral("foo", types.LongText),
	//					nil,
	//				),
	//				plan.NewShowTables(sql.UnresolvedDatabase("mydb"), false, expression.NewLiteral("abc", types.LongText)),
	//			),
	//		},
	//		{
	//			input: "SHOW TABLES FROM bar WHERE `Tables_in_bar` = 'foo'",
	//			plan: plan.NewFilter(
	//				expression.NewEquals(
	//					expression.NewUnresolvedColumn("Tables_in_bar"),
	//					expression.NewLiteral("foo", types.LongText),
	//				),
	//				plan.NewShowTables(sql.UnresolvedDatabase("bar"), false, nil),
	//			),
	//		},
	//		{
	//			input: `SHOW FULL TABLES FROM mydb LIKE 'foo'`,
	//			plan: plan.NewFilter(
	//				expression.NewLike(
	//					expression.NewUnresolvedColumn("Tables_in_mydb"),
	//					expression.NewLiteral("foo", types.LongText),
	//					nil,
	//				),
	//				plan.NewShowTables(sql.UnresolvedDatabase("mydb"), true, nil),
	//			),
	//		},
	//		{
	//			input: "SHOW FULL TABLES FROM bar WHERE `Tables_in_bar` = 'foo'",
	//			plan: plan.NewFilter(
	//				expression.NewEquals(
	//					expression.NewUnresolvedColumn("Tables_in_bar"),
	//					expression.NewLiteral("foo", types.LongText),
	//				),
	//				plan.NewShowTables(sql.UnresolvedDatabase("bar"), true, nil),
	//			),
	//		},
	//		{
	//			input: `SHOW FULL TABLES FROM bar LIKE 'foo'`,
	//			plan: plan.NewFilter(
	//				expression.NewLike(
	//					expression.NewUnresolvedColumn("Tables_in_bar"),
	//					expression.NewLiteral("foo", types.LongText),
	//					nil,
	//				),
	//				plan.NewShowTables(sql.UnresolvedDatabase("bar"), true, nil),
	//			),
	//		},
	//		{
	//			input: `SHOW FULL TABLES FROM bar AS OF 'abc' LIKE 'foo'`,
	//			plan: plan.NewFilter(
	//				expression.NewLike(
	//					expression.NewUnresolvedColumn("Tables_in_bar"),
	//					expression.NewLiteral("foo", types.LongText),
	//					nil,
	//				),
	//				plan.NewShowTables(sql.UnresolvedDatabase("bar"), true, expression.NewLiteral("abc", types.LongText)),
	//			),
	//		},
	//		{
	//			input: "SHOW FULL TABLES FROM bar WHERE `Tables_in_bar` = 'test'",
	//			plan: plan.NewFilter(
	//				expression.NewEquals(
	//					expression.NewUnresolvedColumn("Tables_in_bar"),
	//					expression.NewLiteral("test", types.LongText),
	//				),
	//				plan.NewShowTables(sql.UnresolvedDatabase("bar"), true, nil),
	//			),
	//		},
	//		{
	//			input: `SELECT DISTINCT foo, bar FROM foo;`,
	//			plan: plan.NewDistinct(
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewUnresolvedColumn("bar"),
	//					},
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo LIMIT 2 OFFSET 5;`,
	//			plan: plan.NewLimit(expression.NewLiteral(int8(2), types.Int8),
	//				plan.NewOffset(expression.NewLiteral(int8(5), types.Int8), plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewUnresolvedColumn("bar"),
	//					},
	//					plan.NewUnresolvedTable("foo", ""),
	//				)),
	//			),
	//		},
	//		{
	//			input: `SELECT foo, bar FROM foo LIMIT 5,2;`,
	//			plan: plan.NewLimit(expression.NewLiteral(int8(2), types.Int8),
	//				plan.NewOffset(expression.NewLiteral(int8(5), types.Int8), plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("foo"),
	//						expression.NewUnresolvedColumn("bar"),
	//					},
	//					plan.NewUnresolvedTable("foo", ""),
	//				)),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE (a = 1)`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewFilter(
	//					expression.NewEquals(
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewLiteral(int8(1), types.Int8),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo, bar, baz, qux`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewCrossJoin(
	//					plan.NewCrossJoin(
	//						plan.NewCrossJoin(
	//							plan.NewUnresolvedTable("foo", ""),
	//							plan.NewUnresolvedTable("bar", ""),
	//						),
	//						plan.NewUnresolvedTable("baz", ""),
	//					),
	//					plan.NewUnresolvedTable("qux", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo join bar join baz join qux`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewCrossJoin(
	//					plan.NewCrossJoin(
	//						plan.NewCrossJoin(
	//							plan.NewUnresolvedTable("foo", ""),
	//							plan.NewUnresolvedTable("bar", ""),
	//						),
	//						plan.NewUnresolvedTable("baz", ""),
	//					),
	//					plan.NewUnresolvedTable("qux", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE a = b AND c = d`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewAnd(
	//						expression.NewEquals(
	//							expression.NewUnresolvedColumn("a"),
	//							expression.NewUnresolvedColumn("b"),
	//						),
	//						expression.NewEquals(
	//							expression.NewUnresolvedColumn("c"),
	//							expression.NewUnresolvedColumn("d"),
	//						),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE a = b OR c = d`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewOr(
	//						expression.NewEquals(
	//							expression.NewUnresolvedColumn("a"),
	//							expression.NewUnresolvedColumn("b"),
	//						),
	//						expression.NewEquals(
	//							expression.NewUnresolvedColumn("c"),
	//							expression.NewUnresolvedColumn("d"),
	//						),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo as bar`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewTableAlias(
	//					"bar",
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM (SELECT * FROM foo) AS bar`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewSubqueryAlias(
	//					"bar", "select * from foo",
	//					plan.NewProject(
	//						[]sql.Expression{expression.NewStar()},
	//						plan.NewUnresolvedTable("foo", ""),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE 1 NOT BETWEEN 2 AND 5`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewNot(
	//						expression.NewBetween(
	//							expression.NewLiteral(int8(1), types.Int8),
	//							expression.NewLiteral(int8(2), types.Int8),
	//							expression.NewLiteral(int8(5), types.Int8),
	//						),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE 1 BETWEEN 2 AND 5`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewBetween(
	//						expression.NewLiteral(int8(1), types.Int8),
	//						expression.NewLiteral(int8(2), types.Int8),
	//						expression.NewLiteral(int8(5), types.Int8),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT 0x01AF`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("0x01AF",
	//						expression.NewLiteral([]byte{1, 175}, types.LongBlob),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT 0x12345`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("0x12345",
	//						expression.NewLiteral([]byte{1, 35, 69}, types.LongBlob),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT X'41'`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("X'41'",
	//						expression.NewLiteral([]byte{'A'}, types.LongBlob),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM b WHERE SOMEFUNC((1, 2), (3, 4))`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewUnresolvedFunction(
	//						"somefunc",
	//						false,
	//						nil,
	//						expression.NewTuple(
	//							expression.NewLiteral(int8(1), types.Int8),
	//							expression.NewLiteral(int8(2), types.Int8),
	//						),
	//						expression.NewTuple(
	//							expression.NewLiteral(int8(3), types.Int8),
	//							expression.NewLiteral(int8(4), types.Int8),
	//						),
	//					),
	//					plan.NewUnresolvedTable("b", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE :foo_id = 2`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewEquals(
	//						expression.NewBindVar("foo_id"),
	//						expression.NewLiteral(int8(2), types.Int8),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE ? = 2 and foo.s = ? and ? <> foo.i`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewAnd(
	//						expression.NewAnd(
	//							expression.NewEquals(
	//								expression.NewBindVar("v1"),
	//								expression.NewLiteral(int8(2), types.Int8),
	//							),
	//							expression.NewEquals(
	//								expression.NewUnresolvedQualifiedColumn("foo", "s"),
	//								expression.NewBindVar("v2"),
	//							),
	//						),
	//						expression.NewNot(expression.NewEquals(
	//							expression.NewBindVar("v3"),
	//							expression.NewUnresolvedQualifiedColumn("foo", "i"),
	//						)),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo INNER JOIN bar ON a = b`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewInnerJoin(
	//					plan.NewUnresolvedTable("foo", ""),
	//					plan.NewUnresolvedTable("bar", ""),
	//					expression.NewEquals(
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewUnresolvedColumn("b"),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo.a FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedQualifiedColumn("foo", "a"),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT CAST(-3 AS UNSIGNED) FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("CAST(-3 AS UNSIGNED)",
	//						expression.NewConvert(expression.NewLiteral(int8(-3), types.Int8), expression.ConvertToUnsigned),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT 2 = 2 FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("2 = 2",
	//						expression.NewEquals(expression.NewLiteral(int8(2), types.Int8), expression.NewLiteral(int8(2), types.Int8))),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT *, bar FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//					expression.NewUnresolvedColumn("bar"),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT *, foo.* FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//					expression.NewQualifiedStar("foo"),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT bar, foo.* FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("bar"),
	//					expression.NewQualifiedStar("foo"),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT bar, *, foo.* FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("bar"),
	//					expression.NewStar(),
	//					expression.NewQualifiedStar("foo"),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT *, * FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//					expression.NewStar(),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE 1 IN ('1', 2)`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewInTuple(
	//						expression.NewLiteral(int8(1), types.Int8),
	//						expression.NewTuple(
	//							expression.NewLiteral("1", types.LongText),
	//							expression.NewLiteral(int8(2), types.Int8),
	//						),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE 1 NOT IN ('1', 2)`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewNotInTuple(
	//						expression.NewLiteral(int8(1), types.Int8),
	//						expression.NewTuple(
	//							expression.NewLiteral("1", types.LongText),
	//							expression.NewLiteral(int8(2), types.Int8),
	//						),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE i IN (SELECT j FROM baz)`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					plan.NewInSubquery(
	//						expression.NewUnresolvedColumn("i"),
	//						plan.NewSubquery(plan.NewProject(
	//							[]sql.Expression{expression.NewUnresolvedColumn("j")},
	//							plan.NewUnresolvedTable("baz", ""),
	//						), "select j from baz"),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE i NOT IN (SELECT j FROM baz)`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					plan.NewNotInSubquery(
	//						expression.NewUnresolvedColumn("i"),
	//						plan.NewSubquery(plan.NewProject(
	//							[]sql.Expression{expression.NewUnresolvedColumn("j")},
	//							plan.NewUnresolvedTable("baz", ""),
	//						), "select j from baz"),
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT a, b FROM t ORDER BY 2, 1`,
	//			plan: plan.NewSort(
	//				[]sql.SortField{
	//					{
	//						Column:       expression.NewLiteral(int8(2), types.Int8),
	//						Column2:      expression.NewLiteral(int8(2), types.Int8),
	//						Order:        sql.Ascending,
	//						NullOrdering: sql.NullsFirst,
	//					},
	//					{
	//						Column:       expression.NewLiteral(int8(1), types.Int8),
	//						Column2:      expression.NewLiteral(int8(1), types.Int8),
	//						Order:        sql.Ascending,
	//						NullOrdering: sql.NullsFirst,
	//					},
	//				},
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewUnresolvedColumn("b"),
	//					},
	//					plan.NewUnresolvedTable("t", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT -i FROM mytable`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewUnaryMinus(
	//						expression.NewUnresolvedColumn("i"),
	//					),
	//				},
	//				plan.NewUnresolvedTable("mytable", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT +i FROM mytable`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("+i",
	//						expression.NewUnresolvedColumn("i"),
	//					),
	//				},
	//				plan.NewUnresolvedTable("mytable", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT - 4 - - 80`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("- 4 - - 80",
	//						expression.NewMinus(
	//							expression.NewLiteral(int8(-4), types.Int8),
	//							expression.NewLiteral(int8(-80), types.Int8),
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT + - - i FROM mytable`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("+ - - i",
	//						expression.NewUnaryMinus(
	//							expression.NewUnaryMinus(
	//								expression.NewUnresolvedColumn("i"),
	//							),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("mytable", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT 1 + 1;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("1 + 1",
	//						expression.NewPlus(expression.NewLiteral(int8(1), types.Int8), expression.NewLiteral(int8(1), types.Int8))),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT 1 + 1 as foo;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("foo",
	//						expression.NewPlus(expression.NewLiteral(int8(1), types.Int8), expression.NewLiteral(int8(1), types.Int8))),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT 1 * (2 + 1);`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("1 * (2 + 1)",
	//						expression.NewMult(expression.NewLiteral(int8(1), types.Int8),
	//							expression.NewPlus(expression.NewLiteral(int8(2), types.Int8), expression.NewLiteral(int8(1), types.Int8))),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT (0 - 1) * (1 | 1);`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("(0 - 1) * (1 | 1)",
	//						expression.NewMult(
	//							expression.NewMinus(expression.NewLiteral(int8(0), types.Int8), expression.NewLiteral(int8(1), types.Int8)),
	//							expression.NewBitOr(expression.NewLiteral(int8(1), types.Int8), expression.NewLiteral(int8(1), types.Int8)),
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT (1 << 3) % (2 div 1);`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("(1 << 3) % (2 div 1)",
	//						expression.NewMod(
	//							expression.NewShiftLeft(expression.NewLiteral(int8(1), types.Int8), expression.NewLiteral(int8(3), types.Int8)),
	//							expression.NewIntDiv(expression.NewLiteral(int8(2), types.Int8), expression.NewLiteral(int8(1), types.Int8))),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT 1.0 * a + 2.0 * b FROM t;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("1.0 * a + 2.0 * b",
	//						expression.NewPlus(
	//							expression.NewMult(expression.NewLiteral(decimal.RequireFromString("1.0"), types.MustCreateDecimalType(2, 1)), expression.NewUnresolvedColumn("a")),
	//							expression.NewMult(expression.NewLiteral(decimal.RequireFromString("2.0"), types.MustCreateDecimalType(2, 1)), expression.NewUnresolvedColumn("b")),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("t", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT '1.0' + 2;`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("'1.0' + 2",
	//						expression.NewPlus(
	//							expression.NewLiteral("1.0", types.LongText), expression.NewLiteral(int8(2), types.Int8),
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT '1' + '2';`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("'1' + '2'",
	//						expression.NewPlus(
	//							expression.NewLiteral("1", types.LongText), expression.NewLiteral("2", types.LongText),
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `CREATE INDEX foo USING qux ON bar (baz)`,
	//			plan: plan.NewCreateIndex(
	//				"foo",
	//				plan.NewUnresolvedTable("bar", ""),
	//				[]sql.Expression{expression.NewUnresolvedColumn("baz")},
	//				"qux",
	//				make(map[string]string),
	//			),
	//		},
	//		{
	//			input: `CREATE INDEX idx USING BTREE ON foo (bar)`,
	//			plan: plan.NewAlterCreateIndex(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("foo", ""),
	//				"idx",
	//				sql.IndexUsing_BTree,
	//				sql.IndexConstraint_None,
	//				[]sql.IndexColumn{
	//					{"bar", 0},
	//				},
	//				"",
	//			),
	//		},
	//		{
	//			input: `      CREATE INDEX idx USING BTREE ON foo(bar)`,
	//			plan: plan.NewAlterCreateIndex(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("foo", ""),
	//				"idx",
	//				sql.IndexUsing_BTree,
	//				sql.IndexConstraint_None,
	//				[]sql.IndexColumn{
	//					{"bar", 0},
	//				},
	//				"",
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo NATURAL JOIN bar`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewNaturalJoin(
	//					plan.NewUnresolvedTable("foo", ""),
	//					plan.NewUnresolvedTable("bar", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo NATURAL JOIN bar NATURAL JOIN baz`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewNaturalJoin(
	//					plan.NewNaturalJoin(
	//						plan.NewUnresolvedTable("foo", ""),
	//						plan.NewUnresolvedTable("bar", ""),
	//					),
	//					plan.NewUnresolvedTable("baz", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `DROP INDEX foo ON bar`,
	//			plan: plan.NewAlterDropIndex(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("bar", ""),
	//				"foo",
	//			),
	//		},
	//		{
	//			input: `alter table t add index (i), drop index i, add check (i = 0), drop check chk, drop constraint c, add column i int, modify column i text, drop column i, rename column i to j`,
	//			plan: plan.NewBlock([]sql.Node{
	//				plan.NewAlterCreateIndex(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t", ""), "", sql.IndexUsing_BTree, sql.IndexConstraint_None, []sql.IndexColumn{{Name: "i", Length: 0}}, ""),
	//				plan.NewAlterDropIndex(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t", ""), "i"),
	//				plan.NewAlterAddCheck(plan.NewUnresolvedTable("t", ""), &sql.CheckConstraint{Name: "", Expr: expression.NewEquals(expression.NewUnresolvedColumn("i"), expression.NewLiteral(int8(0), types.Int8)), Enforced: true}),
	//				plan.NewAlterDropCheck(plan.NewUnresolvedTable("t", ""), "chk"),
	//				plan.NewDropConstraint(plan.NewUnresolvedTable("t", ""), "c"),
	//				plan.NewAddColumn(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t", ""), &sql.Column{Name: "i", Type: types.Int32, Nullable: true, Source: "t"}, nil),
	//				plan.NewModifyColumn(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t", ""), "i", &sql.Column{Name: "i", Type: types.CreateText(sql.Collation_Unspecified), Nullable: true, Source: "t"}, nil),
	//				plan.NewDropColumn(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t", ""), "i"),
	//				plan.NewRenameColumn(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t", ""), "i", "j"),
	//			}),
	//		},
	//		{
	//			input: `DESCRIBE FORMAT=TREE SELECT * FROM foo`,
	//			plan: plan.NewDescribeQuery(
	//				"tree",
	//				plan.NewProject(
	//					[]sql.Expression{expression.NewStar()},
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT MAX(i)/2 FROM foo`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("MAX(i)/2",
	//						expression.NewDiv(
	//							expression.NewUnresolvedFunction(
	//								"max", true, nil, expression.NewUnresolvedColumn("i"),
	//							),
	//							expression.NewLiteral(int8(2), types.Int8),
	//						),
	//					),
	//				},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT current_user FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("current_user",
	//						expression.NewUnresolvedFunction("current_user", false, nil),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT current_USER(    ) FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("current_USER(    )",
	//						expression.NewUnresolvedFunction("current_user", false, nil),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SHOW INDEXES FROM foo`,
	//			plan:  plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	//		},
	//		{
	//			input: `SHOW INDEX FROM foo`,
	//			plan:  plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	//		},
	//		{
	//			input: `SHOW KEYS FROM foo`,
	//			plan:  plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	//		},
	//		{
	//			input: `SHOW INDEXES IN foo`,
	//			plan:  plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	//		},
	//		{
	//			input: `SHOW INDEX IN foo`,
	//			plan:  plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	//		},
	//		{
	//			input: `SHOW KEYS IN foo`,
	//			plan:  plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	//		},
	//		{
	//			input: `SHOW FULL PROCESSLIST`,
	//			plan:  plan.NewShowProcessList(),
	//		},
	//		{
	//			input: `SHOW PROCESSLIST`,
	//			plan:  plan.NewShowProcessList(),
	//		},
	//		{
	//			input: `SELECT @@allowed_max_packet`,
	//			plan: plan.NewProject([]sql.Expression{
	//				expression.NewUnresolvedColumn("@@allowed_max_packet"),
	//			}, plan.NewResolvedDualTable()),
	//		},
	//		{
	//			input: `SET autocommit=1, foo="bar", baz=ON, qux=bareword`,
	//			plan: plan.NewSet(
	//				[]sql.Expression{
	//					expression.NewSetField(expression.NewUnresolvedColumn("autocommit"), expression.NewLiteral(int8(1), types.Int8)),
	//					expression.NewSetField(expression.NewUnresolvedColumn("foo"), expression.NewLiteral("bar", types.LongText)),
	//					expression.NewSetField(expression.NewUnresolvedColumn("baz"), expression.NewLiteral("ON", types.LongText)),
	//					expression.NewSetField(expression.NewUnresolvedColumn("qux"), expression.NewUnresolvedColumn("bareword")),
	//				},
	//			),
	//		},
	//		{
	//			input: `SET @@session.autocommit=1, foo="true"`,
	//			plan: plan.NewSet(
	//				[]sql.Expression{
	//					expression.NewSetField(expression.NewSystemVar("autocommit", sql.SystemVariableScope_Session), expression.NewLiteral(int8(1), types.Int8)),
	//					expression.NewSetField(expression.NewUnresolvedColumn("foo"), expression.NewLiteral("true", types.LongText)),
	//				},
	//			),
	//		},
	//		{
	//			input: `SET SESSION NET_READ_TIMEOUT= 700, SESSION NET_WRITE_TIMEOUT= 700`,
	//			plan: plan.NewSet(
	//				[]sql.Expression{
	//					expression.NewSetField(expression.NewSystemVar("NET_READ_TIMEOUT", sql.SystemVariableScope_Session), expression.NewLiteral(int16(700), types.Int16)),
	//					expression.NewSetField(expression.NewSystemVar("NET_WRITE_TIMEOUT", sql.SystemVariableScope_Session), expression.NewLiteral(int16(700), types.Int16)),
	//				},
	//			),
	//		},
	//		{
	//			input: `SET gtid_mode=DEFAULT`,
	//			plan: plan.NewSet(
	//				[]sql.Expression{
	//					expression.NewSetField(expression.NewUnresolvedColumn("gtid_mode"), expression.NewDefaultColumn("")),
	//				},
	//			),
	//		},
	//		{
	//			input: `SET @@sql_select_limit=default`,
	//			plan: plan.NewSet(
	//				[]sql.Expression{
	//					expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.SystemVariableScope_Session), expression.NewDefaultColumn("")),
	//				},
	//			),
	//		},
	//		{
	//			input: "",
	//			plan:  plan.NothingImpl,
	//		},
	//		{
	//			input: "/* just a comment */",
	//			plan:  plan.NothingImpl,
	//		},
	//		{
	//			input: `/*!40101 SET NAMES utf8 */`,
	//			plan: plan.NewSet(
	//				[]sql.Expression{
	//					expression.NewSetField(expression.NewUnresolvedColumn("character_set_client"), expression.NewLiteral("utf8", types.LongText)),
	//					expression.NewSetField(expression.NewUnresolvedColumn("character_set_connection"), expression.NewLiteral("utf8", types.LongText)),
	//					expression.NewSetField(expression.NewUnresolvedColumn("character_set_results"), expression.NewLiteral("utf8", types.LongText)),
	//				},
	//			),
	//		},
	//		{
	//			input: `SELECT /* a comment */ * FROM foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewUnresolvedTable("foo", "").WithComment("/* a comment */"),
	//			),
	//		},
	//		{
	//			input: `SELECT /*!40101 * from */ foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//			// TODO: other optimizer hints than join_order are ignored for now
	//		},
	//		{
	//			input: `SELECT /*+ JOIN_ORDER(a,b) */ * from foo`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewUnresolvedTable("foo", "").WithComment("/*+ JOIN_ORDER(a,b) */"),
	//			),
	//		},
	//		{
	//			input: `SELECT /*+ JOIN_ORDER(a,b) */ * FROM b join a on c = d limit 5`,
	//			plan: plan.NewLimit(expression.NewLiteral(int8(5), types.Int8),
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewStar(),
	//					},
	//					plan.NewInnerJoin(
	//						plan.NewUnresolvedTable("b", ""),
	//						plan.NewUnresolvedTable("a", ""),
	//						expression.NewEquals(
	//							expression.NewUnresolvedColumn("c"),
	//							expression.NewUnresolvedColumn("d"),
	//						),
	//					).WithComment("/*+ JOIN_ORDER(a,b) */"),
	//				),
	//			),
	//		},
	//		{
	//			input: `SHOW DATABASES`,
	//			plan:  plan.NewShowDatabases(),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE i LIKE 'foo'`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewLike(
	//						expression.NewUnresolvedColumn("i"),
	//						expression.NewLiteral("foo", types.LongText),
	//						nil,
	//					),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo WHERE i NOT LIKE 'foo'`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewFilter(
	//					expression.NewNot(expression.NewLike(
	//						expression.NewUnresolvedColumn("i"),
	//						expression.NewLiteral("foo", types.LongText),
	//						nil,
	//					)),
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SHOW FIELDS FROM foo`,
	//			plan:  plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	//		},
	//		{
	//			input: `SHOW FULL COLUMNS FROM foo`,
	//			plan:  plan.NewShowColumns(true, plan.NewUnresolvedTable("foo", "")),
	//		},
	//		{
	//			input: `SHOW FIELDS FROM foo WHERE Field = 'bar'`,
	//			plan: plan.NewFilter(
	//				expression.NewEquals(
	//					expression.NewUnresolvedColumn("Field"),
	//					expression.NewLiteral("bar", types.LongText),
	//				),
	//				plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	//			),
	//		},
	//		{
	//			input: `SHOW FIELDS FROM foo LIKE 'bar'`,
	//			plan: plan.NewFilter(
	//				expression.NewLike(
	//					expression.NewUnresolvedColumn("Field"),
	//					expression.NewLiteral("bar", types.LongText),
	//					nil,
	//				),
	//				plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	//			),
	//		},
	//		{
	//			input: `SHOW TABLE STATUS LIKE 'foo'`,
	//			plan: plan.NewFilter(
	//				expression.NewLike(
	//					expression.NewUnresolvedColumn("Name"),
	//					expression.NewLiteral("foo", types.LongText),
	//					nil,
	//				),
	//				plan.NewShowTableStatus(sql.UnresolvedDatabase("")),
	//			),
	//		},
	//		{
	//			input: `SHOW TABLE STATUS FROM foo`,
	//			plan:  plan.NewShowTableStatus(sql.UnresolvedDatabase("foo")),
	//		},
	//		{
	//			input: `SHOW TABLE STATUS IN foo`,
	//			plan:  plan.NewShowTableStatus(sql.UnresolvedDatabase("foo")),
	//		},
	//		{
	//			input: `SHOW TABLE STATUS`,
	//			plan:  plan.NewShowTableStatus(sql.UnresolvedDatabase("")),
	//		},
	//		{
	//			input: `SHOW TABLE STATUS WHERE Name = 'foo'`,
	//			plan: plan.NewFilter(
	//				expression.NewEquals(
	//					expression.NewUnresolvedColumn("Name"),
	//					expression.NewLiteral("foo", types.LongText),
	//				),
	//				plan.NewShowTableStatus(sql.UnresolvedDatabase("")),
	//			),
	//		},
	//		{
	//			input: `USE foo`,
	//			plan:  plan.NewUse(sql.UnresolvedDatabase("foo")),
	//		},
	//		{
	//			input: `DESCRIBE foo.bar`,
	//			plan: plan.NewShowColumns(false,
	//				plan.NewUnresolvedTable("bar", "foo"),
	//			),
	//		},
	//		{
	//			input: `DESC foo.bar`,
	//			plan: plan.NewShowColumns(false,
	//				plan.NewUnresolvedTable("bar", "foo"),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo.bar`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewStar(),
	//				},
	//				plan.NewUnresolvedTable("bar", "foo"),
	//			),
	//		},
	//		{
	//			input: `SHOW VARIABLES`,
	//			plan:  plan.NewShowVariables(expression.NewLiteral(true, types.Boolean), false),
	//		},
	//		{
	//			input: `SHOW GLOBAL VARIABLES`,
	//			plan:  plan.NewShowVariables(expression.NewLiteral(true, types.Boolean), true),
	//		},
	//		{
	//			input: `SHOW SESSION VARIABLES`,
	//			plan:  plan.NewShowVariables(expression.NewLiteral(true, types.Boolean), false),
	//		},
	//		{
	//			input: `SHOW VARIABLES LIKE 'gtid_mode'`,
	//			plan: plan.NewShowVariables(expression.NewLike(
	//				expression.NewGetField(0, types.LongText, "variable_name", false),
	//				expression.NewLiteral("gtid_mode", types.LongText),
	//				nil,
	//			), false),
	//		},
	//		{
	//			input: `SHOW SESSION VARIABLES LIKE 'autocommit'`,
	//			plan: plan.NewShowVariables(expression.NewLike(
	//				expression.NewGetField(0, types.LongText, "variable_name", false),
	//				expression.NewLiteral("autocommit", types.LongText),
	//				nil,
	//			), false),
	//		},
	//		{
	//			input: `UNLOCK TABLES`,
	//			plan:  plan.NewUnlockTables(),
	//		},
	//		{
	//			input: `LOCK TABLES foo READ`,
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewUnresolvedTable("foo", "")},
	//			}),
	//		},
	//		{
	//			input: `LOCK TABLES foo123 READ`,
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewUnresolvedTable("foo123", "")},
	//			}),
	//		},
	//		{
	//			input: `LOCK TABLES foo AS f READ`,
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewTableAlias("f", plan.NewUnresolvedTable("foo", ""))},
	//			}),
	//		},
	//		{
	//			input: `LOCK TABLES foo READ LOCAL`,
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewUnresolvedTable("foo", "")},
	//			}),
	//		},
	//		{
	//			input: `LOCK TABLES foo WRITE`,
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
	//			}),
	//		},
	//		{
	//			input: `LOCK TABLES foo LOW_PRIORITY WRITE`,
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
	//			}),
	//		},
	//		{
	//			input: `LOCK TABLES foo WRITE, bar READ`,
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
	//				{Table: plan.NewUnresolvedTable("bar", "")},
	//			}),
	//		},
	//		{
	//			input: "LOCK TABLES `foo` WRITE, `bar` READ",
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
	//				{Table: plan.NewUnresolvedTable("bar", "")},
	//			}),
	//		},
	//		{
	//			input: `LOCK TABLES foo READ, bar WRITE, baz READ`,
	//			plan: plan.NewLockTables([]*plan.TableLock{
	//				{Table: plan.NewUnresolvedTable("foo", "")},
	//				{Table: plan.NewUnresolvedTable("bar", ""), Write: true},
	//				{Table: plan.NewUnresolvedTable("baz", "")},
	//			}),
	//		},
	//		{
	//			input: `SHOW CREATE DATABASE foo`,
	//			plan:  plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	//		},
	//		{
	//			input: `SHOW CREATE SCHEMA foo`,
	//			plan:  plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	//		},
	//		{
	//			input: `SHOW CREATE DATABASE IF NOT EXISTS foo`,
	//			plan:  plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	//		},
	//		{
	//			input: `SHOW CREATE SCHEMA IF NOT EXISTS foo`,
	//			plan:  plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	//		},
	//		{
	//			input: `SHOW WARNINGS`,
	//			plan:  plan.ShowWarnings(sql.NewEmptyContext().Warnings()),
	//		},
	//		{
	//			input: `SHOW WARNINGS LIMIT 10`,
	//			plan:  plan.NewLimit(expression.NewLiteral(int8(10), types.Int8), plan.ShowWarnings(sql.NewEmptyContext().Warnings())),
	//		},
	//		{
	//			input: `SHOW WARNINGS LIMIT 5,10`,
	//			plan:  plan.NewLimit(expression.NewLiteral(int8(10), types.Int8), plan.NewOffset(expression.NewLiteral(int8(5), types.Int8), plan.ShowWarnings(sql.NewEmptyContext().Warnings()))),
	//		},
	//		{
	//			input: "SHOW CREATE DATABASE `foo`",
	//			plan:  plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	//		},
	//		{
	//			input: "SHOW CREATE SCHEMA `foo`",
	//			plan:  plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	//		},
	//		{
	//			input: "SHOW CREATE DATABASE IF NOT EXISTS `foo`",
	//			plan:  plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	//		},
	//		{
	//			input: "SHOW CREATE SCHEMA IF NOT EXISTS `foo`",
	//			plan:  plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	//		},
	//		{
	//			input: "SELECT CASE foo WHEN 1 THEN 'foo' WHEN 2 THEN 'bar' ELSE 'baz' END",
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("CASE foo WHEN 1 THEN 'foo' WHEN 2 THEN 'bar' ELSE 'baz' END",
	//						expression.NewCase(
	//							expression.NewUnresolvedColumn("foo"),
	//							[]expression.CaseBranch{
	//								{
	//									Cond:  expression.NewLiteral(int8(1), types.Int8),
	//									Value: expression.NewLiteral("foo", types.LongText),
	//								},
	//								{
	//									Cond:  expression.NewLiteral(int8(2), types.Int8),
	//									Value: expression.NewLiteral("bar", types.LongText),
	//								},
	//							},
	//							expression.NewLiteral("baz", types.LongText),
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: "SELECT CASE foo WHEN 1 THEN 'foo' WHEN 2 THEN 'bar' END",
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("CASE foo WHEN 1 THEN 'foo' WHEN 2 THEN 'bar' END",
	//						expression.NewCase(
	//							expression.NewUnresolvedColumn("foo"),
	//							[]expression.CaseBranch{
	//								{
	//									Cond:  expression.NewLiteral(int8(1), types.Int8),
	//									Value: expression.NewLiteral("foo", types.LongText),
	//								},
	//								{
	//									Cond:  expression.NewLiteral(int8(2), types.Int8),
	//									Value: expression.NewLiteral("bar", types.LongText),
	//								},
	//							},
	//							nil,
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: "SELECT CASE WHEN foo = 1 THEN 'foo' WHEN foo = 2 THEN 'bar' ELSE 'baz' END",
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("CASE WHEN foo = 1 THEN 'foo' WHEN foo = 2 THEN 'bar' ELSE 'baz' END",
	//						expression.NewCase(
	//							nil,
	//							[]expression.CaseBranch{
	//								{
	//									Cond: expression.NewEquals(
	//										expression.NewUnresolvedColumn("foo"),
	//										expression.NewLiteral(int8(1), types.Int8),
	//									),
	//									Value: expression.NewLiteral("foo", types.LongText),
	//								},
	//								{
	//									Cond: expression.NewEquals(
	//										expression.NewUnresolvedColumn("foo"),
	//										expression.NewLiteral(int8(2), types.Int8),
	//									),
	//									Value: expression.NewLiteral("bar", types.LongText),
	//								},
	//							},
	//							expression.NewLiteral("baz", types.LongText),
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: "SHOW COLLATION",
	//			plan:  showCollationProjection,
	//		},
	//		{
	//			input: "SHOW COLLATION LIKE 'foo'",
	//			plan: plan.NewHaving(
	//				expression.NewLike(
	//					expression.NewUnresolvedColumn("collation"),
	//					expression.NewLiteral("foo", types.LongText),
	//					nil,
	//				),
	//				showCollationProjection,
	//			),
	//		},
	//		{
	//			input: "SHOW COLLATION WHERE Charset = 'foo'",
	//			plan: plan.NewHaving(
	//				expression.NewEquals(
	//					expression.NewUnresolvedColumn("Charset"),
	//					expression.NewLiteral("foo", types.LongText),
	//				),
	//				showCollationProjection,
	//			),
	//		},
	//		{
	//			input: "BEGIN",
	//			plan:  plan.NewStartTransaction(sql.ReadWrite),
	//		},
	//		{
	//			input: "START TRANSACTION",
	//			plan:  plan.NewStartTransaction(sql.ReadWrite),
	//		},
	//		{
	//			input: "COMMIT",
	//			plan:  plan.NewCommit(),
	//		},
	//		{
	//			input: `ROLLBACK`,
	//			plan:  plan.NewRollback(),
	//		},
	//		{
	//			input: "SAVEPOINT abc",
	//			plan:  plan.NewCreateSavepoint("abc"),
	//		},
	//		{
	//			input: "ROLLBACK TO SAVEPOINT abc",
	//			plan:  plan.NewRollbackSavepoint("abc"),
	//		},
	//		{
	//			input: "RELEASE SAVEPOINT abc",
	//			plan:  plan.NewReleaseSavepoint("abc"),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE `mytable`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", ""), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE mytable",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", ""), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE mydb.`mytable`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE `mydb`.mytable",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE `mydb`.`mytable`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE `my.table`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("my.table", ""), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE `my.db`.`my.table`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("my.table", "my.db"), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE `my``table`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("my`table", ""), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE `my``db`.`my``table`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("my`table", "my`db"), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE ````",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("`", ""), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE `.`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable(".", ""), false),
	//		},
	//		{
	//			input: "SHOW CREATE TABLE mytable as of 'version'",
	//			plan: plan.NewShowCreateTableWithAsOf(
	//				plan.NewUnresolvedTableAsOf("mytable", "", expression.NewLiteral("version", types.LongText)),
	//				false, expression.NewLiteral("version", types.LongText)),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW `mytable`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", ""), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW mytable",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", ""), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW mydb.`mytable`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW `mydb`.mytable",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW `mydb`.`mytable`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW `my.table`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("my.table", ""), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW `my.db`.`my.table`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("my.table", "my.db"), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW `my``table`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("my`table", ""), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW `my``db`.`my``table`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("my`table", "my`db"), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW ````",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable("`", ""), true),
	//		},
	//		{
	//			input: "SHOW CREATE VIEW `.`",
	//			plan:  plan.NewShowCreateTable(plan.NewUnresolvedTable(".", ""), true),
	//		},
	//		{
	//			input: `SELECT '2018-05-01' + INTERVAL 1 DAY`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("'2018-05-01' + INTERVAL 1 DAY",
	//						expression.NewArithmetic(
	//							expression.NewLiteral("2018-05-01", types.LongText),
	//							expression.NewInterval(
	//								expression.NewLiteral(int8(1), types.Int8),
	//								"DAY",
	//							),
	//							"+",
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT '2018-05-01' - INTERVAL 1 DAY`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("'2018-05-01' - INTERVAL 1 DAY",
	//						expression.NewArithmetic(
	//							expression.NewLiteral("2018-05-01", types.LongText),
	//							expression.NewInterval(
	//								expression.NewLiteral(int8(1), types.Int8),
	//								"DAY",
	//							),
	//							"-",
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT INTERVAL 1 DAY + '2018-05-01'`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("INTERVAL 1 DAY + '2018-05-01'",
	//						expression.NewArithmetic(
	//							expression.NewInterval(
	//								expression.NewLiteral(int8(1), types.Int8),
	//								"DAY",
	//							),
	//							expression.NewLiteral("2018-05-01", types.LongText),
	//							"+",
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT '2018-05-01' + INTERVAL 1 DAY + INTERVAL 1 DAY`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewAlias("'2018-05-01' + INTERVAL 1 DAY + INTERVAL 1 DAY",
	//						expression.NewArithmetic(
	//							expression.NewArithmetic(
	//								expression.NewLiteral("2018-05-01", types.LongText),
	//								expression.NewInterval(
	//									expression.NewLiteral(int8(1), types.Int8),
	//									"DAY",
	//								),
	//								"+",
	//							),
	//							expression.NewInterval(
	//								expression.NewLiteral(int8(1), types.Int8),
	//								"DAY",
	//							),
	//							"+",
	//						),
	//					),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `SELECT bar, AVG(baz) FROM foo GROUP BY bar HAVING COUNT(*) > 5`,
	//			plan: plan.NewHaving(
	//				expression.NewGreaterThan(
	//					expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
	//					expression.NewLiteral(int8(5), types.Int8),
	//				),
	//				plan.NewGroupBy(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("bar"),
	//						expression.NewAlias("AVG(baz)",
	//							expression.NewUnresolvedFunction("avg", true, nil, expression.NewUnresolvedColumn("baz")),
	//						),
	//					},
	//					[]sql.Expression{expression.NewUnresolvedColumn("bar")},
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT foo FROM t GROUP BY foo HAVING i > 5`,
	//			plan: plan.NewHaving(
	//				expression.NewGreaterThan(
	//					expression.NewUnresolvedColumn("i"),
	//					expression.NewLiteral(int8(5), types.Int8),
	//				),
	//				plan.NewGroupBy(
	//					[]sql.Expression{expression.NewUnresolvedColumn("foo")},
	//					[]sql.Expression{expression.NewUnresolvedColumn("foo")},
	//					plan.NewUnresolvedTable("t", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT COUNT(*) FROM foo GROUP BY a HAVING COUNT(*) > 5`,
	//			plan: plan.NewHaving(
	//				expression.NewGreaterThan(
	//					expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
	//					expression.NewLiteral(int8(5), types.Int8),
	//				),
	//				plan.NewGroupBy(
	//					[]sql.Expression{
	//						expression.NewAlias("COUNT(*)",
	//							expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
	//						),
	//					},
	//					[]sql.Expression{expression.NewUnresolvedColumn("a")},
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT DISTINCT COUNT(*) FROM foo GROUP BY a HAVING COUNT(*) > 5`,
	//			plan: plan.NewDistinct(
	//				plan.NewHaving(
	//					expression.NewGreaterThan(
	//						expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
	//						expression.NewLiteral(int8(5), types.Int8),
	//					),
	//					plan.NewGroupBy(
	//						[]sql.Expression{
	//							expression.NewAlias("COUNT(*)",
	//								expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
	//							),
	//						},
	//						[]sql.Expression{expression.NewUnresolvedColumn("a")},
	//						plan.NewUnresolvedTable("foo", ""),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo LEFT JOIN bar ON 1=1`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewLeftOuterJoin(
	//					plan.NewUnresolvedTable("foo", ""),
	//					plan.NewUnresolvedTable("bar", ""),
	//					expression.NewEquals(
	//						expression.NewLiteral(int8(1), types.Int8),
	//						expression.NewLiteral(int8(1), types.Int8),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo LEFT OUTER JOIN bar ON 1=1`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewLeftOuterJoin(
	//					plan.NewUnresolvedTable("foo", ""),
	//					plan.NewUnresolvedTable("bar", ""),
	//					expression.NewEquals(
	//						expression.NewLiteral(int8(1), types.Int8),
	//						expression.NewLiteral(int8(1), types.Int8),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo RIGHT JOIN bar ON 1=1`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewRightOuterJoin(
	//					plan.NewUnresolvedTable("foo", ""),
	//					plan.NewUnresolvedTable("bar", ""),
	//					expression.NewEquals(
	//						expression.NewLiteral(int8(1), types.Int8),
	//						expression.NewLiteral(int8(1), types.Int8),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT * FROM foo RIGHT OUTER JOIN bar ON 1=1`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{expression.NewStar()},
	//				plan.NewRightOuterJoin(
	//					plan.NewUnresolvedTable("foo", ""),
	//					plan.NewUnresolvedTable("bar", ""),
	//					expression.NewEquals(
	//						expression.NewLiteral(int8(1), types.Int8),
	//						expression.NewLiteral(int8(1), types.Int8),
	//					),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT FIRST(i) FROM foo`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("FIRST(i)",
	//						expression.NewUnresolvedFunction("first", true, nil, expression.NewUnresolvedColumn("i")),
	//					),
	//				},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT LAST(i) FROM foo`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("LAST(i)",
	//						expression.NewUnresolvedFunction("last", true, nil, expression.NewUnresolvedColumn("i")),
	//					),
	//				},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT COUNT(DISTINCT i) FROM foo`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("COUNT(DISTINCT i)",
	//						aggregation.NewCountDistinct(expression.NewUnresolvedColumn("i"))),
	//				},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT AVG(DISTINCT a) FROM foo`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("AVG(DISTINCT a)",
	//						expression.NewUnresolvedFunction("avg", true, nil, expression.NewDistinctExpression(expression.NewUnresolvedColumn("a")))),
	//				},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT SUM(DISTINCT a*b) FROM foo`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("SUM(DISTINCT a*b)",
	//						expression.NewUnresolvedFunction("sum", true, nil,
	//							expression.NewDistinctExpression(
	//								expression.NewMult(expression.NewUnresolvedColumn("a"),
	//									expression.NewUnresolvedColumn("b")))))},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT AVG(DISTINCT a / b) FROM foo`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("AVG(DISTINCT a / b)",
	//						expression.NewUnresolvedFunction("avg", true, nil,
	//							expression.NewDistinctExpression(
	//								expression.NewDiv(expression.NewUnresolvedColumn("a"),
	//									expression.NewUnresolvedColumn("b")))))},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT SUM(DISTINCT POWER(a, 2)) FROM foo`,
	//			plan: plan.NewGroupBy(
	//				[]sql.Expression{
	//					expression.NewAlias("SUM(DISTINCT POWER(a, 2))",
	//						expression.NewUnresolvedFunction("sum", true, nil,
	//							expression.NewDistinctExpression(
	//								expression.NewUnresolvedFunction("power", false, nil,
	//									expression.NewUnresolvedColumn("a"), expression.NewLiteral(int8(2), types.Int8)))))},
	//				[]sql.Expression{},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a, row_number() over (partition by s order by x) FROM foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//					expression.NewAlias("row_number() over (partition by s order by x)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("s"),
	//						}, sql.SortFields{
	//							{
	//								Column:       expression.NewUnresolvedColumn("x"),
	//								Column2:      expression.NewUnresolvedColumn("x"),
	//								Order:        sql.Ascending,
	//								NullOrdering: sql.NullsFirst,
	//							},
	//						}, nil, "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a, count(i) over () FROM foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//					expression.NewAlias("count(i) over ()",
	//						expression.NewUnresolvedFunction("count", true,
	//							sql.NewWindowDefinition([]sql.Expression{}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", ""),
	//							expression.NewUnresolvedColumn("i")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a, row_number() over (order by x), row_number() over (partition by y) FROM foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//					expression.NewAlias("row_number() over (order by x)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, sql.SortFields{
	//							{
	//								Column:       expression.NewUnresolvedColumn("x"),
	//								Column2:      expression.NewUnresolvedColumn("x"),
	//								Order:        sql.Ascending,
	//								NullOrdering: sql.NullsFirst,
	//							},
	//						}, nil, "", "")),
	//					),
	//					expression.NewAlias("row_number() over (partition by y)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("y"),
	//						}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a, row_number() over (order by x), max(b) over () FROM foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//					expression.NewAlias("row_number() over (order by x)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, sql.SortFields{
	//							{
	//								Column:       expression.NewUnresolvedColumn("x"),
	//								Column2:      expression.NewUnresolvedColumn("x"),
	//								Order:        sql.Ascending,
	//								NullOrdering: sql.NullsFirst,
	//							},
	//						}, nil, "", "")),
	//					),
	//					expression.NewAlias("max(b) over ()",
	//						expression.NewUnresolvedFunction("max", true, sql.NewWindowDefinition([]sql.Expression{}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", ""),
	//							expression.NewUnresolvedColumn("b"),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a, row_number() over (partition by b), max(b) over (partition by b) FROM foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//					expression.NewAlias("row_number() over (partition by b)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("b"),
	//						}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", "")),
	//					),
	//					expression.NewAlias("max(b) over (partition by b)",
	//						expression.NewUnresolvedFunction("max", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("b"),
	//						}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", ""),
	//							expression.NewUnresolvedColumn("b"),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a, row_number() over (partition by c), max(b) over (partition by b) FROM foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//					expression.NewAlias("row_number() over (partition by c)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("c"),
	//						}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", "")),
	//					),
	//					expression.NewAlias("max(b) over (partition by b)",
	//						expression.NewUnresolvedFunction("max", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("b"),
	//						}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", ""),
	//							expression.NewUnresolvedColumn("b"),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a, count(i) over (order by x) FROM foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//					expression.NewAlias("count(i) over (order by x)",
	//						expression.NewUnresolvedFunction("count", true, sql.NewWindowDefinition([]sql.Expression{}, sql.SortFields{
	//							{
	//								Column:       expression.NewUnresolvedColumn("x"),
	//								Column2:      expression.NewUnresolvedColumn("x"),
	//								Order:        sql.Ascending,
	//								NullOrdering: sql.NullsFirst,
	//							},
	//						}, nil, "", ""),
	//							expression.NewUnresolvedColumn("i"),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT a, count(i) over (partition by y) FROM foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("a"),
	//					expression.NewAlias("count(i) over (partition by y)",
	//						expression.NewUnresolvedFunction("count", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("y"),
	//						}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", ""),
	//							expression.NewUnresolvedColumn("i"),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT i, row_number() over (order by a), max(b) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewUnresolvedColumn("i"),
	//					expression.NewAlias("row_number() over (order by a)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, sql.SortFields{
	//							{
	//								Column:       expression.NewUnresolvedColumn("a"),
	//								Column2:      expression.NewUnresolvedColumn("a"),
	//								Order:        sql.Ascending,
	//								NullOrdering: sql.NullsFirst,
	//							},
	//						}, nil, "", "")),
	//					),
	//					expression.NewAlias("max(b)",
	//						expression.NewUnresolvedFunction("max", true, nil,
	//							expression.NewUnresolvedColumn("b"),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x ROWS UNBOUNDED PRECEDING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x ROWS UNBOUNDED PRECEDING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRowsUnboundedPrecedingToCurrentRowFrame(), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRowsNPrecedingToNFollowingFrame(
	//							expression.NewLiteral(int8(1), types.Int8),
	//							expression.NewLiteral(int8(1), types.Int8),
	//						), "", ""),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRowsNFollowingToNFollowingFrame(
	//							expression.NewLiteral(int8(1), types.Int8),
	//							expression.NewLiteral(int8(2), types.Int8),
	//						), "", ""),
	//						),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x ROWS BETWEEN CURRENT ROW AND CURRENT ROW) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x ROWS BETWEEN CURRENT ROW AND CURRENT ROW)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRowsCurrentRowToCurrentRowFrame(), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRowsCurrentRowToNFollowingFrame(
	//							expression.NewLiteral(int8(1), types.Int8),
	//						), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE CURRENT ROW) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE CURRENT ROW)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeCurrentRowToCurrentRowFrame(), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE 2 PRECEDING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE 2 PRECEDING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeNPrecedingToCurrentRowFrame(
	//							expression.NewLiteral(int8(2), types.Int8),
	//						), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE UNBOUNDED PRECEDING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE UNBOUNDED PRECEDING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeUnboundedPrecedingToCurrentRowFrame(), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE interval 5 DAY PRECEDING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE interval 5 DAY PRECEDING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeNPrecedingToCurrentRowFrame(
	//							expression.NewInterval(
	//								expression.NewLiteral(int8(5), types.Int8),
	//								"DAY",
	//							),
	//						), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE interval '2:30' MINUTE_SECOND PRECEDING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE interval '2:30' MINUTE_SECOND PRECEDING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeNPrecedingToCurrentRowFrame(
	//							expression.NewInterval(
	//								expression.NewLiteral("2:30", types.LongText),
	//								"MINUTE_SECOND",
	//							),
	//						), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeNPrecedingToNFollowingFrame(
	//							expression.NewLiteral(int8(1), types.Int8),
	//							expression.NewLiteral(int8(1), types.Int8),
	//						), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE BETWEEN CURRENT ROW AND CURRENT ROW) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE BETWEEN CURRENT ROW AND CURRENT ROW)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeCurrentRowToCurrentRowFrame(), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeCurrentRowToNFollowingFrame(
	//							expression.NewLiteral(int8(1), types.Int8),
	//						), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE BETWEEN interval 5 DAY PRECEDING AND CURRENT ROW) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE BETWEEN interval 5 DAY PRECEDING AND CURRENT ROW)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeNPrecedingToCurrentRowFrame(
	//							expression.NewInterval(
	//								expression.NewLiteral(int8(5), types.Int8),
	//								"DAY",
	//							),
	//						), "", ""),
	//						)),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (partition by x RANGE BETWEEN interval '2:30' MINUTE_SECOND PRECEDING AND CURRENT ROW) from foo`,
	//			plan: plan.NewWindow(
	//				[]sql.Expression{
	//					expression.NewAlias("row_number() over (partition by x RANGE BETWEEN interval '2:30' MINUTE_SECOND PRECEDING AND CURRENT ROW)",
	//						expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{
	//							expression.NewUnresolvedColumn("x"),
	//						}, nil, plan.NewRangeNPrecedingToCurrentRowFrame(
	//							expression.NewInterval(
	//								expression.NewLiteral("2:30", types.LongText),
	//								"MINUTE_SECOND",
	//							),
	//						), "", "")),
	//					),
	//				},
	//				plan.NewUnresolvedTable("foo", ""),
	//			),
	//		},
	//		{
	//			input: `SELECT row_number() over (w) from foo WINDOW w as (partition by x RANGE BETWEEN interval '2:30' MINUTE_SECOND PRECEDING AND CURRENT ROW)`,
	//			plan: plan.NewNamedWindows(
	//				map[string]*sql.WindowDefinition{
	//					"w": sql.NewWindowDefinition([]sql.Expression{
	//						expression.NewUnresolvedColumn("x"),
	//					}, nil, plan.NewRangeNPrecedingToCurrentRowFrame(
	//						expression.NewInterval(
	//							expression.NewLiteral("2:30", types.LongText),
	//							"MINUTE_SECOND",
	//						),
	//					), "", "w"),
	//				},
	//				plan.NewWindow(
	//					[]sql.Expression{
	//						expression.NewAlias("row_number() over (w)",
	//							expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "w", "")),
	//						),
	//					},
	//					plan.NewUnresolvedTable("foo", ""),
	//				)),
	//		},
	//		{
	//			input: `SELECT a, row_number() over (w1), max(b) over (w2) FROM foo WINDOW w1 as (w2 order by x), w2 as ()`,
	//			plan: plan.NewNamedWindows(
	//				map[string]*sql.WindowDefinition{
	//					"w1": sql.NewWindowDefinition([]sql.Expression{}, sql.SortFields{
	//						{
	//							Column:       expression.NewUnresolvedColumn("x"),
	//							Column2:      expression.NewUnresolvedColumn("x"),
	//							Order:        sql.Ascending,
	//							NullOrdering: sql.NullsFirst,
	//						},
	//					}, nil, "w2", "w1"),
	//					"w2": sql.NewWindowDefinition([]sql.Expression{}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", "w2"),
	//				},
	//				plan.NewWindow(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewAlias("row_number() over (w1)",
	//							expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition([]sql.Expression{}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "w1", "")),
	//						),
	//						expression.NewAlias("max(b) over (w2)",
	//							expression.NewUnresolvedFunction("max", true, sql.NewWindowDefinition([]sql.Expression{}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "w2", ""),
	//								expression.NewUnresolvedColumn("b"),
	//							),
	//						),
	//					},
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `SELECT a, row_number() over (w1 partition by y), max(b) over (w2) FROM foo WINDOW w1 as (w2 order by x), w2 as ()`,
	//			plan: plan.NewNamedWindows(
	//				map[string]*sql.WindowDefinition{
	//					"w1": sql.NewWindowDefinition([]sql.Expression{}, sql.SortFields{
	//						{
	//							Column:       expression.NewUnresolvedColumn("x"),
	//							Column2:      expression.NewUnresolvedColumn("x"),
	//							Order:        sql.Ascending,
	//							NullOrdering: sql.NullsFirst,
	//						},
	//					}, nil, "w2", "w1"),
	//					"w2": sql.NewWindowDefinition([]sql.Expression{}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "", "w2"),
	//				}, plan.NewWindow(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("a"),
	//						expression.NewAlias("row_number() over (w1 partition by y)",
	//							expression.NewUnresolvedFunction("row_number", true, sql.NewWindowDefinition(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("y"),
	//								},
	//								nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "w1", "")),
	//						),
	//						expression.NewAlias("max(b) over (w2)",
	//							expression.NewUnresolvedFunction("max", true, sql.NewWindowDefinition([]sql.Expression{}, nil, plan.NewRowsUnboundedPrecedingToUnboundedFollowingFrame(), "w2", ""),
	//								expression.NewUnresolvedColumn("b"),
	//							),
	//						),
	//					},
	//					plan.NewUnresolvedTable("foo", ""),
	//				),
	//			),
	//		},
	//		{
	//			input: `with cte1 as (select a from b) select * from cte1`,
	//			plan: plan.NewWith(
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewStar(),
	//					},
	//					plan.NewUnresolvedTable("cte1", "")),
	//				[]*plan.CommonTableExpression{
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte1", "select a from b",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("a"),
	//								},
	//								plan.NewUnresolvedTable("b", ""),
	//							),
	//						),
	//						[]string{},
	//					),
	//				},
	//				false,
	//			),
	//		},
	//		{
	//			input: `with cte1 as (select a from b) update c set d = e where f in (select * from cte1)`,
	//			plan: plan.NewWith(
	//				plan.NewUpdate(
	//					plan.NewFilter(
	//						plan.NewInSubquery(
	//							expression.NewUnresolvedColumn("f"),
	//							plan.NewSubquery(plan.NewProject(
	//								[]sql.Expression{expression.NewStar()},
	//								plan.NewUnresolvedTable("cte1", ""),
	//							), "select * from cte1"),
	//						),
	//						plan.NewUnresolvedTable("c", ""),
	//					),
	//					false,
	//					[]sql.Expression{
	//						expression.NewSetField(expression.NewUnresolvedColumn("d"), expression.NewUnresolvedColumn("e")),
	//					},
	//				),
	//				[]*plan.CommonTableExpression{
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte1", "select a from b",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("a"),
	//								},
	//								plan.NewUnresolvedTable("b", ""),
	//							),
	//						),
	//						[]string{},
	//					),
	//				},
	//				false,
	//			),
	//		},
	//		{
	//			input: `with cte1 as (select a from b) delete from c where d in (select * from cte1)`,
	//			plan: plan.NewWith(
	//				plan.NewDeleteFrom(
	//					plan.NewFilter(
	//						plan.NewInSubquery(
	//							expression.NewUnresolvedColumn("d"),
	//							plan.NewSubquery(plan.NewProject(
	//								[]sql.Expression{expression.NewStar()},
	//								plan.NewUnresolvedTable("cte1", ""),
	//							), "select * from cte1"),
	//						),
	//						plan.NewUnresolvedTable("c", ""),
	//					), nil),
	//				[]*plan.CommonTableExpression{
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte1", "select a from b",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("a"),
	//								},
	//								plan.NewUnresolvedTable("b", ""),
	//							),
	//						),
	//						[]string{},
	//					),
	//				},
	//				false,
	//			),
	//		},
	//		{
	//			input: `with cte1 as (select a from b) insert into c (select * from cte1)`,
	//			plan: plan.NewWith(
	//				plan.NewInsertInto(
	//					sql.UnresolvedDatabase(""),
	//					plan.NewUnresolvedTable("c", ""),
	//					plan.NewProject(
	//						[]sql.Expression{expression.NewStar()},
	//						plan.NewUnresolvedTable("cte1", ""),
	//					),
	//					false, []string{}, []sql.Expression{}, false,
	//				),
	//				[]*plan.CommonTableExpression{
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte1", "select a from b",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("a"),
	//								},
	//								plan.NewUnresolvedTable("b", ""),
	//							),
	//						),
	//						[]string{},
	//					),
	//				},
	//				false,
	//			),
	//		},
	//		{
	//			input: `with recursive cte1 as (select 1 union select n+1 from cte1 where n < 10) select * from cte1`,
	//			plan: plan.NewWith(
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewStar(),
	//					},
	//					plan.NewUnresolvedTable("cte1", "")),
	//				[]*plan.CommonTableExpression{
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte1", "select 1 union select n + 1 from cte1 where n < 10",
	//							plan.NewSetOp(plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewLiteral(int8(1), types.Int8),
	//								},
	//								plan.NewResolvedDualTable(),
	//							), plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewArithmetic(
	//										expression.NewUnresolvedColumn("n"),
	//										expression.NewLiteral(int8(1), types.Int8),
	//										sqlparser.PlusStr,
	//									),
	//								},
	//								plan.NewFilter(
	//									expression.NewLessThan(
	//										expression.NewUnresolvedColumn("n"),
	//										expression.NewLiteral(int8(10), types.Int8),
	//									),
	//									plan.NewUnresolvedTable("cte1", ""),
	//								),
	//							), true, nil, nil, nil),
	//						),
	//						[]string{},
	//					),
	//				},
	//				true,
	//			),
	//		},
	//		{
	//			input: `with cte1 as (select a from b), cte2 as (select c from d) select * from cte1`,
	//			plan: plan.NewWith(
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewStar(),
	//					},
	//					plan.NewUnresolvedTable("cte1", "")),
	//				[]*plan.CommonTableExpression{
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte1", "select a from b",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("a"),
	//								},
	//								plan.NewUnresolvedTable("b", ""),
	//							),
	//						),
	//						[]string{},
	//					),
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte2", "select c from d",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("c"),
	//								},
	//								plan.NewUnresolvedTable("d", ""),
	//							),
	//						),
	//						[]string{},
	//					),
	//				},
	//				false,
	//			),
	//		},
	//		{
	//			input: `with cte1 (x) as (select a from b), cte2 (y,z) as (select c from d) select * from cte1`,
	//			plan: plan.NewWith(
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewStar(),
	//					},
	//					plan.NewUnresolvedTable("cte1", "")),
	//				[]*plan.CommonTableExpression{
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte1", "select a from b",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("a"),
	//								},
	//								plan.NewUnresolvedTable("b", ""),
	//							),
	//						),
	//						[]string{"x"},
	//					),
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte2", "select c from d",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("c"),
	//								},
	//								plan.NewUnresolvedTable("d", ""),
	//							),
	//						),
	//						[]string{"y", "z"},
	//					),
	//				},
	//				false,
	//			),
	//		},
	//		{
	//			input: `with cte1 as (select a from b) select c, (with cte2 as (select c from d) select e from cte2) from cte1`,
	//			plan: plan.NewWith(
	//				plan.NewProject(
	//					[]sql.Expression{
	//						expression.NewUnresolvedColumn("c"),
	//						expression.NewAlias("(with cte2 as (select c from d) select e from cte2)",
	//							plan.NewSubquery(
	//								plan.NewWith(
	//									plan.NewProject(
	//										[]sql.Expression{
	//											expression.NewUnresolvedColumn("e"),
	//										},
	//										plan.NewUnresolvedTable("cte2", "")),
	//									[]*plan.CommonTableExpression{
	//										plan.NewCommonTableExpression(
	//											plan.NewSubqueryAlias("cte2", "select c from d",
	//												plan.NewProject(
	//													[]sql.Expression{
	//														expression.NewUnresolvedColumn("c"),
	//													},
	//													plan.NewUnresolvedTable("d", ""),
	//												),
	//											),
	//											[]string{},
	//										),
	//									},
	//									false,
	//								),
	//								"with cte2 as (select c from d) select e from cte2",
	//							),
	//						),
	//					},
	//					plan.NewUnresolvedTable("cte1", ""),
	//				),
	//				[]*plan.CommonTableExpression{
	//					plan.NewCommonTableExpression(
	//						plan.NewSubqueryAlias("cte1", "select a from b",
	//							plan.NewProject(
	//								[]sql.Expression{
	//									expression.NewUnresolvedColumn("a"),
	//								},
	//								plan.NewUnresolvedTable("b", ""),
	//							),
	//						),
	//						[]string{},
	//					),
	//				},
	//				false,
	//			),
	//		},
	//		{
	//			input: `SELECT -128, 127, 255, -32768, 32767, 65535, -2147483648, 2147483647, 4294967295, -9223372036854775808, 9223372036854775807, 18446744073709551615`,
	//			plan: plan.NewProject(
	//				[]sql.Expression{
	//					expression.NewLiteral(int8(math.MinInt8), types.Int8),
	//					expression.NewLiteral(int8(math.MaxInt8), types.Int8),
	//					expression.NewLiteral(uint8(math.MaxUint8), types.Uint8),
	//					expression.NewLiteral(int16(math.MinInt16), types.Int16),
	//					expression.NewLiteral(int16(math.MaxInt16), types.Int16),
	//					expression.NewLiteral(uint16(math.MaxUint16), types.Uint16),
	//					expression.NewLiteral(int32(math.MinInt32), types.Int32),
	//					expression.NewLiteral(int32(math.MaxInt32), types.Int32),
	//					expression.NewLiteral(uint32(math.MaxUint32), types.Uint32),
	//					expression.NewLiteral(int64(math.MinInt64), types.Int64),
	//					expression.NewLiteral(int64(math.MaxInt64), types.Int64),
	//					expression.NewLiteral(uint64(math.MaxUint64), types.Uint64),
	//				},
	//				plan.NewResolvedDualTable(),
	//			),
	//		},
	//		{
	//			input: `CREATE VIEW v AS SELECT * FROM foo`,
	//			plan: plan.NewCreateView(
	//				sql.UnresolvedDatabase(""),
	//				"v",
	//				[]string{},
	//				plan.NewSubqueryAlias(
	//					"v", "SELECT * FROM foo",
	//					plan.NewProject(
	//						[]sql.Expression{expression.NewStar()},
	//						plan.NewUnresolvedTable("foo", ""),
	//					),
	//				),
	//				false,
	//				"CREATE VIEW v AS SELECT * FROM foo", "", "``@``", "",
	//			),
	//		},
	//		{
	//			input: `CREATE VIEW myview AS SELECT AVG(DISTINCT foo) FROM b`,
	//			plan: plan.NewCreateView(
	//				sql.UnresolvedDatabase(""),
	//				"myview",
	//				[]string{},
	//				plan.NewSubqueryAlias(
	//					"myview", "SELECT AVG(DISTINCT foo) FROM b",
	//					plan.NewGroupBy(
	//						[]sql.Expression{
	//							expression.NewUnresolvedFunction("avg", true, nil, expression.NewDistinctExpression(expression.NewUnresolvedColumn("foo"))),
	//						},
	//						[]sql.Expression{},
	//						plan.NewUnresolvedTable("b", ""),
	//					),
	//				),
	//				false,
	//				"CREATE VIEW myview AS SELECT AVG(DISTINCT foo) FROM b", "", "``@``", "",
	//			),
	//		},
	//		{
	//			input: `CREATE OR REPLACE VIEW v AS SELECT * FROM foo`,
	//			plan: plan.NewCreateView(
	//				sql.UnresolvedDatabase(""),
	//				"v",
	//				[]string{},
	//				plan.NewSubqueryAlias(
	//					"v", "SELECT * FROM foo",
	//					plan.NewProject(
	//						[]sql.Expression{expression.NewStar()},
	//						plan.NewUnresolvedTable("foo", ""),
	//					),
	//				),
	//				true,
	//				"CREATE OR REPLACE VIEW v AS SELECT * FROM foo", "", "``@``", "",
	//			),
	//		},
	//		{
	//			input: `SELECT 2 UNION SELECT 3`,
	//			plan: plan.NewSetOp(plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), true, nil, nil, nil),
	//		},
	//		{
	//			input: `(SELECT 2) UNION (SELECT 3)`,
	//			plan: plan.NewSetOp(plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), true, nil, nil, nil),
	//		},
	//		{
	//			input: `SELECT 2 UNION ALL SELECT 3 UNION DISTINCT SELECT 4`,
	//			plan: plan.NewSetOp(plan.NewSetOp(plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), false, nil, nil, nil),
	//				plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(4), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, nil, nil, nil),
	//		},
	//		{
	//			input: `SELECT 2 UNION SELECT 3 UNION ALL SELECT 4`,
	//			plan: plan.NewSetOp(
	//				plan.NewSetOp(plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, nil, nil, nil),
	//				plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(4), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), false, nil, nil, nil),
	//		},
	//		{
	//			input: `SELECT 2 UNION SELECT 3 UNION SELECT 4`,
	//			plan: plan.NewSetOp(
	//				plan.NewSetOp(plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, nil, nil, nil),
	//				plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(4), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, nil, nil, nil),
	//		},
	//		{
	//			input: `SELECT 2 UNION (SELECT 3 UNION SELECT 4)`,
	//			plan: plan.NewSetOp(
	//				plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				),
	//				plan.NewSetOp(plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(4), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, nil, nil, nil),
	//				true, nil, nil, nil,
	//			),
	//		},
	//		{
	//			input: `SELECT 2 UNION ALL SELECT 3`,
	//			plan: plan.NewSetOp(plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), false, nil, nil, nil),
	//		},
	//		{
	//			input: `SELECT 2 UNION DISTINCT SELECT 3`,
	//			plan: plan.NewSetOp(plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), plan.NewProject(
	//				[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//				plan.NewResolvedDualTable(),
	//			), true, nil, nil, nil),
	//		},
	//		{
	//			input: `SELECT 2 UNION SELECT 3 UNION SELECT 4 LIMIT 10`,
	//			plan: plan.NewSetOp(
	//				plan.NewSetOp(plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, nil, nil, nil),
	//				plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(4), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, expression.NewLiteral(int8(10), types.Int8), nil, nil),
	//		},
	//		{
	//			input: `SELECT 2 UNION SELECT 3 UNION SELECT 4 ORDER BY 2`,
	//			plan: plan.NewSetOp(
	//				plan.NewSetOp(plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(2), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(3), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, nil, nil, nil),
	//				plan.NewProject(
	//					[]sql.Expression{expression.NewLiteral(int8(4), types.Int8)},
	//					plan.NewResolvedDualTable(),
	//				), true, nil, nil, []sql.SortField{
	//					{
	//						Column:       expression.NewLiteral(int8(2), types.Int8),
	//						Column2:      expression.NewLiteral(int8(2), types.Int8),
	//						Order:        sql.Ascending,
	//						NullOrdering: sql.NullsFirst,
	//					},
	//				}),
	//		},
	//		{
	//			input: `CREATE DATABASE test`,
	//			plan:  plan.NewCreateDatabase("test", false, sql.Collation_Unspecified),
	//		},
	//		{
	//			input: `CREATE DATABASE IF NOT EXISTS test`,
	//			plan:  plan.NewCreateDatabase("test", true, sql.Collation_Unspecified),
	//		},
	//		{
	//			input: `DROP DATABASE test`,
	//			plan:  plan.NewDropDatabase("test", false),
	//		},
	//		{
	//			input: `DROP DATABASE IF EXISTS test`,
	//			plan:  plan.NewDropDatabase("test", true),
	//		},
	//		{
	//			input: `KILL QUERY 1`,
	//			plan:  plan.NewKill(plan.KillType_Query, 1),
	//		},
	//		{
	//			input: `KILL CONNECTION 1`,
	//			plan:  plan.NewKill(plan.KillType_Connection, 1),
	//		},
	//		{
	//			input: `CREATE PROCEDURE p1(INOUT a INT, IN b SMALLINT)
	//BEGIN
	//	DECLARE c BIGINT;
	//	DECLARE cur1 CURSOR FOR SELECT 1;
	//    OPEN cur1;
	//    FETCH cur1 INTO c;
	//    CLOSE cur1;
	//END;`,
	//			plan: plan.NewCreateProcedure(
	//				sql.UnresolvedDatabase(""),
	//				"p1",
	//				"",
	//				[]plan.ProcedureParam{
	//					{
	//						Direction: plan.ProcedureParamDirection_Inout,
	//						Name:      "a",
	//						Type:      types.Int32,
	//						Variadic:  false,
	//					},
	//					{
	//						Direction: plan.ProcedureParamDirection_In,
	//						Name:      "b",
	//						Type:      types.Int16,
	//						Variadic:  false,
	//					},
	//				},
	//				time.Now(),
	//				time.Now(),
	//				plan.ProcedureSecurityContext_Definer,
	//				nil,
	//				plan.NewBeginEndBlock(
	//					"",
	//					plan.NewBlock([]sql.Node{
	//						plan.NewDeclareVariables([]string{"c"}, types.Int64, nil),
	//						plan.NewDeclareCursor("cur1", plan.NewProject(
	//							[]sql.Expression{expression.NewLiteral(int8(1), types.Int8)},
	//							plan.NewResolvedDualTable(),
	//						)),
	//						plan.NewOpen("cur1"),
	//						plan.NewFetch("cur1", []sql.Expression{expression.NewUnresolvedColumn("c")}),
	//						plan.NewClose("cur1"),
	//					}),
	//				),
	//				"",
	//				`CREATE PROCEDURE p1(INOUT a INT, IN b SMALLINT)
	//BEGIN
	//	DECLARE c BIGINT;
	//	DECLARE cur1 CURSOR FOR SELECT 1;
	//    OPEN cur1;
	//    FETCH cur1 INTO c;
	//    CLOSE cur1;
	//END`,
	//				`BEGIN
	//	DECLARE c BIGINT;
	//	DECLARE cur1 CURSOR FOR SELECT 1;
	//    OPEN cur1;
	//    FETCH cur1 INTO c;
	//    CLOSE cur1;
	//END`,
	//			),
	//		},
	//		{
	//			input: "CREATE DEFINER = `user1`@`localhost` EVENT IF NOT EXISTS mydb.event1 ON SCHEDULE AT CURRENT_TIMESTAMP COMMENT 'my event' DO SELECT 1",
	//			plan: plan.NewCreateEvent(
	//				sql.UnresolvedDatabase("mydb"),
	//				"event1",
	//				"`user1`@`localhost`",
	//				plan.NewOnScheduleTimestamp(
	//					expression.NewUnresolvedFunction("current_timestamp", false, nil),
	//					nil,
	//				),
	//				nil,
	//				nil,
	//				nil,
	//				false,
	//				plan.EventStatus_Enable,
	//				"my event",
	//				"SELECT 1",
	//				plan.NewProject([]sql.Expression{expression.NewLiteral(int8(1), types.Int8)}, plan.NewResolvedDualTable()),
	//				true,
	//			),
	//		},
	//		{
	//			input: "CREATE DEFINER = `user1`@`localhost` EVENT IF NOT EXISTS mydb.event1 ON SCHEDULE AT '2037-10-16 23:59:00' + INTERVAL '2:3' HOUR_MINUTE ON COMPLETION NOT PRESERVE ENABLE DO SELECT 1",
	//			plan: plan.NewCreateEvent(
	//				sql.UnresolvedDatabase("mydb"),
	//				"event1",
	//				"`user1`@`localhost`",
	//				plan.NewOnScheduleTimestamp(
	//					expression.NewLiteral("2037-10-16 23:59:00", types.LongText),
	//					[]sql.Expression{expression.NewInterval(expression.NewLiteral("2:3", types.LongText), "HOUR_MINUTE")},
	//				),
	//				nil,
	//				nil,
	//				nil,
	//				false,
	//				plan.EventStatus_Enable,
	//				"",
	//				"SELECT 1",
	//				plan.NewProject([]sql.Expression{expression.NewLiteral(int8(1), types.Int8)}, plan.NewResolvedDualTable()),
	//				true,
	//			),
	//		},
	//		{
	//			input: "CREATE DEFINER = `user1`@`localhost` EVENT IF NOT EXISTS mydb.event1 ON SCHEDULE EVERY 1 HOUR ON COMPLETION PRESERVE DISABLE DO SELECT 1",
	//			plan: plan.NewCreateEvent(
	//				sql.UnresolvedDatabase("mydb"),
	//				"event1",
	//				"`user1`@`localhost`",
	//				nil,
	//				nil,
	//				nil,
	//				expression.NewInterval(expression.NewLiteral(int8(1), types.Int8), "HOUR"),
	//				true,
	//				plan.EventStatus_Disable,
	//				"",
	//				"SELECT 1",
	//				plan.NewProject([]sql.Expression{expression.NewLiteral(int8(1), types.Int8)}, plan.NewResolvedDualTable()),
	//				true,
	//			),
	//		},
	//		{
	//			input: "CREATE EVENT mydb.event1 ON SCHEDULE EVERY 1 HOUR STARTS CURRENT_TIMESTAMP + INTERVAL 30 MINUTE ENDS '2037-10-16 23:59:00' ON COMPLETION PRESERVE DISABLE DO SELECT 1",
	//			plan: plan.NewCreateEvent(
	//				sql.UnresolvedDatabase("mydb"),
	//				"event1",
	//				"``@``",
	//				nil,
	//				plan.NewOnScheduleTimestamp(
	//					expression.NewUnresolvedFunction("current_timestamp", false, nil),
	//					[]sql.Expression{expression.NewInterval(expression.NewLiteral(int8(30), types.Int8), "MINUTE")},
	//				),
	//				plan.NewOnScheduleTimestamp(
	//					expression.NewLiteral("2037-10-16 23:59:00", types.LongText),
	//					nil,
	//				),
	//				expression.NewInterval(expression.NewLiteral(int8(1), types.Int8), "HOUR"),
	//				true,
	//				plan.EventStatus_Disable,
	//				"",
	//				"SELECT 1",
	//				plan.NewProject([]sql.Expression{expression.NewLiteral(int8(1), types.Int8)}, plan.NewResolvedDualTable()),
	//				false,
	//			),
	//		},
	//		{
	//			input: "INSERT INTO instance(id, setup_complete)\n  VALUES (CONVERT(UUID() USING utf8mb4), FALSE)",
	//			plan: plan.NewInsertInto(
	//				sql.UnresolvedDatabase(""),
	//				plan.NewUnresolvedTable("instance", ""),
	//				plan.NewValues([][]sql.Expression{
	//					{
	//						expression.NewCollatedExpression(
	//							expression.NewUnresolvedFunction("uuid", false, nil),
	//							sql.CharacterSet_utf8mb4.DefaultCollation()),
	//						expression.NewLiteral(false, types.Boolean),
	//					},
	//				}),
	//				false,
	//				[]string{"id", "setup_complete"},
	//				[]sql.Expression{},
	//				false,
	//			),
	//		},
	//	}

	// TODO use planbuilder
	//for _, tt := range fixtures {
	//	t.Run(tt.input, func(t *testing.T) {
	//		require := require.New(t)
	//		ctx := sql.NewEmptyContext()
	//		p, err := Parse(ctx, tt.input)
	//		require.NoError(err)
	//		if createTable, ok := p.(*plan.CreateTable); ok {
	//			for _, col := range createTable.CreateSchema.Schema {
	//				if collatedType, ok := col.Type.(sql.TypeWithCollation); ok {
	//					col.Type, err = collatedType.WithNewCollation(sql.Collation_Default)
	//					require.NoError(err)
	//				}
	//			}
	//		} else if createProcedure, ok := p.(*plan.CreateProcedure); ok {
	//			createProcedure.CreatedAt = tt.plan.(*plan.CreateProcedure).CreatedAt
	//			createProcedure.ModifiedAt = tt.plan.(*plan.CreateProcedure).ModifiedAt
	//		}
	//		if !assertNodesEqualWithDiff(t, tt.plan, p) {
	//			t.Logf("Unexpected result for query %s", tt.input)
	//		}
	//	})
	//}
}

func TestParseCreateTrigger(t *testing.T) {
	var triggerFixtures = map[string]sql.Node{
		`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW 
   BEGIN 
     UPDATE bar SET x = old.y WHERE z = new.y;
		 DELETE FROM baz WHERE a = old.b;
		 INSERT INTO zzz (a,b) VALUES (old.a, old.b);
   END`: plan.NewCreateTrigger(sql.UnresolvedDatabase(""), "myTrigger", "before", "update", nil,
			plan.NewUnresolvedTable("foo", ""),
			plan.NewBeginEndBlock(
				"",
				plan.NewBlock([]sql.Node{
					plan.NewUpdate(plan.NewFilter(
						expression.NewEquals(expression.NewUnresolvedColumn("z"), expression.NewUnresolvedQualifiedColumn("new", "y")),
						plan.NewUnresolvedTable("bar", ""),
					), false, []sql.Expression{
						expression.NewSetField(expression.NewUnresolvedColumn("x"), expression.NewUnresolvedQualifiedColumn("old", "y")),
					}),
					plan.NewDeleteFrom(
						plan.NewFilter(
							expression.NewEquals(expression.NewUnresolvedColumn("a"), expression.NewUnresolvedQualifiedColumn("old", "b")),
							plan.NewUnresolvedTable("baz", ""),
						), nil),
					plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("zzz", ""), plan.NewValues([][]sql.Expression{{
						expression.NewUnresolvedQualifiedColumn("old", "a"),
						expression.NewUnresolvedQualifiedColumn("old", "b"),
					}},
					), false, []string{"a", "b"}, []sql.Expression{}, false),
				}),
			),
			`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW 
   BEGIN 
     UPDATE bar SET x = old.y WHERE z = new.y;
		 DELETE FROM baz WHERE a = old.b;
		 INSERT INTO zzz (a,b) VALUES (old.a, old.b);
   END`,
			`BEGIN 
     UPDATE bar SET x = old.y WHERE z = new.y;
		 DELETE FROM baz WHERE a = old.b;
		 INSERT INTO zzz (a,b) VALUES (old.a, old.b);
   END`,
			time.Unix(0, 0),
			"``@``",
		),
		`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW INSERT INTO zzz (a,b) VALUES (old.a, old.b)`: plan.NewCreateTrigger(sql.UnresolvedDatabase(""),
			"myTrigger", "before", "update", nil,
			plan.NewUnresolvedTable("foo", ""),
			plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("zzz", ""), plan.NewValues([][]sql.Expression{{
				expression.NewUnresolvedQualifiedColumn("old", "a"),
				expression.NewUnresolvedQualifiedColumn("old", "b"),
			}},
			), false, []string{"a", "b"}, []sql.Expression{}, false),
			`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW INSERT INTO zzz (a,b) VALUES (old.a, old.b)`,
			`INSERT INTO zzz (a,b) VALUES (old.a, old.b)`,
			time.Unix(0, 0),
			"``@``",
		),
		`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW FOLLOWS yourTrigger INSERT INTO zzz (a,b) VALUES (old.a, old.b)`: plan.NewCreateTrigger(sql.UnresolvedDatabase(""),
			"myTrigger", "before", "update",
			&plan.TriggerOrder{PrecedesOrFollows: sqlparser.FollowsStr, OtherTriggerName: "yourTrigger"},
			plan.NewUnresolvedTable("foo", ""),
			plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("zzz", ""), plan.NewValues([][]sql.Expression{{
				expression.NewUnresolvedQualifiedColumn("old", "a"),
				expression.NewUnresolvedQualifiedColumn("old", "b"),
			}},
			), false, []string{"a", "b"}, []sql.Expression{}, false),
			`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW FOLLOWS yourTrigger INSERT INTO zzz (a,b) VALUES (old.a, old.b)`,
			`INSERT INTO zzz (a,b) VALUES (old.a, old.b)`,
			time.Unix(0, 0),
			"``@``",
		),
		`create trigger signal_with_user_var
    BEFORE DELETE ON FOO FOR EACH ROW
		BEGIN
        SET @message_text = CONCAT('ouch', 'oof');
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = @message_text;
    END`: plan.NewCreateTrigger(sql.UnresolvedDatabase(""),
			"signal_with_user_var", "before", "delete",
			nil,
			plan.NewUnresolvedTable("FOO", ""),
			plan.NewBeginEndBlock("", plan.NewBlock([]sql.Node{
				plan.NewSet([]sql.Expression{
					expression.NewSetField(
						expression.NewUserVar("message_text"),
						expression.NewUnresolvedFunction("concat", false, nil, expression.NewLiteral("ouch", types.LongText), expression.NewLiteral("oof", types.LongText)),
					),
				}),
				plan.NewSignal("45000", map[plan.SignalConditionItemName]plan.SignalInfo{
					plan.SignalConditionItemName_MessageText: {
						ConditionItemName: plan.SignalConditionItemName_MessageText,
						ExprVal:           expression.NewUnresolvedColumn("@message_text"),
					},
				}),
			},
			)),
			`create trigger signal_with_user_var
    BEFORE DELETE ON FOO FOR EACH ROW
		BEGIN
        SET @message_text = CONCAT('ouch', 'oof');
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = @message_text;
    END`,
			`BEGIN
        SET @message_text = CONCAT('ouch', 'oof');
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = @message_text;
    END`,
			time.Unix(0, 0),
			"``@``"),
	}

	var queriesInOrder []string
	for q := range triggerFixtures {
		queriesInOrder = append(queriesInOrder, q)
	}
	sort.Strings(queriesInOrder)

	// TODO use planbuilder
	//date := time.Unix(0, 0)
	//for _, query := range queriesInOrder {
	//	expectedPlan := triggerFixtures[query]
	//	t.Run(query, func(t *testing.T) {
	//		sql.RunWithNowFunc(func() time.Time { return date }, func() error {
	//			require := require.New(t)
	//			ctx := sql.NewEmptyContext()
	//			p, err := Parse(ctx, query)
	//			require.NoError(err)
	//			if !assertNodesEqualWithDiff(t, expectedPlan, p) {
	//				t.Logf("Unexpected result for query %s", query)
	//			}
	//			return nil
	//		})
	//	})
	//}
}

// assertNodesEqualWithDiff asserts the two nodes given to be equal and prints any diff according to their DebugString
// methods.
func assertNodesEqualWithDiff(t *testing.T, expected, actual sql.Node) bool {
	if !assert.Equal(t, expected, actual) {
		expectedStr := sql.DebugString(expected)
		actualStr := sql.DebugString(actual)
		diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
			A:        difflib.SplitLines(expectedStr),
			B:        difflib.SplitLines(actualStr),
			FromFile: "expected",
			FromDate: "",
			ToFile:   "actual",
			ToDate:   "",
			Context:  1,
		})
		require.NoError(t, err)

		if len(diff) > 0 {
			fmt.Println(diff)
		} else {
			// No textual diff found, but not equal. Ugh. Let's at least figure out which node in the plans isn't equal.
		Top:
			for {
				for i := range expected.Children() {
					if !assertNodesEqualWithDiff(t, expected.Children()[i], actual.Children()[i]) {
						expected, actual = expected.Children()[i], actual.Children()[i]
						continue Top
					}
				}
				// Either no children, or all children were equal. This must the node that's different. Probably should add
				// enough information in DebugPrint for this node that it shows up in the textual diff.
				fmt.Printf("Non-textual difference found in node %s -- implement a better DebugPrint?\n", sql.DebugString(expected))
				break
			}
		}

		return false
	}
	return true
}

var fixturesErrors = map[string]*errors.Kind{
	`SELECT INTERVAL 1 DAY - '2018-05-01'`:                      sql.ErrUnsupportedSyntax,
	`SELECT INTERVAL 1 DAY * '2018-05-01'`:                      sql.ErrUnsupportedSyntax,
	`SELECT '2018-05-01' * INTERVAL 1 DAY`:                      sql.ErrUnsupportedSyntax,
	`SELECT '2018-05-01' / INTERVAL 1 DAY`:                      sql.ErrUnsupportedSyntax,
	`SELECT INTERVAL 1 DAY + INTERVAL 1 DAY`:                    sql.ErrUnsupportedSyntax,
	`SELECT '2018-05-01' + (INTERVAL 1 DAY + INTERVAL 1 DAY)`:   sql.ErrUnsupportedSyntax,
	"DESCRIBE FORMAT=pretty SELECT * FROM foo":                  errInvalidDescribeFormat,
	`CREATE TABLE test (pk int null primary key)`:               ErrPrimaryKeyOnNullField,
	`CREATE TABLE test (pk int not null null primary key)`:      ErrPrimaryKeyOnNullField,
	`CREATE TABLE test (pk int null, primary key(pk))`:          ErrPrimaryKeyOnNullField,
	`CREATE TABLE test (pk int not null null, primary key(pk))`: ErrPrimaryKeyOnNullField,
	`SELECT i, row_number() over (order by a) group by 1`:       sql.ErrUnsupportedFeature,
	`SHOW COUNT(*) WARNINGS`:                                    sql.ErrUnsupportedFeature,
	`SHOW ERRORS`:                                               sql.ErrUnsupportedFeature,
	`SHOW VARIABLES WHERE Value = ''`:                           sql.ErrUnsupportedFeature,
	`SHOW SESSION VARIABLES WHERE Value IS NOT NULL`:            sql.ErrUnsupportedFeature,
	`KILL CONNECTION 4294967296`:                                sql.ErrUnsupportedFeature,
	`DROP TABLE IF EXISTS curdb.foo, otherdb.bar`:               sql.ErrUnsupportedFeature,
	`DROP TABLE curdb.t1, t2`:                                   sql.ErrUnsupportedFeature,
}

// TODO use planbuilder
//func TestParseOne(t *testing.T) {
//	type testCase struct {
//		input string
//		parts []string
//	}
//
//	cases := []testCase{
//		{
//			"SELECT 1",
//			[]string{"SELECT 1"},
//		},
//		{
//			"SELECT 1;",
//			[]string{"SELECT 1"},
//		},
//		{
//			"SELECT 1; SELECT 2",
//			[]string{"SELECT 1", "SELECT 2"},
//		},
//		{
//			"SELECT 1 /* testing */ ;",
//			[]string{"SELECT 1 /* testing */"},
//		},
//		{
//			"SELECT 1 -- this is a test",
//			[]string{"SELECT 1 -- this is a test"},
//		},
//		{
//			"-- empty statement with comment\n; SELECT 1; SELECT 2",
//			[]string{"-- empty statement with comment", "SELECT 1", "SELECT 2"},
//		},
//		{
//			"SELECT 1; -- empty statement with comment\n; SELECT 2",
//			[]string{"SELECT 1", "-- empty statement with comment", "SELECT 2"},
//		},
//		{
//			"SELECT 1; SELECT 2; -- empty statement with comment\n",
//			[]string{"SELECT 1", "SELECT 2", "-- empty statement with comment"},
//		},
//	}
//	for _, tc := range cases {
//		t.Run(tc.input, func(t *testing.T) {
//			ctx := sql.NewEmptyContext()
//			q := tc.input
//			for i := 0; i < len(tc.parts); i++ {
//				tree, p, r, err := ParseOne(ctx, q)
//				require.NoError(t, err)
//				require.NotNil(t, tree)
//				require.Equal(t, tc.parts[i], p)
//				if i == len(tc.parts)-1 {
//					require.Empty(t, r)
//				}
//				q = r
//			}
//		})
//	}
//}

func TestParseErrors(t *testing.T) {
	t.Skip("todo use planbuilder")
	for query, _ := range fixturesErrors {
		t.Run(query, func(t *testing.T) {
			//require := require.New(t)
			//ctx := sql.NewEmptyContext()
			//_, err := Parse(ctx, query)
			//require.Error(err)
			//require.True(expectedError.Is(err), "Expected %T but got %T (%v)", expectedError, err, err)
		})
	}
}
