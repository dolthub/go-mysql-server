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

package parse

import (
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/expression/function/aggregation"
	"github.com/dolthub/go-mysql-server/sql/plan"
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

var fixtures = map[string]sql.Node{
	`CREATE TABLE t1(a INTEGER, b TEXT, c DATE, d TIMESTAMP, e VARCHAR(20), f BLOB NOT NULL, g DATETIME, h CHAR(40))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:     "a",
				Type:     sql.Int32,
				Nullable: true,
			}, {
				Name:     "b",
				Type:     sql.Text,
				Nullable: true,
			}, {
				Name:     "c",
				Type:     sql.Date,
				Nullable: true,
			}, {
				Name:     "d",
				Type:     sql.Timestamp,
				Nullable: true,
			}, {
				Name:     "e",
				Type:     sql.MustCreateStringWithDefaults(sqltypes.VarChar, 20),
				Nullable: true,
			}, {
				Name:     "f",
				Type:     sql.Blob,
				Nullable: false,
			}, {
				Name:     "g",
				Type:     sql.Datetime,
				Nullable: true,
			}, {
				Name:     "h",
				Type:     sql.MustCreateStringWithDefaults(sqltypes.Char, 40),
				Nullable: true,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER NOT NULL PRIMARY KEY, b TEXT)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Text,
				Nullable:   true,
				PrimaryKey: false,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER NOT NULL PRIMARY KEY COMMENT "hello", b TEXT COMMENT "goodbye")`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
				Comment:    "hello",
			}, {
				Name:       "b",
				Type:       sql.Text,
				Nullable:   true,
				PrimaryKey: false,
				Comment:    "goodbye",
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER, b TEXT, PRIMARY KEY (a))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Text,
				Nullable:   true,
				PrimaryKey: false,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER, b TEXT, PRIMARY KEY (a, b))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Text,
				Nullable:   false,
				PrimaryKey: true,
			}},
		},
	),
	`CREATE TABLE IF NOT EXISTS t1(a INTEGER, b TEXT, PRIMARY KEY (a, b))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExists,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Text,
				Nullable:   false,
				PrimaryKey: true,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX (b))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			IdxDefs: []*plan.IndexDefinition{{
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_None,
				Columns:    []sql.IndexColumn{{"b", 0}},
				Comment:    "",
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX idx_name (b))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			IdxDefs: []*plan.IndexDefinition{{
				IndexName:  "idx_name",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_None,
				Columns:    []sql.IndexColumn{{"b", 0}},
				Comment:    "",
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX idx_name (b) COMMENT 'hi')`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			IdxDefs: []*plan.IndexDefinition{{
				IndexName:  "idx_name",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_None,
				Columns:    []sql.IndexColumn{{"b", 0}},
				Comment:    "hi",
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, UNIQUE INDEX (b))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			IdxDefs: []*plan.IndexDefinition{{
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_Unique,
				Columns:    []sql.IndexColumn{{"b", 0}},
				Comment:    "",
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, UNIQUE (b))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			IdxDefs: []*plan.IndexDefinition{{
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_Unique,
				Columns:    []sql.IndexColumn{{"b", 0}},
				Comment:    "",
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX (b, a))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			IdxDefs: []*plan.IndexDefinition{{
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_None,
				Columns:    []sql.IndexColumn{{"b", 0}, {"a", 0}},
				Comment:    "",
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b INTEGER, INDEX (b), INDEX (b, a))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			IdxDefs: []*plan.IndexDefinition{{
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_None,
				Columns:    []sql.IndexColumn{{"b", 0}},
				Comment:    "",
			}, {
				IndexName:  "",
				Using:      sql.IndexUsing_Default,
				Constraint: sql.IndexConstraint_None,
				Columns:    []sql.IndexColumn{{"b", 0}, {"a", 0}},
				Comment:    "",
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES t0(b))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b_id",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			FkDefs: []*sql.ForeignKeyConstraint{{
				Name:              "",
				Columns:           []string{"b_id"},
				ReferencedTable:   "t0",
				ReferencedColumns: []string{"b"},
				OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
				OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, CONSTRAINT fk_name FOREIGN KEY (b_id) REFERENCES t0(b))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b_id",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			FkDefs: []*sql.ForeignKeyConstraint{{
				Name:              "fk_name",
				Columns:           []string{"b_id"},
				ReferencedTable:   "t0",
				ReferencedColumns: []string{"b"},
				OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
				OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES t0(b) ON UPDATE CASCADE)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b_id",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			FkDefs: []*sql.ForeignKeyConstraint{{
				Name:              "",
				Columns:           []string{"b_id"},
				ReferencedTable:   "t0",
				ReferencedColumns: []string{"b"},
				OnUpdate:          sql.ForeignKeyReferenceOption_Cascade,
				OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES t0(b) ON DELETE RESTRICT)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b_id",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},
			FkDefs: []*sql.ForeignKeyConstraint{{
				Name:              "",
				Columns:           []string{"b_id"},
				ReferencedTable:   "t0",
				ReferencedColumns: []string{"b"},
				OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
				OnDelete:          sql.ForeignKeyReferenceOption_Restrict,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, FOREIGN KEY (b_id) REFERENCES t0(b) ON UPDATE SET NULL ON DELETE NO ACTION)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b_id",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}},

			FkDefs: []*sql.ForeignKeyConstraint{{
				Name:              "",
				Columns:           []string{"b_id"},
				ReferencedTable:   "t0",
				ReferencedColumns: []string{"b"},
				OnUpdate:          sql.ForeignKeyReferenceOption_SetNull,
				OnDelete:          sql.ForeignKeyReferenceOption_NoAction,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, c_id BIGINT, FOREIGN KEY (b_id, c_id) REFERENCES t0(b, c))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b_id",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}, {
				Name:       "c_id",
				Type:       sql.Int64,
				Nullable:   true,
				PrimaryKey: false,
			}},
			FkDefs: []*sql.ForeignKeyConstraint{{
				Name:              "",
				Columns:           []string{"b_id", "c_id"},
				ReferencedTable:   "t0",
				ReferencedColumns: []string{"b", "c"},
				OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
				OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, b_id INTEGER, c_id BIGINT, CONSTRAINT fk_name FOREIGN KEY (b_id, c_id) REFERENCES t0(b, c) ON UPDATE RESTRICT ON DELETE CASCADE)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}, {
				Name:       "b_id",
				Type:       sql.Int32,
				Nullable:   true,
				PrimaryKey: false,
			}, {
				Name:       "c_id",
				Type:       sql.Int64,
				Nullable:   true,
				PrimaryKey: false,
			}},
			FkDefs: []*sql.ForeignKeyConstraint{{
				Name:              "fk_name",
				Columns:           []string{"b_id", "c_id"},
				ReferencedTable:   "t0",
				ReferencedColumns: []string{"b", "c"},
				OnUpdate:          sql.ForeignKeyReferenceOption_Restrict,
				OnDelete:          sql.ForeignKeyReferenceOption_Cascade,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, CHECK (a > 0))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}},
			ChDefs: []*sql.CheckConstraint{{
				Name: "",
				Expr: expression.NewGreaterThan(
					expression.NewUnresolvedColumn("a"),
					expression.NewLiteral(int8(0), sql.Int8),
				),
				Enforced: true,
			}},
		},
	),
	`
CREATE TABLE t4
(
  CHECK (c1 = c2),
  c1 INT CHECK (c1 > 10),
  c2 INT CONSTRAINT c2_positive CHECK (c2 > 0),
  CHECK (c1 > c3)
);`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t4",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{
				{
					Name:     "c1",
					Source:   "t4",
					Type:     sql.Int32,
					Nullable: true,
				},
				{
					Name:     "c2",
					Source:   "t4",
					Type:     sql.Int32,
					Nullable: true,
				},
			},
			ChDefs: []*sql.CheckConstraint{
				{
					Expr: expression.NewEquals(
						expression.NewUnresolvedColumn("c1"),
						expression.NewUnresolvedColumn("c2"),
					),
					Enforced: true,
				},
				{
					Expr: expression.NewGreaterThan(
						expression.NewUnresolvedColumn("c1"),
						expression.NewLiteral(int8(10), sql.Int8),
					),
					Enforced: true,
				},
				{
					Name: "c2_positive",
					Expr: expression.NewGreaterThan(
						expression.NewUnresolvedColumn("c2"),
						expression.NewLiteral(int8(0), sql.Int8),
					),
					Enforced: true,
				},
				{
					Expr: expression.NewGreaterThan(
						expression.NewUnresolvedColumn("c1"),
						expression.NewUnresolvedColumn("c3"),
					),
					Enforced: true,
				},
			},
		},
	),
	`
CREATE TABLE t2
(
  CHECK (c1 = c2),
  c1 INT CHECK (c1 > 10),
  c2 INT CONSTRAINT c2_positive CHECK (c2 > 0),
  c3 INT CHECK (c3 < 100),
  CONSTRAINT c1_nonzero CHECK (c1 = 0),
  CHECK (c1 > c3)
);`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t2",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{
				{
					Name:     "c1",
					Source:   "t2",
					Type:     sql.Int32,
					Nullable: true,
				},
				{
					Name:     "c2",
					Source:   "t2",
					Type:     sql.Int32,
					Nullable: true,
				},
				{
					Name:     "c3",
					Source:   "t2",
					Type:     sql.Int32,
					Nullable: true,
				},
			},
			ChDefs: []*sql.CheckConstraint{
				{
					Expr: expression.NewEquals(
						expression.NewUnresolvedColumn("c1"),
						expression.NewUnresolvedColumn("c2"),
					),
					Enforced: true,
				},
				{
					Expr: expression.NewGreaterThan(
						expression.NewUnresolvedColumn("c1"),
						expression.NewLiteral(int8(10), sql.Int8),
					),
					Enforced: true,
				},
				{
					Name: "c2_positive",
					Expr: expression.NewGreaterThan(
						expression.NewUnresolvedColumn("c2"),
						expression.NewLiteral(int8(0), sql.Int8),
					),
					Enforced: true,
				},
				{
					Expr: expression.NewLessThan(
						expression.NewUnresolvedColumn("c3"),
						expression.NewLiteral(int8(100), sql.Int8),
					),
					Enforced: true,
				},
				{
					Name: "c1_nonzero",
					Expr: expression.NewEquals(
						expression.NewUnresolvedColumn("c1"),
						expression.NewLiteral(int8(0), sql.Int8),
					),
					Enforced: true,
				},
				{
					Expr: expression.NewGreaterThan(
						expression.NewUnresolvedColumn("c1"),
						expression.NewUnresolvedColumn("c3"),
					),
					Enforced: true,
				},
			},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY CHECK (a > 0))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}},
			ChDefs: []*sql.CheckConstraint{{
				Name: "",
				Expr: expression.NewGreaterThan(
					expression.NewUnresolvedColumn("a"),
					expression.NewLiteral(int8(0), sql.Int8),
				),
				Enforced: true,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY, CONSTRAINT ch1 CHECK (a > 0))`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}},
			ChDefs: []*sql.CheckConstraint{{
				Name: "ch1",
				Expr: expression.NewGreaterThan(
					expression.NewUnresolvedColumn("a"),
					expression.NewLiteral(int8(0), sql.Int8),
				),
				Enforced: true,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY CHECK (a > 0) ENFORCED)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}},
			ChDefs: []*sql.CheckConstraint{{
				Name: "",
				Expr: expression.NewGreaterThan(
					expression.NewUnresolvedColumn("a"),
					expression.NewLiteral(int8(0), sql.Int8),
				),
				Enforced: true,
			}},
		},
	),
	`CREATE TABLE t1(a INTEGER PRIMARY KEY CHECK (a > 0) NOT ENFORCED)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTableAbsent,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:       "a",
				Type:       sql.Int32,
				Nullable:   false,
				PrimaryKey: true,
			}},
			ChDefs: []*sql.CheckConstraint{{
				Name: "",
				Expr: expression.NewGreaterThan(
					expression.NewUnresolvedColumn("a"),
					expression.NewLiteral(int8(0), sql.Int8),
				),
				Enforced: false,
			}},
		},
	),
	`CREATE TEMPORARY TABLE t1(a INTEGER, b TEXT)`: plan.NewCreateTable(
		sql.UnresolvedDatabase(""),
		"t1",
		plan.IfNotExistsAbsent,
		plan.IsTempTable,
		&plan.TableSpec{
			Schema: sql.Schema{{
				Name:     "a",
				Type:     sql.Int32,
				Nullable: true,
			}, {
				Name:     "b",
				Type:     sql.Text,
				Nullable: true,
			}},
		},
	),
	`CREATE TEMPORARY TABLE mytable AS SELECT * from othertable`: plan.NewCreateTableSelect(
		sql.UnresolvedDatabase(""),
		"mytable",
		plan.NewProject([]sql.Expression{expression.NewStar()}, plan.NewUnresolvedTable("othertable", "")),
		&plan.TableSpec{},
		plan.IfNotExistsAbsent,
		plan.IsTempTable),
	`DROP TABLE foo;`: plan.NewDropTable(
		sql.UnresolvedDatabase(""), false, "foo",
	),
	`DROP TABLE IF EXISTS foo;`: plan.NewDropTable(
		sql.UnresolvedDatabase(""), true, "foo",
	),
	`DROP TABLE IF EXISTS foo, bar, baz;`: plan.NewDropTable(
		sql.UnresolvedDatabase(""), true, "foo", "bar", "baz",
	),
	`RENAME TABLE foo TO bar`: plan.NewRenameTable(
		sql.UnresolvedDatabase(""), []string{"foo"}, []string{"bar"},
	),
	`RENAME TABLE foo TO bar, baz TO qux`: plan.NewRenameTable(
		sql.UnresolvedDatabase(""), []string{"foo", "baz"}, []string{"bar", "qux"},
	),
	`ALTER TABLE foo RENAME bar`: plan.NewRenameTable(
		sql.UnresolvedDatabase(""), []string{"foo"}, []string{"bar"},
	),
	`ALTER TABLE foo RENAME TO bar`: plan.NewRenameTable(
		sql.UnresolvedDatabase(""), []string{"foo"}, []string{"bar"},
	),
	`ALTER TABLE foo RENAME COLUMN bar TO baz`: plan.NewRenameColumn(
		sql.UnresolvedDatabase(""), "foo", "bar", "baz",
	),
	`ALTER TABLE foo ADD COLUMN bar INT NOT NULL`: plan.NewAddColumn(
		sql.UnresolvedDatabase(""), "foo", &sql.Column{
			Name:     "bar",
			Type:     sql.Int32,
			Nullable: false,
		}, nil,
	),
	`ALTER TABLE foo ADD COLUMN bar INT NOT NULL DEFAULT 42 COMMENT 'hello' AFTER baz`: plan.NewAddColumn(
		sql.UnresolvedDatabase(""), "foo", &sql.Column{
			Name:     "bar",
			Type:     sql.Int32,
			Nullable: false,
			Comment:  "hello",
			Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "42", nil, true),
		}, &sql.ColumnOrder{AfterColumn: "baz"},
	),
	`ALTER TABLE foo ADD COLUMN bar INT NOT NULL DEFAULT -42.0 COMMENT 'hello' AFTER baz`: plan.NewAddColumn(
		sql.UnresolvedDatabase(""), "foo", &sql.Column{
			Name:     "bar",
			Type:     sql.Int32,
			Nullable: false,
			Comment:  "hello",
			Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "-42.0", nil, true),
		}, &sql.ColumnOrder{AfterColumn: "baz"},
	),
	`ALTER TABLE foo ADD COLUMN bar INT NOT NULL DEFAULT (2+2)/2 COMMENT 'hello' AFTER baz`: plan.NewAddColumn(
		sql.UnresolvedDatabase(""), "foo", &sql.Column{
			Name:     "bar",
			Type:     sql.Int32,
			Nullable: false,
			Comment:  "hello",
			Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "(2+2)/2", nil, true),
		}, &sql.ColumnOrder{AfterColumn: "baz"},
	),
	`ALTER TABLE foo ADD COLUMN bar VARCHAR(10) NULL DEFAULT 'string' COMMENT 'hello'`: plan.NewAddColumn(
		sql.UnresolvedDatabase(""), "foo", &sql.Column{
			Name:     "bar",
			Type:     sql.MustCreateString(sqltypes.VarChar, 10, sql.Collation_Default),
			Nullable: true,
			Comment:  "hello",
			Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), `"string"`, nil, true),
		}, nil,
	),
	`ALTER TABLE foo ADD COLUMN bar FLOAT NULL DEFAULT 32.0 COMMENT 'hello'`: plan.NewAddColumn(
		sql.UnresolvedDatabase(""), "foo", &sql.Column{
			Name:     "bar",
			Type:     sql.Float32,
			Nullable: true,
			Comment:  "hello",
			Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "32.0", nil, true),
		}, nil,
	),
	`ALTER TABLE foo ADD COLUMN bar INT DEFAULT 1 FIRST`: plan.NewAddColumn(
		sql.UnresolvedDatabase(""), "foo", &sql.Column{
			Name:     "bar",
			Type:     sql.Int32,
			Nullable: true,
			Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), "1", nil, true),
		}, &sql.ColumnOrder{First: true},
	),
	`ALTER TABLE foo ADD INDEX (v1)`: plan.NewAlterCreateIndex(
		plan.NewUnresolvedTable("foo", ""),
		"",
		sql.IndexUsing_BTree,
		sql.IndexConstraint_None,
		[]sql.IndexColumn{{"v1", 0}},
		"",
	),
	`ALTER TABLE foo DROP COLUMN bar`: plan.NewDropColumn(
		sql.UnresolvedDatabase(""), "foo", "bar",
	),
	`ALTER TABLE foo MODIFY COLUMN bar VARCHAR(10) NULL DEFAULT 'string' COMMENT 'hello' FIRST`: plan.NewModifyColumn(
		sql.UnresolvedDatabase(""), "foo", "bar", &sql.Column{
			Name:     "bar",
			Type:     sql.MustCreateString(sqltypes.VarChar, 10, sql.Collation_Default),
			Nullable: true,
			Comment:  "hello",
			Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), `"string"`, nil, true),
		}, &sql.ColumnOrder{First: true},
	),
	`ALTER TABLE foo CHANGE COLUMN bar baz VARCHAR(10) NULL DEFAULT 'string' COMMENT 'hello' FIRST`: plan.NewModifyColumn(
		sql.UnresolvedDatabase(""), "foo", "bar", &sql.Column{
			Name:     "baz",
			Type:     sql.MustCreateString(sqltypes.VarChar, 10, sql.Collation_Default),
			Nullable: true,
			Comment:  "hello",
			Default:  MustStringToColumnDefaultValue(sql.NewEmptyContext(), `"string"`, nil, true),
		}, &sql.ColumnOrder{First: true},
	),
	`ALTER TABLE t1 ADD FOREIGN KEY (b_id) REFERENCES t0(b)`: plan.NewAlterAddForeignKey(
		sql.UnresolvedDatabase(""),
		"t1",
		"t0",
		&sql.ForeignKeyConstraint{
			Name:              "",
			Columns:           []string{"b_id"},
			ReferencedTable:   "t0",
			ReferencedColumns: []string{"b"},
			OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
			OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
		},
	),
	`ALTER TABLE t1 ADD CONSTRAINT fk_name FOREIGN KEY (b_id) REFERENCES t0(b)`: plan.NewAlterAddForeignKey(
		sql.UnresolvedDatabase(""),
		"t1",
		"t0",
		&sql.ForeignKeyConstraint{
			Name:              "fk_name",
			Columns:           []string{"b_id"},
			ReferencedTable:   "t0",
			ReferencedColumns: []string{"b"},
			OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
			OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
		},
	),
	`ALTER TABLE t1 ADD FOREIGN KEY (b_id) REFERENCES t0(b) ON UPDATE CASCADE`: plan.NewAlterAddForeignKey(
		sql.UnresolvedDatabase(""),
		"t1",
		"t0",
		&sql.ForeignKeyConstraint{
			Name:              "",
			Columns:           []string{"b_id"},
			ReferencedTable:   "t0",
			ReferencedColumns: []string{"b"},
			OnUpdate:          sql.ForeignKeyReferenceOption_Cascade,
			OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
		},
	),
	`ALTER TABLE t1 ADD FOREIGN KEY (b_id) REFERENCES t0(b) ON DELETE RESTRICT`: plan.NewAlterAddForeignKey(
		sql.UnresolvedDatabase(""),
		"t1",
		"t0",
		&sql.ForeignKeyConstraint{
			Name:              "",
			Columns:           []string{"b_id"},
			ReferencedTable:   "t0",
			ReferencedColumns: []string{"b"},
			OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
			OnDelete:          sql.ForeignKeyReferenceOption_Restrict,
		},
	),
	`ALTER TABLE t1 ADD FOREIGN KEY (b_id) REFERENCES t0(b) ON UPDATE SET NULL ON DELETE NO ACTION`: plan.NewAlterAddForeignKey(
		sql.UnresolvedDatabase(""),
		"t1",
		"t0",
		&sql.ForeignKeyConstraint{
			Name:              "",
			Columns:           []string{"b_id"},
			ReferencedTable:   "t0",
			ReferencedColumns: []string{"b"},
			OnUpdate:          sql.ForeignKeyReferenceOption_SetNull,
			OnDelete:          sql.ForeignKeyReferenceOption_NoAction,
		},
	),
	`ALTER TABLE t1 ADD FOREIGN KEY (b_id, c_id) REFERENCES t0(b, c)`: plan.NewAlterAddForeignKey(
		sql.UnresolvedDatabase(""),
		"t1",
		"t0",
		&sql.ForeignKeyConstraint{
			Name:              "",
			Columns:           []string{"b_id", "c_id"},
			ReferencedTable:   "t0",
			ReferencedColumns: []string{"b", "c"},
			OnUpdate:          sql.ForeignKeyReferenceOption_DefaultAction,
			OnDelete:          sql.ForeignKeyReferenceOption_DefaultAction,
		},
	),
	`ALTER TABLE t1 ADD CONSTRAINT fk_name FOREIGN KEY (b_id, c_id) REFERENCES t0(b, c) ON UPDATE RESTRICT ON DELETE CASCADE`: plan.NewAlterAddForeignKey(
		sql.UnresolvedDatabase(""),
		"t1",
		"t0",
		&sql.ForeignKeyConstraint{
			Name:              "fk_name",
			Columns:           []string{"b_id", "c_id"},
			ReferencedTable:   "t0",
			ReferencedColumns: []string{"b", "c"},
			OnUpdate:          sql.ForeignKeyReferenceOption_Restrict,
			OnDelete:          sql.ForeignKeyReferenceOption_Cascade,
		},
	),
	`ALTER TABLE t1 ADD CHECK (a > 0)`: plan.NewAlterAddCheck(
		plan.NewUnresolvedTable("t1", ""),
		&sql.CheckConstraint{
			Name: "",
			Expr: expression.NewGreaterThan(
				expression.NewUnresolvedColumn("a"),
				expression.NewLiteral(int8(0), sql.Int8),
			),
			Enforced: true,
		},
	),
	`ALTER TABLE t1 ADD CONSTRAINT ch1 CHECK (a > 0)`: plan.NewAlterAddCheck(
		plan.NewUnresolvedTable("t1", ""),
		&sql.CheckConstraint{
			Name: "ch1",
			Expr: expression.NewGreaterThan(
				expression.NewUnresolvedColumn("a"),
				expression.NewLiteral(int8(0), sql.Int8),
			),
			Enforced: true,
		},
	),
	`ALTER TABLE t1 ADD CONSTRAINT CHECK (a > 0)`: plan.NewAlterAddCheck(
		plan.NewUnresolvedTable("t1", ""),
		&sql.CheckConstraint{
			Name: "",
			Expr: expression.NewGreaterThan(
				expression.NewUnresolvedColumn("a"),
				expression.NewLiteral(int8(0), sql.Int8),
			),
			Enforced: true,
		},
	),
	`ALTER TABLE t1 DROP FOREIGN KEY fk_name`: plan.NewAlterDropForeignKey(
		plan.NewUnresolvedTable("t1", ""),
		"fk_name",
	),
	`ALTER TABLE t1 DROP CONSTRAINT fk_name`: plan.NewDropConstraint(
		plan.NewUnresolvedTable("t1", ""),
		"fk_name",
	),
	`DESCRIBE foo;`: plan.NewShowColumns(false,
		plan.NewUnresolvedTable("foo", ""),
	),
	`DESC foo;`: plan.NewShowColumns(false,
		plan.NewUnresolvedTable("foo", ""),
	),
	"DESCRIBE FORMAT=tree SELECT * FROM foo": plan.NewDescribeQuery(
		"tree", plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("foo", ""),
		)),
	"DESC FORMAT=tree SELECT * FROM foo": plan.NewDescribeQuery(
		"tree", plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("foo", ""),
		)),
	"EXPLAIN FORMAT=tree SELECT * FROM foo": plan.NewDescribeQuery(
		"tree", plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("foo", "")),
	),
	"DESCRIBE SELECT * FROM foo": plan.NewDescribeQuery(
		"tree", plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("foo", ""),
		)),
	"DESC SELECT * FROM foo": plan.NewDescribeQuery(
		"tree", plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("foo", ""),
		)),
	"EXPLAIN SELECT * FROM foo": plan.NewDescribeQuery(
		"tree", plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("foo", "")),
	),
	`SELECT foo, bar FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo IS NULL, bar IS NOT NULL FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewIsNull(expression.NewUnresolvedColumn("foo")),
			expression.NewAlias("bar IS NOT NULL",
				expression.NewNot(expression.NewIsNull(expression.NewUnresolvedColumn("bar"))),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo IS TRUE, bar IS NOT FALSE FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewIsTrue(expression.NewUnresolvedColumn("foo")),
			expression.NewAlias("bar IS NOT FALSE",
				expression.NewNot(expression.NewIsFalse(expression.NewUnresolvedColumn("bar"))),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo AS bar FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("bar", expression.NewUnresolvedColumn("foo")),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo AS bAz FROM foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("bAz", expression.NewUnresolvedColumn("foo")),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo AS bar FROM foo AS OF '2019-01-01' AS baz;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("bar", expression.NewUnresolvedColumn("foo")),
		},
		plan.NewTableAlias("baz",
			plan.NewUnresolvedTableAsOf("foo", "",
				expression.NewLiteral("2019-01-01", sql.LongText))),
	),
	`SELECT foo, bar FROM foo WHERE foo = bar;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo, bar FROM foo WHERE foo = 'bar';`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewLiteral("bar", sql.LongText),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo, bar FROM foo WHERE foo = ?;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewBindVar("v1"),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM (SELECT * FROM foo WHERE bar = ?) a;`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewSubqueryAlias(
			"a",
			"select * from foo where bar = :v1",
			plan.NewProject(
				[]sql.Expression{
					expression.NewStar(),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewUnresolvedColumn("bar"),
						expression.NewBindVar("v1"),
					),
					plan.NewUnresolvedTable("foo", ""),
				),
			),
		),
	),
	`SELECT * FROM (values row(1,2), row(3,4)) a;`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewValueDerivedTable(
			plan.NewValues([][]sql.Expression{
				{
					expression.NewLiteral(int8(1), sql.Int8),
					expression.NewLiteral(int8(2), sql.Int8),
				},
				{
					expression.NewLiteral(int8(3), sql.Int8),
					expression.NewLiteral(int8(4), sql.Int8),
				},
			}),
			"a"),
	),
	`SELECT * FROM (values row(1+1,2+2), row(rand(),concat("a","b"))) a;`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewValueDerivedTable(
			plan.NewValues([][]sql.Expression{
				{
					expression.NewArithmetic(
						expression.NewLiteral(int8(1), sql.Int8),
						expression.NewLiteral(int8(1), sql.Int8),
						"+",
					),
					expression.NewArithmetic(
						expression.NewLiteral(int8(2), sql.Int8),
						expression.NewLiteral(int8(2), sql.Int8),
						"+",
					),
				},
				{
					expression.NewUnresolvedFunction("rand", false, nil),
					expression.NewUnresolvedFunction("concat", false, nil, expression.NewLiteral("a", sql.LongText), expression.NewLiteral("b", sql.LongText)),
				},
			}),
			"a"),
	),
	`SELECT column_0 FROM (values row(1,2), row(3,4)) a limit 1`: plan.NewLimit(expression.NewLiteral(int8(1), sql.Int8),
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("column_0"),
			},
			plan.NewValueDerivedTable(
				plan.NewValues([][]sql.Expression{
					{
						expression.NewLiteral(int8(1), sql.Int8),
						expression.NewLiteral(int8(2), sql.Int8),
					},
					{
						expression.NewLiteral(int8(3), sql.Int8),
						expression.NewLiteral(int8(4), sql.Int8),
					},
				}),
				"a"),
		),
	),
	`SELECT foo, bar FROM foo WHERE foo <=> bar;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewFilter(
			expression.NewNullSafeEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo, bar FROM foo WHERE foo = :var;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewBindVar("var"),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE foo != 'bar';`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewFilter(
			expression.NewNot(expression.NewEquals(
				expression.NewUnresolvedColumn("foo"),
				expression.NewLiteral("bar", sql.LongText),
			)),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo, bar FROM foo LIMIT 10;`: plan.NewLimit(expression.NewLiteral(int8(10), sql.Int8),
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo, bar FROM foo ORDER BY baz DESC;`: plan.NewSort(
		[]sql.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: sql.Descending, NullOrdering: sql.NullsFirst}},
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo, bar FROM foo WHERE foo = bar LIMIT 10;`: plan.NewLimit(expression.NewLiteral(int8(10), sql.Int8),
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewFilter(
				expression.NewEquals(
					expression.NewUnresolvedColumn("foo"),
					expression.NewUnresolvedColumn("bar"),
				),
				plan.NewUnresolvedTable("foo", ""),
			),
		),
	),
	`SELECT foo, bar FROM foo ORDER BY baz DESC LIMIT 1;`: plan.NewLimit(expression.NewLiteral(int8(1), sql.Int8),
		plan.NewSort(
			[]sql.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: sql.Descending, NullOrdering: sql.NullsFirst}},
			plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("foo"),
					expression.NewUnresolvedColumn("bar"),
				},
				plan.NewUnresolvedTable("foo", ""),
			),
		),
	),
	`SELECT foo, bar FROM foo WHERE qux = 1 ORDER BY baz DESC LIMIT 1;`: plan.NewLimit(expression.NewLiteral(int8(1), sql.Int8),
		plan.NewSort(
			[]sql.SortField{{Column: expression.NewUnresolvedColumn("baz"), Order: sql.Descending, NullOrdering: sql.NullsFirst}},
			plan.NewProject(
				[]sql.Expression{
					expression.NewUnresolvedColumn("foo"),
					expression.NewUnresolvedColumn("bar"),
				},
				plan.NewFilter(
					expression.NewEquals(
						expression.NewUnresolvedColumn("qux"),
						expression.NewLiteral(int8(1), sql.Int8),
					),
					plan.NewUnresolvedTable("foo", ""),
				),
			),
		),
	),
	`SELECT foo, bar FROM t1, t2;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewCrossJoin(
			plan.NewUnresolvedTable("t1", ""),
			plan.NewUnresolvedTable("t2", ""),
		),
	),
	`SELECT foo, bar FROM t1 JOIN t2;`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewCrossJoin(
			plan.NewUnresolvedTable("t1", ""),
			plan.NewUnresolvedTable("t2", ""),
		),
	),
	`SELECT foo, bar FROM t1 GROUP BY foo, bar;`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("t1", ""),
	),
	`SELECT foo, bar FROM t1 GROUP BY 1, 2;`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		[]sql.Expression{
			expression.NewUnresolvedColumn("foo"),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("t1", ""),
	),
	`SELECT COUNT(*) FROM t1;`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("COUNT(*)",
				expression.NewUnresolvedFunction("count", true, nil,
					expression.NewStar()),
			),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("t1", ""),
	),
	`SELECT a FROM t1 where a regexp '.*test.*';`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
		},
		plan.NewFilter(
			expression.NewRegexp(
				expression.NewUnresolvedColumn("a"),
				expression.NewLiteral(".*test.*", sql.LongText),
			),
			plan.NewUnresolvedTable("t1", ""),
		),
	),
	`SELECT a FROM t1 where a regexp '*main.go';`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
		},
		plan.NewFilter(
			expression.NewRegexp(
				expression.NewUnresolvedColumn("a"),
				expression.NewLiteral("*main.go", sql.LongText),
			),
			plan.NewUnresolvedTable("t1", ""),
		),
	),
	`SELECT a FROM t1 where a not regexp '.*test.*';`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
		},
		plan.NewFilter(
			expression.NewNot(
				expression.NewRegexp(
					expression.NewUnresolvedColumn("a"),
					expression.NewLiteral(".*test.*", sql.LongText),
				),
			),
			plan.NewUnresolvedTable("t1", ""),
		),
	),
	`INSERT INTO t1 (col1, col2) VALUES ('a', 1)`: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
		expression.NewLiteral("a", sql.LongText),
		expression.NewLiteral(int8(1), sql.Int8),
	}}), false, []string{"col1", "col2"}, []sql.Expression{}, false),
	`INSERT INTO mydb.t1 (col1, col2) VALUES ('a', 1)`: plan.NewInsertInto(sql.UnresolvedDatabase("mydb"), plan.NewUnresolvedTable("t1", "mydb"), plan.NewValues([][]sql.Expression{{
		expression.NewLiteral("a", sql.LongText),
		expression.NewLiteral(int8(1), sql.Int8),
	}}), false, []string{"col1", "col2"}, []sql.Expression{}, false),
	`INSERT INTO t1 (col1, col2) VALUES (?, ?)`: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
		expression.NewBindVar("v1"),
		expression.NewBindVar("v2"),
	}}), false, []string{"col1", "col2"}, []sql.Expression{}, false),
	`INSERT INTO t1 VALUES (b'0111')`: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
		expression.NewLiteral(uint64(7), sql.Uint64),
	}}), false, []string{}, []sql.Expression{}, false),
	`UPDATE t1 SET col1 = ?, col2 = ? WHERE id = ?`: plan.NewUpdate(
		plan.NewFilter(
			expression.NewEquals(expression.NewUnresolvedColumn("id"), expression.NewBindVar("v3")),
			plan.NewUnresolvedTable("t1", ""),
		),
		[]sql.Expression{
			expression.NewSetField(expression.NewUnresolvedColumn("col1"), expression.NewBindVar("v1")),
			expression.NewSetField(expression.NewUnresolvedColumn("col2"), expression.NewBindVar("v2")),
		},
	),
	`REPLACE INTO t1 (col1, col2) VALUES ('a', 1)`: plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("t1", ""), plan.NewValues([][]sql.Expression{{
		expression.NewLiteral("a", sql.LongText),
		expression.NewLiteral(int8(1), sql.Int8),
	}}), true, []string{"col1", "col2"}, []sql.Expression{}, false),
	`SHOW TABLES`:                           plan.NewShowTables(sql.UnresolvedDatabase(""), false, nil),
	`SHOW FULL TABLES`:                      plan.NewShowTables(sql.UnresolvedDatabase(""), true, nil),
	`SHOW TABLES FROM foo`:                  plan.NewShowTables(sql.UnresolvedDatabase("foo"), false, nil),
	`SHOW TABLES IN foo`:                    plan.NewShowTables(sql.UnresolvedDatabase("foo"), false, nil),
	`SHOW FULL TABLES FROM foo`:             plan.NewShowTables(sql.UnresolvedDatabase("foo"), true, nil),
	`SHOW FULL TABLES IN foo`:               plan.NewShowTables(sql.UnresolvedDatabase("foo"), true, nil),
	`SHOW TABLES AS OF 'abc'`:               plan.NewShowTables(sql.UnresolvedDatabase(""), false, expression.NewLiteral("abc", sql.LongText)),
	`SHOW FULL TABLES AS OF 'abc'`:          plan.NewShowTables(sql.UnresolvedDatabase(""), true, expression.NewLiteral("abc", sql.LongText)),
	`SHOW TABLES FROM foo AS OF 'abc'`:      plan.NewShowTables(sql.UnresolvedDatabase("foo"), false, expression.NewLiteral("abc", sql.LongText)),
	`SHOW FULL TABLES FROM foo AS OF 'abc'`: plan.NewShowTables(sql.UnresolvedDatabase("foo"), true, expression.NewLiteral("abc", sql.LongText)),
	`SHOW FULL TABLES IN foo AS OF 'abc'`:   plan.NewShowTables(sql.UnresolvedDatabase("foo"), true, expression.NewLiteral("abc", sql.LongText)),
	`SHOW TABLES LIKE 'foo'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Table"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTables(sql.UnresolvedDatabase(""), false, nil),
	),
	`SHOW TABLES AS OF 'abc' LIKE 'foo'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Table"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTables(sql.UnresolvedDatabase(""), false, expression.NewLiteral("abc", sql.LongText)),
	),
	"SHOW TABLES WHERE `Table` = 'foo'": plan.NewFilter(
		expression.NewEquals(
			expression.NewUnresolvedColumn("Table"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTables(sql.UnresolvedDatabase(""), false, nil),
	),
	`SHOW FULL TABLES LIKE 'foo'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Table"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTables(sql.UnresolvedDatabase(""), true, nil),
	),
	"SHOW FULL TABLES WHERE `Table` = 'foo'": plan.NewFilter(
		expression.NewEquals(
			expression.NewUnresolvedColumn("Table"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTables(sql.UnresolvedDatabase(""), true, nil),
	),
	`SHOW FULL TABLES FROM bar LIKE 'foo'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Table"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTables(sql.UnresolvedDatabase("bar"), true, nil),
	),
	`SHOW FULL TABLES FROM bar AS OF 'abc' LIKE 'foo'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Table"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTables(sql.UnresolvedDatabase("bar"), true, expression.NewLiteral("abc", sql.LongText)),
	),
	"SHOW FULL TABLES FROM bar WHERE `Table` = 'foo'": plan.NewFilter(
		expression.NewEquals(
			expression.NewUnresolvedColumn("Table"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTables(sql.UnresolvedDatabase("bar"), true, nil),
	),
	`SELECT DISTINCT foo, bar FROM foo;`: plan.NewDistinct(
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT foo, bar FROM foo LIMIT 2 OFFSET 5;`: plan.NewLimit(expression.NewLiteral(int8(2), sql.Int8),
		plan.NewOffset(expression.NewLiteral(int8(5), sql.Int8), plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
		)),
	),
	`SELECT foo, bar FROM foo LIMIT 5,2;`: plan.NewLimit(expression.NewLiteral(int8(2), sql.Int8),
		plan.NewOffset(expression.NewLiteral(int8(5), sql.Int8), plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("foo"),
				expression.NewUnresolvedColumn("bar"),
			},
			plan.NewUnresolvedTable("foo", ""),
		)),
	),
	`SELECT * FROM foo WHERE (a = 1)`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewUnresolvedColumn("a"),
				expression.NewLiteral(int8(1), sql.Int8),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo, bar, baz, qux`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewCrossJoin(
					plan.NewUnresolvedTable("foo", ""),
					plan.NewUnresolvedTable("bar", ""),
				),
				plan.NewUnresolvedTable("baz", ""),
			),
			plan.NewUnresolvedTable("qux", ""),
		),
	),
	`SELECT * FROM foo join bar join baz join qux`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewCrossJoin(
			plan.NewCrossJoin(
				plan.NewCrossJoin(
					plan.NewUnresolvedTable("foo", ""),
					plan.NewUnresolvedTable("bar", ""),
				),
				plan.NewUnresolvedTable("baz", ""),
			),
			plan.NewUnresolvedTable("qux", ""),
		),
	),
	`SELECT * FROM foo WHERE a = b AND c = d`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewEquals(
					expression.NewUnresolvedColumn("a"),
					expression.NewUnresolvedColumn("b"),
				),
				expression.NewEquals(
					expression.NewUnresolvedColumn("c"),
					expression.NewUnresolvedColumn("d"),
				),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE a = b OR c = d`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewOr(
				expression.NewEquals(
					expression.NewUnresolvedColumn("a"),
					expression.NewUnresolvedColumn("b"),
				),
				expression.NewEquals(
					expression.NewUnresolvedColumn("c"),
					expression.NewUnresolvedColumn("d"),
				),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo as bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewTableAlias(
			"bar",
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM (SELECT * FROM foo) AS bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewSubqueryAlias(
			"bar", "select * from foo",
			plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewUnresolvedTable("foo", ""),
			),
		),
	),
	`SELECT * FROM foo WHERE 1 NOT BETWEEN 2 AND 5`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewNot(
				expression.NewBetween(
					expression.NewLiteral(int8(1), sql.Int8),
					expression.NewLiteral(int8(2), sql.Int8),
					expression.NewLiteral(int8(5), sql.Int8),
				),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE 1 BETWEEN 2 AND 5`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewBetween(
				expression.NewLiteral(int8(1), sql.Int8),
				expression.NewLiteral(int8(2), sql.Int8),
				expression.NewLiteral(int8(5), sql.Int8),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT 0x01AF`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("0x01AF",
				expression.NewLiteral(int16(431), sql.Int16),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT X'41'`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("X'41'",
				expression.NewLiteral([]byte{'A'}, sql.LongBlob),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT * FROM b WHERE SOMEFUNC((1, 2), (3, 4))`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewUnresolvedFunction(
				"somefunc",
				false,
				nil,
				expression.NewTuple(
					expression.NewLiteral(int8(1), sql.Int8),
					expression.NewLiteral(int8(2), sql.Int8),
				),
				expression.NewTuple(
					expression.NewLiteral(int8(3), sql.Int8),
					expression.NewLiteral(int8(4), sql.Int8),
				),
			),
			plan.NewUnresolvedTable("b", ""),
		),
	),
	`SELECT * FROM foo WHERE :foo_id = 2`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewEquals(
				expression.NewBindVar("foo_id"),
				expression.NewLiteral(int8(2), sql.Int8),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE ? = 2 and foo.s = ? and ? <> foo.i`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewAnd(
				expression.NewAnd(
					expression.NewEquals(
						expression.NewBindVar("v1"),
						expression.NewLiteral(int8(2), sql.Int8),
					),
					expression.NewEquals(
						expression.NewUnresolvedQualifiedColumn("foo", "s"),
						expression.NewBindVar("v2"),
					),
				),
				expression.NewNot(expression.NewEquals(
					expression.NewBindVar("v3"),
					expression.NewUnresolvedQualifiedColumn("foo", "i"),
				)),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo INNER JOIN bar ON a = b`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewInnerJoin(
			plan.NewUnresolvedTable("foo", ""),
			plan.NewUnresolvedTable("bar", ""),
			expression.NewEquals(
				expression.NewUnresolvedColumn("a"),
				expression.NewUnresolvedColumn("b"),
			),
		),
	),
	`SELECT foo.a FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedQualifiedColumn("foo", "a"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT CAST(-3 AS UNSIGNED) FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("CAST(-3 AS UNSIGNED)",
				expression.NewConvert(expression.NewLiteral(int8(-3), sql.Int8), expression.ConvertToUnsigned),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT 2 = 2 FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("2 = 2",
				expression.NewEquals(expression.NewLiteral(int8(2), sql.Int8), expression.NewLiteral(int8(2), sql.Int8))),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT *, bar FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewUnresolvedColumn("bar"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT *, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT bar, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("bar"),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT bar, *, foo.* FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnresolvedColumn("bar"),
			expression.NewStar(),
			expression.NewQualifiedStar("foo"),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT *, * FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT * FROM foo WHERE 1 IN ('1', 2)`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewInTuple(
				expression.NewLiteral(int8(1), sql.Int8),
				expression.NewTuple(
					expression.NewLiteral("1", sql.LongText),
					expression.NewLiteral(int8(2), sql.Int8),
				),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE 1 NOT IN ('1', 2)`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewNotInTuple(
				expression.NewLiteral(int8(1), sql.Int8),
				expression.NewTuple(
					expression.NewLiteral("1", sql.LongText),
					expression.NewLiteral(int8(2), sql.Int8),
				),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE i IN (SELECT j FROM baz)`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			plan.NewInSubquery(
				expression.NewUnresolvedColumn("i"),
				plan.NewSubquery(plan.NewProject(
					[]sql.Expression{expression.NewUnresolvedColumn("j")},
					plan.NewUnresolvedTable("baz", ""),
				), "select j from baz"),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE i NOT IN (SELECT j FROM baz)`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			plan.NewNotInSubquery(
				expression.NewUnresolvedColumn("i"),
				plan.NewSubquery(plan.NewProject(
					[]sql.Expression{expression.NewUnresolvedColumn("j")},
					plan.NewUnresolvedTable("baz", ""),
				), "select j from baz"),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT a, b FROM t ORDER BY 2, 1`: plan.NewSort(
		[]sql.SortField{
			{
				Column:       expression.NewLiteral(int8(2), sql.Int8),
				Order:        sql.Ascending,
				NullOrdering: sql.NullsFirst,
			},
			{
				Column:       expression.NewLiteral(int8(1), sql.Int8),
				Order:        sql.Ascending,
				NullOrdering: sql.NullsFirst,
			},
		},
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("a"),
				expression.NewUnresolvedColumn("b"),
			},
			plan.NewUnresolvedTable("t", ""),
		),
	),
	`SELECT -i FROM mytable`: plan.NewProject(
		[]sql.Expression{
			expression.NewUnaryMinus(
				expression.NewUnresolvedColumn("i"),
			),
		},
		plan.NewUnresolvedTable("mytable", ""),
	),
	`SELECT +i FROM mytable`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("+i",
				expression.NewUnresolvedColumn("i"),
			),
		},
		plan.NewUnresolvedTable("mytable", ""),
	),
	`SELECT - 4 - - 80`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("- 4 - - 80",
				expression.NewMinus(
					expression.NewLiteral(int8(-4), sql.Int8),
					expression.NewLiteral(int8(-80), sql.Int8),
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT + - - i FROM mytable`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("+ - - i",
				expression.NewUnaryMinus(
					expression.NewUnaryMinus(
						expression.NewUnresolvedColumn("i"),
					),
				),
			),
		},
		plan.NewUnresolvedTable("mytable", ""),
	),
	`SELECT 1 + 1;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("1 + 1",
				expression.NewPlus(expression.NewLiteral(int8(1), sql.Int8), expression.NewLiteral(int8(1), sql.Int8))),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT 1 + 1 as foo;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("foo",
				expression.NewPlus(expression.NewLiteral(int8(1), sql.Int8), expression.NewLiteral(int8(1), sql.Int8))),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT 1 * (2 + 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("1 * (2 + 1)",
				expression.NewMult(expression.NewLiteral(int8(1), sql.Int8),
					expression.NewPlus(expression.NewLiteral(int8(2), sql.Int8), expression.NewLiteral(int8(1), sql.Int8))),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT (0 - 1) * (1 | 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("(0 - 1) * (1 | 1)",
				expression.NewMult(
					expression.NewMinus(expression.NewLiteral(int8(0), sql.Int8), expression.NewLiteral(int8(1), sql.Int8)),
					expression.NewBitOr(expression.NewLiteral(int8(1), sql.Int8), expression.NewLiteral(int8(1), sql.Int8)),
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT (1 << 3) % (2 div 1);`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("(1 << 3) % (2 div 1)",
				expression.NewMod(
					expression.NewShiftLeft(expression.NewLiteral(int8(1), sql.Int8), expression.NewLiteral(int8(3), sql.Int8)),
					expression.NewIntDiv(expression.NewLiteral(int8(2), sql.Int8), expression.NewLiteral(int8(1), sql.Int8))),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT 1.0 * a + 2.0 * b FROM t;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("1.0 * a + 2.0 * b",
				expression.NewPlus(
					expression.NewMult(expression.NewLiteral(float64(1.0), sql.Float64), expression.NewUnresolvedColumn("a")),
					expression.NewMult(expression.NewLiteral(float64(2.0), sql.Float64), expression.NewUnresolvedColumn("b")),
				),
			),
		},
		plan.NewUnresolvedTable("t", ""),
	),
	`SELECT '1.0' + 2;`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("'1.0' + 2",
				expression.NewPlus(
					expression.NewLiteral("1.0", sql.LongText), expression.NewLiteral(int8(2), sql.Int8),
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT '1' + '2';`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("'1' + '2'",
				expression.NewPlus(
					expression.NewLiteral("1", sql.LongText), expression.NewLiteral("2", sql.LongText),
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`CREATE INDEX foo USING qux ON bar (baz)`: plan.NewCreateIndex(
		"foo",
		plan.NewUnresolvedTable("bar", ""),
		[]sql.Expression{expression.NewUnresolvedColumn("baz")},
		"qux",
		make(map[string]string),
	),
	`CREATE INDEX idx USING BTREE ON foo (bar)`: plan.NewAlterCreateIndex(
		plan.NewUnresolvedTable("foo", ""),
		"idx",
		sql.IndexUsing_BTree,
		sql.IndexConstraint_None,
		[]sql.IndexColumn{
			{"bar", 0},
		},
		"",
	),
	`      CREATE INDEX idx USING BTREE ON foo(bar)`: plan.NewAlterCreateIndex(
		plan.NewUnresolvedTable("foo", ""),
		"idx",
		sql.IndexUsing_BTree,
		sql.IndexConstraint_None,
		[]sql.IndexColumn{
			{"bar", 0},
		},
		"",
	),
	`SELECT * FROM foo NATURAL JOIN bar`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewNaturalJoin(
			plan.NewUnresolvedTable("foo", ""),
			plan.NewUnresolvedTable("bar", ""),
		),
	),
	`SELECT * FROM foo NATURAL JOIN bar NATURAL JOIN baz`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewNaturalJoin(
			plan.NewNaturalJoin(
				plan.NewUnresolvedTable("foo", ""),
				plan.NewUnresolvedTable("bar", ""),
			),
			plan.NewUnresolvedTable("baz", ""),
		),
	),
	`DROP INDEX foo ON bar`: plan.NewAlterDropIndex(
		plan.NewUnresolvedTable("bar", ""),
		"foo",
	),
	`DESCRIBE FORMAT=TREE SELECT * FROM foo`: plan.NewDescribeQuery(
		"tree",
		plan.NewProject(
			[]sql.Expression{expression.NewStar()},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT MAX(i)/2 FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("MAX(i)/2",
				expression.NewArithmetic(
					expression.NewUnresolvedFunction(
						"max", true, nil, expression.NewUnresolvedColumn("i"),
					),
					expression.NewLiteral(int8(2), sql.Int8),
					"/",
				),
			),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT current_user FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("current_user",
				expression.NewUnresolvedFunction("current_user", false, nil),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT current_USER(    ) FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("current_USER(    )",
				expression.NewUnresolvedFunction("current_user", false, nil),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SHOW INDEXES FROM foo`: plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	`SHOW INDEX FROM foo`:   plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	`SHOW KEYS FROM foo`:    plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	`SHOW INDEXES IN foo`:   plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	`SHOW INDEX IN foo`:     plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	`SHOW KEYS IN foo`:      plan.NewShowIndexes(plan.NewUnresolvedTable("foo", "")),
	`SHOW FULL PROCESSLIST`: plan.NewShowProcessList(),
	`SHOW PROCESSLIST`:      plan.NewShowProcessList(),
	`SELECT @@allowed_max_packet`: plan.NewProject([]sql.Expression{
		expression.NewUnresolvedColumn("@@allowed_max_packet"),
	}, plan.NewUnresolvedTable("dual", "")),
	`SET autocommit=1, foo="bar", baz=ON, qux=bareword`: plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewUnresolvedColumn("autocommit"), expression.NewLiteral(int8(1), sql.Int8)),
			expression.NewSetField(expression.NewUnresolvedColumn("foo"), expression.NewLiteral("bar", sql.LongText)),
			expression.NewSetField(expression.NewUnresolvedColumn("baz"), expression.NewLiteral("ON", sql.LongText)),
			expression.NewSetField(expression.NewUnresolvedColumn("qux"), expression.NewUnresolvedColumn("bareword")),
		},
	),
	`SET @@session.autocommit=1, foo="true"`: plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewSystemVar("autocommit", sql.SystemVariableScope_Session), expression.NewLiteral(int8(1), sql.Int8)),
			expression.NewSetField(expression.NewUnresolvedColumn("foo"), expression.NewLiteral("true", sql.LongText)),
		},
	),
	`SET SESSION NET_READ_TIMEOUT= 700, SESSION NET_WRITE_TIMEOUT= 700`: plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewSystemVar("NET_READ_TIMEOUT", sql.SystemVariableScope_Session), expression.NewLiteral(int16(700), sql.Int16)),
			expression.NewSetField(expression.NewSystemVar("NET_WRITE_TIMEOUT", sql.SystemVariableScope_Session), expression.NewLiteral(int16(700), sql.Int16)),
		},
	),
	`SET gtid_mode=DEFAULT`: plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewUnresolvedColumn("gtid_mode"), expression.NewDefaultColumn("")),
		},
	),
	`SET @@sql_select_limit=default`: plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewSystemVar("sql_select_limit", sql.SystemVariableScope_Session), expression.NewDefaultColumn("")),
		},
	),
	"":                     plan.Nothing,
	"/* just a comment */": plan.Nothing,
	`/*!40101 SET NAMES utf8 */`: plan.NewSet(
		[]sql.Expression{
			expression.NewSetField(expression.NewUnresolvedColumn("character_set_client"), expression.NewLiteral("utf8", sql.LongText)),
			expression.NewSetField(expression.NewUnresolvedColumn("character_set_connection"), expression.NewLiteral("utf8", sql.LongText)),
			expression.NewSetField(expression.NewUnresolvedColumn("character_set_results"), expression.NewLiteral("utf8", sql.LongText)),
		},
	),
	`SELECT /* a comment */ * FROM foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT /*!40101 * from */ foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	// TODO: other optimizer hints than join_order are ignored for now
	`SELECT /*+ JOIN_ORDER(a,b) */ * from foo`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT /*+ JOIN_ORDER(a,b) */ * FROM b join a on c = d limit 5`: plan.NewLimit(expression.NewLiteral(int8(5), sql.Int8),
		plan.NewProject(
			[]sql.Expression{
				expression.NewStar(),
			},
			plan.NewInnerJoin(
				plan.NewUnresolvedTable("b", ""),
				plan.NewUnresolvedTable("a", ""),
				expression.NewEquals(
					expression.NewUnresolvedColumn("c"),
					expression.NewUnresolvedColumn("d"),
				),
			).WithComment("/*+ JOIN_ORDER(a,b) */"),
		),
	),
	`SHOW DATABASES`: plan.NewShowDatabases(),
	`SELECT * FROM foo WHERE i LIKE 'foo'`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewLike(
				expression.NewUnresolvedColumn("i"),
				expression.NewLiteral("foo", sql.LongText),
			),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT * FROM foo WHERE i NOT LIKE 'foo'`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewFilter(
			expression.NewNot(expression.NewLike(
				expression.NewUnresolvedColumn("i"),
				expression.NewLiteral("foo", sql.LongText),
			)),
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SHOW FIELDS FROM foo`:       plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	`SHOW FULL COLUMNS FROM foo`: plan.NewShowColumns(true, plan.NewUnresolvedTable("foo", "")),
	`SHOW FIELDS FROM foo WHERE Field = 'bar'`: plan.NewFilter(
		expression.NewEquals(
			expression.NewUnresolvedColumn("Field"),
			expression.NewLiteral("bar", sql.LongText),
		),
		plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	),
	`SHOW FIELDS FROM foo LIKE 'bar'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Field"),
			expression.NewLiteral("bar", sql.LongText),
		),
		plan.NewShowColumns(false, plan.NewUnresolvedTable("foo", "")),
	),
	`SHOW TABLE STATUS LIKE 'foo'`: plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("Name"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTableStatus(sql.UnresolvedDatabase("")),
	),
	`SHOW TABLE STATUS FROM foo`: plan.NewShowTableStatus(sql.UnresolvedDatabase("foo")),
	`SHOW TABLE STATUS IN foo`:   plan.NewShowTableStatus(sql.UnresolvedDatabase("foo")),
	`SHOW TABLE STATUS`:          plan.NewShowTableStatus(sql.UnresolvedDatabase("")),
	`SHOW TABLE STATUS WHERE Name = 'foo'`: plan.NewFilter(
		expression.NewEquals(
			expression.NewUnresolvedColumn("Name"),
			expression.NewLiteral("foo", sql.LongText),
		),
		plan.NewShowTableStatus(sql.UnresolvedDatabase("")),
	),
	`USE foo`: plan.NewUse(sql.UnresolvedDatabase("foo")),
	`DESCRIBE foo.bar`: plan.NewShowColumns(false,
		plan.NewUnresolvedTable("bar", "foo"),
	),
	`DESC foo.bar`: plan.NewShowColumns(false,
		plan.NewUnresolvedTable("bar", "foo"),
	),
	`SELECT * FROM foo.bar`: plan.NewProject(
		[]sql.Expression{
			expression.NewStar(),
		},
		plan.NewUnresolvedTable("bar", "foo"),
	),
	`SHOW VARIABLES`:                           plan.NewShowVariables(""),
	`SHOW GLOBAL VARIABLES`:                    plan.NewShowVariables(""),
	`SHOW SESSION VARIABLES`:                   plan.NewShowVariables(""),
	`SHOW VARIABLES LIKE 'gtid_mode'`:          plan.NewShowVariables("gtid_mode"),
	`SHOW SESSION VARIABLES LIKE 'autocommit'`: plan.NewShowVariables("autocommit"),
	`UNLOCK TABLES`:                            plan.NewUnlockTables(),
	`LOCK TABLES foo READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", "")},
	}),
	`LOCK TABLES foo123 READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo123", "")},
	}),
	`LOCK TABLES foo AS f READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewTableAlias("f", plan.NewUnresolvedTable("foo", ""))},
	}),
	`LOCK TABLES foo READ LOCAL`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", "")},
	}),
	`LOCK TABLES foo WRITE`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
	}),
	`LOCK TABLES foo LOW_PRIORITY WRITE`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
	}),
	`LOCK TABLES foo WRITE, bar READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
		{Table: plan.NewUnresolvedTable("bar", "")},
	}),
	"LOCK TABLES `foo` WRITE, `bar` READ": plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", ""), Write: true},
		{Table: plan.NewUnresolvedTable("bar", "")},
	}),
	`LOCK TABLES foo READ, bar WRITE, baz READ`: plan.NewLockTables([]*plan.TableLock{
		{Table: plan.NewUnresolvedTable("foo", "")},
		{Table: plan.NewUnresolvedTable("bar", ""), Write: true},
		{Table: plan.NewUnresolvedTable("baz", "")},
	}),
	`SHOW CREATE DATABASE foo`:                 plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	`SHOW CREATE SCHEMA foo`:                   plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	`SHOW CREATE DATABASE IF NOT EXISTS foo`:   plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	`SHOW CREATE SCHEMA IF NOT EXISTS foo`:     plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	`SHOW WARNINGS`:                            plan.NewOffset(expression.NewLiteral(0, sql.Int64), plan.ShowWarnings(sql.NewEmptyContext().Warnings())),
	`SHOW WARNINGS LIMIT 10`:                   plan.NewLimit(expression.NewLiteral(10, sql.Int64), plan.NewOffset(expression.NewLiteral(0, sql.Int64), plan.ShowWarnings(sql.NewEmptyContext().Warnings()))),
	`SHOW WARNINGS LIMIT 5,10`:                 plan.NewLimit(expression.NewLiteral(10, sql.Int64), plan.NewOffset(expression.NewLiteral(5, sql.Int64), plan.ShowWarnings(sql.NewEmptyContext().Warnings()))),
	"SHOW CREATE DATABASE `foo`":               plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	"SHOW CREATE SCHEMA `foo`":                 plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), false),
	"SHOW CREATE DATABASE IF NOT EXISTS `foo`": plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	"SHOW CREATE SCHEMA IF NOT EXISTS `foo`":   plan.NewShowCreateDatabase(sql.UnresolvedDatabase("foo"), true),
	"SELECT CASE foo WHEN 1 THEN 'foo' WHEN 2 THEN 'bar' ELSE 'baz' END": plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("CASE foo WHEN 1 THEN 'foo' WHEN 2 THEN 'bar' ELSE 'baz' END",
				expression.NewCase(
					expression.NewUnresolvedColumn("foo"),
					[]expression.CaseBranch{
						{
							Cond:  expression.NewLiteral(int8(1), sql.Int8),
							Value: expression.NewLiteral("foo", sql.LongText),
						},
						{
							Cond:  expression.NewLiteral(int8(2), sql.Int8),
							Value: expression.NewLiteral("bar", sql.LongText),
						},
					},
					expression.NewLiteral("baz", sql.LongText),
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	"SELECT CASE foo WHEN 1 THEN 'foo' WHEN 2 THEN 'bar' END": plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("CASE foo WHEN 1 THEN 'foo' WHEN 2 THEN 'bar' END",
				expression.NewCase(
					expression.NewUnresolvedColumn("foo"),
					[]expression.CaseBranch{
						{
							Cond:  expression.NewLiteral(int8(1), sql.Int8),
							Value: expression.NewLiteral("foo", sql.LongText),
						},
						{
							Cond:  expression.NewLiteral(int8(2), sql.Int8),
							Value: expression.NewLiteral("bar", sql.LongText),
						},
					},
					nil,
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	"SELECT CASE WHEN foo = 1 THEN 'foo' WHEN foo = 2 THEN 'bar' ELSE 'baz' END": plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("CASE WHEN foo = 1 THEN 'foo' WHEN foo = 2 THEN 'bar' ELSE 'baz' END",
				expression.NewCase(
					nil,
					[]expression.CaseBranch{
						{
							Cond: expression.NewEquals(
								expression.NewUnresolvedColumn("foo"),
								expression.NewLiteral(int8(1), sql.Int8),
							),
							Value: expression.NewLiteral("foo", sql.LongText),
						},
						{
							Cond: expression.NewEquals(
								expression.NewUnresolvedColumn("foo"),
								expression.NewLiteral(int8(2), sql.Int8),
							),
							Value: expression.NewLiteral("bar", sql.LongText),
						},
					},
					expression.NewLiteral("baz", sql.LongText),
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	"SHOW COLLATION": showCollationProjection,
	"SHOW COLLATION LIKE 'foo'": plan.NewFilter(
		expression.NewLike(
			expression.NewUnresolvedColumn("collation"),
			expression.NewLiteral("foo", sql.LongText),
		),
		showCollationProjection,
	),
	"SHOW COLLATION WHERE Charset = 'foo'": plan.NewFilter(
		expression.NewEquals(
			expression.NewUnresolvedColumn("Charset"),
			expression.NewLiteral("foo", sql.LongText),
		),
		showCollationProjection,
	),
	"BEGIN":                                  plan.NewStartTransaction(""),
	"START TRANSACTION":                      plan.NewStartTransaction(""),
	"COMMIT":                                 plan.NewCommit(""),
	`ROLLBACK`:                               plan.NewRollback(""),
	"SAVEPOINT abc":                          plan.NewCreateSavepoint("", "abc"),
	"ROLLBACK TO SAVEPOINT abc":              plan.NewRollbackSavepoint("", "abc"),
	"RELEASE SAVEPOINT abc":                  plan.NewReleaseSavepoint("", "abc"),
	"SHOW CREATE TABLE `mytable`":            plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", ""), false),
	"SHOW CREATE TABLE mytable":              plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", ""), false),
	"SHOW CREATE TABLE mydb.`mytable`":       plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), false),
	"SHOW CREATE TABLE `mydb`.mytable":       plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), false),
	"SHOW CREATE TABLE `mydb`.`mytable`":     plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), false),
	"SHOW CREATE TABLE `my.table`":           plan.NewShowCreateTable(plan.NewUnresolvedTable("my.table", ""), false),
	"SHOW CREATE TABLE `my.db`.`my.table`":   plan.NewShowCreateTable(plan.NewUnresolvedTable("my.table", "my.db"), false),
	"SHOW CREATE TABLE `my``table`":          plan.NewShowCreateTable(plan.NewUnresolvedTable("my`table", ""), false),
	"SHOW CREATE TABLE `my``db`.`my``table`": plan.NewShowCreateTable(plan.NewUnresolvedTable("my`table", "my`db"), false),
	"SHOW CREATE TABLE ````":                 plan.NewShowCreateTable(plan.NewUnresolvedTable("`", ""), false),
	"SHOW CREATE TABLE `.`":                  plan.NewShowCreateTable(plan.NewUnresolvedTable(".", ""), false),
	"SHOW CREATE VIEW `mytable`":             plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", ""), true),
	"SHOW CREATE VIEW mytable":               plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", ""), true),
	"SHOW CREATE VIEW mydb.`mytable`":        plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), true),
	"SHOW CREATE VIEW `mydb`.mytable":        plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), true),
	"SHOW CREATE VIEW `mydb`.`mytable`":      plan.NewShowCreateTable(plan.NewUnresolvedTable("mytable", "mydb"), true),
	"SHOW CREATE VIEW `my.table`":            plan.NewShowCreateTable(plan.NewUnresolvedTable("my.table", ""), true),
	"SHOW CREATE VIEW `my.db`.`my.table`":    plan.NewShowCreateTable(plan.NewUnresolvedTable("my.table", "my.db"), true),
	"SHOW CREATE VIEW `my``table`":           plan.NewShowCreateTable(plan.NewUnresolvedTable("my`table", ""), true),
	"SHOW CREATE VIEW `my``db`.`my``table`":  plan.NewShowCreateTable(plan.NewUnresolvedTable("my`table", "my`db"), true),
	"SHOW CREATE VIEW ````":                  plan.NewShowCreateTable(plan.NewUnresolvedTable("`", ""), true),
	"SHOW CREATE VIEW `.`":                   plan.NewShowCreateTable(plan.NewUnresolvedTable(".", ""), true),
	`SELECT '2018-05-01' + INTERVAL 1 DAY`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("'2018-05-01' + INTERVAL 1 DAY",
				expression.NewArithmetic(
					expression.NewLiteral("2018-05-01", sql.LongText),
					expression.NewInterval(
						expression.NewLiteral(int8(1), sql.Int8),
						"DAY",
					),
					"+",
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT '2018-05-01' - INTERVAL 1 DAY`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("'2018-05-01' - INTERVAL 1 DAY",
				expression.NewArithmetic(
					expression.NewLiteral("2018-05-01", sql.LongText),
					expression.NewInterval(
						expression.NewLiteral(int8(1), sql.Int8),
						"DAY",
					),
					"-",
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT INTERVAL 1 DAY + '2018-05-01'`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("INTERVAL 1 DAY + '2018-05-01'",
				expression.NewArithmetic(
					expression.NewInterval(
						expression.NewLiteral(int8(1), sql.Int8),
						"DAY",
					),
					expression.NewLiteral("2018-05-01", sql.LongText),
					"+",
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT '2018-05-01' + INTERVAL 1 DAY + INTERVAL 1 DAY`: plan.NewProject(
		[]sql.Expression{
			expression.NewAlias("'2018-05-01' + INTERVAL 1 DAY + INTERVAL 1 DAY",
				expression.NewArithmetic(
					expression.NewArithmetic(
						expression.NewLiteral("2018-05-01", sql.LongText),
						expression.NewInterval(
							expression.NewLiteral(int8(1), sql.Int8),
							"DAY",
						),
						"+",
					),
					expression.NewInterval(
						expression.NewLiteral(int8(1), sql.Int8),
						"DAY",
					),
					"+",
				),
			),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`SELECT bar, AVG(baz) FROM foo GROUP BY bar HAVING COUNT(*) > 5`: plan.NewHaving(
		expression.NewGreaterThan(
			expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
			expression.NewLiteral(int8(5), sql.Int8),
		),
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewUnresolvedColumn("bar"),
				expression.NewAlias("AVG(baz)",
					expression.NewUnresolvedFunction("avg", true, nil, expression.NewUnresolvedColumn("baz")),
				),
			},
			[]sql.Expression{expression.NewUnresolvedColumn("bar")},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT foo FROM t GROUP BY foo HAVING i > 5`: plan.NewHaving(
		expression.NewGreaterThan(
			expression.NewUnresolvedColumn("i"),
			expression.NewLiteral(int8(5), sql.Int8),
		),
		plan.NewGroupBy(
			[]sql.Expression{expression.NewUnresolvedColumn("foo")},
			[]sql.Expression{expression.NewUnresolvedColumn("foo")},
			plan.NewUnresolvedTable("t", ""),
		),
	),
	`SELECT COUNT(*) FROM foo GROUP BY a HAVING COUNT(*) > 5`: plan.NewHaving(
		expression.NewGreaterThan(
			expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
			expression.NewLiteral(int8(5), sql.Int8),
		),
		plan.NewGroupBy(
			[]sql.Expression{
				expression.NewAlias("COUNT(*)",
					expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
				),
			},
			[]sql.Expression{expression.NewUnresolvedColumn("a")},
			plan.NewUnresolvedTable("foo", ""),
		),
	),
	`SELECT DISTINCT COUNT(*) FROM foo GROUP BY a HAVING COUNT(*) > 5`: plan.NewDistinct(
		plan.NewHaving(
			expression.NewGreaterThan(
				expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
				expression.NewLiteral(int8(5), sql.Int8),
			),
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewAlias("COUNT(*)",
						expression.NewUnresolvedFunction("count", true, nil, expression.NewStar()),
					),
				},
				[]sql.Expression{expression.NewUnresolvedColumn("a")},
				plan.NewUnresolvedTable("foo", ""),
			),
		),
	),
	`SELECT * FROM foo LEFT JOIN bar ON 1=1`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewLeftJoin(
			plan.NewUnresolvedTable("foo", ""),
			plan.NewUnresolvedTable("bar", ""),
			expression.NewEquals(
				expression.NewLiteral(int8(1), sql.Int8),
				expression.NewLiteral(int8(1), sql.Int8),
			),
		),
	),
	`SELECT * FROM foo LEFT OUTER JOIN bar ON 1=1`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewLeftJoin(
			plan.NewUnresolvedTable("foo", ""),
			plan.NewUnresolvedTable("bar", ""),
			expression.NewEquals(
				expression.NewLiteral(int8(1), sql.Int8),
				expression.NewLiteral(int8(1), sql.Int8),
			),
		),
	),
	`SELECT * FROM foo RIGHT JOIN bar ON 1=1`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewRightJoin(
			plan.NewUnresolvedTable("foo", ""),
			plan.NewUnresolvedTable("bar", ""),
			expression.NewEquals(
				expression.NewLiteral(int8(1), sql.Int8),
				expression.NewLiteral(int8(1), sql.Int8),
			),
		),
	),
	`SELECT * FROM foo RIGHT OUTER JOIN bar ON 1=1`: plan.NewProject(
		[]sql.Expression{expression.NewStar()},
		plan.NewRightJoin(
			plan.NewUnresolvedTable("foo", ""),
			plan.NewUnresolvedTable("bar", ""),
			expression.NewEquals(
				expression.NewLiteral(int8(1), sql.Int8),
				expression.NewLiteral(int8(1), sql.Int8),
			),
		),
	),
	`SELECT FIRST(i) FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("FIRST(i)",
				expression.NewUnresolvedFunction("first", true, nil, expression.NewUnresolvedColumn("i")),
			),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT LAST(i) FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("LAST(i)",
				expression.NewUnresolvedFunction("last", true, nil, expression.NewUnresolvedColumn("i")),
			),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT COUNT(DISTINCT i) FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			aggregation.NewCountDistinct(expression.NewUnresolvedColumn("i")),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT AVG(DISTINCT a) FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("AVG(DISTINCT a)",
				expression.NewUnresolvedFunction("avg", true, nil, expression.NewDistinctExpression(expression.NewUnresolvedColumn("a")))),
		},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT SUM(DISTINCT a*b) FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("SUM(DISTINCT a*b)",
				expression.NewUnresolvedFunction("sum", true, nil,
					expression.NewDistinctExpression(
						expression.NewMult(expression.NewUnresolvedColumn("a"),
							expression.NewUnresolvedColumn("b")))))},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT AVG(DISTINCT a / b) FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("AVG(DISTINCT a / b)",
				expression.NewUnresolvedFunction("avg", true, nil,
					expression.NewDistinctExpression(
						expression.NewDiv(expression.NewUnresolvedColumn("a"),
							expression.NewUnresolvedColumn("b")))))},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT SUM(DISTINCT POWER(a, 2)) FROM foo`: plan.NewGroupBy(
		[]sql.Expression{
			expression.NewAlias("SUM(DISTINCT POWER(a, 2))",
				expression.NewUnresolvedFunction("sum", true, nil,
					expression.NewDistinctExpression(
						expression.NewUnresolvedFunction("power", false, nil,
							expression.NewUnresolvedColumn("a"), expression.NewLiteral(int8(2), sql.Int8)))))},
		[]sql.Expression{},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT a, row_number() over (partition by s order by x) FROM foo`: plan.NewWindow(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
			expression.NewAlias("row_number() over (partition by s order by x)",
				expression.NewUnresolvedFunction("row_number", false, sql.NewWindow(
					[]sql.Expression{
						expression.NewUnresolvedColumn("s"),
					},
					sql.SortFields{
						{
							Column:       expression.NewUnresolvedColumn("x"),
							Order:        sql.Ascending,
							NullOrdering: sql.NullsFirst,
						},
					},
				)),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT a, count(i) over () FROM foo`: plan.NewWindow(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
			expression.NewAlias("count(i) over ()",
				expression.NewUnresolvedFunction("count", true, sql.NewWindow(
					[]sql.Expression{},
					nil,
				), expression.NewUnresolvedColumn("i")),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT a, row_number() over (order by x), row_number() over (partition by y) FROM foo`: plan.NewWindow(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
			expression.NewAlias("row_number() over (order by x)",
				expression.NewUnresolvedFunction("row_number", false, sql.NewWindow(
					[]sql.Expression{},
					sql.SortFields{
						{
							Column:       expression.NewUnresolvedColumn("x"),
							Order:        sql.Ascending,
							NullOrdering: sql.NullsFirst,
						},
					},
				)),
			),
			expression.NewAlias("row_number() over (partition by y)",
				expression.NewUnresolvedFunction("row_number", false, sql.NewWindow(
					[]sql.Expression{
						expression.NewUnresolvedColumn("y"),
					},
					nil,
				)),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`SELECT a, row_number() over (order by x), max(b) over () FROM foo`: plan.NewWindow(
		[]sql.Expression{
			expression.NewUnresolvedColumn("a"),
			expression.NewAlias("row_number() over (order by x)",
				expression.NewUnresolvedFunction("row_number", false, sql.NewWindow(
					[]sql.Expression{},
					sql.SortFields{
						{
							Column:       expression.NewUnresolvedColumn("x"),
							Order:        sql.Ascending,
							NullOrdering: sql.NullsFirst,
						},
					},
				)),
			),
			expression.NewAlias("max(b) over ()",
				expression.NewUnresolvedFunction("max", true, sql.NewWindow(
					[]sql.Expression{},
					nil,
				),
					expression.NewUnresolvedColumn("b"),
				),
			),
		},
		plan.NewUnresolvedTable("foo", ""),
	),
	`with cte1 as (select a from b) select * from cte1`: plan.NewWith(
		plan.NewProject(
			[]sql.Expression{
				expression.NewStar(),
			},
			plan.NewUnresolvedTable("cte1", "")),
		[]*plan.CommonTableExpression{
			plan.NewCommonTableExpression(
				plan.NewSubqueryAlias("cte1", "select a from b",
					plan.NewProject(
						[]sql.Expression{
							expression.NewUnresolvedColumn("a"),
						},
						plan.NewUnresolvedTable("b", ""),
					),
				),
				[]string{},
			),
		},
	),
	`with cte1 as (select a from b), cte2 as (select c from d) select * from cte1`: plan.NewWith(
		plan.NewProject(
			[]sql.Expression{
				expression.NewStar(),
			},
			plan.NewUnresolvedTable("cte1", "")),
		[]*plan.CommonTableExpression{
			plan.NewCommonTableExpression(
				plan.NewSubqueryAlias("cte1", "select a from b",
					plan.NewProject(
						[]sql.Expression{
							expression.NewUnresolvedColumn("a"),
						},
						plan.NewUnresolvedTable("b", ""),
					),
				),
				[]string{},
			),
			plan.NewCommonTableExpression(
				plan.NewSubqueryAlias("cte2", "select c from d",
					plan.NewProject(
						[]sql.Expression{
							expression.NewUnresolvedColumn("c"),
						},
						plan.NewUnresolvedTable("d", ""),
					),
				),
				[]string{},
			),
		},
	),
	`with cte1 (x) as (select a from b), cte2 (y,z) as (select c from d) select * from cte1`: plan.NewWith(
		plan.NewProject(
			[]sql.Expression{
				expression.NewStar(),
			},
			plan.NewUnresolvedTable("cte1", "")),
		[]*plan.CommonTableExpression{
			plan.NewCommonTableExpression(
				plan.NewSubqueryAlias("cte1", "select a from b",
					plan.NewProject(
						[]sql.Expression{
							expression.NewUnresolvedColumn("a"),
						},
						plan.NewUnresolvedTable("b", ""),
					),
				),
				[]string{"x"},
			),
			plan.NewCommonTableExpression(
				plan.NewSubqueryAlias("cte2", "select c from d",
					plan.NewProject(
						[]sql.Expression{
							expression.NewUnresolvedColumn("c"),
						},
						plan.NewUnresolvedTable("d", ""),
					),
				),
				[]string{"y", "z"},
			),
		},
	),
	`with cte1 as (select a from b) select c, (with cte2 as (select c from d) select e from cte2) from cte1`: plan.NewWith(
		plan.NewProject(
			[]sql.Expression{
				expression.NewUnresolvedColumn("c"),
				expression.NewAlias("(with cte2 as (select c from d) select e from cte2)",
					plan.NewSubquery(
						plan.NewWith(
							plan.NewProject(
								[]sql.Expression{
									expression.NewUnresolvedColumn("e"),
								},
								plan.NewUnresolvedTable("cte2", "")),
							[]*plan.CommonTableExpression{
								plan.NewCommonTableExpression(
									plan.NewSubqueryAlias("cte2", "select c from d",
										plan.NewProject(
											[]sql.Expression{
												expression.NewUnresolvedColumn("c"),
											},
											plan.NewUnresolvedTable("d", ""),
										),
									),
									[]string{},
								),
							},
						),
						"with cte2 as (select c from d) select e from cte2",
					),
				),
			},
			plan.NewUnresolvedTable("cte1", ""),
		),
		[]*plan.CommonTableExpression{
			plan.NewCommonTableExpression(
				plan.NewSubqueryAlias("cte1", "select a from b",
					plan.NewProject(
						[]sql.Expression{
							expression.NewUnresolvedColumn("a"),
						},
						plan.NewUnresolvedTable("b", ""),
					),
				),
				[]string{},
			),
		},
	),
	`SELECT -128, 127, 255, -32768, 32767, 65535, -2147483648, 2147483647, 4294967295, -9223372036854775808, 9223372036854775807, 18446744073709551615`: plan.NewProject(
		[]sql.Expression{
			expression.NewLiteral(int8(math.MinInt8), sql.Int8),
			expression.NewLiteral(int8(math.MaxInt8), sql.Int8),
			expression.NewLiteral(uint8(math.MaxUint8), sql.Uint8),
			expression.NewLiteral(int16(math.MinInt16), sql.Int16),
			expression.NewLiteral(int16(math.MaxInt16), sql.Int16),
			expression.NewLiteral(uint16(math.MaxUint16), sql.Uint16),
			expression.NewLiteral(int32(math.MinInt32), sql.Int32),
			expression.NewLiteral(int32(math.MaxInt32), sql.Int32),
			expression.NewLiteral(uint32(math.MaxUint32), sql.Uint32),
			expression.NewLiteral(int64(math.MinInt64), sql.Int64),
			expression.NewLiteral(int64(math.MaxInt64), sql.Int64),
			expression.NewLiteral(uint64(math.MaxUint64), sql.Uint64),
		},
		plan.NewUnresolvedTable("dual", ""),
	),
	`CREATE VIEW v AS SELECT * FROM foo`: plan.NewCreateView(
		sql.UnresolvedDatabase(""),
		"v",
		[]string{},
		plan.NewSubqueryAlias(
			"v", "SELECT * FROM foo",
			plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewUnresolvedTable("foo", ""),
			),
		),
		false,
	),
	`CREATE VIEW myview AS SELECT AVG(DISTINCT foo) FROM b`: plan.NewCreateView(
		sql.UnresolvedDatabase(""),
		"myview",
		[]string{},
		plan.NewSubqueryAlias(
			"myview", "SELECT AVG(DISTINCT foo) FROM b",
			plan.NewGroupBy(
				[]sql.Expression{
					expression.NewUnresolvedFunction("avg", true, nil, expression.NewDistinctExpression(expression.NewUnresolvedColumn("foo"))),
				},
				[]sql.Expression{},
				plan.NewUnresolvedTable("b", ""),
			),
		),
		false,
	),
	`CREATE OR REPLACE VIEW v AS SELECT * FROM foo`: plan.NewCreateView(
		sql.UnresolvedDatabase(""),
		"v",
		[]string{},
		plan.NewSubqueryAlias(
			"v", "SELECT * FROM foo",
			plan.NewProject(
				[]sql.Expression{expression.NewStar()},
				plan.NewUnresolvedTable("foo", ""),
			),
		),
		true,
	),
	`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW 
   BEGIN 
     UPDATE bar SET x = old.y WHERE z = new.y;
		 DELETE FROM baz WHERE a = old.b;
		 INSERT INTO zzz (a,b) VALUES (old.a, old.b);
   END`: plan.NewCreateTrigger("myTrigger", "before", "update", nil,
		plan.NewUnresolvedTable("foo", ""),
		plan.NewBeginEndBlock(
			plan.NewBlock([]sql.Node{
				plan.NewUpdate(
					plan.NewFilter(
						expression.NewEquals(expression.NewUnresolvedColumn("z"), expression.NewUnresolvedQualifiedColumn("new", "y")),
						plan.NewUnresolvedTable("bar", ""),
					),
					[]sql.Expression{
						expression.NewSetField(expression.NewUnresolvedColumn("x"), expression.NewUnresolvedQualifiedColumn("old", "y")),
					},
				),
				plan.NewDeleteFrom(
					plan.NewFilter(
						expression.NewEquals(expression.NewUnresolvedColumn("a"), expression.NewUnresolvedQualifiedColumn("old", "b")),
						plan.NewUnresolvedTable("baz", ""),
					),
				),
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
	),
	`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW INSERT INTO zzz (a,b) VALUES (old.a, old.b)`: plan.NewCreateTrigger("myTrigger", "before", "update", nil,
		plan.NewUnresolvedTable("foo", ""),
		plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("zzz", ""), plan.NewValues([][]sql.Expression{{
			expression.NewUnresolvedQualifiedColumn("old", "a"),
			expression.NewUnresolvedQualifiedColumn("old", "b"),
		}},
		), false, []string{"a", "b"}, []sql.Expression{}, false),
		`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW INSERT INTO zzz (a,b) VALUES (old.a, old.b)`,
		`INSERT INTO zzz (a,b) VALUES (old.a, old.b)`,
	),
	`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW FOLLOWS yourTrigger INSERT INTO zzz (a,b) VALUES (old.a, old.b)`: plan.NewCreateTrigger("myTrigger", "before", "update",
		&plan.TriggerOrder{PrecedesOrFollows: sqlparser.FollowsStr, OtherTriggerName: "yourTrigger"},
		plan.NewUnresolvedTable("foo", ""),
		plan.NewInsertInto(sql.UnresolvedDatabase(""), plan.NewUnresolvedTable("zzz", ""), plan.NewValues([][]sql.Expression{{
			expression.NewUnresolvedQualifiedColumn("old", "a"),
			expression.NewUnresolvedQualifiedColumn("old", "b"),
		}},
		), false, []string{"a", "b"}, []sql.Expression{}, false),
		`CREATE TRIGGER myTrigger BEFORE UPDATE ON foo FOR EACH ROW FOLLOWS yourTrigger INSERT INTO zzz (a,b) VALUES (old.a, old.b)`,
		`INSERT INTO zzz (a,b) VALUES (old.a, old.b)`,
	),
	`SELECT 2 UNION SELECT 3`: plan.NewDistinct(
		plan.NewUnion(
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(2), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(3), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
		),
	),
	`(SELECT 2) UNION (SELECT 3)`: plan.NewDistinct(
		plan.NewUnion(
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(2), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(3), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
		),
	),
	`SELECT 2 UNION ALL SELECT 3 UNION DISTINCT SELECT 4`: plan.NewDistinct(
		plan.NewUnion(
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int8(2), sql.Int8)},
					plan.NewUnresolvedTable("dual", ""),
				),
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int8(3), sql.Int8)},
					plan.NewUnresolvedTable("dual", ""),
				),
			),
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(4), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
		),
	),
	`SELECT 2 UNION SELECT 3 UNION ALL SELECT 4`: plan.NewUnion(
		plan.NewDistinct(
			plan.NewUnion(
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int8(2), sql.Int8)},
					plan.NewUnresolvedTable("dual", ""),
				),
				plan.NewProject(
					[]sql.Expression{expression.NewLiteral(int8(3), sql.Int8)},
					plan.NewUnresolvedTable("dual", ""),
				),
			),
		),
		plan.NewProject(
			[]sql.Expression{expression.NewLiteral(int8(4), sql.Int8)},
			plan.NewUnresolvedTable("dual", ""),
		),
	),
	`SELECT 2 UNION SELECT 3 UNION SELECT 4`: plan.NewDistinct(
		plan.NewUnion(
			plan.NewDistinct(
				plan.NewUnion(
					plan.NewProject(
						[]sql.Expression{expression.NewLiteral(int8(2), sql.Int8)},
						plan.NewUnresolvedTable("dual", ""),
					),
					plan.NewProject(
						[]sql.Expression{expression.NewLiteral(int8(3), sql.Int8)},
						plan.NewUnresolvedTable("dual", ""),
					),
				),
			),
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(4), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
		),
	),
	`SELECT 2 UNION (SELECT 3 UNION SELECT 4)`: plan.NewDistinct(
		plan.NewUnion(
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(2), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
			plan.NewDistinct(
				plan.NewUnion(
					plan.NewProject(
						[]sql.Expression{expression.NewLiteral(int8(3), sql.Int8)},
						plan.NewUnresolvedTable("dual", ""),
					),
					plan.NewProject(
						[]sql.Expression{expression.NewLiteral(int8(4), sql.Int8)},
						plan.NewUnresolvedTable("dual", ""),
					),
				),
			),
		),
	),
	`SELECT 2 UNION ALL SELECT 3`: plan.NewUnion(
		plan.NewProject(
			[]sql.Expression{expression.NewLiteral(int8(2), sql.Int8)},
			plan.NewUnresolvedTable("dual", ""),
		),
		plan.NewProject(
			[]sql.Expression{expression.NewLiteral(int8(3), sql.Int8)},
			plan.NewUnresolvedTable("dual", ""),
		),
	),
	`SELECT 2 UNION DISTINCT SELECT 3`: plan.NewDistinct(
		plan.NewUnion(
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(2), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
			plan.NewProject(
				[]sql.Expression{expression.NewLiteral(int8(3), sql.Int8)},
				plan.NewUnresolvedTable("dual", ""),
			),
		),
	),
	`CREATE DATABASE test`:               plan.NewCreateDatabase("test", false),
	`CREATE DATABASE IF NOT EXISTS test`: plan.NewCreateDatabase("test", true),
	`DROP DATABASE test`:                 plan.NewDropDatabase("test", false),
	`DROP DATABASE IF EXISTS test`:       plan.NewDropDatabase("test", true),
}

func TestParse(t *testing.T) {
	var queriesInOrder []string
	for q := range fixtures {
		queriesInOrder = append(queriesInOrder, q)
	}
	sort.Strings(queriesInOrder)

	for _, query := range queriesInOrder {
		expectedPlan := fixtures[query]
		t.Run(query, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()
			p, err := Parse(ctx, query)
			require.NoError(err)
			if !assertNodesEqualWithDiff(t, expectedPlan, p) {
				t.Logf("Unexpected result for query %s", query)
			}
		})
	}
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
	`SHOW METHEMONEY`:                                         ErrUnsupportedFeature,
	`SELECT INTERVAL 1 DAY - '2018-05-01'`:                    ErrUnsupportedSyntax,
	`SELECT INTERVAL 1 DAY * '2018-05-01'`:                    ErrUnsupportedSyntax,
	`SELECT '2018-05-01' * INTERVAL 1 DAY`:                    ErrUnsupportedSyntax,
	`SELECT '2018-05-01' / INTERVAL 1 DAY`:                    ErrUnsupportedSyntax,
	`SELECT INTERVAL 1 DAY + INTERVAL 1 DAY`:                  ErrUnsupportedSyntax,
	`SELECT '2018-05-01' + (INTERVAL 1 DAY + INTERVAL 1 DAY)`: ErrUnsupportedSyntax,
	"DESCRIBE FORMAT=pretty SELECT * FROM foo":                errInvalidDescribeFormat,
	`CREATE TABLE test (pk int, primary key(pk, noexist))`:    ErrUnknownIndexColumn,
	`SELECT a, count(i) over (order by x) FROM foo`:           ErrUnsupportedFeature,
	`SELECT a, count(i) over (partition by y) FROM foo`:       ErrUnsupportedFeature,
	`SELECT i, row_number() over (order by a) group by 1`:     ErrUnsupportedFeature,
	`SELECT i, row_number() over (order by a), max(b)`:        ErrUnsupportedFeature,
}

func TestParseErrors(t *testing.T) {
	for query, expectedError := range fixturesErrors {
		t.Run(query, func(t *testing.T) {
			require := require.New(t)
			ctx := sql.NewEmptyContext()
			_, err := Parse(ctx, query)
			require.Error(err)
			require.True(expectedError.Is(err), "Expected %T but got %T", expectedError, err)
		})
	}
}

func TestFixSetQuery(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"set session foo = 1, session bar = 2", "set @@session.foo = 1, @@session.bar = 2"},
		{"set global foo = 1, session bar = 2", "set @@global.foo = 1, @@session.bar = 2"},
		{"set SESSION foo = 1, GLOBAL bar = 2", "set @@session.foo = 1, @@global.bar = 2"},
	}

	for _, tt := range testCases {
		t.Run(tt.in, func(t *testing.T) {
			require.Equal(t, tt.out, fixSetQuery(tt.in))
		})
	}
}

func TestPrintTree(t *testing.T) {
	require := require.New(t)
	node, err := Parse(sql.NewEmptyContext(), `
		SELECT t.foo, bar.baz
		FROM tbl t
		INNER JOIN bar
			ON foo = baz
		WHERE foo > qux
		LIMIT 5
		OFFSET 2`)
	require.NoError(err)
	require.Equal(`Limit(5)
 └─ Offset(2)
     └─ Project(t.foo, bar.baz)
         └─ Filter(foo > qux)
             └─ InnerJoin(foo = baz)
                 ├─ TableAlias(t)
                 │   └─ UnresolvedTable(tbl)
                 └─ UnresolvedTable(bar)
`, node.String())
}
