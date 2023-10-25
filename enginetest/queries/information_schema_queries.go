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

package queries

import (
	"time"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/stats"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var InfoSchemaQueries = []QueryTest{
	{
		Query:    "SHOW KEYS FROM `columns` FROM `information_schema`;",
		Expected: []sql.Row{},
	},
	{
		Query: `SELECT 
     table_name, index_name, comment, non_unique, GROUP_CONCAT(column_name ORDER BY seq_in_index) AS COLUMNS 
   FROM information_schema.statistics 
   WHERE table_schema='mydb' AND table_name='mytable' AND index_name!="PRIMARY" 
   GROUP BY index_name;`,
		ExpectedColumns: sql.Schema{
			{
				Name: "TABLE_NAME",
				Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default),
			},
			{
				Name: "INDEX_NAME",
				Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default),
			},
			{
				Name: "COMMENT",
				Type: types.MustCreateString(sqltypes.VarChar, 8, sql.Collation_Information_Schema_Default),
			},
			{
				Name: "NON_UNIQUE",
				Type: types.Int32,
			},
			{
				Name: "COLUMNS",
				Type: types.Text,
			},
		},
		Expected: []sql.Row{
			{"mytable", "idx_si", "", 1, "s,i"},
			{"mytable", "mytable_i_s", "", 1, "i,s"},
			{"mytable", "mytable_s", "", 0, "s"},
		},
	},
	{
		Query: `select table_name from information_schema.tables where table_name = 'mytable' limit 1;`,
		ExpectedColumns: sql.Schema{
			{
				Name: "TABLE_NAME",
				Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default),
			},
		},
		Expected: []sql.Row{{"mytable"}},
	},
	{
		Query: `select table_catalog, table_schema, table_name from information_schema.tables where table_name = 'mytable' limit 1;`,
		ExpectedColumns: sql.Schema{
			{Name: "TABLE_CATALOG", Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default)},
			{Name: "TABLE_SCHEMA", Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default)},
			{Name: "TABLE_NAME", Type: types.MustCreateString(sqltypes.VarChar, 64, sql.Collation_Information_Schema_Default)},
		},
		Expected: []sql.Row{{"def", "mydb", "mytable"}},
	},
	{
		Query: `select table_name from information_schema.tables where table_schema = 'information_schema' order by table_name;`,
		Expected: []sql.Row{
			{"administrable_role_authorizations"},
			{"applicable_roles"},
			{"character_sets"},
			{"check_constraints"},
			{"collations"},
			{"collation_character_set_applicability"},
			{"columns"},
			{"columns_extensions"},
			{"column_privileges"},
			{"column_statistics"},
			{"enabled_roles"},
			{"engines"},
			{"events"},
			{"files"},
			{"innodb_buffer_page"},
			{"innodb_buffer_page_lru"},
			{"innodb_buffer_pool_stats"},
			{"innodb_cached_indexes"},
			{"innodb_cmp"},
			{"innodb_cmpmem"},
			{"innodb_cmpmem_reset"},
			{"innodb_cmp_per_index"},
			{"innodb_cmp_per_index_reset"},
			{"innodb_cmp_reset"},
			{"innodb_columns"},
			{"innodb_datafiles"},
			{"innodb_fields"},
			{"innodb_foreign"},
			{"innodb_foreign_cols"},
			{"innodb_ft_being_deleted"},
			{"innodb_ft_config"},
			{"innodb_ft_default_stopword"},
			{"innodb_ft_deleted"},
			{"innodb_ft_index_cache"},
			{"innodb_ft_index_table"},
			{"innodb_indexes"},
			{"innodb_metrics"},
			{"innodb_session_temp_tablespaces"},
			{"innodb_tables"},
			{"innodb_tablespaces"},
			{"innodb_tablespaces_brief"},
			{"innodb_tablestats"},
			{"innodb_temp_table_info"},
			{"innodb_trx"},
			{"innodb_virtual"},
			{"keywords"},
			{"key_column_usage"},
			{"optimizer_trace"},
			{"parameters"},
			{"partitions"},
			{"plugins"},
			{"processlist"},
			{"profiling"},
			{"referential_constraints"},
			{"resource_groups"},
			{"role_column_grants"},
			{"role_routine_grants"},
			{"role_table_grants"},
			{"routines"},
			{"schemata"},
			{"schemata_extensions"},
			{"schema_privileges"},
			{"statistics"},
			{"st_geometry_columns"},
			{"st_spatial_reference_systems"},
			{"st_units_of_measure"},
			{"tables"},
			{"tablespaces"},
			{"tablespaces_extensions"},
			{"tables_extensions"},
			{"table_constraints"},
			{"table_constraints_extensions"},
			{"table_privileges"},
			{"triggers"},
			{"user_attributes"},
			{"user_privileges"},
			{"views"},
			{"view_routine_usage"},
			{"view_table_usage"},
		},
	},
	{
		Query: "SHOW TABLES",
		Expected: []sql.Row{
			{"myview"},
			{"fk_tbl"},
			{"mytable"},
		},
	},
	{
		Query: "SHOW FULL TABLES",
		Expected: []sql.Row{
			{"fk_tbl", "BASE TABLE"},
			{"myview", "VIEW"},
			{"mytable", "BASE TABLE"},
		},
	},
	{
		Query: "SHOW TABLES FROM foo",
		Expected: []sql.Row{
			{"othertable"},
		},
	},
	{
		Query: "SHOW TABLES LIKE '%table'",
		Expected: []sql.Row{
			{"mytable"},
		},
	},
	{
		Query: `SHOW COLUMNS FROM mytable`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "NULL", ""},
			{"s", "varchar(20)", "NO", "UNI", "NULL", ""},
		},
	},
	{
		Query: `DESCRIBE mytable`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "NULL", ""},
			{"s", "varchar(20)", "NO", "UNI", "NULL", ""},
		},
	},
	{
		Query: `DESC mytable`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "NULL", ""},
			{"s", "varchar(20)", "NO", "UNI", "NULL", ""},
		},
	},
	{
		Query: `SHOW COLUMNS FROM mytable WHERE Field = 'i'`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "NULL", ""},
		},
	},
	{
		Query: `SHOW COLUMNS FROM mytable LIKE 'i'`,
		Expected: []sql.Row{
			{"i", "bigint", "NO", "PRI", "NULL", ""},
		},
	},
	{
		Query: `SHOW FULL COLUMNS FROM mytable`,
		Expected: []sql.Row{
			{"i", "bigint", nil, "NO", "PRI", "NULL", "", "", ""},
			{"s", "varchar(20)", "utf8mb4_0900_bin", "NO", "UNI", "NULL", "", "", "column s"},
		},
	},
	{
		Query: "SHOW TABLES WHERE `Tables_in_mydb` = 'mytable'",
		Expected: []sql.Row{
			{"mytable"},
		},
	},
	{
		Query: `
		SELECT
			LOGFILE_GROUP_NAME, FILE_NAME, TOTAL_EXTENTS, INITIAL_SIZE, ENGINE, EXTRA
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'UNDO LOG'
			AND FILE_NAME IS NOT NULL
			AND LOGFILE_GROUP_NAME IS NOT NULL
		GROUP BY LOGFILE_GROUP_NAME, FILE_NAME, ENGINE, TOTAL_EXTENTS, INITIAL_SIZE
		ORDER BY LOGFILE_GROUP_NAME
		`,
		Expected: nil,
	},
	{
		Query: `
		SELECT DISTINCT
			TABLESPACE_NAME, FILE_NAME, LOGFILE_GROUP_NAME, EXTENT_SIZE, INITIAL_SIZE, ENGINE
		FROM INFORMATION_SCHEMA.FILES
		WHERE FILE_TYPE = 'DATAFILE'
		ORDER BY TABLESPACE_NAME, LOGFILE_GROUP_NAME
		`,
		Expected: nil,
	},
	{
		Query: `
		SELECT TABLE_NAME FROM information_schema.TABLES
		WHERE TABLE_SCHEMA='mydb' AND (TABLE_TYPE='BASE TABLE' OR TABLE_TYPE='VIEW')
		ORDER BY 1
		`,
		Expected: []sql.Row{
			{"fk_tbl"},
			{"mytable"},
			{"myview"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='mytable'
		`,
		Expected: []sql.Row{
			{"s", "varchar"},
			{"i", "bigint"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY COLUMN_NAME
		`,
		Expected: []sql.Row{
			{"s"},
			{"i"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1
		`,
		Expected: []sql.Row{
			{"s"},
			{"i"},
		},
	},
	{
		Query: `
		SELECT COLUMN_NAME AS COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1
		`,
		Expected: []sql.Row{
			{"s"},
			{"i"},
		},
	},
	{
		Query: `SHOW INDEXES FROM mytaBLE`,
		Expected: []sql.Row{
			{"mytable", 0, "PRIMARY", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 0, "mytable_s", 1, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 2, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "idx_si", 1, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "idx_si", 2, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		Query: `SHOW KEYS FROM mytaBLE`,
		Expected: []sql.Row{
			{"mytable", 0, "PRIMARY", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 0, "mytable_s", 1, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 1, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "mytable_i_s", 2, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "idx_si", 1, "s", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
			{"mytable", 1, "idx_si", 2, "i", nil, 0, nil, nil, "", "BTREE", "", "", "YES", nil},
		},
	},
	{
		Query: `SHOW CREATE TABLE mytaBLE`,
		Expected: []sql.Row{
			{"mytable", "CREATE TABLE `mytable` (\n" +
				"  `i` bigint NOT NULL,\n" +
				"  `s` varchar(20) NOT NULL COMMENT 'column s',\n" +
				"  PRIMARY KEY (`i`),\n" +
				"  KEY `idx_si` (`s`,`i`),\n" +
				"  KEY `mytable_i_s` (`i`,`s`),\n" +
				"  UNIQUE KEY `mytable_s` (`s`)\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
	{
		Query: `SHOW CREATE TABLE fk_TBL`,
		Expected: []sql.Row{
			{"fk_tbl", "CREATE TABLE `fk_tbl` (\n" +
				"  `pk` bigint NOT NULL,\n" +
				"  `a` bigint,\n" +
				"  `b` varchar(20),\n" +
				"  PRIMARY KEY (`pk`),\n" +
				"  KEY `ab` (`a`,`b`),\n" +
				"  CONSTRAINT `fk1` FOREIGN KEY (`a`,`b`) REFERENCES `mytable` (`i`,`s`) ON DELETE CASCADE\n" +
				") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin"},
		},
	},
	{

		Query: "SELECT table_name, `auto_increment` FROM information_schema.tables " +
			"WHERE TABLE_SCHEMA='mydb' AND TABLE_TYPE='BASE TABLE' ORDER BY 1",
		Expected: []sql.Row{
			{"fk_tbl", nil},
			{"mytable", nil},
		},
	},
	{
		Query: "SHOW ENGINES",
		Expected: []sql.Row{
			{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
		},
	},
	{
		Query: "SELECT * FROM information_schema.table_constraints ORDER BY table_name, constraint_type;",
		Expected: []sql.Row{
			{"def", "mydb", "fk1", "mydb", "fk_tbl", "FOREIGN KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "fk_tbl", "PRIMARY KEY", "YES"},
			{"def", "mydb", "PRIMARY", "mydb", "mytable", "PRIMARY KEY", "YES"},
			{"def", "mydb", "mytable_s", "mydb", "mytable", "UNIQUE", "YES"},
			{"def", "foo", "PRIMARY", "foo", "othertable", "PRIMARY KEY", "YES"},
		},
	},
	{
		Query:    "SELECT * FROM information_schema.check_constraints ORDER BY constraint_schema, constraint_name, check_clause ",
		Expected: []sql.Row{},
	},
	{
		Query: "SELECT * FROM information_schema.key_column_usage ORDER BY constraint_schema, table_name",
		Expected: []sql.Row{
			{"def", "foo", "PRIMARY", "def", "foo", "othertable", "text", 1, nil, nil, nil, nil},
			{"def", "mydb", "PRIMARY", "def", "mydb", "fk_tbl", "pk", 1, nil, nil, nil, nil},
			{"def", "mydb", "fk1", "def", "mydb", "fk_tbl", "a", 1, 1, "mydb", "mytable", "i"},
			{"def", "mydb", "fk1", "def", "mydb", "fk_tbl", "b", 2, 2, "mydb", "mytable", "s"},
			{"def", "mydb", "PRIMARY", "def", "mydb", "mytable", "i", 1, nil, nil, nil, nil},
			{"def", "mydb", "mytable_s", "def", "mydb", "mytable", "s", 1, nil, nil, nil, nil},
		},
	},
	{
		Query: `
				select CONCAT(tbl.table_schema, '.', tbl.table_name) as the_table,
				       col.column_name, GROUP_CONCAT(kcu.column_name SEPARATOR ',') as pk
				from information_schema.tables as tbl
				join information_schema.columns as col
				  on tbl.table_name = col.table_name
				join information_schema.key_column_usage as kcu
				  on tbl.table_name = kcu.table_name
				join information_schema.table_constraints as tc
				  on kcu.constraint_name = tc.constraint_name
				where tbl.table_schema = 'mydb' and
					  tbl.table_name = kcu.table_name and
					  tc.constraint_type = 'PRIMARY KEY' and
					  col.column_name like 'pk%'
				group by the_table, col.column_name
				`,
		Expected: []sql.Row{
			{"mydb.fk_tbl", "pk", "pk,pk,pk"},
		},
	},
	{
		Query:    `SELECT count(*) FROM information_schema.COLLATIONS`,
		Expected: []sql.Row{{286}},
	},
	{
		Query: `SELECT * FROM information_schema.COLLATIONS ORDER BY collation_name LIMIT 4`,
		Expected: []sql.Row{
			{"armscii8_bin", "armscii8", uint64(64), "", "Yes", uint32(1), "PAD SPACE"},
			{"armscii8_general_ci", "armscii8", uint64(32), "Yes", "Yes", uint32(1), "PAD SPACE"},
			{"ascii_bin", "ascii", uint64(65), "", "Yes", uint32(1), "PAD SPACE"},
			{"ascii_general_ci", "ascii", uint64(11), "Yes", "Yes", uint32(1), "PAD SPACE"},
		},
	},
	{
		Query: `SELECT * FROM information_schema.COLLATION_CHARACTER_SET_APPLICABILITY ORDER BY collation_name LIMIT 4 `,
		Expected: []sql.Row{
			{"armscii8_bin", "armscii8"},
			{"armscii8_general_ci", "armscii8"},
			{"ascii_bin", "ascii"},
			{"ascii_general_ci", "ascii"},
		},
	},
	{
		Query: `SELECT * FROM information_schema.ENGINES ORDER BY engine`,
		Expected: []sql.Row{
			{"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys", "YES", "YES", "YES"},
		},
	},
	{
		Query:    `SELECT * from information_schema.administrable_role_authorizations`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.applicable_roles`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.column_privileges`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.optimizer_trace`,
		Expected: []sql.Row{},
	},
	{
		Query:    "SELECT * FROM information_schema.partitions",
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.plugins`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.profiling`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.resource_groups`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.role_column_grants`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.role_routine_grants`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.tablespaces`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.tablespaces_extensions`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.view_routine_usage`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * FROM information_schema.view_table_usage`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_buffer_page`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_buffer_page_lru`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_buffer_pool_stats`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_cached_indexes`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_cmp`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_cmp_reset`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_cmpmem`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_cmpmem_reset`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_cmp_per_index`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_cmp_per_index_reset`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_columns`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_datafiles`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_fields`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_foreign`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_foreign_cols`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_ft_being_deleted`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_ft_config`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_ft_default_stopword`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_ft_deleted`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_ft_index_cache`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_ft_index_table`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_indexes`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_metrics`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_session_temp_tablespaces`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_tables`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_tablespaces`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_tablespaces_brief`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_tablestats`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_temp_table_info`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_trx`,
		Expected: []sql.Row{},
	},
	{
		Query:    `SELECT * from information_schema.innodb_virtual`,
		Expected: []sql.Row{},
	},
	{
		Query: `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, SEQ_IN_INDEX, 'PRIMARY' AS PK_NAME 
FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = 'mydb' AND INDEX_NAME='PRIMARY' ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;`,
		Expected: []sql.Row{
			{"mydb", "fk_tbl", "pk", 1, "PRIMARY"},
			{"mydb", "mytable", "i", 1, "PRIMARY"},
		},
	},
	{
		Query:    "select * from information_schema.character_sets;",
		Expected: []sql.Row{{"utf8mb4", "utf8mb4_0900_ai_ci", "UTF-8 Unicode", uint32(4)}},
	},
	{
		Query: `show columns from fk_tbl from mydb`,
		Expected: []sql.Row{
			{"pk", "bigint", "NO", "PRI", "NULL", ""},
			{"a", "bigint", "YES", "MUL", "NULL", ""},
			{"b", "varchar(20)", "YES", "", "NULL", ""},
		},
	},
	{
		Query: "SELECT * FROM information_schema.referential_constraints where CONSTRAINT_SCHEMA = 'mydb'",
		Expected: []sql.Row{
			{"def", "mydb", "fk1", "def", "mydb", nil, "NONE", "NO ACTION", "CASCADE", "fk_tbl", "mytable"},
		},
	},
	{
		Query:    "SELECT count(*) FROM information_schema.keywords",
		Expected: []sql.Row{{747}},
	},
	{
		Query: "SELECT * FROM information_schema.st_spatial_reference_systems order by srs_id desc limit 10",
		Expected: []sql.Row{
			{`WGS 84 / TM 36 SE`, uint32(32766), `EPSG`, 32766, `PROJCS["WGS 84 / TM 36 SE",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",36,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32766"]]`, nil},
			{`WGS 84 / UPS South (N,E)`, uint32(32761), `EPSG`, 32761, `PROJCS["WGS 84 / UPS South (N,E)",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Polar Stereographic (variant A)",AUTHORITY["EPSG","9810"]],PARAMETER["Latitude of natural origin",-90,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",0,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.994,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",2000000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",2000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["N",NORTH],AXIS["E",NORTH],AUTHORITY["EPSG","32761"]]`, nil},
			{`WGS 84 / UTM zone 60S`, uint32(32760), `EPSG`, 32760, `PROJCS["WGS 84 / UTM zone 60S",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",177,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32760"]]`, nil},
			{`WGS 84 / UTM zone 59S`, uint32(32759), `EPSG`, 32759, `PROJCS["WGS 84 / UTM zone 59S",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",171,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32759"]]`, nil},
			{`WGS 84 / UTM zone 58S`, uint32(32758), `EPSG`, 32758, `PROJCS["WGS 84 / UTM zone 58S",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",165,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32758"]]`, nil},
			{`WGS 84 / UTM zone 57S`, uint32(32757), `EPSG`, 32757, `PROJCS["WGS 84 / UTM zone 57S",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",159,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32757"]]`, nil},
			{`WGS 84 / UTM zone 56S`, uint32(32756), `EPSG`, 32756, `PROJCS["WGS 84 / UTM zone 56S",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",153,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32756"]]`, nil},
			{`WGS 84 / UTM zone 55S`, uint32(32755), `EPSG`, 32755, `PROJCS["WGS 84 / UTM zone 55S",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",147,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32755"]]`, nil},
			{`WGS 84 / UTM zone 54S`, uint32(32754), `EPSG`, 32754, `PROJCS["WGS 84 / UTM zone 54S",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",141,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32754"]]`, nil},
			{`WGS 84 / UTM zone 53S`, uint32(32753), `EPSG`, 32753, `PROJCS["WGS 84 / UTM zone 53S",GEOGCS["WGS 84",DATUM["World Geodetic System 1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.017453292519943278,AUTHORITY["EPSG","9122"]],AXIS["Lat",NORTH],AXIS["Lon",EAST],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse Mercator",AUTHORITY["EPSG","9807"]],PARAMETER["Latitude of natural origin",0,AUTHORITY["EPSG","8801"]],PARAMETER["Longitude of natural origin",135,AUTHORITY["EPSG","8802"]],PARAMETER["Scale factor at natural origin",0.9996,AUTHORITY["EPSG","8805"]],PARAMETER["False easting",500000,AUTHORITY["EPSG","8806"]],PARAMETER["False northing",10000000,AUTHORITY["EPSG","8807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["E",EAST],AXIS["N",NORTH],AUTHORITY["EPSG","32753"]]`, nil},
		},
	},
	{
		Query:    "SELECT count(*) FROM information_schema.st_units_of_measure",
		Expected: []sql.Row{{47}},
	},
	{
		Query:    "SELECT * FROM information_schema.schemata_extensions",
		Expected: []sql.Row{{"def", "information_schema", ""}, {"def", "foo", ""}, {"def", "mydb", ""}},
	},
	{
		Query:    `SELECT * FROM information_schema.columns_extensions where table_name = 'mytable'`,
		Expected: []sql.Row{{"def", "mydb", "mytable", "i", nil, nil}, {"def", "mydb", "mytable", "s", nil, nil}},
	},
	{
		Query:    `SELECT * FROM information_schema.table_constraints_extensions where table_name = 'fk_tbl'`,
		Expected: []sql.Row{{"def", "mydb", "PRIMARY", "fk_tbl", nil, nil}, {"def", "mydb", "ab", "fk_tbl", nil, nil}},
	},
	{
		Query:    `SELECT * FROM information_schema.tables_extensions where table_name = 'mytable'`,
		Expected: []sql.Row{{"def", "mydb", "mytable", nil, nil}},
	},
	{
		Query:    "SELECT table_rows FROM INFORMATION_SCHEMA.TABLES where table_name='mytable'",
		Expected: []sql.Row{{uint64(3)}},
	},
	{
		Query:    "select table_name from information_schema.tables where table_schema collate utf8_general_ci = 'information_schema' and table_name collate utf8_general_ci = 'parameters'",
		Expected: []sql.Row{{"parameters"}},
	},
}

var SkippedInfoSchemaQueries = []QueryTest{
	{
		// TODO: this query works in MySQL, but getting `Illegal mix of collations (utf8mb3_general_ci) and (utf8mb4_0900_bin)` error
		Query: `
		SELECT COLUMN_NAME AS COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME LIKE '%table'
		GROUP BY 1 HAVING SUBSTRING(COLUMN_NAME, 1, 1) = "s"
		`,
		Expected: []sql.Row{{"s"}},
	},
}

var InfoSchemaScripts = []ScriptTest{
	{
		Name: "query does not use optimization rule on LIKE clause because info_schema db charset is utf8mb3",
		SetUpScript: []string{
			"CREATE TABLE t1 (a int, condition_choose varchar(10));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select column_name from information_schema.columns where column_name like 'condition%';",
				Expected: []sql.Row{{"condition_choose"}},
			},
			{
				Query:    "select column_name from information_schema.columns where column_name like '%condition%';",
				Expected: []sql.Row{{"ACTION_CONDITION"}, {"condition_choose"}},
			},
		},
	},
	{
		Name: "test databases created with non default collation and charset",
		SetUpScript: []string{
			"CREATE DATABASE test_db CHARACTER SET utf8mb3 COLLATE utf8mb3_bin;",
			"USE test_db",
			"CREATE TABLE small_table (a binary, b VARCHAR(50));",
			"CREATE TABLE test_table (id INT PRIMARY KEY, col1 TEXT, col2 CHAR(20) CHARACTER SET latin1 COLLATE latin1_german1_ci) CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT table_schema, table_name, column_name, character_set_name, collation_name, column_type FROM information_schema.columns where table_schema = 'test_db' order by column_name",
				Expected: []sql.Row{
					{"test_db", "small_table", "a", nil, nil, "binary(1)"},
					{"test_db", "small_table", "b", "utf8mb3", "utf8mb3_bin", "varchar(50)"},
					{"test_db", "test_table", "col1", "utf8mb4", "utf8mb4_0900_bin", "text"},
					{"test_db", "test_table", "col2", "latin1", "latin1_german1_ci", "char(20)"},
					{"test_db", "test_table", "id", nil, nil, "int"},
				},
			},
		},
	},
	{
		Name: "information_schema.table_constraints ignores non-unique indexes",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key, test_score int, height int)",
			"CREATE INDEX myindex on t(test_score)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.table_constraints where table_name='t' ORDER BY constraint_type,constraint_name",
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "mydb", "t", "PRIMARY KEY", "YES"},
				},
			},
		},
	},
	{
		Name: "information_schema.key_column_usage ignores non-unique indexes",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key, test_score int, height int)",
			"CREATE INDEX myindex on t(test_score)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.key_column_usage where table_name='t'",
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "def", "mydb", "t", "pk", 1, nil, nil, nil, nil},
				},
			},
		},
	},
	{
		Name: "information_schema.key_column_usage works with composite foreign and primary keys",
		SetUpScript: []string{
			"CREATE TABLE ptable (pk int primary key, test_score int, height int)",
			"CREATE INDEX myindex on ptable(test_score, height)",
			"CREATE TABLE ptable2 (pk int primary key, test_score2 int, height2 int, CONSTRAINT fkr FOREIGN KEY (test_score2, height2) REFERENCES ptable(test_score,height));",

			"CREATE TABLE atable (pk int, test_score int, height int, PRIMARY KEY (pk, test_score))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.key_column_usage where table_name='ptable2' ORDER BY constraint_name",
				Expected: []sql.Row{
					{"def", "mydb", "fkr", "def", "mydb", "ptable2", "test_score2", 1, 1, "mydb", "ptable", "test_score"},
					{"def", "mydb", "fkr", "def", "mydb", "ptable2", "height2", 2, 2, "mydb", "ptable", "height"},
					{"def", "mydb", "PRIMARY", "def", "mydb", "ptable2", "pk", 1, nil, nil, nil, nil},
				},
			},
			{
				Query: "SELECT * FROM information_schema.key_column_usage where table_name='atable' ORDER BY constraint_name",
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "def", "mydb", "atable", "pk", 1, nil, nil, nil, nil},
					{"def", "mydb", "PRIMARY", "def", "mydb", "atable", "test_score", 2, nil, nil, nil, nil},
				},
			},
		},
	},
	{
		Name: "information_schema.referential_constraints works with primary, non-unique and unique keys",
		SetUpScript: []string{
			"CREATE TABLE my_table (i int primary key, height int, weight int)",
			"CREATE INDEX h on my_TABLE(height)",
			"CREATE UNIQUE INDEX w on my_TABLE(weight)",
			"CREATE TABLE ref_table (a int primary key, height int, weight int)",
			"alter table ref_table add constraint fk_across_dbs_ref_pk foreign key (a) references my_table(i)",
			"alter table ref_table add constraint fk_across_dbs_key foreign key (a) references my_table(height)",
			"alter table ref_table add constraint fk_across_dbs_unique foreign key (a) references my_table(weight)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.referential_constraints where constraint_schema = 'mydb' and table_name = 'ref_table'",
				Expected: []sql.Row{
					{"def", "mydb", "fk_across_dbs_ref_pk", "def", "mydb", "PRIMARY", "NONE", "NO ACTION", "NO ACTION", "ref_table", "my_table"},
					{"def", "mydb", "fk_across_dbs_key", "def", "mydb", nil, "NONE", "NO ACTION", "NO ACTION", "ref_table", "my_table"},
					{"def", "mydb", "fk_across_dbs_unique", "def", "mydb", "w", "NONE", "NO ACTION", "NO ACTION", "ref_table", "my_table"},
				},
			},
		},
	},
	{
		Name: "information_schema.triggers create trigger definer defined",
		SetUpScript: []string{
			"CREATE TABLE aa (x INT PRIMARY KEY, y INT)",
			"CREATE DEFINER=`dolt`@`localhost` TRIGGER trigger1 BEFORE INSERT ON aa FOR EACH ROW SET NEW.x = NEW.x + 1",
			"CREATE TRIGGER trigger2 BEFORE INSERT ON aa FOR EACH ROW SET NEW.y = NEW.y + 2",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT trigger_name, event_object_table, definer FROM INFORMATION_SCHEMA.TRIGGERS WHERE trigger_name = 'trigger1'",
				Expected: []sql.Row{
					{"trigger1", "aa", "dolt@localhost"},
				},
			},
			{
				Query: `SELECT trigger_catalog, trigger_schema, trigger_name, event_manipulation, event_object_catalog,
event_object_schema, event_object_table, action_order, action_condition, action_statement, action_orientation, action_timing,
action_reference_old_table, action_reference_new_table, action_reference_old_row, action_reference_new_row, sql_mode, definer,
character_set_client, collation_connection, database_collation
FROM INFORMATION_SCHEMA.TRIGGERS WHERE trigger_schema = 'mydb'`,
				Expected: []sql.Row{
					{"def", "mydb", "trigger1", "INSERT", "def", "mydb", "aa", 1, nil, "SET NEW.x = NEW.x + 1", "ROW", "BEFORE", nil, nil, "OLD", "NEW",
						"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", "dolt@localhost", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
					{"def", "mydb", "trigger2", "INSERT", "def", "mydb", "aa", 2, nil, "SET NEW.y = NEW.y + 2", "ROW", "BEFORE", nil, nil, "OLD", "NEW",
						"STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY", "root@localhost", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "information_schema.statistics shows non unique index",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key, test_score int, height int)",
			"CREATE INDEX myindex on t(test_score)",
			"INSERT INTO t VALUES (2,23,25), (3,24,26)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.statistics where table_name='t'",
				Expected: []sql.Row{
					{"def", "mydb", "t", 1, "mydb", "myindex", 1, "test_score", "A", 0, nil, nil, "YES", "BTREE", "", "", "YES", nil},
					{"def", "mydb", "t", 0, "mydb", "PRIMARY", 1, "pk", "A", 0, nil, nil, "", "BTREE", "", "", "YES", nil},
				},
			},
		},
	},
	{
		Name: "information_schema.columns shows default value",
		SetUpScript: []string{
			"CREATE TABLE t (pk int primary key, fname varchar(20), lname varchar(20), height int)",
			"ALTER TABLE t CHANGE fname fname varchar(20) NOT NULL DEFAULT ''",
			"ALTER TABLE t CHANGE lname lname varchar(20) NOT NULL DEFAULT 'ln'",
			"ALTER TABLE t CHANGE height h int DEFAULT NULL",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT table_name, column_name, column_default, is_nullable FROM information_schema.columns where table_name='t' order by 1,2",
				Expected: []sql.Row{
					{"t", "fname", "", "NO"},
					{"t", "h", nil, "YES"},
					{"t", "lname", "ln", "NO"},
					{"t", "pk", nil, "NO"},
				},
			},
		},
	},
	{
		Name: "information_schema.columns shows default value with more types",
		SetUpScript: []string{
			"CREATE TABLE test_table (pk int primary key, col2 float NOT NULL DEFAULT 4.5, col3 double NOT NULL DEFAULT 3.14159, col4 datetime NULL DEFAULT '2008-04-22 16:16:16', col5 boolean NULL DEFAULT FALSE)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT table_name, column_name, column_default, is_nullable FROM information_schema.CoLuMnS where table_name='test_table'",
				Expected: []sql.Row{
					{"test_table", "pk", nil, "NO"},
					{"test_table", "col2", "4.5", "NO"},
					{"test_table", "col3", "3.14159", "NO"},
					{"test_table", "col4", "2008-04-22 16:16:16", "YES"},
					{"test_table", "col5", "0", "YES"},
				},
			},
		},
	},
	{
		Name: "information_schema.columns shows default value with more types",
		SetUpScript: []string{
			"CREATE TABLE test_table (pk int primary key, col2 float DEFAULT (length('he`Llo')), col3 int DEFAULT (greatest(`pk`, 2)), col4 int DEFAULT (5 + 5), col5 datetime default NOW(), create_time timestamp(6) NOT NULL DEFAULT NOW(6));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT table_name, column_name, column_default, is_nullable FROM information_schema.columns where table_name='test_table'",
				Expected: []sql.Row{
					{"test_table", "pk", nil, "NO"},
					{"test_table", "col2", "length('he`Llo')", "YES"},
					{"test_table", "col3", "greatest(pk,2)", "YES"},
					{"test_table", "col4", "(5 + 5)", "YES"},
					{"test_table", "col5", "CURRENT_TIMESTAMP", "YES"},
					{"test_table", "create_time", "CURRENT_TIMESTAMP(6)", "NO"},
				},
			},
		},
	},
	{
		Name: "information_schema.columns correctly shows numeric precision and scale for a wide variety of types",
		SetUpScript: []string{
			"CREATE TABLE `digits` (`c0` tinyint,`c1` tinyint unsigned,`c2` smallint,`c3` smallint unsigned,`c4` mediumint,`c5` mediumint unsigned,`c6` int,`c7` int unsigned,`c8` bigint,`c9` bigint unsigned,`c10` float,`c11` dec(5,2),`st` varchar(100))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select column_name, numeric_precision, numeric_scale from information_schema.columns where table_name='digits' order by ordinal_position;",
				Expected: []sql.Row{
					{"c0", 3, 0},
					{"c1", 3, 0},
					{"c2", 5, 0},
					{"c3", 5, 0},
					{"c4", 7, 0},
					{"c5", 7, 0},
					{"c6", 10, 0},
					{"c7", 10, 0},
					{"c8", 19, 0},
					{"c9", 20, 0},
					{"c10", 12, nil},
					{"c11", 5, 2},
					{"st", nil, nil},
				},
			},
		},
	},
	{
		Name: "information_schema.routines",
		SetUpScript: []string{
			"CREATE PROCEDURE p1() COMMENT 'hi' DETERMINISTIC SELECT 6",
			"CREATE definer=`user` PROCEDURE p2() SQL SECURITY INVOKER SELECT 7",
			"CREATE PROCEDURE p21() SQL SECURITY DEFINER SELECT 8",
			"USE foo",
			"CREATE PROCEDURE p12() COMMENT 'hello' DETERMINISTIC SELECT 6",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT specific_name, routine_catalog, routine_schema, routine_name, routine_type, " +
					"data_type, character_maximum_length, character_octet_length, numeric_precision, numeric_scale, " +
					"datetime_precision, character_set_name, collation_name, dtd_identifier, " +
					"routine_body, external_name, external_language, parameter_style, is_deterministic, " +
					"sql_data_access, sql_path, security_type, sql_mode, routine_comment, definer, " +
					"character_set_client, collation_connection, database_collation FROM information_schema.routines",
				Expected: []sql.Row{
					{"p1", "def", "mydb", "p1", "PROCEDURE", "", nil, nil, nil, nil, nil, nil, nil, nil, "SQL",
						nil, "SQL", "SQL", "YES", "CONTAINS SQL", nil, "DEFINER", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY",
						"hi", "", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
					{"p2", "def", "mydb", "p2", "PROCEDURE", "", nil, nil, nil, nil, nil, nil, nil, nil, "SQL",
						nil, "SQL", "SQL", "NO", "CONTAINS SQL", nil, "INVOKER", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY",
						"", "user@%", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
					{"p12", "def", "foo", "p12", "PROCEDURE", "", nil, nil, nil, nil, nil, nil, nil, nil, "SQL",
						nil, "SQL", "SQL", "YES", "CONTAINS SQL", nil, "DEFINER", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY",
						"hello", "", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
					{"p21", "def", "mydb", "p21", "PROCEDURE", "", nil, nil, nil, nil, nil, nil, nil, nil, "SQL",
						nil, "SQL", "SQL", "NO", "CONTAINS SQL", nil, "DEFINER", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY",
						"", "", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "information_schema.columns for view",
		SetUpScript: []string{
			"USE foo",
			"drop table othertable",
			"CREATE TABLE t (i int)",
			"CREATE VIEW v as select * from t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = 'foo'",
				Expected: []sql.Row{
					{"def", "foo", "t", "i", uint32(1), nil, "YES", "int", nil, nil, int64(10), int64(0), nil, nil, nil, "int", "", "", "insert,references,select,update", "", "", nil},
					{"def", "foo", "v", "", uint32(0), nil, "", nil, nil, nil, nil, nil, nil, "", "", "", "", "", "select", "", "", nil},
				},
			},
		},
	},
	{
		Name: "information_schema.columns with column key check for PRI and UNI",
		SetUpScript: []string{
			"CREATE TABLE about (id int unsigned NOT NULL AUTO_INCREMENT, uuid char(36) NOT NULL, " +
				"status varchar(255) NOT NULL DEFAULT 'draft', date_created timestamp DEFAULT NULL, date_updated timestamp DEFAULT NULL, " +
				"url_key varchar(255) NOT NULL, PRIMARY KEY (uuid), UNIQUE KEY about_url_key_unique (url_key), UNIQUE KEY id (id))",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, COLUMN_TYPE, COLUMN_KEY, CHARACTER_MAXIMUM_LENGTH, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'about'",
				Expected: []sql.Row{
					{"about", "id", nil, "NO", "int unsigned", "UNI", nil, "auto_increment"},
					{"about", "uuid", nil, "NO", "char(36)", "PRI", 36, ""},
					{"about", "status", "draft", "NO", "varchar(255)", "", 255, ""},
					{"about", "date_created", nil, "YES", "timestamp", "", nil, ""},
					{"about", "date_updated", nil, "YES", "timestamp", "", nil, ""},
					{"about", "url_key", nil, "NO", "varchar(255)", "UNI", 255, ""},
				},
			},
		},
	},
	{
		Name: "information_schema.columns with column key check for MUL",
		SetUpScript: []string{
			"create table new_table (id int, name varchar(30), cname varbinary(100));",
			"alter table new_table modify column id int NOT NULL, add key(id);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY, CHARACTER_MAXIMUM_LENGTH, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'new_table'",
				Expected: []sql.Row{
					{"new_table", "id", "NO", "int", "int", "MUL", nil, ""},
					{"new_table", "name", "YES", "varchar", "varchar(30)", "", 30, ""},
					{"new_table", "cname", "YES", "varbinary", "varbinary(100)", "", 100, ""},
				},
			},
		},
	},
	{
		Name: "information_schema.columns with column key check for MUL for only the first column of composite unique key",
		SetUpScript: []string{
			"create table comp_uni (pk int not null, c0 int, c1 int, primary key (pk), unique key c0c1 (c0, c1));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'comp_uni'",
				Expected: []sql.Row{
					{"comp_uni", "pk", "NO", "int", "PRI"},
					{"comp_uni", "c0", "YES", "int", "MUL"},
					{"comp_uni", "c1", "YES", "int", ""},
				},
			},
		},
	},
	{
		Name: "information_schema.columns with column key UNI is displayed as PRI if it cannot contain NULL values and there is no PRIMARY KEY in the table",
		SetUpScript: []string{
			"create table ptable (id int not null, id2 int not null, col1 bool, UNIQUE KEY unique_key (id), UNIQUE KEY unique_key2 (id2));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT TABLE_NAME, COLUMN_NAME, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'ptable'",
				Expected: []sql.Row{
					{"ptable", "id", "NO", "int", "int", "PRI"},
					{"ptable", "id2", "NO", "int", "int", "UNI"},
					{"ptable", "col1", "YES", "tinyint", "tinyint(1)", ""},
				},
			},
		},
	},
	{
		Name: "information_schema.columns with srs_id defined in spatial columns",
		SetUpScript: []string{
			"CREATE TABLE stable (geo GEOMETRY NOT NULL DEFAULT (POINT(2, 5)), line LINESTRING NOT NULL, pnt POINT SRID 4326, pol POLYGON NOT NULL SRID 0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, COLUMN_TYPE, COLUMN_KEY, SRS_ID FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'stable'",
				Expected: []sql.Row{
					{"stable", "geo", "point(2,5)", "NO", "geometry", "geometry", "", nil},
					{"stable", "line", nil, "NO", "linestring", "linestring", "", nil},
					{"stable", "pnt", nil, "YES", "point", "point", "", uint32(4326)},
					{"stable", "pol", nil, "NO", "polygon", "polygon", "", uint32(0)},
				},
			},
		},
	},
	{
		Name: "column specific tests information_schema.statistics table",
		SetUpScript: []string{
			`create table ptable (i int primary key, b blob, c char(10))`,
			`alter table ptable add unique index (c(3))`,
			`alter table ptable add unique index (b(4))`,
			`create index b_and_c on ptable (b(5), c(6))`,
			`insert into ptable values (0 , ('abc'), 'abc'), (1 , ('bcd'), 'bcdefg'), (2 , null, 'bceff')`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select index_name, seq_in_index, column_name, sub_part from information_schema.statistics where table_schema = 'mydb' and table_name = 'ptable' ORDER BY INDEX_NAME`,
				Expected: []sql.Row{
					{"b", 1, "b", 4},
					{"b_and_c", 1, "b", 5},
					{"b_and_c", 2, "c", 6},
					{"c", 1, "c", 3},
					{"PRIMARY", 1, "i", nil},
				},
			},
			{
				// TODO: cardinality not supported
				Skip:     true,
				Query:    `select index_name, seq_in_index, column_name, cardinality, sub_part from information_schema.statistics where table_schema = 'mydb' and table_name = 'ptable' ORDER BY INDEX_NAME`,
				Expected: []sql.Row{{2}, {2}, {2}, {2}, {2}},
			},
			{
				Query: `SELECT seq_in_index, sub_part, index_name, index_type, CASE non_unique WHEN 0 THEN 'TRUE' ELSE 'FALSE' END AS is_unique, column_name
	FROM information_schema.statistics WHERE table_schema='mydb' AND table_name='ptable' ORDER BY index_name, seq_in_index;`,
				Expected: []sql.Row{
					{1, 4, "b", "BTREE", "TRUE", "b"},
					{1, 5, "b_and_c", "BTREE", "FALSE", "b"},
					{2, 6, "b_and_c", "BTREE", "FALSE", "c"},
					{1, 3, "c", "BTREE", "TRUE", "c"},
					{1, nil, "PRIMARY", "BTREE", "TRUE", "i"},
				},
			},
		},
	},
	{
		Name: "column specific tests on information_schema.columns table",
		SetUpScript: []string{
			`CREATE TABLE all_types (
pk int NOT NULL,
binary_1 binary(1) DEFAULT "1",
big_int bigint DEFAULT "1",
bit_2 bit(2) DEFAULT 2,
some_blob blob DEFAULT ("abc"),
char_1 char(1) DEFAULT "A",
some_date date DEFAULT "2022-02-22",
date_time datetime(6) DEFAULT "2022-02-22 22:22:21",
decimal_52 decimal(5,2) DEFAULT "994.45",
some_double double DEFAULT "1.1",
some_enum enum('s','m','l') DEFAULT "s",
some_float float DEFAULT "4.4",
some_geometry geometry srid 4326 DEFAULT (POINT(1, 2)),
some_int int DEFAULT "3",
some_json json DEFAULT (JSON_OBJECT("a", 1)),
line_string linestring DEFAULT (LINESTRING(POINT(0, 0),POINT(1, 2))),
long_blob longblob DEFAULT ("abc"),
long_text longtext DEFAULT ("abc"),
medium_blob mediumblob DEFAULT ("abc"),
medium_int mediumint DEFAULT "7",
medium_text mediumtext DEFAULT ("abc"),
some_point point DEFAULT (POINT(2, 2)),
some_polygon polygon DEFAULT NULL,
some_set set('one','two') DEFAULT "one,two",
small_int smallint DEFAULT "5",
some_text text DEFAULT ("abc"),
time_6 time(6) DEFAULT "11:59:59.000010",
time_stamp timestamp(6) DEFAULT (CURRENT_TIMESTAMP()),
tiny_blob tinyblob DEFAULT ("abc"),
tiny_int tinyint DEFAULT "4",
tiny_text tinytext DEFAULT ("abc"),
var_char varchar(255) DEFAULT "varchar value",
var_binary varbinary(255) DEFAULT "11111",
some_year year DEFAULT "2023",
PRIMARY KEY (pk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT table_catalog, table_schema, table_name, column_name, ordinal_position
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='all_types' ORDER BY ORDINAL_POSITION`,
				Expected: []sql.Row{
					{"def", "mydb", "all_types", "pk", uint32(1)},
					{"def", "mydb", "all_types", "binary_1", uint32(2)},
					{"def", "mydb", "all_types", "big_int", uint32(3)},
					{"def", "mydb", "all_types", "bit_2", uint32(4)},
					{"def", "mydb", "all_types", "some_blob", uint32(5)},
					{"def", "mydb", "all_types", "char_1", uint32(6)},
					{"def", "mydb", "all_types", "some_date", uint32(7)},
					{"def", "mydb", "all_types", "date_time", uint32(8)},
					{"def", "mydb", "all_types", "decimal_52", uint32(9)},
					{"def", "mydb", "all_types", "some_double", uint32(10)},
					{"def", "mydb", "all_types", "some_enum", uint32(11)},
					{"def", "mydb", "all_types", "some_float", uint32(12)},
					{"def", "mydb", "all_types", "some_geometry", uint32(13)},
					{"def", "mydb", "all_types", "some_int", uint32(14)},
					{"def", "mydb", "all_types", "some_json", uint32(15)},
					{"def", "mydb", "all_types", "line_string", uint32(16)},
					{"def", "mydb", "all_types", "long_blob", uint32(17)},
					{"def", "mydb", "all_types", "long_text", uint32(18)},
					{"def", "mydb", "all_types", "medium_blob", uint32(19)},
					{"def", "mydb", "all_types", "medium_int", uint32(20)},
					{"def", "mydb", "all_types", "medium_text", uint32(21)},
					{"def", "mydb", "all_types", "some_point", uint32(22)},
					{"def", "mydb", "all_types", "some_polygon", uint32(23)},
					{"def", "mydb", "all_types", "some_set", uint32(24)},
					{"def", "mydb", "all_types", "small_int", uint32(25)},
					{"def", "mydb", "all_types", "some_text", uint32(26)},
					{"def", "mydb", "all_types", "time_6", uint32(27)},
					{"def", "mydb", "all_types", "time_stamp", uint32(28)},
					{"def", "mydb", "all_types", "tiny_blob", uint32(29)},
					{"def", "mydb", "all_types", "tiny_int", uint32(30)},
					{"def", "mydb", "all_types", "tiny_text", uint32(31)},
					{"def", "mydb", "all_types", "var_char", uint32(32)},
					{"def", "mydb", "all_types", "var_binary", uint32(33)},
					{"def", "mydb", "all_types", "some_year", uint32(34)},
				},
			},
			{
				Query: `SELECT column_name, column_default, is_nullable, data_type, column_type, character_maximum_length, character_octet_length
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='all_types' ORDER BY ORDINAL_POSITION`,
				Expected: []sql.Row{
					{"pk", nil, "NO", "int", "int", nil, nil},
					{"binary_1", "0x31", "YES", "binary", "binary(1)", 1, 1},
					{"big_int", "1", "YES", "bigint", "bigint", nil, nil},
					{"bit_2", "b'10'", "YES", "bit", "bit(2)", nil, nil},
					{"some_blob", "'abc'", "YES", "blob", "blob", 65535, 65535},
					{"char_1", "A", "YES", "char", "char(1)", 1, 4},
					{"some_date", "2022-02-22 00:00:00", "YES", "date", "date", nil, nil},
					{"date_time", "2022-02-22 22:22:21", "YES", "datetime", "datetime(6)", nil, nil},
					{"decimal_52", "994.45", "YES", "decimal", "decimal(5,2)", nil, nil},
					{"some_double", "1.1", "YES", "double", "double", nil, nil},
					{"some_enum", "s", "YES", "enum", "enum('s','m','l')", 1, 4},
					{"some_float", "4.4", "YES", "float", "float", nil, nil},
					{"some_geometry", "point(1,2)", "YES", "geometry", "geometry", nil, nil},
					{"some_int", "3", "YES", "int", "int", nil, nil},
					{"some_json", "json_object('a',1)", "YES", "json", "json", nil, nil},
					{"line_string", "linestring(point(0,0),point(1,2))", "YES", "linestring", "linestring", nil, nil},
					{"long_blob", "'abc'", "YES", "longblob", "longblob", 4294967295, 4294967295},
					{"long_text", "'abc'", "YES", "longtext", "longtext", 1073741823, 4294967295},
					{"medium_blob", "'abc'", "YES", "mediumblob", "mediumblob", 16777215, 16777215},
					{"medium_int", "7", "YES", "mediumint", "mediumint", nil, nil},
					{"medium_text", "'abc'", "YES", "mediumtext", "mediumtext", 4194303, 16777215},
					{"some_point", "point(2,2)", "YES", "point", "point", nil, nil},
					{"some_polygon", nil, "YES", "polygon", "polygon", nil, nil},
					{"some_set", "one,two", "YES", "set", "set('one','two')", 7, 28},
					{"small_int", "5", "YES", "smallint", "smallint", nil, nil},
					{"some_text", "'abc'", "YES", "text", "text", 16383, 65535},
					{"time_6", "11:59:59.000010", "YES", "time", "time(6)", nil, nil},
					{"time_stamp", "CURRENT_TIMESTAMP", "YES", "timestamp", "timestamp(6)", nil, nil},
					{"tiny_blob", "'abc'", "YES", "tinyblob", "tinyblob", 255, 255},
					{"tiny_int", "4", "YES", "tinyint", "tinyint", nil, nil},
					{"tiny_text", "'abc'", "YES", "tinytext", "tinytext", 63, 255},
					{"var_char", "varchar value", "YES", "varchar", "varchar(255)", 255, 1020},
					{"var_binary", "0x3131313131", "YES", "varbinary", "varbinary(255)", 255, 255},
					{"some_year", "2023", "YES", "year", "year", nil, nil},
				},
			},
			{
				Query: `SELECT column_name, column_type, numeric_precision, numeric_scale, datetime_precision, character_set_name, collation_name, column_key, extra, column_comment, generation_expression, srs_id
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='all_types' ORDER BY ORDINAL_POSITION`,
				Expected: []sql.Row{
					{"pk", "int", 10, 0, nil, nil, nil, "PRI", "", "", "", nil},
					{"binary_1", "binary(1)", nil, nil, nil, nil, nil, "", "", "", "", nil},
					{"big_int", "bigint", 19, 0, nil, nil, nil, "", "", "", "", nil},
					{"bit_2", "bit(2)", 2, nil, nil, nil, nil, "", "", "", "", nil},
					{"some_blob", "blob", nil, nil, nil, nil, nil, "", "DEFAULT_GENERATED", "", "", nil},
					{"char_1", "char(1)", nil, nil, nil, "utf8mb4", "utf8mb4_0900_bin", "", "", "", "", nil},
					{"some_date", "date", nil, nil, nil, nil, nil, "", "", "", "", nil},
					{"date_time", "datetime(6)", nil, nil, 0, nil, nil, "", "", "", "", nil},
					{"decimal_52", "decimal(5,2)", 5, 2, nil, nil, nil, "", "", "", "", nil},
					{"some_double", "double", 22, nil, nil, nil, nil, "", "", "", "", nil},
					{"some_enum", "enum('s','m','l')", nil, nil, nil, "utf8mb4", "utf8mb4_0900_bin", "", "", "", "", nil},
					{"some_float", "float", 12, nil, nil, nil, nil, "", "", "", "", nil},
					{"some_geometry", "geometry", nil, nil, nil, nil, nil, "", "DEFAULT_GENERATED", "", "", uint32(4326)},
					{"some_int", "int", 10, 0, nil, nil, nil, "", "", "", "", nil},
					{"some_json", "json", nil, nil, nil, nil, nil, "", "DEFAULT_GENERATED", "", "", nil},
					{"line_string", "linestring", nil, nil, nil, nil, nil, "", "DEFAULT_GENERATED", "", "", nil},
					{"long_blob", "longblob", nil, nil, nil, nil, nil, "", "DEFAULT_GENERATED", "", "", nil},
					{"long_text", "longtext", nil, nil, nil, "utf8mb4", "utf8mb4_0900_bin", "", "DEFAULT_GENERATED", "", "", nil},
					{"medium_blob", "mediumblob", nil, nil, nil, nil, nil, "", "DEFAULT_GENERATED", "", "", nil},
					{"medium_int", "mediumint", 7, 0, nil, nil, nil, "", "", "", "", nil},
					{"medium_text", "mediumtext", nil, nil, nil, "utf8mb4", "utf8mb4_0900_bin", "", "DEFAULT_GENERATED", "", "", nil},
					{"some_point", "point", nil, nil, nil, nil, nil, "", "DEFAULT_GENERATED", "", "", nil},
					{"some_polygon", "polygon", nil, nil, nil, nil, nil, "", "", "", "", nil},
					{"some_set", "set('one','two')", nil, nil, nil, "utf8mb4", "utf8mb4_0900_bin", "", "", "", "", nil},
					{"small_int", "smallint", 5, 0, nil, nil, nil, "", "", "", "", nil},
					{"some_text", "text", nil, nil, nil, "utf8mb4", "utf8mb4_0900_bin", "", "DEFAULT_GENERATED", "", "", nil},
					{"time_6", "time(6)", nil, nil, 6, nil, nil, "", "", "", "", nil},
					{"time_stamp", "timestamp(6)", nil, nil, 0, nil, nil, "", "DEFAULT_GENERATED", "", "", nil},
					{"tiny_blob", "tinyblob", nil, nil, nil, nil, nil, "", "DEFAULT_GENERATED", "", "", nil},
					{"tiny_int", "tinyint", 3, 0, nil, nil, nil, "", "", "", "", nil},
					{"tiny_text", "tinytext", nil, nil, nil, "utf8mb4", "utf8mb4_0900_bin", "", "DEFAULT_GENERATED", "", "", nil},
					{"var_char", "varchar(255)", nil, nil, nil, "utf8mb4", "utf8mb4_0900_bin", "", "", "", "", nil},
					{"var_binary", "varbinary(255)", nil, nil, nil, nil, nil, "", "", "", "", nil},
					{"some_year", "year", nil, nil, nil, nil, nil, "", "", "", "", nil},
				},
			},
		},
	},
	{
		Name: "column specific tests on information_schema.tables table",
		SetUpScript: []string{
			`create table bigtable (text varchar(20) primary key, number mediumint, pt point default (POINT(1,1)))`,
			`insert into bigtable values ('a',4,POINT(1,4)),('b',2,null),('c',0,null),('d',2,POINT(1, 2)),('e',2,POINT(1, 2))`,
			`create index bigtable_number on bigtable (number)`,
			`CREATE VIEW myview1 AS SELECT * FROM mytable`,
			`CREATE VIEW myview2 AS SELECT * FROM myview1 WHERE i = 1`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT table_catalog, table_schema, table_name, table_type, table_comment FROM information_schema.tables WHERE table_schema = 'mydb' and table_type IN ('VIEW') ORDER BY TABLE_NAME;`,
				Expected: []sql.Row{
					{"def", "mydb", "myview", "VIEW", "VIEW"},
					{"def", "mydb", "myview1", "VIEW", "VIEW"},
					{"def", "mydb", "myview2", "VIEW", "VIEW"},
				},
			},
			{
				Query: "SELECT table_rows as count FROM information_schema.TABLES WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='bigtable';",
				Expected: []sql.Row{
					{uint64(5)},
				},
			},
		},
	},
	{
		Name: "column specific tests on information_schema table, check and referential constraints",
		SetUpScript: []string{
			`CREATE TABLE checks (a INTEGER PRIMARY KEY, b INTEGER, c varchar(20))`,
			`ALTER TABLE checks ADD CONSTRAINT chk1 CHECK (B > 0)`,
			`ALTER TABLE checks ADD CONSTRAINT chk2 CHECK (b > 0) NOT ENFORCED`,
			`ALTER TABLE checks ADD CONSTRAINT chk3 CHECK (B > 1)`,
			`ALTER TABLE checks ADD CONSTRAINT chk4 CHECK (upper(C) = c)`,

			`create table ptable (i int primary key, b blob, c char(10))`,
			`alter table ptable add index (c(3))`,
			`alter table ptable add unique index (b(4))`,
			`create index b_and_c on ptable (b(5), c(6))`,
			`ALTER TABLE ptable ADD CONSTRAINT ptable_checks FOREIGN KEY (i) REFERENCES checks(a)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT TC.CONSTRAINT_NAME, CC.CHECK_CLAUSE, TC.ENFORCED 
FROM information_schema.TABLE_CONSTRAINTS TC, information_schema.CHECK_CONSTRAINTS CC 
WHERE TABLE_SCHEMA = 'mydb' AND TABLE_NAME = 'checks' AND TC.TABLE_SCHEMA = CC.CONSTRAINT_SCHEMA AND TC.CONSTRAINT_NAME = CC.CONSTRAINT_NAME AND TC.CONSTRAINT_TYPE = 'CHECK';`,
				Expected: []sql.Row{
					{"chk1", "(B > 0)", "YES"},
					{"chk2", "(b > 0)", "NO"},
					{"chk3", "(B > 1)", "YES"},
					{"chk4", "(upper(C) = c)", "YES"},
				},
			},
			{
				Query: `select * from information_schema.table_constraints where table_schema = 'mydb' and table_name = 'checks';`,
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "mydb", "checks", "PRIMARY KEY", "YES"},
					{"def", "mydb", "chk1", "mydb", "checks", "CHECK", "YES"},
					{"def", "mydb", "chk2", "mydb", "checks", "CHECK", "NO"},
					{"def", "mydb", "chk3", "mydb", "checks", "CHECK", "YES"},
					{"def", "mydb", "chk4", "mydb", "checks", "CHECK", "YES"},
				},
			},
			{
				Query: `select * from information_schema.check_constraints where constraint_schema = 'mydb';`,
				Expected: []sql.Row{
					{"def", "mydb", "chk1", "(B > 0)"},
					{"def", "mydb", "chk2", "(b > 0)"},
					{"def", "mydb", "chk3", "(B > 1)"},
					{"def", "mydb", "chk4", "(upper(C) = c)"},
				},
			},
			{
				Query: `select * from information_schema.table_constraints where table_schema = 'mydb' and table_name = 'ptable';`,
				Expected: []sql.Row{
					{"def", "mydb", "PRIMARY", "mydb", "ptable", "PRIMARY KEY", "YES"},
					{"def", "mydb", "b", "mydb", "ptable", "UNIQUE", "YES"},
					{"def", "mydb", "ptable_checks", "mydb", "ptable", "FOREIGN KEY", "YES"},
				},
			},
		},
	},
	{
		Name: "column specific tests on information_schema.routines table",
		SetUpScript: []string{
			`CREATE DEFINER=root@localhost PROCEDURE count_i_from_mytable(OUT total_i INT)
    READS SQL DATA
BEGIN
     SELECT SUM(i)
     FROM mytable
     INTO total_i;
END ;`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `select specific_name, routine_catalog, routine_schema, routine_name, routine_type, data_type,
routine_body, external_language, parameter_style, is_deterministic, sql_data_access, security_type, sql_mode, 
routine_comment, definer, character_set_client, collation_connection, database_collation
from information_schema.routines where routine_schema = 'mydb' and routine_type like 'PROCEDURE' order by routine_name;`,
				Expected: []sql.Row{
					{"count_i_from_mytable", "def", "mydb", "count_i_from_mytable", "PROCEDURE", "", "SQL", "SQL", "SQL", "NO",
						"READS SQL DATA", "DEFINER", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY",
						"", "root@localhost", "utf8mb4", "utf8mb4_0900_bin", "utf8mb4_0900_bin"},
				},
			},
			{
				Query: `select routine_definition from information_schema.routines where routine_schema = 'mydb' and routine_type like 'PROCEDURE' order by routine_name;`,
				Expected: []sql.Row{
					{"BEGIN\n     SELECT SUM(i)\n     FROM mytable\n     INTO total_i;\nEND"},
				},
			},
		},
	},
	{
		Name: "column specific tests on information_schema.tables table",
		SetUpScript: []string{
			`create table bigtable (text varchar(20) primary key, number mediumint, pt point default (POINT(1,1)))`,
			`insert into bigtable values ('a',4,POINT(1,4)),('b',2,null),('c',0,null),('d',2,POINT(1, 2)),('e',2,POINT(1, 2))`,
			`create index bigtable_number on bigtable (number)`,
			`CREATE TABLE names (actor_id smallint PRIMARY KEY AUTO_INCREMENT, first_name varchar(45) NOT NULL);`,
			`INSERT INTO names (first_name) VALUES ('PENELOPE'), ('NICK'), ('JUNE');`,
			`CREATE VIEW myview1 AS SELECT * FROM myview WHERE i = 1`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT table_catalog, table_schema, table_name, table_type, engine, version, row_format, table_rows,
				auto_increment, table_collation, checksum, create_options, table_comment
				FROM information_schema.tables where table_schema = 'mydb' order by table_name`,
				Expected: []sql.Row{
					{"def", "mydb", "bigtable", "BASE TABLE", "InnoDB", 10, "Dynamic", uint64(5), nil, "utf8mb4_0900_bin", nil, "", ""},
					{"def", "mydb", "fk_tbl", "BASE TABLE", "InnoDB", 10, "Dynamic", uint64(0), nil, "utf8mb4_0900_bin", nil, "", ""},
					{"def", "mydb", "mytable", "BASE TABLE", "InnoDB", 10, "Dynamic", uint64(3), nil, "utf8mb4_0900_bin", nil, "", ""},
					{"def", "mydb", "myview", "VIEW", nil, nil, nil, nil, nil, nil, nil, nil, "VIEW"},
					{"def", "mydb", "myview1", "VIEW", nil, nil, nil, nil, nil, nil, nil, nil, "VIEW"},
					{"def", "mydb", "names", "BASE TABLE", "InnoDB", 10, "Dynamic", uint64(3), uint64(4), "utf8mb4_0900_bin", nil, "", ""},
				},
			},
			{
				Query: "SELECT table_comment,table_rows,auto_increment FROM information_schema.tables WHERE TABLE_NAME = 'names' AND TABLE_SCHEMA = 'mydb';",
				Expected: []sql.Row{
					{"", uint64(3), uint64(4)},
				},
			},
		},
	},
	{
		Name: "information_schema.views has definer and security information",
		SetUpScript: []string{
			"create view myview1 as select count(*) from mytable;",
			"CREATE ALGORITHM=TEMPTABLE DEFINER=UserName@localhost SQL SECURITY INVOKER VIEW myview2 AS SELECT * FROM myview WHERE i > 1;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select * from information_schema.views where table_schema = 'mydb' order by table_name",
				Expected: []sql.Row{
					{"def", "mydb", "myview", "SELECT * FROM mytable", "NONE", "YES", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"},
					{"def", "mydb", "myview1", "select count(*) from mytable", "NONE", "NO", "root@localhost", "DEFINER", "utf8mb4", "utf8mb4_0900_bin"},
					{"def", "mydb", "myview2", "SELECT * FROM myview WHERE i > 1", "NONE", "NO", "UserName@localhost", "INVOKER", "utf8mb4", "utf8mb4_0900_bin"},
				},
			},
		},
	},
	{
		Name: "information_schema.schemata shows all column values",
		SetUpScript: []string{
			"CREATE DATABASE mydb1 COLLATE latin1_general_ci;",
			"CREATE DATABASE mydb2 COLLATE utf8mb3_general_ci;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.schemata where schema_name like 'mydb%' order by schema_name",
				Expected: []sql.Row{
					{"def", "mydb", "utf8mb4", "utf8mb4_0900_bin", nil, "NO"},
					{"def", "mydb1", "latin1", "latin1_general_ci", nil, "NO"},
					{"def", "mydb2", "utf8mb3", "utf8mb3_general_ci", nil, "NO"},
				},
			},
		},
	},
	{
		Name: "information_schema.st_geometry_columns shows all column values",
		SetUpScript: []string{
			"CREATE TABLE spatial_table (id INT PRIMARY KEY, g GEOMETRY SRID 0, m MULTIPOINT, p POLYGON SRID 4326);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.st_geometry_columns where table_schema = 'mydb' order by column_name",
				Expected: []sql.Row{
					{"def", "mydb", "spatial_table", "g", "", uint32(0), "geometry"},
					{"def", "mydb", "spatial_table", "m", nil, nil, "multipoint"},
					{"def", "mydb", "spatial_table", "p", "WGS 84", uint32(4326), "polygon"},
				},
			},
		},
	},
	{
		Name: "information_schema.parameters shows all column values",
		SetUpScript: []string{
			"CREATE PROCEDURE testabc(IN x DOUBLE, IN y FLOAT, OUT abc DECIMAL(5,1)) SELECT x*y INTO abc",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.parameters where specific_name = 'testabc'",
				Expected: []sql.Row{
					{"def", "mydb", "testabc", uint64(1), "IN", "x", "double", nil, nil, 22, 0, nil, nil, nil, "double", "PROCEDURE"},
					{"def", "mydb", "testabc", uint64(2), "IN", "y", "float", nil, nil, 12, 0, nil, nil, nil, "float", "PROCEDURE"},
					{"def", "mydb", "testabc", uint64(3), "OUT", "abc", "decimal", nil, nil, 5, 1, nil, nil, nil, "decimal(5,1)", "PROCEDURE"},
				},
			},
		},
	},
	{

		Name:        "information_schema.st_spatial_reference_systems can be modified",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "create or replace spatial reference system 1234 " +
					"organization 'test_org' identified by 1234 " +
					"definition 'test_definition' " +
					"description 'test_description'",
				ExpectedErrStr: "missing mandatory attribute NAME",
			},
			{
				Query: "create or replace spatial reference system 1234 " +
					"name 'test_name' " +
					"definition 'test_definition' " +
					"description 'test_description'",
				ExpectedErrStr: "missing mandatory attribute ORGANIZATION NAME",
			},
			{
				Query: "create or replace spatial reference system 1234 " +
					"name 'test_name' " +
					"organization 'test_org' identified by 1234 " +
					"description 'test_description'",
				ExpectedErrStr: "missing mandatory attribute DEFINITION",
			},
			{
				Query: "create or replace spatial reference system 1234 " +
					"name ' test_name ' " +
					"definition 'test_definition' " +
					"organization 'test_org' identified by 1234 " +
					"description 'test_description'",
				ExpectedErrStr: "the spatial reference system name can't be an empty string or start or end with whitespace",
			},
			{
				Query: "create or replace spatial reference system 1234 " +
					"name 'test_name' " +
					"definition 'test_definition' " +
					"organization ' test_org ' identified by 1234 " +
					"description 'test_description'",
				ExpectedErrStr: "the organization name can't be an empty string or start or end with whitespace",
			},
			{
				// TODO: can't reliably test this along with the prepared version as the information_schema is persisted between test runs
				Skip: true,
				Query: "create spatial reference system 1234 " +
					"name 'test_name' " +
					"organization 'test_org' identified by 1234 " +
					"definition 'test_definition' " +
					"description 'test_description'",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "create or replace spatial reference system 1234 " +
					"name 'test_name' " +
					"organization 'test_org' identified by 1234 " +
					"definition 'test_definition' " +
					"description 'test_description'",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "select srs_id, srs_name, organization, organization_coordsys_id, definition, description from information_schema.st_spatial_reference_systems where srs_id = 1234",
				Expected: []sql.Row{
					{uint32(1234), "test_name", "test_org", uint32(1234), "test_definition", "test_description"},
				},
			},
			{
				Query: "create spatial reference system if not exists 1234 " +
					"name 'new_test_name' " +
					"organization 'new_test_org' identified by 1234 " +
					"definition 'new_test_definition' " +
					"description 'new_test_description'",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "select srs_id, srs_name, organization, organization_coordsys_id, definition, description from information_schema.st_spatial_reference_systems where srs_id = 1234",
				Expected: []sql.Row{
					{uint32(1234), "test_name", "test_org", uint32(1234), "test_definition", "test_description"},
				},
			},
		},
	},
}

var SkippedInfoSchemaScripts = []ScriptTest{
	{
		Name: "information_schema.key_column_usage works with foreign key across different databases",
		SetUpScript: []string{
			"CREATE TABLE my_table (i int primary key, height int)",
			"CREATE DATABASE keydb",
			"USE keydb",
			"CREATE TABLE key_table (a int primary key, weight int)",
			"alter table key_table add constraint fk_across_dbs foreign key (a) references mydb.my_table(i)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.key_column_usage where constraint_name = 'fk_across_dbs'",
				Expected: []sql.Row{
					{"def", "keydb", "fk_across_dbs", "def", "keydb", "key_table", "a", 1, 1, "mydb", "my_table", "i"},
				},
			},
		},
	},
}

var StatisticsQueries = []ScriptTest{
	{
		Name: "analyze single int column",
		SetUpScript: []string{
			"CREATE TABLE t (i bigint primary key)",
			"INSERT INTO t VALUES (1), (2), (3)",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{
					{"mydb", "t", "i", stats.NewStatistic(3, 3, 0, 24, time.Now(), sql.NewStatQualifier("mydb", "t", "primary"), []string{"i"}, []sql.Type{types.Int64},
						[]*stats.Bucket{
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(1)}, nil, nil),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(2)}, nil, nil),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(3)}, nil, nil),
						}),
					},
				},
			},
		},
	},
	{
		Name: "analyze update/drop",
		SetUpScript: []string{
			"CREATE TABLE t (i bigint primary key, j bigint, key(j))",
			"INSERT INTO t VALUES (1, 4), (2, 5), (3, 6)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "analyze table t update histogram on (i) using data '{\"row_count\": 40, \"distinct_count\": 40, \"null_count\": 1, \"buckets\": [{\"row_count\": 20, \"distinct_count\": 20, \"upper_bound\": [50], \"bound_count\": 1}, {\"row_count\": 20, \"distinct_count\": 20, \"upper_bound\": [80], \"bound_count\": 1}]}'",
				Expected: []sql.Row{{"t", "histogram", "status", "OK"}},
			},
			{
				Query: "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{
					{"mydb", "t", "i", stats.NewStatistic(40, 40, 1, 0, time.Now(),
						sql.NewStatQualifier("mydb", "t", ""),
						[]string{"i"},
						[]sql.Type{types.Int64},
						[]*stats.Bucket{
							stats.NewHistogramBucket(20, 20, 0, 1, sql.Row{float64(50)}, nil, nil),
							stats.NewHistogramBucket(20, 20, 0, 1, sql.Row{float64(80)}, nil, nil),
						}),
					},
				},
			},
			{
				Query:    "analyze table t drop histogram on (i)",
				Expected: []sql.Row{{"t", "histogram", "status", "OK"}},
			},
			{
				Query:    "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "analyze two int columns",
		SetUpScript: []string{
			"CREATE TABLE t (i bigint primary key, j bigint, key(j))",
			"INSERT INTO t VALUES (1, 4), (2, 5), (3, 6)",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{
					{"mydb", "t", "i", stats.NewStatistic(3, 3, 0, 48, time.Now(),
						sql.NewStatQualifier("mydb", "t", "primary"),
						[]string{"i"},
						[]sql.Type{types.Int64},
						[]*stats.Bucket{
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(1)}, nil, []sql.Row{}),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(2)}, nil, []sql.Row{}),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(3)}, nil, []sql.Row{}),
						}),
					},
					{"mydb", "t", "j", stats.NewStatistic(3, 3, 0, 48, time.Now(),
						sql.NewStatQualifier("mydb", "t", "j"),
						[]string{"j"},
						[]sql.Type{types.Int64},
						[]*stats.Bucket{
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(4)}, nil, []sql.Row{}),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(5)}, nil, []sql.Row{}),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{int64(6)}, nil, []sql.Row{}),
						}),
					},
				},
			},
		},
	},
	{
		Name: "analyze float columns",
		SetUpScript: []string{
			"CREATE TABLE t (i double primary key)",
			"INSERT INTO t VALUES (1.25), (45.25), (7.5), (10.5)",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{
					{"mydb", "t", "i", stats.NewStatistic(4, 4, 0, 32, time.Now(),
						sql.NewStatQualifier("mydb", "t", "primary"),
						[]string{"i"},
						[]sql.Type{types.Float64},
						[]*stats.Bucket{
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{float64(1.25)}, nil, []sql.Row{}),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{float64(7.5)}, nil, []sql.Row{}),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{float64(10.5)}, nil, []sql.Row{}),
							stats.NewHistogramBucket(1, 1, 0, 1, sql.Row{float64(45.25)}, nil, []sql.Row{}),
						}),
					},
				},
			},
		},
	},
	{
		Name: "analyze empty table creates stats with 0s",
		SetUpScript: []string{
			"CREATE TABLE t (i float)",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name: "analyze columns that can't be converted to float throws error",
		SetUpScript: []string{
			"CREATE TABLE t (t longtext)",
			"INSERT INTO t VALUES ('not a number')",
			"ANALYZE TABLE t",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT * FROM information_schema.column_statistics",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Query: `
		SELECT
			COLUMN_NAME,
			JSON_EXTRACT(HISTOGRAM, '$."number-of-buckets-specified"')
		FROM information_schema.COLUMN_STATISTICS
		WHERE SCHEMA_NAME = 'mydb'
		AND TABLE_NAME = 'mytable'
		`,
		Expected: nil,
	},
}
