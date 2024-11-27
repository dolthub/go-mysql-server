package queries

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var NumericErrorQueries = []ScriptTest{
	{
		Name: "range key float fuzzer fail",
		SetUpScript: []string{
			// float
			"create table float_pk (a int, b double, primary key(a,b))",
			"create table float_uk (a int, b double, primary key(a), unique key (b))",
			"create table float_nuk (a int, b double, primary key(a), key (b))",
			"insert into float_pk values (1, -4e+212)",
			"insert into float_uk values (1, -4e+212)",
			"insert into float_nuk values (1, -4e+212)",
			// decimal
			"create table decimal_pk (a int, b decimal(65,30), primary key(a,b))",
			"create table decimal_uk (a int, b decimal(65,30), primary key(a), unique key (b))",
			"create table decimal_nuk (a int, b decimal(65,30), primary key(a), key (b))",
			"insert into decimal_pk values (1, 9999999999999999999999999.9999999999999999999999999999999)",
			"insert into decimal_uk values (1, 9999999999999999999999999.9999999999999999999999999999999)",
			"insert into decimal_nuk values (1, 9999999999999999999999999.9999999999999999999999999999999)",
			// int overflows
			"create table i8 (i tinyint primary key)",
			"create table i16 (i smallint primary key)",
			"create table i32 (i int primary key)",
			"create table i64 (i bigint primary key)",
			"create table ui8 (i tinyint unsigned primary key)",
			"create table ui16 (i smallint unsigned primary key)",
			"create table ui32 (i int unsigned primary key)",
			"create table ui64 (i bigint unsigned primary key)",
			"insert into i8 values (127)",
			"insert into i16 values (32767)",
			"insert into i32 values (2147483647)",
			"insert into i64 values (9223372036854775807)",
			"insert into ui8 values (255)",
			"insert into ui16 values (65535)",
			"insert into ui32 values (4294967295)",
			"insert into ui64 values (18446744073709551615)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "delete from float_pk where a = 1 and b = -4e+212",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from decimal_uk where b = 9999999999999999999999999.9999999999999999999999999999999",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from decimal_nuk where b = 9999999999999999999999999.9999999999999999999999999999999",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from decimal_pk where a = 1 and b = 9999999999999999999999999.9999999999999999999999999999999",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from i8 where i = 127",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from i16 where i = 32767",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from i32 where i = 2147483647",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from i64 where i = 9223372036854775807",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from ui8 where i = 255",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from ui16 where i = 65535",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from ui32 where i = 4294967295",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from ui64 where i = 18446744073709551615",
				Expected: []sql.UntypedSqlRow{{types.NewOkResult(1)}},
			},
		},
	},
}
