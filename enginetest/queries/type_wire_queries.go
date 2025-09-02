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

import "github.com/dolthub/go-mysql-server/sql"

// TypeWireTest is used to ensure that types are properly represented over the wire (vs being directly returned from the
// engine).
type TypeWireTest struct {
	Name        string
	SetUpScript []string
	Queries     []string
	Results     [][]sql.Row
}

// TypeWireTests are used to ensure that types are properly represented over the wire (vs being directly returned from
// the engine).
var TypeWireTests = []TypeWireTest{
	{
		Name: "TINYINT",
		SetUpScript: []string{
			`CREATE TABLE test (pk TINYINT PRIMARY KEY, v1 TINYINT);`,
			`INSERT INTO test VALUES (-75, "-25"), (0, 0), (107.2, 0025), (107.5, 0025), (120, -120);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk < 0;`,
			`DELETE FROM test WHERE pk > "119";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"-75", "-26"}, {"0", "0"}, {"107", "25"}, {"108", "25"}},
			{{"-26", "-75"}, {"0", "0"}, {"25", "107"}, {"25", "108"}},
			{{"-52", "-74"}, {"0", "1"}, {"50", "108"}, {"50", "109"}},
		},
	},
	{
		Name: "SMALLINT",
		SetUpScript: []string{
			`CREATE TABLE test (pk SMALLINT PRIMARY KEY, v1 SMALLINT);`,
			`INSERT INTO test VALUES (-75.7, "-2531"), (-75, "-2531"), (0, 0), (2547.2, 03325), (2547.6, 03325), (9999, 9999);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk < 0;`,
			`DELETE FROM test WHERE pk >= "9999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"-76", "-2532"}, {"-75", "-2532"}, {"0", "0"}, {"2547", "3325"}, {"2548", "3325"}},
			{{"-2532", "-76"}, {"-2532", "-75"}, {"0", "0"}, {"3325", "2547"}, {"3325", "2548"}},
			{{"-5064", "-75"}, {"-5064", "-74"}, {"0", "1"}, {"6650", "2548"}, {"6650", "2549"}},
		},
	},
	{
		Name: "MEDIUMINT",
		SetUpScript: []string{
			`CREATE TABLE test (pk MEDIUMINT PRIMARY KEY, v1 MEDIUMINT);`,
			`INSERT INTO test VALUES (-75, "-2531"), (0, 0), (2547.2, 03325), (2547.7, 03325), (999999, 999999);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk < 0;`,
			`DELETE FROM test WHERE pk > "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"-75", "-2532"}, {"0", "0"}, {"2547", "3325"}, {"2548", "3325"}},
			{{"-2532", "-75"}, {"0", "0"}, {"3325", "2547"}, {"3325", "2548"}},
			{{"-5064", "-74"}, {"0", "1"}, {"6650", "2548"}, {"6650", "2549"}},
		},
	},
	{
		Name: "INT",
		SetUpScript: []string{
			`CREATE TABLE test (pk INT PRIMARY KEY, v1 INT);`,
			`INSERT INTO test VALUES (-75, "-2531"), (0, 0), (2547.2, 03325), (2547.8, 03325), (999999, 999999);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk < 0;`,
			`DELETE FROM test WHERE pk > "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"-75", "-2532"}, {"0", "0"}, {"2547", "3325"}, {"2548", "3325"}},
			{{"-2532", "-75"}, {"0", "0"}, {"3325", "2547"}, {"3325", "2548"}},
			{{"-5064", "-74"}, {"0", "1"}, {"6650", "2548"}, {"6650", "2549"}},
		},
	},
	{
		Name: "BIGINT",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BIGINT);`,
			`INSERT INTO test VALUES (-75, "-2531"), (0, 0), (2547.2, 03325), (2547.9, 03325), (999999, 999999);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk < 0;`,
			`DELETE FROM test WHERE pk > "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"-75", "-2532"}, {"0", "0"}, {"2547", "3325"}, {"2548", "3325"}},
			{{"-2532", "-75"}, {"0", "0"}, {"3325", "2547"}, {"3325", "2548"}},
			{{"-5064", "-74"}, {"0", "1"}, {"6650", "2548"}, {"6650", "2549"}},
		},
	},
	{
		Name: "TINYINT UNSIGNED",
		SetUpScript: []string{
			`CREATE TABLE test (pk TINYINT UNSIGNED PRIMARY KEY, v1 TINYINT UNSIGNED);`,
			`INSERT INTO test VALUES (0, 0), (25, "26"), (32.1, 0126), (42.8, 0126), (255, 255);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk > 0 AND pk < 30;`,
			`DELETE FROM test WHERE pk >= "255";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"0", "0"}, {"25", "25"}, {"32", "126"}, {"43", "126"}},
			{{"0", "0"}, {"25", "25"}, {"126", "32"}, {"126", "43"}},
			{{"0", "1"}, {"50", "26"}, {"252", "33"}, {"252", "44"}},
		},
	},
	{
		Name: "SMALLINT UNSIGNED",
		SetUpScript: []string{
			`CREATE TABLE test (pk SMALLINT UNSIGNED PRIMARY KEY, v1 SMALLINT UNSIGNED);`,
			`INSERT INTO test VALUES (0, 0), (25, "2531"), (2547.2, 03325), (2547.5, 03325), (9999, 9999);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk > 0 AND pk < 100;`,
			`DELETE FROM test WHERE pk >= "9999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"0", "0"}, {"25", "2530"}, {"2547", "3325"}, {"2548", "3325"}},
			{{"0", "0"}, {"2530", "25"}, {"3325", "2547"}, {"3325", "2548"}},
			{{"0", "1"}, {"5060", "26"}, {"6650", "2548"}, {"6650", "2549"}},
		},
	},
	{
		Name: "MEDIUMINT UNSIGNED",
		SetUpScript: []string{
			`CREATE TABLE test (pk MEDIUMINT UNSIGNED PRIMARY KEY, v1 MEDIUMINT UNSIGNED);`,
			`INSERT INTO test VALUES (75, "2531"), (0, 0), (2547.2, 03325), (2547.6, 03325), (999999, 999999);`,
			`UPDATE test SET v1 = v1 + 1 WHERE pk < 100;`,
			`DELETE FROM test WHERE pk > "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"0", "1"}, {"75", "2532"}, {"2547", "3325"}, {"2548", "3325"}},
			{{"1", "0"}, {"2532", "75"}, {"3325", "2547"}, {"3325", "2548"}},
			{{"2", "1"}, {"5064", "76"}, {"6650", "2548"}, {"6650", "2549"}},
		},
	},
	{
		Name: "INT UNSIGNED",
		SetUpScript: []string{
			`CREATE TABLE test (pk INT UNSIGNED PRIMARY KEY, v1 INT UNSIGNED);`,
			`INSERT INTO test VALUES (75, "2531"), (0, 0), (2547.2, 03325), (2547.7, 03325), (999999, 999999);`,
			`UPDATE test SET v1 = v1 + 1 WHERE pk < 100;`,
			`DELETE FROM test WHERE pk > "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"0", "1"}, {"75", "2532"}, {"2547", "3325"}, {"2548", "3325"}},
			{{"1", "0"}, {"2532", "75"}, {"3325", "2547"}, {"3325", "2548"}},
			{{"2", "1"}, {"5064", "76"}, {"6650", "2548"}, {"6650", "2549"}},
		},
	},
	{
		Name: "BIGINT UNSIGNED",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT UNSIGNED PRIMARY KEY, v1 BIGINT UNSIGNED);`,
			`INSERT INTO test VALUES (75, "2531"), (0, 0), (2547.2, 03325), (2547.8, 03325), (999999, 999999);`,
			`UPDATE test SET v1 = v1 + 1 WHERE pk < 100;`,
			`DELETE FROM test WHERE pk > "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"0", "1"}, {"75", "2532"}, {"2547", "3325"}, {"2548", "3325"}},
			{{"1", "0"}, {"2532", "75"}, {"3325", "2547"}, {"3325", "2548"}},
			{{"2", "1"}, {"5064", "76"}, {"6650", "2548"}, {"6650", "2549"}},
		},
	},
	{
		Name: "FLOAT",
		SetUpScript: []string{
			`CREATE TABLE test (pk FLOAT PRIMARY KEY, v1 FLOAT);`,
			`INSERT INTO test VALUES (-75.11, "-2531"), (0, 0), ("2547.2", 03325), (999999, 999999);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk < 0;`,
			`DELETE FROM test WHERE pk > "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"-75.11", "-2532"}, {"0", "0"}, {"2547.2", "3325"}},
			{{"-2532", "-75.11"}, {"0", "0"}, {"3325", "2547.2"}},
			{{"-5064", "-74.11000061035156"}, {"0", "1"}, {"6650", "2548.199951171875"}},
		},
	},
	{
		Name: "DOUBLE",
		SetUpScript: []string{
			`CREATE TABLE test (pk DOUBLE PRIMARY KEY, v1 DOUBLE);`,
			`INSERT INTO test VALUES (-75.11, "-2531"), (0, 0), ("2547.2", 03325), (999999, 999999);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk < 0;`,
			`DELETE FROM test WHERE pk > "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"-75.11", "-2532"}, {"0", "0"}, {"2547.2", "3325"}},
			{{"-2532", "-75.11"}, {"0", "0"}, {"3325", "2547.2"}},
			{{"-5064", "-74.11"}, {"0", "1"}, {"6650", "2548.2"}},
		},
	},
	{
		Name: "DECIMAL",
		SetUpScript: []string{
			`CREATE TABLE test (pk DECIMAL(5,0) PRIMARY KEY, v1 DECIMAL(25,5));`,
			`INSERT INTO test VALUES (-75, "-2531.356"), (0, 0), (2547.2, 03325), (99999, 999999);`,
			`UPDATE test SET v1 = v1 - 1 WHERE pk < 0;`,
			`DELETE FROM test WHERE pk >= "99999";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*2, pk+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"-75", "-2532.35600"}, {"0", "0.00000"}, {"2547", "3325.00000"}},
			{{"-2532.35600", "-75"}, {"0.00000", "0"}, {"3325.00000", "2547"}},
			{{"-5064.71200", "-74"}, {"0.00000", "1"}, {"6650.00000", "2548"}},
		},
	},
	{
		Name: "BIT",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIT(55) PRIMARY KEY, v1 BIT(1), v2 BIT(24));`,
			`INSERT INTO test VALUES (75, 0, "21"), (0, 0, 0), (2547.2, 1, 03325), (999999, 1, 999999);`,
			`UPDATE test SET v2 = v2 - 1 WHERE pk > 0 AND pk < 100;`,
			`DELETE FROM test WHERE pk > 99999;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v2, v1, pk FROM test ORDER BY pk;`,
			`SELECT v1*1, pk/10, v2+1 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"\x00\x00\x00\x00\x00\x00\x00", "\x00", "\x00\x00\x00"}, {"\x00\x00\x00\x00\x00\x00K", "\x00", "\x0020"}, {"\x00\x00\x00\x00\x00\t\xf3", "", "\x00\xfd"}},
			{{"\x00\x00\x00", "\x00", "\x00\x00\x00\x00\x00\x00\x00"}, {"\x0020", "\x00", "\x00\x00\x00\x00\x00\x00K"}, {"\x00\xfd", "", "\x00\x00\x00\x00\x00\t\xf3"}},
			{{"0", "0.0000", "1"}, {"0", "7.5000", "12849"}, {"1", "254.7000", "3326"}},
		},
	},
	{
		Name: "YEAR",
		SetUpScript: []string{
			`CREATE TABLE test (pk YEAR PRIMARY KEY, v1 YEAR);`,
			`INSERT INTO test VALUES (1901, 1901), (1950, "1950"), (1979.2, 01986), (2122, 2122);`,
			`UPDATE test SET v1 = v1 + 1 WHERE pk < 1975;`,
			`DELETE FROM test WHERE pk > "2100";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT v1+3, pk+2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1901", "1902"}, {"1950", "1951"}, {"1979", "1986"}},
			{{"1902", "1901"}, {"1951", "1950"}, {"1986", "1979"}},
			{{"1905", "1903"}, {"1954", "1952"}, {"1989", "1981"}},
		},
	},
	{
		Name: "TIMESTAMP",
		SetUpScript: []string{
			`CREATE TABLE test (pk TIMESTAMP PRIMARY KEY, v1 TIMESTAMP);`,
			`INSERT INTO test VALUES ("1980-04-12 12:02:11", "1986-08-02 17:04:22"), ("1999-11-28 13:06:33", "2022-01-14 15:08:44"), ("2020-05-06 18:10:55", "1975-09-15 11:12:16");`,
			`UPDATE test SET v1 = "2000-01-01 00:00:00" WHERE pk < "1990-01-01 00:00:00";`,
			`DELETE FROM test WHERE pk > "2015-01-01 00:00:00";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT DATE_ADD(TIMESTAMP('2022-10-26 13:14:15'), INTERVAL 1 DAY);`,
			`SELECT DATE_ADD('2022-10-26 13:14:15', INTERVAL 1 DAY);`,
			`SELECT DATE_ADD('2022-10-26', INTERVAL 1 SECOND);`,
			`SELECT DATE_ADD('2022-10-26', INTERVAL 1 MINUTE);`,
			`SELECT DATE_ADD('2022-10-26', INTERVAL 1 HOUR);`,
		},
		Results: [][]sql.Row{
			{{"1980-04-12 12:02:11", "2000-01-01 00:00:00"}, {"1999-11-28 13:06:33", "2022-01-14 15:08:44"}},
			{{"1980-04-12 12:02:11", "2000-01-01 00:00:00"}, {"1999-11-28 13:06:33", "2022-01-14 15:08:44"}},
			{{"2000-01-01 00:00:00", "1980-04-12 12:02:11"}, {"2022-01-14 15:08:44", "1999-11-28 13:06:33"}},
			{{"2022-10-27 13:14:15"}},
			{{"2022-10-27 13:14:15"}},
			{{"2022-10-26 00:00:01"}},
			{{"2022-10-26 00:01:00"}},
			{{"2022-10-26 01:00:00"}},
		},
	},
	{
		Name: "DATETIME",
		SetUpScript: []string{
			`CREATE TABLE test (pk DATETIME PRIMARY KEY, v1 DATETIME);`,
			`INSERT INTO test VALUES ("1000-04-12 12:02:11", "1986-08-02 17:04:22"), ("1999-11-28 13:06:33", "2022-01-14 15:08:44"), ("5020-05-06 18:10:55", "1975-09-15 11:12:16");`,
			`UPDATE test SET v1 = "2000-01-01 00:00:00" WHERE pk < "1990-01-01 00:00:00";`,
			`DELETE FROM test WHERE pk > "5000-01-01 00:00:00";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT DATE_ADD('2022-10-26 13:14:15', INTERVAL 1 DAY);`,
			`SELECT DATE_ADD('2022-10-26', INTERVAL 1 SECOND);`,
			`SELECT DATE_ADD('2022-10-26', INTERVAL 1 MINUTE);`,
			`SELECT DATE_ADD('2022-10-26', INTERVAL 1 HOUR);`,
		},
		Results: [][]sql.Row{
			{{"1000-04-12 12:02:11", "2000-01-01 00:00:00"}, {"1999-11-28 13:06:33", "2022-01-14 15:08:44"}},
			{{"1000-04-12 12:02:11", "2000-01-01 00:00:00"}, {"1999-11-28 13:06:33", "2022-01-14 15:08:44"}},
			{{"2000-01-01 00:00:00", "1000-04-12 12:02:11"}, {"2022-01-14 15:08:44", "1999-11-28 13:06:33"}},
			{{"2022-10-27 13:14:15"}},
			{{"2022-10-26 00:00:01"}},
			{{"2022-10-26 00:01:00"}},
			{{"2022-10-26 01:00:00"}},
		},
	},
	{
		Name: "DATE",
		SetUpScript: []string{
			`CREATE TABLE test (pk DATE PRIMARY KEY, v1 DATE);`,
			`INSERT INTO test VALUES ("1000-04-12", "1986-08-02"), ("1999-11-28", "2022-01-14"), ("5020-05-06", "1975-09-15");`,
			`UPDATE test SET v1 = "2000-01-01" WHERE pk < "1990-01-01";`,
			`DELETE FROM test WHERE pk > "5000-01-01";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT DATE_ADD(DATE('2022-10-26'), INTERVAL 1 DAY);`,
			`SELECT DATE_ADD(DATE('2022-10-26'), INTERVAL 1 WEEK);`,
			`SELECT DATE_ADD(DATE('2022-10-26'), INTERVAL 1 MONTH);`,
			`SELECT DATE_ADD(DATE('2022-10-26'), INTERVAL 1 QUARTER);`,
			`SELECT DATE_ADD(DATE('2022-10-26'), INTERVAL 1 YEAR);`,
		},
		Results: [][]sql.Row{
			{{"1000-04-12", "2000-01-01"}, {"1999-11-28", "2022-01-14"}},
			{{"1000-04-12", "2000-01-01"}, {"1999-11-28", "2022-01-14"}},
			{{"2000-01-01", "1000-04-12"}, {"2022-01-14", "1999-11-28"}},
			{{"2022-10-27"}},
			{{"2022-11-02"}},
			{{"2022-11-26"}},
			{{"2023-01-26"}},
			{{"2023-10-26"}},
		},
	},
	{
		Name: "TIME",
		SetUpScript: []string{
			`CREATE TABLE test (pk TIME PRIMARY KEY, v1 TIME);`,
			`INSERT INTO test VALUES ("-800:00:00", "-20:21:22"), ("00:00:00", "00:00:00"), ("10:26:57", "30:53:14"), ("700:23:51", "300:25:52");`,
			`UPDATE test SET v1 =  "-120:12:20" WHERE pk < "00:00:00";`,
			`DELETE FROM test WHERE pk > "600:00:00";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			// Known bug  - https://github.com/dolthub/dolt/issues/4643
			//`SELECT DATE_ADD(TIMEDIFF('12:13:14', '0:0:0'), INTERVAL 1 SECOND);`,
			//`SELECT DATE_ADD(TIMEDIFF('12:13:14', '0:0:0'), INTERVAL 1 MINUTE);`,
			//`SELECT DATE_ADD(TIMEDIFF('12:13:14', '0:0:0'), INTERVAL 1 HOUR);`,
		},
		Results: [][]sql.Row{
			{{"-800:00:00", "-120:12:20"}, {"00:00:00", "00:00:00"}, {"10:26:57", "30:53:14"}},
			{{"-800:00:00", "-120:12:20"}, {"00:00:00", "00:00:00"}, {"10:26:57", "30:53:14"}},
			{{"-120:12:20", "-800:00:00"}, {"00:00:00", "00:00:00"}, {"30:53:14", "10:26:57"}},
		},
	},
	{
		Name: "CHAR",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 CHAR(5), v2 CHAR(10));`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = "a-c" WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "a-c", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "a-c"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"a-cr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "VARCHAR",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 VARCHAR(5), v2 VARCHAR(10));`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
			`SELECT DATE_ADD('2022-10-26 13:14:15', INTERVAL 1 DAY);`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
			{{"2022-10-27 13:14:15"}},
		},
	},
	{
		Name: "BINARY",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BINARY(5), v2 BINARY(10));`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = "a-c" WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc\x00\x00", "def\x00\x00\x00\x00\x00\x00\x00"}, {"2", "a-c\x00\x00", "123\x00\x00\x00\x00\x00\x00\x00"}, {"3", "__2\x00\x00", "456\x00\x00\x00\x00\x00\x00\x00"}},
			{{"1", "def\x00\x00\x00\x00\x00\x00\x00", "abc\x00\x00"}, {"2", "123\x00\x00\x00\x00\x00\x00\x00", "a-c\x00\x00"}, {"3", "456\x00\x00\x00\x00\x00\x00\x00", "__2\x00\x00"}},
			{{"abc\x00\x00r", "1", "def\x00\x00\x00\x00\x00\x00\x00"}, {"a-c\x00\x00r", "2", "123\x00\x00\x00\x00\x00\x00\x00"}, {"__2\x00\x00r", "3", "456\x00\x00\x00\x00\x00\x00\x00"}},
		},
	},
	{
		Name: "VARBINARY",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 VARBINARY(5), v2 VARBINARY(10));`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "TINYTEXT",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 TINYTEXT, v2 TINYTEXT);`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "TEXT",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 TEXT, v2 TEXT);`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "MEDIUMTEXT",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 MEDIUMTEXT, v2 MEDIUMTEXT);`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "LONGTEXT",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 LONGTEXT, v2 LONGTEXT);`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "TINYBLOB",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 TINYBLOB, v2 TINYBLOB);`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "BLOB",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 BLOB, v2 BLOB);`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "MEDIUMBLOB",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 MEDIUMBLOB, v2 MEDIUMBLOB);`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "LONGBLOB",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 LONGBLOB, v2 LONGBLOB);`,
			`INSERT INTO test VALUES (1, "abc", "def"), (2, "c-a", "123"), (3, "__2", 456), (4, "?hi?", "\\n");`,
			`UPDATE test SET v1 = CONCAT(v1, "x") WHERE pk = 2;`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v2, v1 FROM test ORDER BY pk;`,
			`SELECT CONCAT(v1, "r"), pk, v2 FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "abc", "def"}, {"2", "c-ax", "123"}, {"3", "__2", "456"}},
			{{"1", "def", "abc"}, {"2", "123", "c-ax"}, {"3", "456", "__2"}},
			{{"abcr", "1", "def"}, {"c-axr", "2", "123"}, {"__2r", "3", "456"}},
		},
	},
	{
		Name: "ENUM",
		SetUpScript: []string{
			`CREATE TABLE test (pk ENUM("a","b","c") PRIMARY KEY, v1 ENUM("x","y","z"));`,
			`INSERT INTO test VALUES (1, 1), ("b", "y"), (3, "z");`,
			`UPDATE test SET v1 = "x" WHERE pk = 2;`,
			`DELETE FROM test WHERE pk > 2;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"a", "x"}, {"b", "x"}},
			{{"a", "x"}, {"b", "x"}},
			{{"x", "a"}, {"x", "b"}},
		},
	},
	{
		Name: "SET",
		SetUpScript: []string{
			`CREATE TABLE test (pk SET("a","b","c") PRIMARY KEY, v1 SET("w","x","y","z"));`,
			`INSERT INTO test VALUES (0, 1), ("b", "y"), ("b,c", "z,z"), ("a,c,b", 10);`,
			`UPDATE test SET v1 = "y,x,w" WHERE pk >= 4`,
			`DELETE FROM test WHERE pk > "b,c";`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"", "w"}, {"b", "y"}, {"b,c", "w,x,y"}},
			{{"", "w"}, {"b", "y"}, {"b,c", "w,x,y"}},
			{{"w", ""}, {"y", "b"}, {"w,x,y", "b,c"}},
		},
	},
	{
		Name: "GEOMETRY",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 GEOMETRY);`,
			`INSERT INTO test VALUES (1, POINT(1, 2)), (2, LINESTRING(POINT(1, 2), POINT(3, 4))), (3, ST_GeomFromText('POLYGON((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))'));`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT pk, ST_ASWKT(v1) FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{
				{"1", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40})},
				{"2", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40})},
				{"3", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F})},
			},
			{
				{"1", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40})},
				{"2", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40})},
				{"3", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F})},
			},
			{
				{"1", "POINT(1 2)"},
				{"2", "LINESTRING(1 2,3 4)"},
				{"3", "POLYGON((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))"},
			},
		},
	},
	{
		Name: "POINT",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 POINT);`,
			`INSERT INTO test VALUES (1, POINT(1, 2)), (2, POINT(3.4, 5.6)), (3, POINT(10, -20)), (4, POINT(1000, -1000));`,
			`DELETE FROM test WHERE pk = 4;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT pk, ST_ASWKT(v1) FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{
				{"1", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40})},
				{"2", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x0B, 0x40, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x16, 0x40})},
				{"3", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0xC0})},
			},
			{
				{"1", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40})},
				{"2", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x0B, 0x40, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x16, 0x40})},
				{"3", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0xC0})},
			},
			{
				{"1", "POINT(1 2)"},
				{"2", "POINT(3.4 5.6)"},
				{"3", "POINT(10 -20)"},
			},
		},
	},
	{
		Name: "LINESTRING",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 LINESTRING);`,
			`INSERT INTO test VALUES (1, LINESTRING(POINT(1, 2), POINT(3, 4))), (2, LINESTRING(POINT(5, 6), POINT(7, 8)));`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT pk, ST_ASWKT(v1) FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{
				{"1", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40})},
				{"2", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40})},
			},
			{
				{"1", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40})},
				{"2", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40})},
			},
			{
				{"1", "LINESTRING(1 2,3 4)"},
				{"2", "LINESTRING(5 6,7 8)"},
			},
		},
	},
	{
		Name: "POLYGON",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 POLYGON);`,
			`INSERT INTO test VALUES (1, ST_GeomFromText('POLYGON((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))'));`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT pk, v1 FROM test ORDER BY pk;`,
			`SELECT pk, ST_ASWKT(v1) FROM test ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{
				{"1", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F})},
			},
			{
				{"1", string([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F})},
			},
			{
				{"1", "POLYGON((0 0,0 3,3 0,0 0),(1 1,1 2,2 1,1 1))"},
			},
		},
	},
	{
		Name: "JSON",
		SetUpScript: []string{
			`CREATE TABLE test (pk BIGINT PRIMARY KEY, v1 JSON);`,
			`INSERT INTO test VALUES (1, '{"key1": {"key": "value"}}'), (2, '{"key1": "value1", "key2": "value2"}'), (3, '{"key1": {"key": [2,3]}}');`,
			`UPDATE test SET v1 = '["a", 1]' WHERE pk = 1;`,
			`DELETE FROM test WHERE pk = 3;`,
		},
		Queries: []string{
			`SELECT * FROM test ORDER BY pk;`,
			`SELECT v1, pk FROM test ORDER BY pk;`,
			`SELECT pk, JSON_ARRAYAGG(v1) FROM (SELECT * FROM test ORDER BY pk) as sub GROUP BY pk, v1 ORDER BY pk;`,
		},
		Results: [][]sql.Row{
			{{"1", "[\"a\",1]"}, {"2", "{\"key1\":\"value1\",\"key2\":\"value2\"}"}},
			{{"[\"a\",1]", "1"}, {"{\"key1\":\"value1\",\"key2\":\"value2\"}", "2"}},
			{{"1", "[[\"a\",1]]"}, {"2", "[{\"key1\":\"value1\",\"key2\":\"value2\"}]"}},
		},
	},
}
