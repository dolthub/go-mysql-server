// Copyright 2026 Dolthub, Inc.
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
	"math"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// NumericScriptTests contains self-contained numeric script tests.
var NumericScriptTests = []ScriptTest{
	{
		// https://github.com/dolthub/dolt/issues/11906
		Name:    "cast out-of-range bigint unsigned to signed",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t0 (id INT PRIMARY KEY, c0 BIGINT UNSIGNED NULL);",
			"INSERT INTO t0 VALUES (1, 18446744073709551615), (2, 9223372036854775808), (3, 9223372036854775807), (4, 1), (5, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT CAST(CAST(18446744073709551615 AS UNSIGNED) AS SIGNED);",
				Expected: []sql.Row{{int64(-1)}},
			},
			{
				Query:    "SELECT CAST(CAST(9223372036854775808 AS UNSIGNED) AS SIGNED);",
				Expected: []sql.Row{{int64(-9223372036854775808)}},
			},
			{
				Query:    "SELECT CAST(CAST(9223372036854775807 AS UNSIGNED) AS SIGNED);",
				Expected: []sql.Row{{int64(9223372036854775807)}},
			},
			{
				Query: "SELECT id, CAST(c0 AS SIGNED) FROM t0 ORDER BY id;",
				Expected: []sql.Row{
					{1, int64(-1)},
					{2, int64(-9223372036854775808)},
					{3, int64(9223372036854775807)},
					{4, int64(1)},
					{5, nil},
				},
			},
			{
				Query:    "SELECT id FROM t0 WHERE CAST(c0 AS SIGNED) < 0 ORDER BY id;",
				Expected: []sql.Row{{1}, {2}},
			},
		},
	},
	{
		Name:    "cast out-of-range integer strings to signed",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t0 (id INT PRIMARY KEY, c0 VARCHAR(30));",
			"INSERT INTO t0 VALUES (1, '18446744073709551615'), (2, '9223372036854775808'), (3, '9223372036854775807'), (4, NULL);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:                           "SELECT CAST('18446744073709551615' AS SIGNED);",
				Expected:                        []sql.Row{{int64(-1)}},
				ExpectedWarning:                 1105,
				ExpectedWarningsCount:           1,
				ExpectedWarningMessageSubstring: "negative complement",
			},
			{
				Query:                 "SELECT CONVERT('9223372036854775808', SIGNED);",
				Expected:              []sql.Row{{int64(-9223372036854775808)}},
				ExpectedWarning:       1105,
				ExpectedWarningsCount: 1,
			},
			{
				Query:                 "SELECT CAST('18446744073709551616' AS SIGNED);",
				Expected:              []sql.Row{{int64(-1)}},
				ExpectedWarning:       1292,
				ExpectedWarningsCount: 1,
			},
			{
				Query: "SELECT id, CAST(c0 AS SIGNED) FROM t0 ORDER BY id;",
				Expected: []sql.Row{
					{1, int64(-1)},
					{2, int64(-9223372036854775808)},
					{3, int64(9223372036854775807)},
					{4, nil},
				},
				ExpectedWarning:       1105,
				ExpectedWarningsCount: 2,
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9927
		// https://github.com/dolthub/dolt/issues/9053
		Name:    "double negation of integer minimum values",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t0(c0 BIGINT);",
			"INSERT INTO t0(c0) VALUES (-9223372036854775808);",
			"CREATE TABLE t1(c0 INT);",
			"INSERT INTO t1(c0) VALUES (-2147483648);",
			"CREATE TABLE t2(c0 SMALLINT);",
			"INSERT INTO t2(c0) VALUES (-32768);",
			"CREATE TABLE t3(c0 TINYINT);",
			"INSERT INTO t3(c0) VALUES (-128);",
			"CREATE TABLE tab1 (col4 INT)",
			"INSERT INTO tab1 VALUES (10), (20), (30)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:           "SELECT -(-128);",
				Expected:        []sql.Row{{int16(128)}},
				ExpectedColumns: sql.Schema{{Name: "-(-128)", Type: types.Int64}},
			},
			{
				Query:           "SELECT -(-32768);",
				Expected:        []sql.Row{{int32(32768)}},
				ExpectedColumns: sql.Schema{{Name: "-(-32768)", Type: types.Int64}},
			},
			{
				Query:           "SELECT -(-2147483648);",
				Expected:        []sql.Row{{int64(2147483648)}},
				ExpectedColumns: sql.Schema{{Name: "-(-2147483648)", Type: types.Int64}},
			},
			{
				Query:           "SELECT -(-9223372036854775808)",
				Expected:        []sql.Row{{"9223372036854775808"}},
				ExpectedColumns: sql.Schema{{Name: "-(-9223372036854775808)", Type: types.InternalDecimalType}},
			},
			{
				Query:       "SELECT -t0.c0 FROM t0;",
				ExpectedErr: sql.ErrValueOutOfRange,
			},
			{
				Query:    "SELECT -t1.c0 FROM t1;",
				Expected: []sql.Row{{2147483648}},
			},
			{
				Query:    "SELECT -t2.c0 FROM t2;",
				Expected: []sql.Row{{32768}},
			},
			{
				Query:    "SELECT -t3.c0 FROM t3;",
				Expected: []sql.Row{{128}},
			},
			{
				Query:    "SELECT -(-t1.c0 + 1) FROM t1;",
				Expected: []sql.Row{{-2147483649}},
			},
			{
				Query:    "SELECT -(-(t2.c0 - 1)) FROM t2;",
				Expected: []sql.Row{{-32769}},
			},
			{
				Query:    "SELECT -(-t3.c0 * 2) FROM t3;",
				Expected: []sql.Row{{-256}},
			},
			{
				Query:    "SELECT -(-(-128));",
				Expected: []sql.Row{{int8(-128)}},
			},
			{
				Query:    "SELECT -(-(-(-128)));",
				Expected: []sql.Row{{int16(128)}},
			},
			{
				Query:    "SELECT -(-NULL);",
				Expected: []sql.Row{{nil}},
			},
			{
				Query:           "SELECT -(-CAST(-128 AS SIGNED));",
				Expected:        []sql.Row{{int64(-128)}},
				ExpectedColumns: sql.Schema{{Name: "-(-CAST(-128 AS SIGNED))", Type: types.Int64}},
			},
			{
				Query:    "SELECT * FROM tab1 AS cor0 WHERE NOT - CAST(NULL AS SIGNED) < +35 * +col4 + - -39",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:    "bits don't work on server",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t (b bit(1));",
			"insert into t values (1)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from t;",
				Expected: []sql.Row{{uint64(1)}},
			},
		},
	},
	{
		Name: "update exponential parsing",
		SetUpScript: []string{
			"create table a (a int primary key, b double);",
			"insert into a values (0, 0.0),(1, 1.0)",
			"update a set b = 5.0E-5 where a = 0",
			"update a set b = 5.0e-5 where a = 1",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from a",
				Expected: []sql.Row{{0, .00005}, {1, .00005}},
			},
		},
	},
	{
		Name: "Ensure proper DECIMAL support (found by fuzzer)",
		SetUpScript: []string{
			"CREATE TABLE `GzaKtwgIya` (`K7t5WY` DECIMAL(64,5), `qBjVrN` VARBINARY(1000), `PvqQtc` SET('c3q6y','kxMqhfkK','XlRI8','dF0N63H','hMPjt0KXRLwCGRr','27fi2s','1FSJ','NcPzIN','Za18lbIgxmZ','on4BKKXykVTbJ','WBfO','RMNG','Sd7','FDzbEO','cLRdLOj1y','syo4','Ul','jfsfDCx6s','yEW3','JyQcWFDl'), `1kv7el` FLOAT, `Y3vfRG` BLOB, `Ijq8CK` TINYTEXT, `tzeStN` MEDIUMINT, `Ak83FQ` BINARY(64), `8Nbp3L` DOUBLE, PRIMARY KEY (`K7t5WY`));",
			"REPLACE INTO `GzaKtwgIya` VALUES ('58567047399981325523662211357420045483361289734772861386428.89028','bvo5~Tt8%kMW2nm2!8HghaeulI6!pMadE+j-J2LeU1O1*-#@Lm8Ibh00bTYiA*H1Q8P1_kQq 24Rrd4@HeF%#7#C#U7%mqOMrQ0%!HVrGV1li.XyYa:7#3V^DtAMDTQ9 cY=07T4|DStrwy4.MAQxOG#1d#fcq+7675$y0e96-2@8-WlQ^p|%E!a^TV!Yj2_eqZZys1z:883l5I%zAT:i56K^T!cx#us $60Tb#gH$1#$P.709E#VrH9FbQ5QZK2hZUH!qUa4Xl8*I*0fT~oAha$8jU5AoWs+Uv!~:14Yq%pLXpP9RlZ:Gd1g|*$Qa.9*^K~YlYWVaxwY~_g6zOMpU$YijT+!_*m3=||cMNn#uN0!!OyCg~GTQlJ11+#@Ohqc7b#2|Jp2Aei56GOmq^I=7cQ=sQh~V.D^HzwK5~4E$QzFXfWNVN5J_w2b4dkR~bB~7F%=@R@9qE~e:-_RnoJcOLfBS@0:*hTIP$5ui|5Ea-l+qU4nx98X6rV2bLBxn8am@p~:xLF#T^_9kJVN76q^18=i *FJo.v-xA2GP==^C^Jz3yBF0OY4bIxC59Y#6G=$w:xh71kMxBcYJKf3+$Ci_uWx0P*AfFNne0_1E0Lwv#3J8vm:. 8Wo~F3VT:@w.t@w .JZz$bok9Tls7RGo=~4 Y$~iELr$s@53YuTPM8oqu!x*1%GswpJR=0K#qs00nW-1MqEUc:0wZv#X4qY^pzVDb:!:!yDhjhh+KIT%2%w@+t8c!f~o!%EnwBIr_OyzL6e1$-R8n0nWPU.toODd*|fW3H$9ZLc9!dMS:QfjI0M$nK 8aGvUVP@9kS~W#Y=Q%=37$@pAUkDTXkJo~-DRvCG6phPp*Xji@9|AEODHi+-6p%X4YM5Y3WasPHcZQ8QgTwi9 N=2RQD_MtVU~0J~3SAx*HrMlKvCPTswZq#q_96ny_A@7g!E2jyaxWFJD:C233onBdchW$WdAc.LZdZHYDR^uwZb9B9p-q.BkD1I',608583,'-7.276514330627342e-28','FN3O_E:$ 5S40T7^Vu1g!Ktn^N|4RE!9GnZiW5dG:%SJb5|SNuuI.d2^qnMY.Xn*_fRfk Eo7OhqY8OZ~pA0^ !2P.uN~r@pZ2!A0+4b*%nxO.tm%S6=$CZ9+c1zu-p $b:7:fOkC%@E3951st@2Q93~8hj:ZGeJ6S@nw-TAG+^lad37aB#xN*rD^9TO0|hleA#.Nh28S2PB72L*TxD0$|XE3S5eVVmbI*pkzE~lPecopX1fUyFj#LC+%~pjmab7^ Kdd4B%8I!ohOCQV.oiw++N|#W2=D4:_sK0@~kTTeNA8_+FMKRwro.M0| LdKHf-McKm0Z-R9+H%!9r l6%7UEB50yNH-ld%eW8!f=LKgZLc*TuTP2DA_o0izvzZokNp3ShR+PA7Fk* 1RcSt5KXe+8tLc+WGP','3RvfN2N.Q1tIffE965#2r=u_-4!u:9w!F1p7+mSsO8ckio|ib 1t@~GtgUkJX',1858932,'DJMaQcI=vS-Jk2L#^2N8qZcRpMJ2Ga!30A+@I!+35d-9bwVEVi5-~i.a%!KdoF5h','1.0354401044541863e+255');",
			"INSERT INTO `GzaKtwgIya` VALUES ('91198031969464085142628031466155813748261645250257051732159.65596','96Lu=focmodq4otVAUN6TD-F$@k^4443higo=KH!1WBDH9|vpEGdO* 1uF6yWjT4:7G|altXnWSv+d:c8Km8vL!b%-nuB8mAxO9E|a5N5#v@z!ij5ifeIEoZGXrhBJl.m*Rx-@%g~t:y$3Pp3Q7Bd3y$=YG%6yibqXWO9$SS+g=*6QzdSCzuR~@v!:.ATye0A@y~DG=uq!PaZd6wN7.2S Aq868-RN3RM61V#N+Qywqo=%iYV*554@h6GPKZ| pmNwQw=PywuyBhr*MHAOXV+u9_-#imKI-wT4gEcA1~lGg1cfL2IvhkwOXRhrjAx-8+R3#4!Ai J6SYP|YUuuGalJ_N8k_8K^~h!JyiH$0JbGQ4AOxO3-eW=BaopOd8FF1.cfFMK!tXR ^I15g:npOuZZO$Vq3yQ4bl4s$E9:t2^.4f.:I4_@u9_UI1ApBthJZNiv~o#*uhs9K@ufZ1YPJQY-pMj$v-lQ2#%=Uu!iEAO3%vQ^5YITKcWRk~$kd1H#F675r@P5#M%*F_xP3Js7$YuEC4YuQjZ A74tMw:KwQ8dR:k_ Sa85G~42-K3%:jk5G9csC@iW3nY|@-:_dg~5@J!FWF5F+nyBgz4fDpdkdk9^:_.t$A3W-C@^Ax.~o|Rq96_i%HeG*7jBjOGhY-e1k@aD@WW.@GmpGAI|T-84gZFG3BU9@#9lpL|U2YCEA.BEA%sxDZ Kw:n+d$Y!SZw0Iml$Bdtyr:02Np=DZpiI%$N9*U=%Jq#$P5BI60WOTK+UynVx9Dd**5q8y9^v+I|PPa#_2XheV5YQU.ONdQQNJxsiRaEl!*=xv4bTWj1wBH#_-eM3T',490529,'-8.419238802182018e+25','|WD!NpWJOfN+_Au 1y!|XF8l38#%%R5%$TRUEaFt%4ywKQ8 O1LD-3qRDrnHAXboH~0uivbo87f+V%=q9~Mvz1EIxsU!whSmPqtb9r*11346R_@L+H#@@Z9H-Dc6j%.D0o##m@B9o7jO#~N81ACI|f#J3z4dho:jc54Xws$8r%cxuov^1$w_58Fv2*.8qbAW$TF153A:8wwj4YIhkd#^Q7 |g7I0iQG0p+yE64rk!Pu!SA-z=ELtLNOCJBk_4!lV$izn%sB6JwM+uq~ 49I7','v|eUA_h2@%t~bn26ci8Ngjm@Lk*G=l2MhxhceV2V|ka#c',8150267,'nX-=1Q$3riw_jlukGuHmjodT_Y_SM$xRbEt$%$%hlIUF1+GpRp~U6JvRX^: k@n#','7.956726808353253e+267');",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "DELETE FROM `GzaKtwgIya` WHERE `K7t5WY` = '58567047399981325523662211357420045483361289734772861386428.89028';",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM GzaKtwgIya",
				Expected: []sql.Row{{1}},
			},
		},
	},
	{
		Name:    "Ensure scale is not rounded when inserting to DECIMAL type through float64",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table test (number decimal(40,16));",
			"insert into test values ('11981.5923291839784651');",
			"create table small_test (n decimal(3,2));",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('11981.5923291839784651', DECIMAL)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "INSERT INTO test VALUES (11981.5923291839784651);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('11981.5923291839784651', DECIMAL)",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "INSERT INTO test VALUES (119815923291839784651.11981592329183978465111981592329183978465144);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('119815923291839784651.1198159232918398', DECIMAL)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "INSERT INTO test VALUES (1.1981592329183978465111981592329183978465111981592329183978465144);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('1.1981592329183978', DECIMAL)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "INSERT INTO test VALUES (1.1981592329183978545111981592329183978465111981592329183978465144);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "SELECT COUNT(*) FROM test WHERE number = CONVERT('1.1981592329183979', DECIMAL)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:       "INSERT INTO small_test VALUES (12.1);",
				ExpectedErr: types.ErrConvertToDecimalLimit,
			},
		},
	},
	{
		Name: "compare DECIMAL type columns with different precision and scale",
		SetUpScript: []string{
			"create table t (id int primary key, val1 decimal(2, 1), val2 decimal(3, 1));",
			"insert into t values (1, 1.2, 1.1), (2, 1.2, 10.1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select if(val1 < val2, 'YES', 'NO') from t order by id;",
				Expected: []sql.Row{{"NO"}, {"YES"}},
			},
		},
	},
	{
		Name: "'/' division operation result in decimal or float",
		SetUpScript: []string{
			"create table floats (f float);",
			"insert into floats values (1.1), (1.2), (1.3);",
			"create table decimals (d decimal(2,1));",
			"insert into decimals values (1.0), (2.0), (2.5);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select f/2 from floats;",
				Expected: []sql.Row{{0.550000011920929}, {0.6000000238418579}, {0.6499999761581421}},
			},
			{
				Query:    "select 2/f from floats;",
				Expected: []sql.Row{{1.8181817787737895}, {1.6666666004392863}, {1.5384615948919735}},
			},
			{
				Query:    "select d/2 from decimals;",
				Expected: []sql.Row{{"0.50000"}, {"1.00000"}, {"1.25000"}},
			},
			{
				Query:    "select 2/d from decimals;",
				Expected: []sql.Row{{"2.0000"}, {"1.0000"}, {"0.8000"}},
			},
			{
				Query: "select f/d from floats, decimals;",
				Expected: []sql.Row{{1.2999999523162842}, {1.2000000476837158}, {1.100000023841858},
					{0.6499999761581421}, {0.6000000238418579}, {0.550000011920929},
					{0.5199999809265137}, {0.48000001907348633}, {0.4400000095367432}},
			},
			{
				Query: "select d/f from floats, decimals;",
				Expected: []sql.Row{{0.7692307974459868}, {0.8333333002196431}, {0.9090908893868948},
					{1.5384615948919735}, {1.6666666004392863}, {1.8181817787737895},
					{1.9230769936149668}, {2.083333250549108}, {2.272727223467237}},
			},
			{
				Dialect:  "mysql",
				Query:    `select f/'a' from floats;`,
				Expected: []sql.Row{{nil}, {nil}, {nil}},
			},
		},
	},
	{
		Name:    "'%' mod operation result in decimal or float",
		Dialect: "mysql", // % operator between types not defined in other dialects
		SetUpScript: []string{
			"create table a (pk int primary key, c1 int, c2 double, c3 decimal(5,3));",
			"insert into a values (1, 1, 1.111, 1.111), (2, 2, 2.111, 2.111);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select c1 % 2, c2 % 2, c3 % 2 from a;",
				Expected: []sql.Row{{"1", 1.111, "1.111"}, {"0", 0.11100000000000021, "0.111"}},
			},
			{
				Query:    "select c1 % 0.5, c2 % 0.5, c3 % 0.5 from a;",
				Expected: []sql.Row{{"0.0", 0.11099999999999999, "0.111"}, {"0.0", 0.11100000000000021, "0.111"}},
			},
			{
				Query:    "select 20 % c1, 20 % c2, 20 % c3 from a;",
				Expected: []sql.Row{{"0", 0.002000000000000224, "0.002"}, {"0", 1.0009999999999981, "1.001"}},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/11411
		Name: "integer arithmetic rejects signed and unsigned BIGINT overflow",
		// MySQL-only: PostgreSQL does not support unsigned integer types.
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE integer_bounds (id INT PRIMARY KEY, u BIGINT UNSIGNED, s BIGINT)",
			"INSERT INTO integer_bounds VALUES (1, 18446744073709551615, 9223372036854775807)",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT u, u + 0, u + -1, u - 1 FROM integer_bounds",
				Expected: []sql.Row{{uint64(math.MaxUint64), uint64(math.MaxUint64), uint64(math.MaxUint64 - 1), uint64(math.MaxUint64 - 1)}},
			},
			{
				Query:       "SELECT u + 1 FROM integer_bounds",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(18446744073709551615 AS UNSIGNED) * 2",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(0 AS UNSIGNED) - 1",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:    "SELECT -1 + CAST(1 AS UNSIGNED)",
				Expected: []sql.Row{{uint64(0)}},
			},
			{
				Query:    "SELECT CAST(-1 AS SIGNED) * CAST(0 AS UNSIGNED), CAST(0 AS UNSIGNED) * CAST(-1 AS SIGNED)",
				Expected: []sql.Row{{uint64(0), uint64(0)}},
			},
			{
				Query:    "SELECT CAST(1 AS UNSIGNED) - -1, 2 - CAST(1 AS UNSIGNED)",
				Expected: []sql.Row{{uint64(2), uint64(1)}},
			},
			{
				Query:       "SELECT -2 + CAST(1 AS UNSIGNED)",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT 1 - CAST(2 AS UNSIGNED)",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(1 AS UNSIGNED) * -1",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT s + 1 FROM integer_bounds",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(-9223372036854775807 AS SIGNED) - 2",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(3037000500 AS SIGNED) * CAST(3037000500 AS SIGNED)",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
		},
	},
	{
		Name: "NO_UNSIGNED_SUBTRACTION returns signed BIGINT arithmetic results",
		// MySQL-only: NO_UNSIGNED_SUBTRACTION is a MySQL SQL mode.
		Dialect: "mysql",
		SetUpScript: []string{
			"SET SESSION sql_mode = 'NO_UNSIGNED_SUBTRACTION'",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT CAST(0 AS UNSIGNED) - 1",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "SELECT 1 - CAST(2 AS UNSIGNED)",
				Expected: []sql.Row{{-1}},
			},
			{
				Query:    "SELECT CAST(9223372036854775808 AS UNSIGNED) - 1",
				Expected: []sql.Row{{math.MaxInt64}},
			},
			{
				Query:    "SELECT CAST(9223372036854775807 AS SIGNED) - CAST(18446744073709551615 AS UNSIGNED)",
				Expected: []sql.Row{{math.MinInt64}},
			},
			{
				Query:       "SELECT CAST(18446744073709551615 AS UNSIGNED) - 1",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
			{
				Query:       "SELECT CAST(-9223372036854775808 AS SIGNED) - CAST(1 AS UNSIGNED)",
				ExpectedErr: sql.ErrIntegerOutOfRange,
			},
		},
	},
	{
		Name: "arithmetic bit operations on int, float and decimal types",
		SetUpScript: []string{
			"CREATE TABLE num_types (pk int primary key, a int, b float, c decimal(5,3));",
			"insert into num_types values (1,1,1.1,1.1), (2,2,1.2,2.2), (3,3,1.6,3.7), (4,4,1.7,4.0);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "select a & 2.4, a | 2.4, a ^ 2.4 from num_types;",
				Expected: []sql.Row{
					{uint64(0), uint64(3), uint64(3)},
					{uint64(2), uint64(2), uint64(0)},
					{uint64(2), uint64(3), uint64(1)},
					{uint64(0), uint64(6), uint64(6)},
				},
			},
			{
				Query: "select b & 2.4, b | 2.4, b ^ 2.4 from num_types;",
				Expected: []sql.Row{
					{uint64(0), uint64(3), uint64(3)},
					{uint64(0), uint64(3), uint64(3)},
					{uint64(2), uint64(2), uint64(0)},
					{uint64(2), uint64(2), uint64(0)},
				},
			},
			{
				Query: "select c & 2.4, c | 2.4, c ^ 2.4 from num_types;",
				Expected: []sql.Row{
					{uint64(0), uint64(3), uint64(3)},
					{uint64(2), uint64(2), uint64(0)},
					{uint64(0), uint64(6), uint64(6)},
					{uint64(0), uint64(6), uint64(6)},
				},
			},
		},
	},
	{
		// https://github.com/dolthub/dolt/issues/9857
		Name:        "UUID_SHORT() function returns 64-bit unsigned integers with proper construction",
		Dialect:     "mysql",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT UUID_SHORT() > 0",
				Expected: []sql.Row{
					{true}, // Should return positive values
				},
			},
			{
				Query: "SELECT UUID_SHORT() != UUID_SHORT()",
				Expected: []sql.Row{
					{true}, // Should return different values on each call
				},
			},
			{
				Query: "SELECT UUID_SHORT() + 0 > 0",
				Expected: []sql.Row{
					{true}, // Should work in arithmetic expressions
				},
			},
			{
				Query: "SELECT CAST(UUID_SHORT() AS CHAR) != ''",
				Expected: []sql.Row{
					{true}, // Should cast to non-empty string
				},
			},
			{
				Query: "SELECT UUID_SHORT() BETWEEN 1 AND 18446744073709551615",
				Expected: []sql.Row{
					{true}, // Should be within uint64 range
				},
			},
			{
				Query: "SELECT (UUID_SHORT() & 0xFF00000000000000) >> 56 BETWEEN 0 AND 255",
				Expected: []sql.Row{
					{true}, // Server ID should be 0-255
				},
			},
			{
				Query: "SET @@global.server_id = 253",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
			{
				Query: "SELECT (UUID_SHORT() & 0xFF00000000000000) >> 56 BETWEEN 0 AND 255",
				Expected: []sql.Row{
					{true}, // server time won't let us pin this down further
				},
			},
			{
				Query: "SET @@global.server_id = 1",
				Expected: []sql.Row{
					{types.NewOkResult(0)},
				},
			},
		},
	},
	{
		Name: "scientific notation for floats",
		SetUpScript: []string{
			"create table t (b bigint unsigned);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "insert into t values (5.2443381514267e+18);",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
		},
	},
	{
		Name: "decimal literals should be parsed correctly",
		SetUpScript: []string{
			"SET @testValue = 809826404100301269648758758005707100;",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "SELECT @testValue;",
				Expected: []sql.Row{{"809826404100301269648758758005707100"}},
			},
		},
	},
	{
		Name: "division and int division operation on negative, small and big value for decimal type column of table",
		SetUpScript: []string{
			"create table t (d decimal(25,10) primary key);",
			"insert into t values (-4990), (2), (22336578);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select d div 314990 from t order by d;",
				Expected: []sql.Row{{0}, {0}, {70}},
			},
			{
				Query:    "select d / 314990 from t order by d;",
				Expected: []sql.Row{{"-0.01584177275469"}, {"0.00000634940792"}, {"70.91202260389219"}},
			},
		},
	},
	{
		Name: "dividing has different rounding behavior",
		SetUpScript: []string{
			"CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER);",
			"INSERT INTO tab0 VALUES(97, 1, 99);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: "SELECT col2 IN ( 98 + col0 / 99 ) from tab0;",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query: "SELECT col2 IN ( 98 + 97 / 99 ) from tab0;",
				Expected: []sql.Row{
					{false},
				},
			},
			{
				Query:    "SELECT * FROM tab0 WHERE col2 IN ( 98 + 97 / 99 );",
				Expected: []sql.Row{},
			},
			{
				Query:    "SELECT ALL * FROM tab0 AS cor0 WHERE col2 IN ( 39 + + 89, col0 + + col1 + + ( - ( - col0 ) ) / col2, + ( col0 ) + - 99, + col1, + col2 * - + col2 * - 12 + col1 + - 66 );",
				Expected: []sql.Row{},
			},
		},
	},
	{
		Name:    "negative int limits",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(i8 tinyint, i16 smallint, i24 mediumint, i32 int, i64 bigint);",
			"INSERT INTO t VALUES(-128, -32768, -8388608, -2147483648, -9223372036854775808);",
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "SELECT -i8, -i16, -i24, -i32 from t;",
				Expected: []sql.Row{
					{128, 32768, 8388608, 2147483648},
				},
			},
			{
				Query:          "SELECT -i64 from t;",
				ExpectedErrStr: "BIGINT out of range for -9223372036854775808",
			},
		},
	},
	{
		Name:    "negative int limits",
		Dialect: "mysql",
		SetUpScript: []string{
			"CREATE TABLE t(i8 tinyint, i16 smallint, i24 mediumint, i32 int, i64 bigint);",
			"INSERT INTO t VALUES(-128, -32768, -8388608, -2147483648, -9223372036854775808);",
		},
		Assertions: []ScriptTestAssertion{
			{
				SkipResultCheckOnServerEngine: true,
				Query:                         "SELECT -i8, -i16, -i24, -i32 from t;",
				Expected: []sql.Row{
					{128, 32768, 8388608, 2147483648},
				},
			},
			{
				Query:          "SELECT -i64 from t;",
				ExpectedErrStr: "BIGINT out of range for -9223372036854775808",
			},
		},
	},
	{
		Name:    "signed int with overflowing filters",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table ti8  (i tinyint primary key);",
			"insert into ti8 values (-128), (-1), (0), (1), (127);",

			"create table ti16 (i smallint primary key);",
			"insert into ti16 values (-32768), (-1), (0), (1), (32767);",

			"create table ti24 (i mediumint primary key);",
			"insert into ti24 values (-8388608), (-1), (0), (1), (8388607);",

			"create table ti32 (i int primary key);",
			"insert into ti32 values (-2147483648), (-1), (0), (1), (2147483647);",

			"create table ti64 (i bigint primary key);",
			"insert into ti64 values (-9223372036854775808), (-1), (0), (1), (9223372036854775807);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from ti8 where i = 999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti8 where i = -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i != 999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query: "select * from ti8 where i != -999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query:    "select * from ti8 where i > 999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i > -999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query:    "select * from ti8 where i >= 999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i >= -999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query: "select * from ti8 where i < 999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query:    "select * from ti8 where i < -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i <= 999;",
				Expected: []sql.Row{
					{-128},
					{-1},
					{0},
					{1},
					{127},
				},
			},
			{
				Query:    "select * from ti8 where i <= -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti8 where i in (0, 999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti8 where i in (0, -999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti8 where i not in (0, 999);",
				Expected: []sql.Row{
					{-128},
					{-1},
					{1},
					{127},
				},
			},
			{
				Query: "select * from ti8 where i not in (0, -999);",
				Expected: []sql.Row{
					{-128},
					{-1},
					{1},
					{127},
				},
			},

			{
				Query:    "select * from ti16 where i = 99999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti16 where i = -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i != 99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query: "select * from ti16 where i != -99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query:    "select * from ti16 where i > 99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i > -99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query:    "select * from ti16 where i >= 99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i >= -99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query: "select * from ti16 where i < 99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query:    "select * from ti16 where i < -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i <= 99999;",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{0},
					{1},
					{32767},
				},
			},
			{
				Query:    "select * from ti16 where i <= -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti16 where i in (0, 99999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti16 where i in (0, -99999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti16 where i not in (0, 99999);",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{1},
					{32767},
				},
			},
			{
				Query: "select * from ti16 where i not in (0, -99999);",
				Expected: []sql.Row{
					{-32768},
					{-1},
					{1},
					{32767},
				},
			},

			{
				Query:    "select * from ti24 where i = 9999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti24 where i = -999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i != 9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query: "select * from ti24 where i != -9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query:    "select * from ti24 where i > 9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i > -9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query:    "select * from ti24 where i >= 9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i >= -9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query: "select * from ti24 where i < 9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query:    "select * from ti24 where i < -9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i <= 9999999;",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{0},
					{1},
					{8388607},
				},
			},
			{
				Query:    "select * from ti24 where i <= -9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti24 where i in (0, 9999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti24 where i in (0, -9999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti24 where i not in (0, 9999999);",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{1},
					{8388607},
				},
			},
			{
				Query: "select * from ti24 where i not in (0, -9999999);",
				Expected: []sql.Row{
					{-8388608},
					{-1},
					{1},
					{8388607},
				},
			},

			{
				Query:    "select * from ti32 where i = 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti32 where i = -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i != 9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query: "select * from ti32 where i != -9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query:    "select * from ti32 where i > 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i > -9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query:    "select * from ti32 where i >= 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i >= -9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query: "select * from ti32 where i < 9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query:    "select * from ti32 where i < -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i <= 9999999999;",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{0},
					{1},
					{2147483647},
				},
			},
			{
				Query:    "select * from ti32 where i <= -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti32 where i in (0, 9999999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti32 where i in (0, -9999999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti32 where i not in (0, 9999999999);",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{1},
					{2147483647},
				},
			},
			{
				Query: "select * from ti32 where i not in (0, -9999999999);",
				Expected: []sql.Row{
					{-2147483648},
					{-1},
					{1},
					{2147483647},
				},
			},

			{
				Query:    "select * from ti64 where i = 9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from ti64 where i = -9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i != 9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query: "select * from ti64 where i != -9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query:    "select * from ti64 where i > 9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i > -9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query:    "select * from ti64 where i >= 9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i >= -9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query: "select * from ti64 where i < 9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query:    "select * from ti64 where i < -9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i <= 9999999999999999999;",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{0},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query:    "select * from ti64 where i <= -9999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from ti64 where i in (0, 9999999999999999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti64 where i in (0, -9999999999999999999);",
				Expected: []sql.Row{
					{0},
				},
			},
			{
				Query: "select * from ti64 where i not in (0, 9999999999999999999);",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{1},
					{9223372036854775807},
				},
			},
			{
				Query: "select * from ti64 where i not in (0, -9999999999999999999);",
				Expected: []sql.Row{
					{-9223372036854775808},
					{-1},
					{1},
					{9223372036854775807},
				},
			},
		},
	},
	{
		Name:    "unsigned int with overflowing filters",
		Dialect: "mysql",
		SetUpScript: []string{
			"create table tui8 (i tinyint unsigned primary key);",
			"insert into tui8 values (0), (1), (255);",

			"create table tui16 (i smallint unsigned primary key);",
			"insert into tui16 values (0), (1), (65535);",

			"create table tui24 (i mediumint unsigned primary key);",
			"insert into tui24 values (0), (1), (16777215);",

			"create table tui32 (i int unsigned primary key);",
			"insert into tui32 values (0), (1), (4294967295);",

			"create table tui64 (i bigint unsigned primary key);",
			"insert into tui64 values (0), (1), (18446744073709551615);",
		},
		Assertions: []ScriptTestAssertion{
			{
				Query:    "select * from tui8 where i = 999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui8 where i = -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i != 999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query: "select * from tui8 where i != -999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query:    "select * from tui8 where i > 999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i > -999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query:    "select * from tui8 where i >= 999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i >= -999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query: "select * from tui8 where i < 999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query:    "select * from tui8 where i < -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i <= 999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query:    "select * from tui8 where i <= -999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui8 where i in (0, 999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui8 where i in (0, -999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui8 where i not in (0, 999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(255)},
				},
			},
			{
				Query: "select * from tui8 where i not in (0, -999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(255)},
				},
			},

			{
				Query:    "select * from tui16 where i = 99999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui16 where i = -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i != 99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query: "select * from tui16 where i != -99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query:    "select * from tui16 where i > 99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i > -99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query:    "select * from tui16 where i >= 99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i >= -99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query: "select * from tui16 where i < 99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query:    "select * from tui16 where i < -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i <= 99999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query:    "select * from tui16 where i <= -99999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui16 where i in (0, 99999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui16 where i in (0, -99999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui16 where i not in (0, 99999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(65535)},
				},
			},
			{
				Query: "select * from tui16 where i not in (0, -99999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(65535)},
				},
			},

			{
				Query:    "select * from tui24 where i = 99999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui24 where i = -9999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i != 99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query: "select * from tui24 where i != -99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query:    "select * from tui24 where i > 99999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i > -99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query:    "select * from tui24 where i >= 99999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i >= -99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query: "select * from tui24 where i < 99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query:    "select * from tui24 where i < -99999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i <= 99999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query:    "select * from tui24 where i <= -99999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui24 where i in (0, 99999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui24 where i in (0, -99999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui24 where i not in (0, 99999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(16777215)},
				},
			},
			{
				Query: "select * from tui24 where i not in (0, -99999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(16777215)},
				},
			},

			{
				Query:    "select * from tui32 where i = 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui32 where i = -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i != 9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query: "select * from tui32 where i != -9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query:    "select * from tui32 where i > 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i > -9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query:    "select * from tui32 where i >= 9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i >= -9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query: "select * from tui32 where i < 9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query:    "select * from tui32 where i < -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i <= 9999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query:    "select * from tui32 where i <= -9999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui32 where i in (0, 9999999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui32 where i in (0, -9999999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui32 where i not in (0, 9999999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(4294967295)},
				},
			},
			{
				Query: "select * from tui32 where i not in (0, -9999999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(4294967295)},
				},
			},

			{
				Query:    "select * from tui64 where i = 99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query:    "select * from tui64 where i = -99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i != 99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query: "select * from tui64 where i != -99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query:    "select * from tui64 where i > 99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i > -99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query:    "select * from tui64 where i >= 99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i >= -99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query: "select * from tui64 where i < 99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query:    "select * from tui64 where i < -99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i <= 99999999999999999999;",
				Expected: []sql.Row{
					{uint64(0)},
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query:    "select * from tui64 where i <= -99999999999999999999;",
				Expected: []sql.Row{},
			},
			{
				Query: "select * from tui64 where i in (0, 99999999999999999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui64 where i in (0, -99999999999999999999);",
				Expected: []sql.Row{
					{uint64(0)},
				},
			},
			{
				Query: "select * from tui64 where i not in (0, 99999999999999999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
			{
				Query: "select * from tui64 where i not in (0, -99999999999999999999);",
				Expected: []sql.Row{
					{uint64(1)},
					{uint64(18446744073709551615)},
				},
			},
		},
	},
	{
		Name: "Greatest and least with decimal arguments",
		// TODO: This should work in Doltgres https://github.com/dolthub/doltgresql/issues/2378
		Dialect: "mysql",
		SetUpScript: []string{
			"create table t(a decimal(6, 2), b decimal(8, 5), c decimal(5, 1));",
			"insert into t values (2.75, 8.8, 3.1);",
		},
		Assertions: []ScriptTestAssertion{
			{
				// https://github.com/dolthub/dolt/issues/10562
				// https://github.com/dolthub/dolt/issues/10567
				Query:    "select greatest(a, b, c), least(a, b, c) from t;",
				Expected: []sql.Row{{"8.80000", "2.75000"}},
			},
			{
				Query: `CREATE TABLE decimal_metadata AS
SELECT
    GREATEST(a, b, c) AS g,
    LEAST(a, b, c) AS l
FROM t
WHERE FALSE`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT
    column_name,
    column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'decimal_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{{"g", "decimal(9,5)"}, {"l", "decimal(9,5)"}},
			},
		},
	},
	{
		Name:    "Greatest and least preserve exact decimal digits",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE exact_values (
    id INT PRIMARY KEY,
    a DECIMAL(31, 30),
    b DECIMAL(31, 30)
)`,
			`INSERT INTO exact_values VALUES
    (1, '1.000000000000000000000000000002', '1.000000000000000000000000000001'),
    (2, '-1.000000000000000000000000000002', '-1.000000000000000000000000000001'),
    (3, NULL, '1.000000000000000000000000000001'),
    (4, '1.000000000000000000000000000002', NULL)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
    id,
    GREATEST(a, b),
    LEAST(a, b),
    GREATEST(b, a),
    LEAST(b, a)
FROM exact_values
ORDER BY id`,
				Expected: []sql.Row{
					{
						1,
						"1.000000000000000000000000000002",
						"1.000000000000000000000000000001",
						"1.000000000000000000000000000002",
						"1.000000000000000000000000000001",
					},
					{
						2,
						"-1.000000000000000000000000000001",
						"-1.000000000000000000000000000002",
						"-1.000000000000000000000000000001",
						"-1.000000000000000000000000000002",
					},
					{3, nil, nil, nil, nil},
					{4, nil, nil, nil, nil},
				},
			},
			{
				Query: `SELECT
    GREATEST(CAST(1.25 AS DECIMAL(3, 2)), NULL),
    LEAST(NULL, CAST(1.25 AS DECIMAL(3, 2)))`,
				Expected: []sql.Row{{nil, nil}},
			},
			{
				Query: `SELECT
    GREATEST(
        1.0000000000000000000000000000000000000002,
        1.0000000000000000000000000000000000000001
    ),
    LEAST(
        1.0000000000000000000000000000000000000002,
        1.0000000000000000000000000000000000000001
    )`,
				Expected: []sql.Row{
					{
						"1.0000000000000000000000000000000000000002",
						"1.0000000000000000000000000000000000000001",
					},
				},
			},
			{
				Query: `SELECT
    GREATEST(1.0, 1.0000000000000000000000000000000000000000),
    LEAST(1.0, 1.0000000000000000000000000000000000000000)`,
				Expected: []sql.Row{
					{
						"1.0000000000000000000000000000000000000000",
						"1.0000000000000000000000000000000000000000",
					},
				},
			},
		},
	},
	{
		Name:    "Greatest and least decimal precision for integer expressions",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE integer_values (i BIGINT, u BIGINT UNSIGNED, b BOOLEAN)`,
			`INSERT INTO integer_values VALUES (2, 2, TRUE)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
    GREATEST(1.1, 2) AS g_literal,
    LEAST(1.1, TRUE) AS l_boolean,
    GREATEST(1.1, CAST(2 AS SIGNED)) AS g_cast,
    LEAST(1.1, CAST(2 AS UNSIGNED)) AS l_cast`,
				Expected: []sql.Row{{"2.0", "1.0", "2.0", "1.1"}},
				ExpectedColumns: sql.Schema{
					{Name: "g_literal", Type: types.MustCreateDecimalType(2, 1)},
					{Name: "l_boolean", Type: types.MustCreateDecimalType(2, 1)},
					{Name: "g_cast", Type: types.MustCreateDecimalType(21, 1)},
					{Name: "l_cast", Type: types.MustCreateDecimalType(22, 1)},
				},
			},
			{
				Query: `CREATE TABLE expression_metadata AS
SELECT
    GREATEST(1.1, 2) AS g_literal,
    LEAST(1.1, 2) AS l_literal,
    GREATEST(1.1, TRUE) AS g_boolean,
    LEAST(1.1, TRUE) AS l_boolean,
    GREATEST(1.1, CAST(2 AS SIGNED)) AS g_cast,
    LEAST(1.1, CAST(2 AS SIGNED)) AS l_cast,
    GREATEST(1.1, CAST(2 AS UNSIGNED)) AS g_unsigned_cast,
    LEAST(1.1, CAST(2 AS UNSIGNED)) AS l_unsigned_cast
FROM integer_values`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: `SELECT column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'expression_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"g_literal", "decimal(2,1)"},
					{"l_literal", "decimal(2,1)"},
					{"g_boolean", "decimal(2,1)"},
					{"l_boolean", "decimal(2,1)"},
					{"g_cast", "decimal(21,1)"},
					{"l_cast", "decimal(21,1)"},
					{"g_unsigned_cast", "decimal(22,1)"},
					{"l_unsigned_cast", "decimal(22,1)"},
				},
			},
			{
				Query: `SELECT * FROM expression_metadata`,
				Expected: []sql.Row{{
					"2.0",
					"1.1",
					"1.1",
					"1.0",
					"2.0",
					"1.1",
					"2.0",
					"1.1",
				}},
			},
			{
				Query: `CREATE TABLE literal_metadata AS
SELECT
    GREATEST(1.1, 0) AS g_zero,
    LEAST(1.1, 0) AS l_zero,
    GREATEST(1.1, FALSE) AS g_false,
    LEAST(1.1, FALSE) AS l_false,
    GREATEST(1.1, -123) AS g_negative,
    LEAST(1.1, -123) AS l_negative,
    GREATEST(1.1, 123) AS g_digits,
    LEAST(1.1, 123) AS l_digits,
    GREATEST(1.1, 9223372036854775807) AS g_signed_max,
    LEAST(1.1, 9223372036854775807) AS l_signed_max,
    GREATEST(1.1, -9223372036854775808) AS g_signed_min,
    LEAST(1.1, -9223372036854775808) AS l_signed_min,
    GREATEST(1.1, 18446744073709551615) AS g_unsigned_max,
    LEAST(1.1, 18446744073709551615) AS l_unsigned_max
FROM integer_values`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: `SELECT column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'literal_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"g_zero", "decimal(2,1)"},
					{"l_zero", "decimal(2,1)"},
					{"g_false", "decimal(2,1)"},
					{"l_false", "decimal(2,1)"},
					{"g_negative", "decimal(4,1)"},
					{"l_negative", "decimal(4,1)"},
					{"g_digits", "decimal(4,1)"},
					{"l_digits", "decimal(4,1)"},
					{"g_signed_max", "decimal(20,1)"},
					{"l_signed_max", "decimal(20,1)"},
					{"g_signed_min", "decimal(20,1)"},
					{"l_signed_min", "decimal(20,1)"},
					{"g_unsigned_max", "decimal(21,1)"},
					{"l_unsigned_max", "decimal(21,1)"},
				},
			},
			{
				Query: `SELECT * FROM literal_metadata`,
				Expected: []sql.Row{{
					"1.1",
					"0.0",
					"1.1",
					"0.0",
					"1.1",
					"-123.0",
					"123.0",
					"1.1",
					"9223372036854775807.0",
					"1.1",
					"1.1",
					"-9223372036854775808.0",
					"18446744073709551615.0",
					"1.1",
				}},
			},
			{
				Query: `CREATE TABLE column_metadata AS
SELECT
    GREATEST(1.1, i) AS g_signed_column,
    LEAST(1.1, i) AS l_signed_column,
    GREATEST(1.1, u) AS g_unsigned_column,
    LEAST(1.1, u) AS l_unsigned_column,
    GREATEST(1.1, b) AS g_boolean_column,
    LEAST(1.1, b) AS l_boolean_column,
    GREATEST(1.1, CAST(i AS SIGNED)) AS g_signed_cast,
    LEAST(1.1, CAST(i AS SIGNED)) AS l_signed_cast,
    GREATEST(1.1, CAST(i AS UNSIGNED)) AS g_unsigned_cast,
    LEAST(1.1, CAST(i AS UNSIGNED)) AS l_unsigned_cast
FROM integer_values`,
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query: `SELECT column_name, column_type
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = 'column_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"g_signed_column", "decimal(20,1)"},
					{"l_signed_column", "decimal(20,1)"},
					{"g_unsigned_column", "decimal(21,1)"},
					{"l_unsigned_column", "decimal(21,1)"},
					{"g_boolean_column", "decimal(4,1)"},
					{"l_boolean_column", "decimal(4,1)"},
					{"g_signed_cast", "decimal(21,1)"},
					{"l_signed_cast", "decimal(21,1)"},
					{"g_unsigned_cast", "decimal(22,1)"},
					{"l_unsigned_cast", "decimal(22,1)"},
				},
			},
			{
				Query: `SELECT * FROM column_metadata`,
				Expected: []sql.Row{{
					"2.0",
					"1.1",
					"2.0",
					"1.1",
					"1.1",
					"1.0",
					"2.0",
					"1.1",
					"2.0",
					"1.1",
				}},
			},
		},
	},
	{
		Name:    "Greatest and least at the decimal precision limit",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE wide_values (
    id INT PRIMARY KEY,
    a DECIMAL(65, 0),
    b DECIMAL(2, 1),
    c DECIMAL(31, 30)
)`,
			`INSERT INTO wide_values VALUES
    (1, '99999999999999999999999999999999999999999999999999999999999999999', 0.1, 0.1),
    (2, '-99999999999999999999999999999999999999999999999999999999999999999', -0.1, -0.1),
    (3, 1, 0.1, 0.1)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
    id,
    GREATEST(a, b),
    LEAST(a, b),
    GREATEST(b, a),
    LEAST(b, a)
FROM wide_values
ORDER BY id`,
				Expected: []sql.Row{
					{
						1,
						"99999999999999999999999999999999999999999999999999999999999999999.0",
						"0.1",
						"99999999999999999999999999999999999999999999999999999999999999999.0",
						"0.1",
					},
					{
						2,
						"-0.1",
						"-99999999999999999999999999999999999999999999999999999999999999999.0",
						"-0.1",
						"-99999999999999999999999999999999999999999999999999999999999999999.0",
					},
					{3, "1.0", "0.1", "1.0", "0.1"},
				},
			},
			{
				Query: "SELECT id, GREATEST(a,c), LEAST(a,c) FROM wide_values ORDER BY id",
				Expected: []sql.Row{
					{
						1,
						"99999999999999999999999999999999999999999999999999999999999999999.000000000",
						"0.100000000000000000000000000000",
					},
					{
						2,
						"-0.100000000000000000000000000000",
						"-99999999999999999999999999999999999999999999999999999999999999999.000000000",
					},
					{3, "1.000000000000000000000000000000", "0.100000000000000000000000000000"},
				},
			},
			{
				Query: `CREATE TABLE wide_metadata AS
SELECT
    GREATEST(a, c) AS g,
    LEAST(a, c) AS l
FROM wide_values
WHERE FALSE`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT
    column_name,
    column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'wide_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{{"g", "decimal(65,30)"}, {"l", "decimal(65,30)"}},
			},
		},
	},
	{
		Name:    "Greatest and least with decimals and integer types",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE mixed_values (
    id INT PRIMARY KEY,
    i BIGINT,
    u BIGINT UNSIGNED,
    d DECIMAL(2, 1),
    f DOUBLE
)`,
			`INSERT INTO mixed_values VALUES
    (1, -9223372036854775808, 18446744073709551615, 0.1, 1.25),
    (2, 9007199254740993, 9007199254740994, 0.1, 1.25),
    (3, 1, 2, NULL, 1.25)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `SELECT
    id,
    GREATEST(i, u, d),
    LEAST(i, u, d),
    GREATEST(d, u, i),
    LEAST(d, u, i)
FROM mixed_values
ORDER BY id`,
				Expected: []sql.Row{
					{
						1,
						"18446744073709551615.0",
						"-9223372036854775808.0",
						"18446744073709551615.0",
						"-9223372036854775808.0",
					},
					{2, "9007199254740994.0", "0.1", "9007199254740994.0", "0.1"},
					{3, nil, nil, nil, nil},
				},
			},
			{
				Query:    "SELECT id, GREATEST(d,f), LEAST(d,f) FROM mixed_values ORDER BY id",
				Expected: []sql.Row{{1, float64(1.25), float64(0.1)}, {2, float64(1.25), float64(0.1)}, {3, nil, nil}},
			},
			{
				Query: `CREATE TABLE mixed_metadata AS
SELECT
    GREATEST(i, d) AS signed_decimal,
    LEAST(u, d) AS unsigned_decimal,
    GREATEST(d, f) AS approximate
FROM mixed_values
WHERE FALSE`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT
    column_name,
    column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'mixed_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"signed_decimal", "decimal(20,1)"},
					{"unsigned_decimal", "decimal(21,1)"},
					{"approximate", "double"},
				},
			},
		},
	},
	{
		Name:    "Greatest and least decimal metadata across integer widths",
		Dialect: "mysql",
		SetUpScript: []string{
			`CREATE TABLE integer_values (
    i8 TINYINT,
    u8 TINYINT UNSIGNED,
    i16 SMALLINT,
    u16 SMALLINT UNSIGNED,
    i24 MEDIUMINT,
    u24 MEDIUMINT UNSIGNED,
    i32 INT,
    u32 INT UNSIGNED,
    i64 BIGINT,
    u64 BIGINT UNSIGNED,
    d DECIMAL(2, 1)
)`,
		},
		Assertions: []ScriptTestAssertion{
			{
				Query: `CREATE TABLE integer_metadata AS
SELECT
    GREATEST(i8, d) AS i8,
    GREATEST(u8, d) AS u8,
    GREATEST(i16, d) AS i16,
    GREATEST(u16, d) AS u16,
    GREATEST(i24, d) AS i24,
    GREATEST(u24, d) AS u24,
    GREATEST(i32, d) AS i32,
    GREATEST(u32, d) AS u32,
    GREATEST(i64, d) AS i64,
    GREATEST(u64, d) AS u64
FROM integer_values`,
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query: `SELECT
    column_name,
    column_type
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name = 'integer_metadata'
ORDER BY ordinal_position`,
				Expected: []sql.Row{
					{"i8", "decimal(4,1)"},
					{"u8", "decimal(4,1)"},
					{"i16", "decimal(6,1)"},
					{"u16", "decimal(6,1)"},
					{"i24", "decimal(9,1)"},
					{"u24", "decimal(9,1)"},
					{"i32", "decimal(11,1)"},
					{"u32", "decimal(11,1)"},
					{"i64", "decimal(20,1)"},
					{"u64", "decimal(21,1)"},
				},
			},
		},
	},
}

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
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from decimal_uk where b = 9999999999999999999999999.9999999999999999999999999999999",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from decimal_nuk where b = 9999999999999999999999999.9999999999999999999999999999999",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from decimal_pk where a = 1 and b = 9999999999999999999999999.9999999999999999999999999999999",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from i8 where i = 127",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from i16 where i = 32767",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from i32 where i = 2147483647",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from i64 where i = 9223372036854775807",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from ui8 where i = 255",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from ui16 where i = 65535",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from ui32 where i = 4294967295",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
			{
				Query:    "delete from ui64 where i = 18446744073709551615",
				Expected: []sql.Row{{types.NewOkResult(1)}},
			},
		},
	},
	// https://github.com/dolthub/dolt/issues/7130
	{
		Name:        "Test large DOUBLE arithmetic",
		SetUpScript: []string{},
		Assertions: []ScriptTestAssertion{
			{Query: "SELECT 1.7e308+0,1.7e308+0.0,1.7e308+1e10,1.7e64+123,1.7e65+123", Expected: []sql.Row{{float64(1.7e308), float64(1.7e308), float64(1.7e308), float64(1.7e64), float64(1.7e65)}}},
		},
	},
}
