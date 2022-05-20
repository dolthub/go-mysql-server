// Copyright 2022 DoltHub, Inc.
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

var IndexPlanTests = []QueryPlanTest{
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<25) OR (v1>24));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>=99 AND v2<>83) OR (v1>=1));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[1, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=38 AND v2<41) OR (v1>60)) OR (v1<22));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 22), (-∞, ∞)}, {[22, 38], (-∞, 41)}, {(60, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>92 AND v2>25) OR (v1 BETWEEN 6 AND 24 AND v2=80));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[6, 24], [80, 80]}, {(92, ∞), (25, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=29) OR (v1=49 AND v2<48));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 29], (-∞, ∞)}, {[49, 49], (-∞, 48)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>75) OR (v1<=11));`,
		ExpectedPlan: "Filter((NOT((comp_index_t0.v1 = 75))) OR (comp_index_t0.v1 <= 11))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 75), (-∞, ∞)}, {(75, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=86) OR (v1<>9)) AND (v1=87 AND v2<=45);`,
		ExpectedPlan: "Filter((comp_index_t0.v1 <= 86) OR (NOT((comp_index_t0.v1 = 9))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[87, 87], (-∞, 45]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=5) OR (v1=71)) OR (v1<>96));`,
		ExpectedPlan: "Filter(((comp_index_t0.v1 <= 5) OR (comp_index_t0.v1 = 71)) OR (NOT((comp_index_t0.v1 = 96))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 96), (-∞, ∞)}, {(96, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=97) OR (v1 BETWEEN 36 AND 98));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 98], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1=86 AND v2>41) OR (v1<>6 AND v2>16));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 6), (16, ∞)}, {(6, ∞), (16, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<>22 AND v2>18) OR (v1<>12)) OR (v1<=34));`,
		ExpectedPlan: "Filter((((NOT((comp_index_t0.v1 = 22))) AND (comp_index_t0.v2 > 18)) OR (NOT((comp_index_t0.v1 = 12)))) OR (comp_index_t0.v1 <= 34))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<11) OR (v1>=66 AND v2=22));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 11), (-∞, ∞)}, {[66, ∞), [22, 22]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>45 AND v2>37) OR (v1<98 AND v2<=35));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 45), (37, ∞)}, {(-∞, 98), (-∞, 35]}, {(45, ∞), (37, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>=16 AND v2>96) OR (v1<80));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 80), (-∞, ∞)}, {[80, ∞), (96, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=98) OR (v1<85 AND v2>60)) OR (v1<>53 AND v2 BETWEEN 82 AND 89));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 98], (-∞, ∞)}, {(98, ∞), [82, 89]}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((((v1<71 AND v2<7) OR (v1<=21 AND v2<=48)) OR (v1=44 AND v2 BETWEEN 21 AND 83)) OR (v1<=72 AND v2<>27)) OR (v1=35 AND v2 BETWEEN 78 AND 89));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 21], (-∞, ∞)}, {(21, 44), (-∞, 27)}, {(21, 44), (27, ∞)}, {[44, 44], (-∞, ∞)}, {(44, 72], (-∞, 27)}, {(44, 72], (27, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=16) OR (v1>=77 AND v2>77)) OR (v1>19 AND v2>27));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 16], (-∞, ∞)}, {(19, ∞), (27, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>=46) AND (v1>=28 AND v2<>68) OR (v1>=33 AND v2<>39));`,
		ExpectedPlan: "Filter(((comp_index_t0.v1 >= 46) AND ((comp_index_t0.v1 >= 28) AND (NOT((comp_index_t0.v2 = 68))))) OR ((comp_index_t0.v1 >= 33) AND (NOT((comp_index_t0.v2 = 39)))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[33, 46), (-∞, 39)}, {[33, 46), (39, ∞)}, {[46, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<39 AND v2<10) OR (v1>64 AND v2<=15)) AND (v1>=41);`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(64, ∞), (-∞, 15]}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=91) OR (v1<70 AND v2>=23)) OR (v1>23 AND v2<38));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 91], (-∞, ∞)}, {(91, ∞), (-∞, 38)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((v1<>45 AND v2=70) OR (v1 BETWEEN 40 AND 96 AND v2 BETWEEN 48 AND 96)) OR (v1<>87 AND v2<31)) OR (v1<>62 AND v2=51)) AND (v1>=47 AND v2<29);`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[47, 87), (-∞, 29)}, {(87, ∞), (-∞, 29)}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<71) OR (v1 BETWEEN 46 AND 79));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 79], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>52) OR (v1<=14));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 14], (-∞, ∞)}, {(52, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>74) OR (v1<>40 AND v2>=54));`,
		ExpectedPlan: "Filter((NOT((comp_index_t0.v1 = 74))) OR ((NOT((comp_index_t0.v1 = 40))) AND (comp_index_t0.v2 >= 54)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 74), (-∞, ∞)}, {[74, 74], [54, ∞)}, {(74, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=69 AND v2<24) OR (v1<77 AND v2<=53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 77), (-∞, 53]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1=78 AND v2=87) OR (v1 BETWEEN 37 AND 58 AND v2>=30)) AND (v1=86 AND v2 BETWEEN 0 AND 70);`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>94) OR (v1<=52));`,
		ExpectedPlan: "Filter((NOT((comp_index_t0.v1 = 94))) OR (comp_index_t0.v1 <= 52))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 94), (-∞, ∞)}, {(94, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<>23 AND v2>64) OR (v1>73 AND v2<=66)) OR (v1 BETWEEN 39 AND 69 AND v2>84));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 23), (64, ∞)}, {(23, 73], (64, ∞)}, {(73, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>54 AND v2<16) OR (v1<74 AND v2>29)) AND (v1 BETWEEN 34 AND 48);`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[34, 48], (29, ∞)}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>44 AND v2>12) OR (v1<=5 AND v2>27));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 44), (12, ∞)}, {(44, ∞), (12, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=54 AND v2<>13) OR (v1>84));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 54], (-∞, 13)}, {(-∞, 54], (13, ∞)}, {(84, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>1 AND v2<>51) OR (v1=28));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(1, 28), (-∞, 51)}, {(1, 28), (51, ∞)}, {[28, 28], (-∞, ∞)}, {(28, ∞), (-∞, 51)}, {(28, ∞), (51, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1>35) OR (v1 BETWEEN 11 AND 21)) OR (v1<>98));`,
		ExpectedPlan: "Filter(((comp_index_t0.v1 > 35) OR (comp_index_t0.v1 BETWEEN 11 AND 21)) OR (NOT((comp_index_t0.v1 = 98))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1=16 AND v2=57) OR (v1<46 AND v2 BETWEEN 78 AND 89));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 46), [78, 89]}, {[16, 16], [57, 57]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<53 AND v2<10) AND (v1<>37) OR (v1>23));`,
		ExpectedPlan: "Filter((((comp_index_t0.v1 < 53) AND (comp_index_t0.v2 < 10)) AND (NOT((comp_index_t0.v1 = 37)))) OR (comp_index_t0.v1 > 23))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 23], (-∞, 10)}, {(23, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((((v1<>30) OR (v1>=6 AND v2 BETWEEN 62 AND 65)) OR (v1<>89)) OR (v1<=40 AND v2>=73)) OR (v1<99));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t0.v1 = 30))) OR ((comp_index_t0.v1 >= 6) AND (comp_index_t0.v2 BETWEEN 62 AND 65))) OR (NOT((comp_index_t0.v1 = 89)))) OR ((comp_index_t0.v1 <= 40) AND (comp_index_t0.v2 >= 73))) OR (comp_index_t0.v1 < 99))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1 BETWEEN 34 AND 34 AND v2 BETWEEN 0 AND 91) OR (v1 BETWEEN 54 AND 77 AND v2>92));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[34, 34], [0, 91]}, {[54, 77], (92, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((((v1<=55) OR (v1>=46 AND v2<=26)) OR (v1 BETWEEN 8 AND 54)) OR (v1>26 AND v2 BETWEEN 62 AND 89)) OR (v1<31 AND v2=11)) OR (v1>9 AND v2=60));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 55], (-∞, ∞)}, {(55, ∞), (-∞, 26]}, {(55, ∞), [60, 60]}, {(55, ∞), [62, 89]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1 BETWEEN 17 AND 54 AND v2>=37) AND (v1<42 AND v2=96) OR (v1<>50));`,
		ExpectedPlan: "Filter((((comp_index_t0.v1 BETWEEN 17 AND 54) AND (comp_index_t0.v2 >= 37)) AND ((comp_index_t0.v1 < 42) AND (comp_index_t0.v2 = 96))) OR (NOT((comp_index_t0.v1 = 50))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 50), (-∞, ∞)}, {(50, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>39 AND v2>66) OR (v1=99));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(39, 99), (66, ∞)}, {[99, 99], (-∞, ∞)}, {(99, ∞), (66, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1 BETWEEN 24 AND 66) OR (v1<=81 AND v2<>29));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 24), (-∞, 29)}, {(-∞, 24), (29, ∞)}, {[24, 66], (-∞, ∞)}, {(66, 81], (-∞, 29)}, {(66, 81], (29, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<>18 AND v2<>8) OR (v1>=10 AND v2>3)) OR (v1=53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 10), (-∞, 8)}, {(-∞, 10), (8, ∞)}, {[10, 18), (-∞, ∞)}, {[18, 18], (3, ∞)}, {(18, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>=42 AND v2>34) OR (v1<=40 AND v2<=49));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 40], (-∞, 49]}, {[42, ∞), (34, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1 BETWEEN 8 AND 38) OR (v1>=23 AND v2 BETWEEN 36 AND 49));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[8, 38], (-∞, ∞)}, {(38, ∞), [36, 49]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>57 AND v2 BETWEEN 2 AND 93) OR (v1=52));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 52), [2, 93]}, {[52, 52], (-∞, ∞)}, {(52, 57), [2, 93]}, {(57, ∞), [2, 93]}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((v1<24) OR (v1<41)) OR (v1<12 AND v2=2)) OR (v1=3 AND v2<>66));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 41), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=52 AND v2<40) AND (v1<30) OR (v1<=75 AND v2 BETWEEN 54 AND 54)) OR (v1<>31 AND v2<>56));`,
		ExpectedPlan: "Filter(((((comp_index_t0.v1 <= 52) AND (comp_index_t0.v2 < 40)) AND (comp_index_t0.v1 < 30)) OR ((comp_index_t0.v1 <= 75) AND (comp_index_t0.v2 BETWEEN 54 AND 54))) OR ((NOT((comp_index_t0.v1 = 31))) AND (NOT((comp_index_t0.v2 = 56)))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 31), (-∞, 56)}, {(-∞, 31), (56, ∞)}, {[31, 31], [54, 54]}, {(31, ∞), (-∞, 56)}, {(31, ∞), (56, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>52 AND v2<90) OR (v1 BETWEEN 27 AND 77 AND v2 BETWEEN 49 AND 83));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 52), (-∞, 90)}, {[52, 52], [49, 83]}, {(52, ∞), (-∞, 90)}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>2) OR (v1<72 AND v2>=21)) AND (v1=69 AND v2 BETWEEN 44 AND 48);`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[69, 69], [44, 48]}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((v1>77) OR (v1=57)) OR (v1>9 AND v2>80)) OR (v1=22));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(9, 22), (80, ∞)}, {[22, 22], (-∞, ∞)}, {(22, 57), (80, ∞)}, {[57, 57], (-∞, ∞)}, {(57, 77], (80, ∞)}, {(77, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((v1>28) OR (v1<=30 AND v2=30)) OR (v1<29)) OR (v1 BETWEEN 54 AND 74));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>30 AND v2 BETWEEN 20 AND 41) OR (v1>=69 AND v2=51));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 30), [20, 41]}, {(30, ∞), [20, 41]}, {[69, ∞), [51, 51]}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>39) OR (v1=55)) AND (v1=67);`,
		ExpectedPlan: "Filter((NOT((comp_index_t0.v1 = 39))) OR (comp_index_t0.v1 = 55))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[67, 67], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<20 AND v2<=46) OR (v1<>4 AND v2=26)) OR (v1>36 AND v2<>13));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 20), (-∞, 46]}, {[20, 36], [26, 26]}, {(36, ∞), (-∞, 13)}, {(36, ∞), (13, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=5 AND v2>66) OR (v1<=0)) OR (v1 BETWEEN 10 AND 87));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 0], (-∞, ∞)}, {(0, 5], (66, ∞)}, {[10, 87], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((((v1<>99 AND v2 BETWEEN 12 AND 31) OR (v1<56 AND v2<>69)) OR (v1>=37 AND v2<47)) OR (v1<=98 AND v2=50)) AND (v1 BETWEEN 15 AND 47) OR (v1>55 AND v2>85)) OR (v1>86));`,
		ExpectedPlan: "Filter((((((((NOT((comp_index_t0.v1 = 99))) AND (comp_index_t0.v2 BETWEEN 12 AND 31)) OR ((comp_index_t0.v1 < 56) AND (NOT((comp_index_t0.v2 = 69))))) OR ((comp_index_t0.v1 >= 37) AND (comp_index_t0.v2 < 47))) OR ((comp_index_t0.v1 <= 98) AND (comp_index_t0.v2 = 50))) AND (comp_index_t0.v1 BETWEEN 15 AND 47)) OR ((comp_index_t0.v1 > 55) AND (comp_index_t0.v2 > 85))) OR (comp_index_t0.v1 > 86))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[15, 47], (-∞, 69)}, {[15, 47], (69, ∞)}, {(55, 86], (85, ∞)}, {(86, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<37) OR (v1<=48 AND v2<=54)) OR (v1=88));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 37), (-∞, ∞)}, {[37, 48], (-∞, 54]}, {[88, 88], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<>31) OR (v1<>43)) OR (v1>37 AND v2>5));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t0.v1 = 31))) OR (NOT((comp_index_t0.v1 = 43)))) OR ((comp_index_t0.v1 > 37) AND (comp_index_t0.v2 > 5)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=91) OR (v1<>79)) OR (v1<64));`,
		ExpectedPlan: "Filter(((comp_index_t0.v1 <= 91) OR (NOT((comp_index_t0.v1 = 79)))) OR (comp_index_t0.v1 < 64))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>48) OR (v1>11));`,
		ExpectedPlan: "Filter((NOT((comp_index_t0.v1 = 48))) OR (comp_index_t0.v1 > 11))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>40) OR (v1>=49 AND v2>=92));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(40, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((v1<40) OR (v1<=59)) OR (v1<99)) AND (v1>=83) OR (v1>9));`,
		ExpectedPlan: "Filter(((((comp_index_t0.v1 < 40) OR (comp_index_t0.v1 <= 59)) OR (comp_index_t0.v1 < 99)) AND (comp_index_t0.v1 >= 83)) OR (comp_index_t0.v1 > 9))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(9, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=53 AND v2<=79) OR (v1>50 AND v2>26)) AND (v1>26) AND (v1>43 AND v2<7);`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(43, 53], (-∞, 7)}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1 BETWEEN 27 AND 84) OR (v1<98 AND v2>38)) OR (v1<>30));`,
		ExpectedPlan: "Filter(((comp_index_t0.v1 BETWEEN 27 AND 84) OR ((comp_index_t0.v1 < 98) AND (comp_index_t0.v2 > 38))) OR (NOT((comp_index_t0.v1 = 30))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1=45) OR (v1=28));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[28, 28], (-∞, ∞)}, {[45, 45], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (v1 BETWEEN 11 AND 18) AND (v1>31 AND v2 BETWEEN 38 AND 88);`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>95 AND v2>5) OR (v1>16 AND v2>=38));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(16, 95], [38, ∞)}, {(95, ∞), (5, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>=23) OR (v1=47 AND v2>23));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[23, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1=30) OR (v1<>67));`,
		ExpectedPlan: "Filter((comp_index_t0.v1 = 30) OR (NOT((comp_index_t0.v1 = 67))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 67), (-∞, ∞)}, {(67, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>=30 AND v2>=67) OR (v1<=52));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 52], (-∞, ∞)}, {(52, ∞), [67, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1 BETWEEN 48 AND 86 AND v2>=29) OR (v1<>82 AND v2<=93)) OR (v1 BETWEEN 79 AND 87 AND v2 BETWEEN 13 AND 69));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 48), (-∞, 93]}, {[48, 82), (-∞, ∞)}, {[82, 82], [13, ∞)}, {(82, 86], (-∞, ∞)}, {(86, ∞), (-∞, 93]}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1 BETWEEN 3 AND 95 AND v2>=36) OR (v1>=40 AND v2<13)) OR (v1 BETWEEN 4 AND 8 AND v2=50));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[3, 95], [36, ∞)}, {[40, ∞), (-∞, 13)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<11 AND v2<>32) OR (v1 BETWEEN 35 AND 41)) OR (v1>=76));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 11), (-∞, 32)}, {(-∞, 11), (32, ∞)}, {[35, 41], (-∞, ∞)}, {[76, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1=15 AND v2=8) AND (v1>2) OR (v1 BETWEEN 50 AND 97));`,
		ExpectedPlan: "Filter((((comp_index_t0.v1 = 15) AND (comp_index_t0.v2 = 8)) AND (comp_index_t0.v1 > 2)) OR (comp_index_t0.v1 BETWEEN 50 AND 97))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[15, 15], [8, 8]}, {[50, 97], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<67 AND v2<>39) OR (v1>36));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 36], (-∞, 39)}, {(-∞, 36], (39, ∞)}, {(36, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>66) OR (v1<50));`,
		ExpectedPlan: "Filter((NOT((comp_index_t0.v1 = 66))) OR (comp_index_t0.v1 < 50))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 66), (-∞, ∞)}, {(66, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1 BETWEEN 5 AND 19) OR (v1<>50 AND v2>=51)) OR (v1>55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 5), [51, ∞)}, {[5, 19], (-∞, ∞)}, {(19, 50), [51, ∞)}, {(50, 55], [51, ∞)}, {(55, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1 BETWEEN 16 AND 65) OR (v1<>18 AND v2>=81)) OR (v1 BETWEEN 6 AND 48));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 6), [81, ∞)}, {[6, 65], (-∞, ∞)}, {(65, ∞), [81, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1>=31 AND v2>=55) OR (v1 BETWEEN 1 AND 28)) OR (v1 BETWEEN 26 AND 41 AND v2<=15));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[1, 28], (-∞, ∞)}, {(28, 41], (-∞, 15]}, {[31, ∞), [55, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<=77 AND v2 BETWEEN 4 AND 26) OR (v1<=1 AND v2<>20)) OR (v1>8 AND v2>40));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 1], (-∞, ∞)}, {(1, 77], [4, 26]}, {(8, ∞), (40, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((((v1=37 AND v2>32) OR (v1>13 AND v2>51)) AND (v1 BETWEEN 8 AND 19) OR (v1<>4)) OR (v1<=58 AND v2<>70)) OR (v1<87 AND v2>=24));`,
		ExpectedPlan: "Filter(((((((comp_index_t0.v1 = 37) AND (comp_index_t0.v2 > 32)) OR ((comp_index_t0.v1 > 13) AND (comp_index_t0.v2 > 51))) AND (comp_index_t0.v1 BETWEEN 8 AND 19)) OR (NOT((comp_index_t0.v1 = 4)))) OR ((comp_index_t0.v1 <= 58) AND (NOT((comp_index_t0.v2 = 70))))) OR ((comp_index_t0.v1 < 87) AND (comp_index_t0.v2 >= 24)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1<>50) OR (v1<=88)) OR (v1>=28 AND v2 BETWEEN 30 AND 85));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t0.v1 = 50))) OR (comp_index_t0.v1 <= 88)) OR ((comp_index_t0.v1 >= 28) AND (comp_index_t0.v2 BETWEEN 30 AND 85)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=94) OR (v1<=87));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 94], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<>56 AND v2<93) OR (v1<73 AND v2<=70));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 56), (-∞, 93)}, {[56, 56], (-∞, 70]}, {(56, ∞), (-∞, 93)}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((v1>=85) OR (v1=91)) OR (v1<88 AND v2<42)) OR (v1<>42 AND v2<=10));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 85), (-∞, 42)}, {[85, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>42 AND v2<=13) OR (v1=7));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[7, 7], (-∞, ∞)}, {(42, ∞), (-∞, 13]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1=63) OR (v1 BETWEEN 55 AND 82 AND v2 BETWEEN 0 AND 6)) OR (v1=46));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[46, 46], (-∞, ∞)}, {[55, 63), [0, 6]}, {[63, 63], (-∞, ∞)}, {(63, 82], [0, 6]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1 BETWEEN 20 AND 77 AND v2>=49) OR (v1<13));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 13), (-∞, ∞)}, {[20, 77], [49, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1>=72) OR (v1<49 AND v2<>36)) OR (v1>=10 AND v2<1));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 49), (-∞, 36)}, {(-∞, 49), (36, ∞)}, {[49, 72), (-∞, 1)}, {[72, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE (((v1 BETWEEN 18 AND 87) OR (v1>=42 AND v2>44)) OR (v1<26 AND v2<=55)) AND (v1<=21);`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 18), (-∞, 55]}, {[18, 21], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>98 AND v2<75) OR (v1=47));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[47, 47], (-∞, ∞)}, {(98, ∞), (-∞, 75)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=57 AND v2>=43) OR (v1<27 AND v2<>3));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 27), (-∞, 3)}, {(-∞, 27), (3, ∞)}, {[27, 57], [43, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1 BETWEEN 16 AND 45 AND v2=22) OR (v1>=87 AND v2=48));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[16, 45], [22, 22]}, {[87, ∞), [48, 48]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1 BETWEEN 45 AND 74 AND v2<=74) OR (v1<>48 AND v2>58));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 45), (58, ∞)}, {[45, 48), (-∞, ∞)}, {[48, 48], (-∞, 74]}, {(48, 74], (-∞, ∞)}, {(74, ∞), (58, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((((v1<32 AND v2>=79) OR (v1<=28)) OR (v1 BETWEEN 46 AND 72)) OR (v1>16));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<10) OR (v1<89));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 89), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1>=64 AND v2>=69) OR (v1>=2));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{[2, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1<=65) OR (v1<64));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 65], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1=46) OR (v1>9 AND v2>=22));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(9, 46), [22, ∞)}, {[46, 46], (-∞, ∞)}, {(46, ∞), [22, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t0 WHERE ((v1 BETWEEN 21 AND 33 AND v2>25) OR (v1<0));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(comp_index_t0 on [comp_index_t0.v1,comp_index_t0.v2] with ranges: [{(-∞, 0), (-∞, ∞)}, {[21, 33], (25, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>87 AND v2 BETWEEN 8 AND 33) OR (v1 BETWEEN 39 AND 69 AND v3<4));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 87))) AND (comp_index_t1.v2 BETWEEN 8 AND 33)) OR ((comp_index_t1.v1 BETWEEN 39 AND 69) AND (comp_index_t1.v3 < 4)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 39), [8, 33], (-∞, ∞)}, {[39, 69], (-∞, ∞), (-∞, ∞)}, {(69, 87), [8, 33], (-∞, ∞)}, {(87, ∞), [8, 33], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=55 AND v2>=72 AND v3=63) AND (v1<>54 AND v2 BETWEEN 3 AND 80) OR (v1=15)) AND (v1<>50);`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 >= 55) AND (comp_index_t1.v2 >= 72)) AND (comp_index_t1.v3 = 63)) AND ((NOT((comp_index_t1.v1 = 54))) AND (comp_index_t1.v2 BETWEEN 3 AND 80))) OR (comp_index_t1.v1 = 15))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[15, 15], (-∞, ∞), (-∞, ∞)}, {[55, ∞), [72, 80], [63, 63]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<93 AND v2<39 AND v3 BETWEEN 30 AND 97) OR (v1>54)) OR (v1<66));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>59 AND v2<=15) OR (v1 BETWEEN 2 AND 51)) OR (v1>15 AND v2 BETWEEN 31 AND 81));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 2), (-∞, 15], (-∞, ∞)}, {[2, 51], (-∞, ∞), (-∞, ∞)}, {(51, 59), (-∞, 15], (-∞, ∞)}, {(51, ∞), [31, 81], (-∞, ∞)}, {(59, ∞), (-∞, 15], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<3 AND v2<>23 AND v3<>11) OR (v1<>49)) AND (v1<=41 AND v2>40);`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 < 3) AND (NOT((comp_index_t1.v2 = 23)))) AND (NOT((comp_index_t1.v3 = 11)))) OR (NOT((comp_index_t1.v1 = 49))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 41], (40, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1 BETWEEN 28 AND 38 AND v3<33) OR (v1 BETWEEN 75 AND 85)) AND (v1>=60) OR (v1>=53 AND v2 BETWEEN 36 AND 53 AND v3>48));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 BETWEEN 28 AND 38) AND (comp_index_t1.v3 < 33)) OR (comp_index_t1.v1 BETWEEN 75 AND 85)) AND (comp_index_t1.v1 >= 60)) OR (((comp_index_t1.v1 >= 53) AND (comp_index_t1.v2 BETWEEN 36 AND 53)) AND (comp_index_t1.v3 > 48)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[53, 75), [36, 53], (48, ∞)}, {[75, 85], (-∞, ∞), (-∞, ∞)}, {(85, ∞), [36, 53], (48, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<6 AND v2<>44) OR (v1 BETWEEN 27 AND 96)) OR (v1>22 AND v2<>30 AND v3<49));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 6), (-∞, 44), (-∞, ∞)}, {(-∞, 6), (44, ∞), (-∞, ∞)}, {(22, 27), (-∞, 30), (-∞, 49)}, {(22, 27), (30, ∞), (-∞, 49)}, {[27, 96], (-∞, ∞), (-∞, ∞)}, {(96, ∞), (-∞, 30), (-∞, 49)}, {(96, ∞), (30, ∞), (-∞, 49)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>29 AND v2=40) OR (v1<=74)) OR (v1<13 AND v2 BETWEEN 27 AND 82 AND v3<82));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 74], (-∞, ∞), (-∞, ∞)}, {(74, ∞), [40, 40], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>6 AND v2 BETWEEN 0 AND 97) OR (v1<>40 AND v3<10 AND v2<>10));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 6))) AND (comp_index_t1.v2 BETWEEN 0 AND 97)) OR (((NOT((comp_index_t1.v1 = 40))) AND (comp_index_t1.v3 < 10)) AND (NOT((comp_index_t1.v2 = 10)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 6), (-∞, 0), (-∞, 10)}, {(-∞, 6), [0, 97], (-∞, ∞)}, {(-∞, 6), (97, ∞), (-∞, 10)}, {[6, 6], (-∞, 10), (-∞, 10)}, {[6, 6], (10, ∞), (-∞, 10)}, {(6, 40), (-∞, 0), (-∞, 10)}, {(6, 40), (97, ∞), (-∞, 10)}, {(6, ∞), [0, 97], (-∞, ∞)}, {(40, ∞), (-∞, 0), (-∞, 10)}, {(40, ∞), (97, ∞), (-∞, 10)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1>=35) OR (v1=86)) OR (v1>41 AND v2>=92)) OR (v1<>28));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 >= 35) OR (comp_index_t1.v1 = 86)) OR ((comp_index_t1.v1 > 41) AND (comp_index_t1.v2 >= 92))) OR (NOT((comp_index_t1.v1 = 28))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 28), (-∞, ∞), (-∞, ∞)}, {(28, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<16 AND v3=63 AND v2>=20) OR (v1<>41)) OR (v1<=74 AND v3 BETWEEN 14 AND 74 AND v2<>13));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 < 16) AND (comp_index_t1.v3 = 63)) AND (comp_index_t1.v2 >= 20)) OR (NOT((comp_index_t1.v1 = 41)))) OR (((comp_index_t1.v1 <= 74) AND (comp_index_t1.v3 BETWEEN 14 AND 74)) AND (NOT((comp_index_t1.v2 = 13)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 41), (-∞, ∞), (-∞, ∞)}, {[41, 41], (-∞, 13), [14, 74]}, {[41, 41], (13, ∞), [14, 74]}, {(41, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1 BETWEEN 1 AND 11) OR (v1>2 AND v3<=93 AND v2 BETWEEN 28 AND 84)) OR (v1 BETWEEN 34 AND 52 AND v2=73)) OR (v1<>80 AND v2<=32 AND v3 BETWEEN 3 AND 7));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 BETWEEN 1 AND 11) OR (((comp_index_t1.v1 > 2) AND (comp_index_t1.v3 <= 93)) AND (comp_index_t1.v2 BETWEEN 28 AND 84))) OR ((comp_index_t1.v1 BETWEEN 34 AND 52) AND (comp_index_t1.v2 = 73))) OR (((NOT((comp_index_t1.v1 = 80))) AND (comp_index_t1.v2 <= 32)) AND (comp_index_t1.v3 BETWEEN 3 AND 7)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 1), (-∞, 32], [3, 7]}, {[1, 11], (-∞, ∞), (-∞, ∞)}, {(11, 34), [28, 84], (-∞, 93]}, {(11, 80), (-∞, 28), [3, 7]}, {[34, 52], [28, 73), (-∞, 93]}, {[34, 52], [73, 73], (-∞, ∞)}, {[34, 52], (73, 84], (-∞, 93]}, {(52, ∞), [28, 84], (-∞, 93]}, {(80, ∞), (-∞, 28), [3, 7]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1<45) OR (v1<>72)) OR (v1 BETWEEN 10 AND 86 AND v2=92)) OR (v1 BETWEEN 32 AND 81 AND v2>59));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 < 45) OR (NOT((comp_index_t1.v1 = 72)))) OR ((comp_index_t1.v1 BETWEEN 10 AND 86) AND (comp_index_t1.v2 = 92))) OR ((comp_index_t1.v1 BETWEEN 32 AND 81) AND (comp_index_t1.v2 > 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 72), (-∞, ∞), (-∞, ∞)}, {[72, 72], (59, ∞), (-∞, ∞)}, {(72, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=11 AND v2>50 AND v3 BETWEEN 5 AND 67) AND (v1>74 AND v2 BETWEEN 6 AND 63 AND v3<=1) OR (v1>=53 AND v2>69 AND v3>54));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 >= 11) AND (comp_index_t1.v2 > 50)) AND (comp_index_t1.v3 BETWEEN 5 AND 67)) AND (((comp_index_t1.v1 > 74) AND (comp_index_t1.v2 BETWEEN 6 AND 63)) AND (comp_index_t1.v3 <= 1))) OR (((comp_index_t1.v1 >= 53) AND (comp_index_t1.v2 > 69)) AND (comp_index_t1.v3 > 54)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[53, ∞), (69, ∞), (54, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>9) OR (v1>14 AND v2>10));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(9, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<=39 AND v2 BETWEEN 17 AND 34) OR (v1=89 AND v3>49 AND v2>58)) OR (v1>97));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 <= 39) AND (comp_index_t1.v2 BETWEEN 17 AND 34)) OR (((comp_index_t1.v1 = 89) AND (comp_index_t1.v3 > 49)) AND (comp_index_t1.v2 > 58))) OR (comp_index_t1.v1 > 97))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 39], [17, 34], (-∞, ∞)}, {[89, 89], (58, ∞), (49, ∞)}, {(97, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<7 AND v2<>43) OR (v1<>5 AND v3<0 AND v2<1));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 < 7) AND (NOT((comp_index_t1.v2 = 43)))) OR (((NOT((comp_index_t1.v1 = 5))) AND (comp_index_t1.v3 < 0)) AND (comp_index_t1.v2 < 1)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 7), (-∞, 43), (-∞, ∞)}, {(-∞, 7), (43, ∞), (-∞, ∞)}, {[7, ∞), (-∞, 1), (-∞, 0)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1>30 AND v2 BETWEEN 23 AND 60 AND v3=58) OR (v1<=3 AND v2 BETWEEN 68 AND 72)) OR (v1<=17)) OR (v1>6 AND v2>=24)) AND (v1<89 AND v2=73);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 89), [73, 73], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>27) OR (v1>=22 AND v2>99 AND v3>=43));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[22, 27], (99, ∞), [43, ∞)}, {(27, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>25 AND v2 BETWEEN 1 AND 82) OR (v1>31 AND v2=86));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(25, ∞), [1, 82], (-∞, ∞)}, {(31, ∞), [86, 86], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>12 AND v2<60 AND v3=91) OR (v1>63 AND v2>=8 AND v3<>32)) OR (v1>35 AND v3>=98));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t1.v1 = 12))) AND (comp_index_t1.v2 < 60)) AND (comp_index_t1.v3 = 91)) OR (((comp_index_t1.v1 > 63) AND (comp_index_t1.v2 >= 8)) AND (NOT((comp_index_t1.v3 = 32))))) OR ((comp_index_t1.v1 > 35) AND (comp_index_t1.v3 >= 98)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 12), (-∞, 60), [91, 91]}, {(12, 35], (-∞, 60), [91, 91]}, {(35, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>27 AND v3=10) OR (v1>=25 AND v2<26)) AND (v1>=62 AND v2<=96 AND v3>28);`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 > 27) AND (comp_index_t1.v3 = 10)) OR ((comp_index_t1.v1 >= 25) AND (comp_index_t1.v2 < 26)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[62, ∞), (-∞, 96], (28, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>30 AND v2=40 AND v3 BETWEEN 35 AND 35) OR (v1 BETWEEN 20 AND 77 AND v2>=56 AND v3>62));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[20, 77], [56, ∞), (62, ∞)}, {(30, ∞), [40, 40], [35, 35]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((((v1<=92 AND v3=0 AND v2>=9) OR (v1 BETWEEN 48 AND 79)) OR (v1>70 AND v2<=26 AND v3 BETWEEN 14 AND 82)) OR (v1>=29 AND v2<>21 AND v3 BETWEEN 37 AND 55)) OR (v1>=6 AND v3<=47));`,
		ExpectedPlan: "Filter(((((((comp_index_t1.v1 <= 92) AND (comp_index_t1.v3 = 0)) AND (comp_index_t1.v2 >= 9)) OR (comp_index_t1.v1 BETWEEN 48 AND 79)) OR (((comp_index_t1.v1 > 70) AND (comp_index_t1.v2 <= 26)) AND (comp_index_t1.v3 BETWEEN 14 AND 82))) OR (((comp_index_t1.v1 >= 29) AND (NOT((comp_index_t1.v2 = 21)))) AND (comp_index_t1.v3 BETWEEN 37 AND 55))) OR ((comp_index_t1.v1 >= 6) AND (comp_index_t1.v3 <= 47)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 6), [9, ∞), [0, 0]}, {[6, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=15 AND v2>28) OR (v1<=84 AND v2<>91));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 15], (-∞, ∞), (-∞, ∞)}, {(15, 84], (-∞, 91), (-∞, ∞)}, {(15, 84], (91, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1=49 AND v2<=52 AND v3 BETWEEN 23 AND 38) OR (v1 BETWEEN 30 AND 84 AND v2=94));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[30, 84], [94, 94], (-∞, ∞)}, {[49, 49], (-∞, 52], [23, 38]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 8 AND 18) OR (v1=27 AND v2<=4 AND v3<14));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[8, 18], (-∞, ∞), (-∞, ∞)}, {[27, 27], (-∞, 4], (-∞, 14)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=4) OR (v1=0 AND v2<=63));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[0, 0], (-∞, 63], (-∞, ∞)}, {[4, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1<=99 AND v2<>86) AND (v1>=21 AND v2>36);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[21, 99], (86, ∞), (-∞, ∞)}, {[21, 99], (36, 86), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>43) OR (v1=14));`,
		ExpectedPlan: "Filter((NOT((comp_index_t1.v1 = 43))) OR (comp_index_t1.v1 = 14))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 43), (-∞, ∞), (-∞, ∞)}, {(43, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1 BETWEEN 21 AND 44 AND v2 BETWEEN 18 AND 88 AND v3=42) AND (v1>=52 AND v2>37 AND v3 BETWEEN 26 AND 91);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>29 AND v2>93 AND v3<64) OR (v1<>54 AND v2>35));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 54), (35, ∞), (-∞, ∞)}, {[54, 54], (93, ∞), (-∞, 64)}, {(54, ∞), (35, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<88) OR (v1<>45 AND v2<89)) AND (v1=98 AND v2<=81 AND v3 BETWEEN 34 AND 77);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[98, 98], (-∞, 81], [34, 77]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>65 AND v2<>86 AND v3<=2) OR (v1<>37 AND v2<=96));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 37), (-∞, 96], (-∞, ∞)}, {(37, 65], (-∞, 96], (-∞, ∞)}, {(65, ∞), (-∞, 96], (-∞, ∞)}, {(65, ∞), (96, ∞), (-∞, 2]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>79) OR (v1>66)) AND (v1<>81 AND v2<34 AND v3>=25) AND (v1<42) OR (v1<>12 AND v2<>17 AND v3<=23));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t1.v1 = 79))) OR (comp_index_t1.v1 > 66)) AND (((NOT((comp_index_t1.v1 = 81))) AND (comp_index_t1.v2 < 34)) AND (comp_index_t1.v3 >= 25))) AND (comp_index_t1.v1 < 42)) OR (((NOT((comp_index_t1.v1 = 12))) AND (NOT((comp_index_t1.v2 = 17)))) AND (comp_index_t1.v3 <= 23)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 12), (-∞, 17), (-∞, 23]}, {(-∞, 12), (17, ∞), (-∞, 23]}, {(-∞, 42), (-∞, 34), [25, ∞)}, {(12, ∞), (-∞, 17), (-∞, 23]}, {(12, ∞), (17, ∞), (-∞, 23]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<81 AND v2>=28) OR (v1=19 AND v2 BETWEEN 9 AND 57));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 19), [28, ∞), (-∞, ∞)}, {[19, 19], [9, ∞), (-∞, ∞)}, {(19, 81), [28, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<32) OR (v1>=52)) OR (v1>=98));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 32), (-∞, ∞), (-∞, ∞)}, {[52, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>47) OR (v1<>25));`,
		ExpectedPlan: "Filter((comp_index_t1.v1 > 47) OR (NOT((comp_index_t1.v1 = 25))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 25), (-∞, ∞), (-∞, ∞)}, {(25, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1>27 AND v2<=80 AND v3 BETWEEN 11 AND 37) AND (v1=87 AND v2<54) AND (v1>29);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[87, 87], (-∞, 54), [11, 37]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>65 AND v2>=52) OR (v1<=85)) OR (v1<=64 AND v3=9 AND v2>=36));`,
		ExpectedPlan: "Filter((((NOT((comp_index_t1.v1 = 65))) AND (comp_index_t1.v2 >= 52)) OR (comp_index_t1.v1 <= 85)) OR (((comp_index_t1.v1 <= 64) AND (comp_index_t1.v3 = 9)) AND (comp_index_t1.v2 >= 36)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 85], (-∞, ∞), (-∞, ∞)}, {(85, ∞), [52, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=12 AND v2>=65) OR (v1=11 AND v2<1));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[11, 11], (-∞, 1), (-∞, ∞)}, {[12, ∞), [65, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=92 AND v2<=42) OR (v1>=58));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 58), (-∞, 42], (-∞, ∞)}, {[58, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>0) OR (v1<81 AND v2>=70)) OR (v1>=52));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 0))) OR ((comp_index_t1.v1 < 81) AND (comp_index_t1.v2 >= 70))) OR (comp_index_t1.v1 >= 52))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 0), (-∞, ∞), (-∞, ∞)}, {[0, 0], [70, ∞), (-∞, ∞)}, {(0, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>5 AND v3<=32) OR (v1 BETWEEN 77 AND 85 AND v3 BETWEEN 16 AND 21 AND v2 BETWEEN 10 AND 42));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 > 5) AND (comp_index_t1.v3 <= 32)) OR (((comp_index_t1.v1 BETWEEN 77 AND 85) AND (comp_index_t1.v3 BETWEEN 16 AND 21)) AND (comp_index_t1.v2 BETWEEN 10 AND 42)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(5, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>43 AND v2<53 AND v3<=20) OR (v1<7 AND v2<>79));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 7), (-∞, 79), (-∞, ∞)}, {(-∞, 7), (79, ∞), (-∞, ∞)}, {[7, 43), (-∞, 53), (-∞, 20]}, {(43, ∞), (-∞, 53), (-∞, 20]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1>=17 AND v2 BETWEEN 17 AND 78 AND v3=10) AND (v1<=67) AND (v1>=81 AND v2<=88 AND v3>=70);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<77 AND v2<35 AND v3=73) OR (v1=85 AND v2>0 AND v3<65)) AND (v1>=20 AND v3<23 AND v2<=81) OR (v1<34 AND v2<=21 AND v3<=45));`,
		ExpectedPlan: "Filter((((((comp_index_t1.v1 < 77) AND (comp_index_t1.v2 < 35)) AND (comp_index_t1.v3 = 73)) OR (((comp_index_t1.v1 = 85) AND (comp_index_t1.v2 > 0)) AND (comp_index_t1.v3 < 65))) AND (((comp_index_t1.v1 >= 20) AND (comp_index_t1.v3 < 23)) AND (comp_index_t1.v2 <= 81))) OR (((comp_index_t1.v1 < 34) AND (comp_index_t1.v2 <= 21)) AND (comp_index_t1.v3 <= 45)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 34), (-∞, 21], (-∞, 45]}, {[85, 85], (0, 81], (-∞, 23)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((((v1<=69) AND (v1>=60 AND v2<18 AND v3=15) OR (v1<=75)) OR (v1>=52 AND v2<10)) OR (v1<37 AND v2<=64)) OR (v1>38 AND v2=27));`,
		ExpectedPlan: "Filter((((((comp_index_t1.v1 <= 69) AND (((comp_index_t1.v1 >= 60) AND (comp_index_t1.v2 < 18)) AND (comp_index_t1.v3 = 15))) OR (comp_index_t1.v1 <= 75)) OR ((comp_index_t1.v1 >= 52) AND (comp_index_t1.v2 < 10))) OR ((comp_index_t1.v1 < 37) AND (comp_index_t1.v2 <= 64))) OR ((comp_index_t1.v1 > 38) AND (comp_index_t1.v2 = 27)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 75], (-∞, ∞), (-∞, ∞)}, {(75, ∞), (-∞, 10), (-∞, ∞)}, {(75, ∞), [27, 27], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1<=76) AND (v1<=94);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 76], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1<>40 AND v2>1) OR (v1>3 AND v2<=42)) OR (v1=99 AND v2>62)) OR (v1<17 AND v2<>75 AND v3=6));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 3], (-∞, 1], [6, 6]}, {(-∞, 3], (1, ∞), (-∞, ∞)}, {(3, 40), (-∞, ∞), (-∞, ∞)}, {[40, 40], (-∞, 42], (-∞, ∞)}, {(40, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1=39) OR (v1=40 AND v2<>49)) OR (v1<>35 AND v2>4 AND v3>26)) OR (v1=32 AND v2<>55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 32), (4, ∞), (26, ∞)}, {[32, 32], (-∞, 55), (-∞, ∞)}, {[32, 32], [55, 55], (26, ∞)}, {[32, 32], (55, ∞), (-∞, ∞)}, {(32, 35), (4, ∞), (26, ∞)}, {(35, 39), (4, ∞), (26, ∞)}, {[39, 39], (-∞, ∞), (-∞, ∞)}, {(39, 40), (4, ∞), (26, ∞)}, {[40, 40], (-∞, 49), (-∞, ∞)}, {[40, 40], [49, 49], (26, ∞)}, {[40, 40], (49, ∞), (-∞, ∞)}, {(40, ∞), (4, ∞), (26, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1=16 AND v2<>25 AND v3<>3) OR (v1>=4 AND v2 BETWEEN 4 AND 93 AND v3>39));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[4, 16), [4, 93], (39, ∞)}, {[16, 16], (-∞, 25), (-∞, 3)}, {[16, 16], (-∞, 25), (3, ∞)}, {[16, 16], [25, 25], (39, ∞)}, {[16, 16], (25, ∞), (-∞, 3)}, {[16, 16], (25, ∞), (3, ∞)}, {(16, ∞), [4, 93], (39, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1>=51 AND v2<83) OR (v1>=15 AND v2>=3)) OR (v1<=49)) OR (v1<69));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1<>43 AND v2>10) AND (v1>30 AND v2 BETWEEN 18 AND 78 AND v3 BETWEEN 75 AND 81);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(30, 43), [18, 78], [75, 81]}, {(43, ∞), [18, 78], [75, 81]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>1) OR (v1<34 AND v2>=57 AND v3 BETWEEN 15 AND 67));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 1], [57, ∞), [15, 67]}, {(1, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>3 AND v2>32) OR (v1<=26 AND v3>=27 AND v2>=5));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 > 3) AND (comp_index_t1.v2 > 32)) OR (((comp_index_t1.v1 <= 26) AND (comp_index_t1.v3 >= 27)) AND (comp_index_t1.v2 >= 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 3], [5, ∞), [27, ∞)}, {(3, 26], [5, 32], [27, ∞)}, {(3, ∞), (32, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>25 AND v2<>70 AND v3<=51) OR (v1<=71 AND v2>59));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 71], (59, ∞), (-∞, ∞)}, {(25, 71], (-∞, 59], (-∞, 51]}, {(71, ∞), (-∞, 70), (-∞, 51]}, {(71, ∞), (70, ∞), (-∞, 51]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1 BETWEEN 0 AND 61 AND v2<0) OR (v1 BETWEEN 0 AND 38 AND v2>34)) OR (v1>=13 AND v2>=41));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[0, 38], (34, ∞), (-∞, ∞)}, {[0, 61], (-∞, 0), (-∞, ∞)}, {(38, ∞), [41, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>68 AND v2<=57) AND (v1<>84 AND v3 BETWEEN 24 AND 98 AND v2 BETWEEN 28 AND 45) OR (v1>0 AND v2<>47 AND v3>=69)) OR (v1>=44));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t1.v1 = 68))) AND (comp_index_t1.v2 <= 57)) AND (((NOT((comp_index_t1.v1 = 84))) AND (comp_index_t1.v3 BETWEEN 24 AND 98)) AND (comp_index_t1.v2 BETWEEN 28 AND 45))) OR (((comp_index_t1.v1 > 0) AND (NOT((comp_index_t1.v2 = 47)))) AND (comp_index_t1.v3 >= 69))) OR (comp_index_t1.v1 >= 44))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 0], [28, 45], [24, 98]}, {(0, 44), (-∞, 28), [69, ∞)}, {(0, 44), [28, 45], [24, ∞)}, {(0, 44), (45, 47), [69, ∞)}, {(0, 44), (47, ∞), [69, ∞)}, {[44, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=48 AND v2 BETWEEN 33 AND 66) OR (v1>=91));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 48], [33, 66], (-∞, ∞)}, {[91, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1 BETWEEN 17 AND 52 AND v2<96) OR (v1<=12 AND v2<>4 AND v3>53)) OR (v1<98 AND v3<94 AND v2=5));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 BETWEEN 17 AND 52) AND (comp_index_t1.v2 < 96)) OR (((comp_index_t1.v1 <= 12) AND (NOT((comp_index_t1.v2 = 4)))) AND (comp_index_t1.v3 > 53))) OR (((comp_index_t1.v1 < 98) AND (comp_index_t1.v3 < 94)) AND (comp_index_t1.v2 = 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 12], (-∞, 4), (53, ∞)}, {(-∞, 12], (4, 5), (53, ∞)}, {(-∞, 12], [5, 5], (-∞, ∞)}, {(-∞, 12], (5, ∞), (53, ∞)}, {(12, 17), [5, 5], (-∞, 94)}, {[17, 52], (-∞, 96), (-∞, ∞)}, {(52, 98), [5, 5], (-∞, 94)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>26 AND v2 BETWEEN 66 AND 79 AND v3<=94) OR (v1 BETWEEN 16 AND 55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 16), [66, 79], (-∞, 94]}, {[16, 26), (-∞, ∞), (-∞, ∞)}, {[26, 55], (-∞, ∞), (-∞, ∞)}, {(55, ∞), [66, 79], (-∞, 94]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1 BETWEEN 36 AND 67 AND v3<74 AND v2=26) AND (v1 BETWEEN 9 AND 10 AND v2=96) AND (v1<=11 AND v2<>63 AND v3>=62);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 28 AND 49 AND v2<47) OR (v1>37 AND v2 BETWEEN 45 AND 61 AND v3<73));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[28, 49], (-∞, 47), (-∞, ∞)}, {(37, 49], [47, 61], (-∞, 73)}, {(49, ∞), [45, 61], (-∞, 73)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<37 AND v2>=26 AND v3<=14) OR (v1<64)) OR (v1 BETWEEN 31 AND 53 AND v2>55 AND v3<=55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 64), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=77) OR (v1<50)) AND (v1<=53 AND v2>35 AND v3<>98);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 50), (35, ∞), (-∞, 98)}, {(-∞, 50), (35, ∞), (98, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1=2 AND v2=40 AND v3 BETWEEN 18 AND 67) OR (v1=14 AND v2<=24 AND v3<=87)) OR (v1 BETWEEN 8 AND 31 AND v2>86)) OR (v1>30));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[2, 2], [40, 40], [18, 67]}, {[8, 30], (86, ∞), (-∞, ∞)}, {[14, 14], (-∞, 24], (-∞, 87]}, {(30, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>46 AND v2<>49 AND v3<=44) OR (v1 BETWEEN 64 AND 80 AND v2=41 AND v3<=68));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(46, 64), (-∞, 49), (-∞, 44]}, {(46, ∞), (49, ∞), (-∞, 44]}, {[64, 80], (-∞, 41), (-∞, 44]}, {[64, 80], [41, 41], (-∞, 68]}, {[64, 80], (41, 49), (-∞, 44]}, {(80, ∞), (-∞, 49), (-∞, 44]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1=95 AND v3<47 AND v2>=97) OR (v1 BETWEEN 11 AND 36 AND v2<=83));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 = 95) AND (comp_index_t1.v3 < 47)) AND (comp_index_t1.v2 >= 97)) OR ((comp_index_t1.v1 BETWEEN 11 AND 36) AND (comp_index_t1.v2 <= 83)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[11, 36], (-∞, 83], (-∞, ∞)}, {[95, 95], [97, ∞), (-∞, 47)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=65 AND v2=39 AND v3 BETWEEN 49 AND 67) OR (v1<57 AND v2>35));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 57), (35, ∞), (-∞, ∞)}, {[65, ∞), [39, 39], [49, 67]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1>71 AND v2=33) OR (v1<>85 AND v2<>50 AND v3 BETWEEN 34 AND 67)) OR (v1 BETWEEN 5 AND 47 AND v3 BETWEEN 13 AND 76 AND v2=4)) OR (v1=16 AND v2>=29 AND v3<>80));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 > 71) AND (comp_index_t1.v2 = 33)) OR (((NOT((comp_index_t1.v1 = 85))) AND (NOT((comp_index_t1.v2 = 50)))) AND (comp_index_t1.v3 BETWEEN 34 AND 67))) OR (((comp_index_t1.v1 BETWEEN 5 AND 47) AND (comp_index_t1.v3 BETWEEN 13 AND 76)) AND (comp_index_t1.v2 = 4))) OR (((comp_index_t1.v1 = 16) AND (comp_index_t1.v2 >= 29)) AND (NOT((comp_index_t1.v3 = 80)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 5), (-∞, 50), [34, 67]}, {(-∞, 16), (50, ∞), [34, 67]}, {[5, 16), (4, 50), [34, 67]}, {[5, 47], (-∞, 4), [34, 67]}, {[5, 47], [4, 4], [13, 76]}, {[16, 16], (4, 29), [34, 67]}, {[16, 16], [29, ∞), (-∞, 80)}, {[16, 16], [29, ∞), (80, ∞)}, {(16, 47], (4, 50), [34, 67]}, {(16, 85), (50, ∞), [34, 67]}, {(47, 71], (-∞, 50), [34, 67]}, {(71, 85), (-∞, 33), [34, 67]}, {(71, 85), (33, 50), [34, 67]}, {(71, ∞), [33, 33], (-∞, ∞)}, {(85, ∞), (-∞, 33), [34, 67]}, {(85, ∞), (33, 50), [34, 67]}, {(85, ∞), (50, ∞), [34, 67]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=17 AND v2>38) AND (v1>=79) OR (v1<>38));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 <= 17) AND (comp_index_t1.v2 > 38)) AND (comp_index_t1.v1 >= 79)) OR (NOT((comp_index_t1.v1 = 38))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 38), (-∞, ∞), (-∞, ∞)}, {(38, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=4 AND v2=26) OR (v1>21 AND v2 BETWEEN 14 AND 64));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[4, 21], [26, 26], (-∞, ∞)}, {(21, ∞), [14, 64], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>50) OR (v1<=58 AND v2<=95)) OR (v1=10));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 50))) OR ((comp_index_t1.v1 <= 58) AND (comp_index_t1.v2 <= 95))) OR (comp_index_t1.v1 = 10))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 50), (-∞, ∞), (-∞, ∞)}, {[50, 50], (-∞, 95], (-∞, ∞)}, {(50, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1<=21 AND v2<>95) OR (v1<>23 AND v2 BETWEEN 15 AND 22)) OR (v1<=53 AND v2>=6)) OR (v1<=13 AND v2<>93 AND v3<15));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 21], (-∞, ∞), (-∞, ∞)}, {(21, 53], [6, ∞), (-∞, ∞)}, {(53, ∞), [15, 22], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1<64 AND v2>=90 AND v3>41) AND (v1>=14 AND v2 BETWEEN 30 AND 70 AND v3>=25);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<27 AND v2<=43) OR (v1<62 AND v2<=99)) OR (v1<>48 AND v2<29 AND v3<>69));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 62), (-∞, 99], (-∞, ∞)}, {[62, ∞), (-∞, 29), (-∞, 69)}, {[62, ∞), (-∞, 29), (69, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<11 AND v2<70 AND v3>27) OR (v1>=80 AND v2<31 AND v3<65)) OR (v1>=98 AND v2 BETWEEN 30 AND 85 AND v3>=30));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 11), (-∞, 70), (27, ∞)}, {[80, 98), (-∞, 31), (-∞, 65)}, {[98, ∞), (-∞, 30), (-∞, 65)}, {[98, ∞), [30, 31), (-∞, ∞)}, {[98, ∞), [31, 85], [30, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1<>44 AND v2>=10) AND (v1=47 AND v2=14 AND v3<30);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[47, 47], [14, 14], (-∞, 30)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>6 AND v2=50) OR (v1>=16));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(6, 16), [50, 50], (-∞, ∞)}, {[16, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=31) OR (v1>53 AND v2<>11 AND v3<>94)) OR (v1>48 AND v2 BETWEEN 11 AND 29 AND v3 BETWEEN 68 AND 72));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[31, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 55 AND 59) OR (v1<=10 AND v2>=24)) AND (v1>93 AND v3<70 AND v2 BETWEEN 44 AND 79) AND (v1>=22 AND v2=27);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=43 AND v2<28 AND v3<>24) OR (v1<36 AND v2=14 AND v3 BETWEEN 16 AND 55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 36), [14, 14], [16, 55]}, {[43, ∞), (-∞, 28), (-∞, 24)}, {[43, ∞), (-∞, 28), (24, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>48 AND v2<=80) OR (v1=72 AND v3 BETWEEN 45 AND 52 AND v2=98));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 > 48) AND (comp_index_t1.v2 <= 80)) OR (((comp_index_t1.v1 = 72) AND (comp_index_t1.v3 BETWEEN 45 AND 52)) AND (comp_index_t1.v2 = 98)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(48, ∞), (-∞, 80], (-∞, ∞)}, {[72, 72], [98, 98], [45, 52]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1>=98 AND v2=51) AND (v1>34);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[98, ∞), [51, 51], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>2) OR (v1<=30)) OR (v1<>35 AND v2 BETWEEN 6 AND 61 AND v3>=16));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>19) OR (v1<>48));`,
		ExpectedPlan: "Filter((NOT((comp_index_t1.v1 = 19))) OR (NOT((comp_index_t1.v1 = 48))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 12 AND 42 AND v2<=12) OR (v1<34 AND v2 BETWEEN 30 AND 47 AND v3<>50));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 34), [30, 47], (-∞, 50)}, {(-∞, 34), [30, 47], (50, ∞)}, {[12, 42], (-∞, 12], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((((v1>=6) OR (v1>7)) OR (v1<88 AND v2<=34 AND v3<=47)) OR (v1>=10)) OR (v1=10));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 6), (-∞, 34], (-∞, 47]}, {[6, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=74) OR (v1>=1)) OR (v1=54 AND v2>=38 AND v3>2)) AND (v1>5);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(5, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=45 AND v2>18) OR (v1<64 AND v2=25 AND v3>97));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 45), [25, 25], (97, ∞)}, {[45, ∞), (18, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<37 AND v3>77) OR (v1>38 AND v3<>57 AND v2=87));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 < 37) AND (comp_index_t1.v3 > 77)) OR (((comp_index_t1.v1 > 38) AND (NOT((comp_index_t1.v3 = 57)))) AND (comp_index_t1.v2 = 87)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 37), (-∞, ∞), (-∞, ∞)}, {(38, ∞), [87, 87], (-∞, 57)}, {(38, ∞), [87, 87], (57, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1<96 AND v2>11 AND v3<76) OR (v1<=14 AND v2=23)) OR (v1<=15 AND v2<21 AND v3<91)) OR (v1=45 AND v2<11 AND v3=1));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 14], [21, 23), (-∞, 76)}, {(-∞, 14], [23, 23], (-∞, ∞)}, {(-∞, 14], (23, ∞), (-∞, 76)}, {(-∞, 15], (-∞, 21), (-∞, 91)}, {(14, 15], [21, ∞), (-∞, 76)}, {(15, 96), (11, ∞), (-∞, 76)}, {[45, 45], (-∞, 11), [1, 1]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>23 AND v3<=52) OR (v1<>19 AND v2=25));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 23))) AND (comp_index_t1.v3 <= 52)) OR ((NOT((comp_index_t1.v1 = 19))) AND (comp_index_t1.v2 = 25)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 23), (-∞, ∞), (-∞, ∞)}, {[23, 23], [25, 25], (-∞, ∞)}, {(23, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1<=12 AND v2>=65) AND (v1<6 AND v2>=92);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 6), [92, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1=62 AND v2<>32) OR (v1>=55 AND v2=41 AND v3>73));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[55, 62), [41, 41], (73, ∞)}, {[62, 62], (-∞, 32), (-∞, ∞)}, {[62, 62], (32, ∞), (-∞, ∞)}, {(62, ∞), [41, 41], (73, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>34 AND v2<=62) OR (v1>5 AND v2 BETWEEN 59 AND 98 AND v3<69)) OR (v1>34));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 34), (-∞, 62], (-∞, ∞)}, {(5, 34), (62, 98], (-∞, 69)}, {[34, 34], [59, 98], (-∞, 69)}, {(34, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1=61 AND v2 BETWEEN 10 AND 22 AND v3<34) OR (v1=68)) OR (v1<=97 AND v3 BETWEEN 7 AND 63 AND v2<67));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 = 61) AND (comp_index_t1.v2 BETWEEN 10 AND 22)) AND (comp_index_t1.v3 < 34)) OR (comp_index_t1.v1 = 68)) OR (((comp_index_t1.v1 <= 97) AND (comp_index_t1.v3 BETWEEN 7 AND 63)) AND (comp_index_t1.v2 < 67)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 61), (-∞, 67), [7, 63]}, {[61, 61], (-∞, 10), [7, 63]}, {[61, 61], [10, 22], (-∞, 63]}, {[61, 61], (22, 67), [7, 63]}, {(61, 68), (-∞, 67), [7, 63]}, {[68, 68], (-∞, ∞), (-∞, ∞)}, {(68, 97], (-∞, 67), [7, 63]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=42) OR (v1 BETWEEN 13 AND 30 AND v2<50));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 42], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 16 AND 49) OR (v1<=69 AND v2>9 AND v3<=8));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 16), (9, ∞), (-∞, 8]}, {[16, 49], (-∞, ∞), (-∞, ∞)}, {(49, 69], (9, ∞), (-∞, 8]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>71 AND v2>44) OR (v1<76 AND v2>=10)) OR (v1>=44 AND v2=66));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 76), [10, ∞), (-∞, ∞)}, {[76, ∞), (44, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((((v1>=26) OR (v1>=13 AND v2 BETWEEN 35 AND 95 AND v3>=29)) OR (v1<>54 AND v2 BETWEEN 0 AND 54)) OR (v1 BETWEEN 17 AND 17 AND v2<=71)) OR (v1>50 AND v3>=42)) OR (v1<>0));`,
		ExpectedPlan: "Filter((((((comp_index_t1.v1 >= 26) OR (((comp_index_t1.v1 >= 13) AND (comp_index_t1.v2 BETWEEN 35 AND 95)) AND (comp_index_t1.v3 >= 29))) OR ((NOT((comp_index_t1.v1 = 54))) AND (comp_index_t1.v2 BETWEEN 0 AND 54))) OR ((comp_index_t1.v1 BETWEEN 17 AND 17) AND (comp_index_t1.v2 <= 71))) OR ((comp_index_t1.v1 > 50) AND (comp_index_t1.v3 >= 42))) OR (NOT((comp_index_t1.v1 = 0))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 0), (-∞, ∞), (-∞, ∞)}, {[0, 0], [0, 54], (-∞, ∞)}, {(0, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=99 AND v2<66) OR (v1 BETWEEN 1 AND 47)) OR (v1<>2 AND v2<30));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 1), (-∞, 30), (-∞, ∞)}, {[1, 47], (-∞, ∞), (-∞, ∞)}, {(47, 99), (-∞, 30), (-∞, ∞)}, {[99, ∞), (-∞, 66), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>9 AND v2<74) AND (v1<=63 AND v2=18) OR (v1<46));`,
		ExpectedPlan: "Filter((((NOT((comp_index_t1.v1 = 9))) AND (comp_index_t1.v2 < 74)) AND ((comp_index_t1.v1 <= 63) AND (comp_index_t1.v2 = 18))) OR (comp_index_t1.v1 < 46))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 46), (-∞, ∞), (-∞, ∞)}, {[46, 63], [18, 18], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<=20 AND v2<=62) OR (v1>45 AND v2=33 AND v3<=4)) OR (v1>29));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 20], (-∞, 62], (-∞, ∞)}, {(29, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1<=55 AND v2 BETWEEN 82 AND 96 AND v3>=13) OR (v1>=89 AND v2<18 AND v3<19)) OR (v1=98 AND v3>=40)) OR (v1 BETWEEN 7 AND 74 AND v2<=73));`,
		ExpectedPlan: "Filter((((((comp_index_t1.v1 <= 55) AND (comp_index_t1.v2 BETWEEN 82 AND 96)) AND (comp_index_t1.v3 >= 13)) OR (((comp_index_t1.v1 >= 89) AND (comp_index_t1.v2 < 18)) AND (comp_index_t1.v3 < 19))) OR ((comp_index_t1.v1 = 98) AND (comp_index_t1.v3 >= 40))) OR ((comp_index_t1.v1 BETWEEN 7 AND 74) AND (comp_index_t1.v2 <= 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 55], [82, 96], [13, ∞)}, {[7, 74], (-∞, 73], (-∞, ∞)}, {[89, 98), (-∞, 18), (-∞, 19)}, {[98, 98], (-∞, ∞), (-∞, ∞)}, {(98, ∞), (-∞, 18), (-∞, 19)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=26 AND v2 BETWEEN 6 AND 80) AND (v1=47 AND v2<67 AND v3<7) OR (v1>63));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 >= 26) AND (comp_index_t1.v2 BETWEEN 6 AND 80)) AND (((comp_index_t1.v1 = 47) AND (comp_index_t1.v2 < 67)) AND (comp_index_t1.v3 < 7))) OR (comp_index_t1.v1 > 63))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[47, 47], [6, 67), (-∞, 7)}, {(63, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<11) OR (v1<>33));`,
		ExpectedPlan: "Filter((comp_index_t1.v1 < 11) OR (NOT((comp_index_t1.v1 = 33))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 33), (-∞, ∞), (-∞, ∞)}, {(33, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1<=35) AND (v1=44 AND v2<78 AND v3>=40) OR (v1<>88 AND v2=8)) AND (v1>=99 AND v2=62) OR (v1<=94)) OR (v1 BETWEEN 22 AND 23 AND v2 BETWEEN 14 AND 46));`,
		ExpectedPlan: "Filter((((((comp_index_t1.v1 <= 35) AND (((comp_index_t1.v1 = 44) AND (comp_index_t1.v2 < 78)) AND (comp_index_t1.v3 >= 40))) OR ((NOT((comp_index_t1.v1 = 88))) AND (comp_index_t1.v2 = 8))) AND ((comp_index_t1.v1 >= 99) AND (comp_index_t1.v2 = 62))) OR (comp_index_t1.v1 <= 94)) OR ((comp_index_t1.v1 BETWEEN 22 AND 23) AND (comp_index_t1.v2 BETWEEN 14 AND 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 94], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<9 AND v2=94 AND v3>8) OR (v1>=63));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 9), [94, 94], (8, ∞)}, {[63, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<43) OR (v1 BETWEEN 40 AND 49 AND v2>26 AND v3 BETWEEN 22 AND 80));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 43), (-∞, ∞), (-∞, ∞)}, {[43, 49], (26, ∞), [22, 80]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 4 AND 85 AND v2<>45 AND v3<=41) OR (v1>67 AND v2<25));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[4, 67], (-∞, 45), (-∞, 41]}, {[4, 85], (45, ∞), (-∞, 41]}, {(67, 85], [25, 45), (-∞, 41]}, {(67, ∞), (-∞, 25), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>77) OR (v1<=54 AND v2<=71 AND v3>=49)) OR (v1>54 AND v2<30 AND v3=6));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 77))) OR (((comp_index_t1.v1 <= 54) AND (comp_index_t1.v2 <= 71)) AND (comp_index_t1.v3 >= 49))) OR (((comp_index_t1.v1 > 54) AND (comp_index_t1.v2 < 30)) AND (comp_index_t1.v3 = 6)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 77), (-∞, ∞), (-∞, ∞)}, {[77, 77], (-∞, 30), [6, 6]}, {(77, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1 BETWEEN 21 AND 53 AND v2=0 AND v3>32) OR (v1=93 AND v2>=94 AND v3<1)) OR (v1<26)) OR (v1<>11 AND v2<>32 AND v3=6)) AND (v1>=45);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[45, 53], [0, 0], (32, ∞)}, {[45, ∞), (-∞, 32), [6, 6]}, {[45, ∞), (32, ∞), [6, 6]}, {[93, 93], [94, ∞), (-∞, 1)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>50) OR (v1<=71));`,
		ExpectedPlan: "Filter((NOT((comp_index_t1.v1 = 50))) OR (comp_index_t1.v1 <= 71))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1=41) OR (v1>29 AND v2<>31));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(29, 41), (-∞, 31), (-∞, ∞)}, {(29, 41), (31, ∞), (-∞, ∞)}, {[41, 41], (-∞, ∞), (-∞, ∞)}, {(41, ∞), (-∞, 31), (-∞, ∞)}, {(41, ∞), (31, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<88 AND v2<91 AND v3>9) AND (v1>=5 AND v2 BETWEEN 21 AND 29 AND v3>18) OR (v1>=40));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 < 88) AND (comp_index_t1.v2 < 91)) AND (comp_index_t1.v3 > 9)) AND (((comp_index_t1.v1 >= 5) AND (comp_index_t1.v2 BETWEEN 21 AND 29)) AND (comp_index_t1.v3 > 18))) OR (comp_index_t1.v1 >= 40))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[5, 40), [21, 29], (18, ∞)}, {[40, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>2 AND v2<76 AND v3<=35) OR (v1<=12 AND v3 BETWEEN 25 AND 30));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 > 2) AND (comp_index_t1.v2 < 76)) AND (comp_index_t1.v3 <= 35)) OR ((comp_index_t1.v1 <= 12) AND (comp_index_t1.v3 BETWEEN 25 AND 30)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 12], (-∞, ∞), (-∞, ∞)}, {(12, ∞), (-∞, 76), (-∞, 35]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1 BETWEEN 25 AND 84 AND v2<=94) OR (v1>66 AND v2>4 AND v3>=57)) OR (v1=78 AND v2>66 AND v3=19)) OR (v1<>48));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 BETWEEN 25 AND 84) AND (comp_index_t1.v2 <= 94)) OR (((comp_index_t1.v1 > 66) AND (comp_index_t1.v2 > 4)) AND (comp_index_t1.v3 >= 57))) OR (((comp_index_t1.v1 = 78) AND (comp_index_t1.v2 > 66)) AND (comp_index_t1.v3 = 19))) OR (NOT((comp_index_t1.v1 = 48))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 48), (-∞, ∞), (-∞, ∞)}, {[48, 48], (-∞, 94], (-∞, ∞)}, {(48, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=24) OR (v1>=47 AND v2<=75 AND v3<=52));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[24, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=21 AND v2<>70) OR (v1<=77 AND v2>4)) OR (v1<28 AND v2<=3 AND v3<>21));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 21), (-∞, 3], (-∞, 21)}, {(-∞, 21), (-∞, 3], (21, ∞)}, {(-∞, 21), (4, ∞), (-∞, ∞)}, {[21, 77], (-∞, ∞), (-∞, ∞)}, {(77, ∞), (-∞, 70), (-∞, ∞)}, {(77, ∞), (70, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=60 AND v2>91) OR (v1<=10));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 10], (-∞, ∞), (-∞, ∞)}, {[60, ∞), (91, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>98 AND v2<52) OR (v1 BETWEEN 65 AND 67)) OR (v1 BETWEEN 18 AND 54)) AND (v1>=14 AND v2=27);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[14, 98), [27, 27], (-∞, ∞)}, {(98, ∞), [27, 27], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=43 AND v2<>39) AND (v1<=32 AND v2<=15 AND v3>=54) OR (v1<>68 AND v2 BETWEEN 42 AND 46));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 >= 43) AND (NOT((comp_index_t1.v2 = 39)))) AND (((comp_index_t1.v1 <= 32) AND (comp_index_t1.v2 <= 15)) AND (comp_index_t1.v3 >= 54))) OR ((NOT((comp_index_t1.v1 = 68))) AND (comp_index_t1.v2 BETWEEN 42 AND 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 68), [42, 46], (-∞, ∞)}, {(68, ∞), [42, 46], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1>=19 AND v2<2) AND (v1<4 AND v3>23 AND v2<>53);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 34 AND 40) OR (v1<=80 AND v2<>53)) AND (v1=81 AND v2=17 AND v3<>12);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>34 AND v2 BETWEEN 18 AND 67 AND v3<67) OR (v1>21));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(21, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>45) OR (v1>=91 AND v2>=8 AND v3<=38)) OR (v1<>58 AND v3<=32 AND v2<>45));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 45))) OR (((comp_index_t1.v1 >= 91) AND (comp_index_t1.v2 >= 8)) AND (comp_index_t1.v3 <= 38))) OR (((NOT((comp_index_t1.v1 = 58))) AND (comp_index_t1.v3 <= 32)) AND (NOT((comp_index_t1.v2 = 45)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 45), (-∞, ∞), (-∞, ∞)}, {[45, 45], (-∞, 45), (-∞, 32]}, {[45, 45], (45, ∞), (-∞, 32]}, {(45, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=48) OR (v1<38 AND v2>=26)) AND (v1<=45 AND v2>21) AND (v1=83 AND v2=20);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>25) OR (v1<53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<95 AND v2>=12) OR (v1 BETWEEN 41 AND 55 AND v2<=81 AND v3<46));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 95), [12, ∞), (-∞, ∞)}, {[41, 55], (-∞, 12), (-∞, 46)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>39 AND v2 BETWEEN 53 AND 73 AND v3<=11) OR (v1<=31 AND v2=68 AND v3>=71)) OR (v1<>18 AND v2<=51));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 18), (-∞, 51], (-∞, ∞)}, {(-∞, 31], [68, 68], [71, ∞)}, {(18, ∞), (-∞, 51], (-∞, ∞)}, {(39, ∞), [53, 73], (-∞, 11]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1>4) AND (v1=3 AND v2 BETWEEN 4 AND 34 AND v3<=40);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>36 AND v2>82) OR (v1 BETWEEN 22 AND 59));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[22, 59], (-∞, ∞), (-∞, ∞)}, {(59, ∞), (82, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=0) OR (v1 BETWEEN 17 AND 45));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 0], (-∞, ∞), (-∞, ∞)}, {[17, 45], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<1 AND v3<=34) OR (v1 BETWEEN 2 AND 57 AND v2<>70));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 < 1) AND (comp_index_t1.v3 <= 34)) OR ((comp_index_t1.v1 BETWEEN 2 AND 57) AND (NOT((comp_index_t1.v2 = 70)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 1), (-∞, ∞), (-∞, ∞)}, {[2, 57], (-∞, 70), (-∞, ∞)}, {[2, 57], (70, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1>4) AND (v1 BETWEEN 8 AND 35 AND v2>=94 AND v3=32) AND (v1>=12);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[12, 35], [94, ∞), [32, 32]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1<=93 AND v3<>47) OR (v1>=93 AND v2 BETWEEN 15 AND 42 AND v3<=6)) OR (v1>15)) OR (v1 BETWEEN 0 AND 1 AND v2>33));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 <= 93) AND (NOT((comp_index_t1.v3 = 47)))) OR (((comp_index_t1.v1 >= 93) AND (comp_index_t1.v2 BETWEEN 15 AND 42)) AND (comp_index_t1.v3 <= 6))) OR (comp_index_t1.v1 > 15)) OR ((comp_index_t1.v1 BETWEEN 0 AND 1) AND (comp_index_t1.v2 > 33)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>12) OR (v1>=26 AND v2 BETWEEN 77 AND 87 AND v3<19)) OR (v1<=89));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1=27 AND v2=16 AND v3>=8) OR (v1<20 AND v2>=1 AND v3 BETWEEN 28 AND 47)) OR (v1 BETWEEN 15 AND 43 AND v2>30));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 15), [1, ∞), [28, 47]}, {[15, 20), [1, 30], [28, 47]}, {[15, 43], (30, ∞), (-∞, ∞)}, {[27, 27], [16, 16], [8, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1=67 AND v2<>69) OR (v1<28 AND v2<62 AND v3>=99));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 28), (-∞, 62), [99, ∞)}, {[67, 67], (-∞, 69), (-∞, ∞)}, {[67, 67], (69, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<45 AND v2>5 AND v3>20) OR (v1<17));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 17), (-∞, ∞), (-∞, ∞)}, {[17, 45), (5, ∞), (20, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1=40 AND v2<>18) OR (v1<>97 AND v2<>17 AND v3<>48));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 40), (-∞, 17), (-∞, 48)}, {(-∞, 40), (-∞, 17), (48, ∞)}, {(-∞, 40), (17, ∞), (-∞, 48)}, {(-∞, 40), (17, ∞), (48, ∞)}, {[40, 40], (-∞, 18), (-∞, ∞)}, {[40, 40], [18, 18], (-∞, 48)}, {[40, 40], [18, 18], (48, ∞)}, {[40, 40], (18, ∞), (-∞, ∞)}, {(40, 97), (-∞, 17), (-∞, 48)}, {(40, 97), (-∞, 17), (48, ∞)}, {(40, 97), (17, ∞), (-∞, 48)}, {(40, 97), (17, ∞), (48, ∞)}, {(97, ∞), (-∞, 17), (-∞, 48)}, {(97, ∞), (-∞, 17), (48, ∞)}, {(97, ∞), (17, ∞), (-∞, 48)}, {(97, ∞), (17, ∞), (48, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>63) AND (v1<=44 AND v2<>43 AND v3=29) OR (v1=38 AND v2>45));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 > 63) AND (((comp_index_t1.v1 <= 44) AND (NOT((comp_index_t1.v2 = 43)))) AND (comp_index_t1.v3 = 29))) OR ((comp_index_t1.v1 = 38) AND (comp_index_t1.v2 > 45)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[38, 38], (45, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=6) OR (v1>0 AND v2 BETWEEN 3 AND 50));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 6], (-∞, ∞), (-∞, ∞)}, {(6, ∞), [3, 50], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 5 AND 35 AND v2<=3 AND v3<>14) OR (v1>11));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[5, 11], (-∞, 3], (-∞, 14)}, {[5, 11], (-∞, 3], (14, ∞)}, {(11, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<50) AND (v1<19 AND v2>=10) OR (v1<36 AND v2>10 AND v3<>65));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 < 50) AND ((comp_index_t1.v1 < 19) AND (comp_index_t1.v2 >= 10))) OR (((comp_index_t1.v1 < 36) AND (comp_index_t1.v2 > 10)) AND (NOT((comp_index_t1.v3 = 65)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 19), [10, ∞), (-∞, ∞)}, {[19, 36), (10, ∞), (-∞, 65)}, {[19, 36), (10, ∞), (65, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1=56 AND v3<=4 AND v2=46) OR (v1 BETWEEN 21 AND 53 AND v2<>63)) OR (v1 BETWEEN 10 AND 62 AND v2>=62)) OR (v1>31));`,
		ExpectedPlan: "Filter((((((comp_index_t1.v1 = 56) AND (comp_index_t1.v3 <= 4)) AND (comp_index_t1.v2 = 46)) OR ((comp_index_t1.v1 BETWEEN 21 AND 53) AND (NOT((comp_index_t1.v2 = 63))))) OR ((comp_index_t1.v1 BETWEEN 10 AND 62) AND (comp_index_t1.v2 >= 62))) OR (comp_index_t1.v1 > 31))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[10, 21), [62, ∞), (-∞, ∞)}, {[21, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<20 AND v2>=1 AND v3=26) OR (v1=12));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 12), [1, ∞), [26, 26]}, {[12, 12], (-∞, ∞), (-∞, ∞)}, {(12, 20), [1, ∞), [26, 26]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>51) AND (v1<>4 AND v2<47 AND v3>=77) OR (v1>41 AND v3>62));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 51))) AND (((NOT((comp_index_t1.v1 = 4))) AND (comp_index_t1.v2 < 47)) AND (comp_index_t1.v3 >= 77))) OR ((comp_index_t1.v1 > 41) AND (comp_index_t1.v3 > 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 4), (-∞, 47), [77, ∞)}, {(4, 41], (-∞, 47), [77, ∞)}, {(41, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<35) OR (v1>=58 AND v2>=0));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 35), (-∞, ∞), (-∞, ∞)}, {[58, ∞), [0, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>28 AND v2<95) OR (v1<91));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 91), (-∞, ∞), (-∞, ∞)}, {[91, ∞), (-∞, 95), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (v1=99 AND v2<=41 AND v3>=61) AND (v1=34 AND v2>68 AND v3<=42);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=74 AND v2<=18) OR (v1>=72)) AND (v1=95 AND v2=31 AND v3 BETWEEN 5 AND 19);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[95, 95], [31, 31], [5, 19]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1=64) OR (v1>=49 AND v2<9 AND v3<=49));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[49, 64), (-∞, 9), (-∞, 49]}, {[64, 64], (-∞, ∞), (-∞, ∞)}, {(64, ∞), (-∞, 9), (-∞, 49]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=46) AND (v1<22 AND v2<>42 AND v3<>54) OR (v1>=55 AND v2 BETWEEN 11 AND 84));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 >= 46) AND (((comp_index_t1.v1 < 22) AND (NOT((comp_index_t1.v2 = 42)))) AND (NOT((comp_index_t1.v3 = 54))))) OR ((comp_index_t1.v1 >= 55) AND (comp_index_t1.v2 BETWEEN 11 AND 84)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[55, ∞), [11, 84], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=7) OR (v1<54));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 54), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<=95 AND v2=55 AND v3>34) OR (v1=19));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 19), [55, 55], (34, ∞)}, {[19, 19], (-∞, ∞), (-∞, ∞)}, {(19, 95], [55, 55], (34, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1=51 AND v2<=9) OR (v1<>50)) OR (v1<>4 AND v2>56)) OR (v1 BETWEEN 3 AND 18 AND v2>10 AND v3=12));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 = 51) AND (comp_index_t1.v2 <= 9)) OR (NOT((comp_index_t1.v1 = 50)))) OR ((NOT((comp_index_t1.v1 = 4))) AND (comp_index_t1.v2 > 56))) OR (((comp_index_t1.v1 BETWEEN 3 AND 18) AND (comp_index_t1.v2 > 10)) AND (comp_index_t1.v3 = 12)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 50), (-∞, ∞), (-∞, ∞)}, {[50, 50], (56, ∞), (-∞, ∞)}, {(50, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1<=90 AND v2<=17) OR (v1=2)) OR (v1<>70 AND v2>=84 AND v3<>42)) OR (v1<11 AND v2<>47 AND v3<55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 2), (-∞, 17], (-∞, ∞)}, {(-∞, 2), (17, 47), (-∞, 55)}, {(-∞, 2), (47, 84), (-∞, 55)}, {(-∞, 2), [84, ∞), (-∞, ∞)}, {[2, 2], (-∞, ∞), (-∞, ∞)}, {(2, 11), (17, 47), (-∞, 55)}, {(2, 11), (47, 84), (-∞, 55)}, {(2, 11), [84, ∞), (-∞, ∞)}, {(2, 90], (-∞, 17], (-∞, ∞)}, {[11, 70), [84, ∞), (-∞, 42)}, {[11, 70), [84, ∞), (42, ∞)}, {(70, ∞), [84, ∞), (-∞, 42)}, {(70, ∞), [84, ∞), (42, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 50 AND 59) OR (v1>=23 AND v3>=87 AND v2<>46));`,
		ExpectedPlan: "Filter((comp_index_t1.v1 BETWEEN 50 AND 59) OR (((comp_index_t1.v1 >= 23) AND (comp_index_t1.v3 >= 87)) AND (NOT((comp_index_t1.v2 = 46)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[23, 50), (-∞, 46), [87, ∞)}, {[23, 50), (46, ∞), [87, ∞)}, {[50, 59], (-∞, ∞), (-∞, ∞)}, {(59, ∞), (-∞, 46), [87, ∞)}, {(59, ∞), (46, ∞), [87, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<53) OR (v1<=3));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 53), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=16 AND v2 BETWEEN 66 AND 94) OR (v1>70 AND v2<=3)) AND (v1<>91) OR (v1=17 AND v2>=7));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 >= 16) AND (comp_index_t1.v2 BETWEEN 66 AND 94)) OR ((comp_index_t1.v1 > 70) AND (comp_index_t1.v2 <= 3))) AND (NOT((comp_index_t1.v1 = 91)))) OR ((comp_index_t1.v1 = 17) AND (comp_index_t1.v2 >= 7)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[16, 17), [66, 94], (-∞, ∞)}, {[17, 17], [7, ∞), (-∞, ∞)}, {(17, 91), [66, 94], (-∞, ∞)}, {(70, 91), (-∞, 3], (-∞, ∞)}, {(91, ∞), (-∞, 3], (-∞, ∞)}, {(91, ∞), [66, 94], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<29 AND v3>=33 AND v2=43) OR (v1<59));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 < 29) AND (comp_index_t1.v3 >= 33)) AND (comp_index_t1.v2 = 43)) OR (comp_index_t1.v1 < 59))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 59), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>19 AND v2>84 AND v3>94) OR (v1>=42 AND v3=41));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 > 19) AND (comp_index_t1.v2 > 84)) AND (comp_index_t1.v3 > 94)) OR ((comp_index_t1.v1 >= 42) AND (comp_index_t1.v3 = 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(19, 42), (84, ∞), (94, ∞)}, {[42, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=8 AND v2<=97 AND v3>=77) OR (v1<>4)) OR (v1<=41));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 >= 8) AND (comp_index_t1.v2 <= 97)) AND (comp_index_t1.v3 >= 77)) OR (NOT((comp_index_t1.v1 = 4)))) OR (comp_index_t1.v1 <= 41))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>33) OR (v1<=28)) OR (v1<>68));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t1.v1 = 33))) OR (comp_index_t1.v1 <= 28)) OR (NOT((comp_index_t1.v1 = 68))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<>15 AND v2>=22 AND v3<=51) OR (v1<>40 AND v2>26 AND v3<95));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 15), [22, 26], (-∞, 51]}, {(-∞, 40), (26, ∞), (-∞, 95)}, {(15, 40), [22, 26], (-∞, 51]}, {[40, 40], [22, ∞), (-∞, 51]}, {(40, ∞), [22, 26], (-∞, 51]}, {(40, ∞), (26, ∞), (-∞, 95)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>6) OR (v1<=67 AND v2<>67 AND v3>=88));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 6], (-∞, 67), [88, ∞)}, {(-∞, 6], (67, ∞), [88, ∞)}, {(6, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<=0) OR (v1<=53)) OR (v1<=38));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 53], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1=60 AND v3 BETWEEN 2 AND 13 AND v2 BETWEEN 10 AND 69) OR (v1 BETWEEN 1 AND 49)) OR (v1=8 AND v2<26));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 = 60) AND (comp_index_t1.v3 BETWEEN 2 AND 13)) AND (comp_index_t1.v2 BETWEEN 10 AND 69)) OR (comp_index_t1.v1 BETWEEN 1 AND 49)) OR ((comp_index_t1.v1 = 8) AND (comp_index_t1.v2 < 26)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[1, 49], (-∞, ∞), (-∞, ∞)}, {[60, 60], [10, 69], [2, 13]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1 BETWEEN 14 AND 20 AND v2<>70) OR (v1>78 AND v2 BETWEEN 31 AND 52 AND v3>16)) OR (v1 BETWEEN 77 AND 78));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[14, 20], (-∞, 70), (-∞, ∞)}, {[14, 20], (70, ∞), (-∞, ∞)}, {[77, 78], (-∞, ∞), (-∞, ∞)}, {(78, ∞), [31, 52], (16, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<80 AND v2 BETWEEN 41 AND 74) OR (v1>=36 AND v2=32));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 80), [41, 74], (-∞, ∞)}, {[36, ∞), [32, 32], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=24 AND v2=62) OR (v1<=24 AND v3<>22 AND v2 BETWEEN 12 AND 25)) OR (v1 BETWEEN 48 AND 49 AND v3>=90)) AND (v1<15 AND v2<>55 AND v3=51);`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 >= 24) AND (comp_index_t1.v2 = 62)) OR (((comp_index_t1.v1 <= 24) AND (NOT((comp_index_t1.v3 = 22)))) AND (comp_index_t1.v2 BETWEEN 12 AND 25))) OR ((comp_index_t1.v1 BETWEEN 48 AND 49) AND (comp_index_t1.v3 >= 90)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 15), [12, 25], [51, 51]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<66 AND v2>=11 AND v3<90) OR (v1<>90)) OR (v1<=7 AND v2=52));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 < 66) AND (comp_index_t1.v2 >= 11)) AND (comp_index_t1.v3 < 90)) OR (NOT((comp_index_t1.v1 = 90)))) OR ((comp_index_t1.v1 <= 7) AND (comp_index_t1.v2 = 52)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 90), (-∞, ∞), (-∞, ∞)}, {(90, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1 BETWEEN 6 AND 74 AND v2=52) OR (v1>44 AND v3>=15 AND v2 BETWEEN 17 AND 94)) OR (v1>84));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 BETWEEN 6 AND 74) AND (comp_index_t1.v2 = 52)) OR (((comp_index_t1.v1 > 44) AND (comp_index_t1.v3 >= 15)) AND (comp_index_t1.v2 BETWEEN 17 AND 94))) OR (comp_index_t1.v1 > 84))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[6, 74], [52, 52], (-∞, ∞)}, {(44, 74], [17, 52), [15, ∞)}, {(44, 74], (52, 94], [15, ∞)}, {(74, 84], [17, 94], [15, ∞)}, {(84, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=38) OR (v1=13)) OR (v1=25 AND v2<=32 AND v3 BETWEEN 12 AND 92));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[13, 13], (-∞, ∞), (-∞, ∞)}, {[25, 25], (-∞, 32], [12, 92]}, {[38, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<=84) OR (v1=41)) OR (v1<83 AND v2=13 AND v3=58));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 84], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<36 AND v2<=79 AND v3>47) OR (v1 BETWEEN 24 AND 89 AND v2<29));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 24), (-∞, 79], (47, ∞)}, {[24, 36), [29, 79], (47, ∞)}, {[24, 89], (-∞, 29), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 3 AND 19 AND v2<=57 AND v3>61) OR (v1<=58 AND v2>=36 AND v3=31)) AND (v1>94);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1<78 AND v2 BETWEEN 55 AND 64 AND v3>=0) OR (v1<74));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 74), (-∞, ∞), (-∞, ∞)}, {[74, 78), [55, 64], [0, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<>1 AND v2=88 AND v3<33) OR (v1<=38)) OR (v1>74 AND v3<>55 AND v2>=9));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t1.v1 = 1))) AND (comp_index_t1.v2 = 88)) AND (comp_index_t1.v3 < 33)) OR (comp_index_t1.v1 <= 38)) OR (((comp_index_t1.v1 > 74) AND (NOT((comp_index_t1.v3 = 55)))) AND (comp_index_t1.v2 >= 9)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 38], (-∞, ∞), (-∞, ∞)}, {(38, 74], [88, 88], (-∞, 33)}, {(74, ∞), [9, ∞), (-∞, 55)}, {(74, ∞), [9, ∞), (55, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1 BETWEEN 15 AND 96 AND v2<>73) OR (v1>=16));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[15, 16), (-∞, 73), (-∞, ∞)}, {[15, 16), (73, ∞), (-∞, ∞)}, {[16, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=72 AND v2<>19 AND v3 BETWEEN 9 AND 12) OR (v1<=77 AND v2=30 AND v3<=10));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 72), [30, 30], (-∞, 10]}, {[72, 77], (19, 30), [9, 12]}, {[72, 77], [30, 30], (-∞, 12]}, {[72, 77], (30, ∞), [9, 12]}, {[72, ∞), (-∞, 19), [9, 12]}, {(77, ∞), (19, ∞), [9, 12]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1>28 AND v2>=73 AND v3=79) AND (v1<=70 AND v2 BETWEEN 5 AND 36) OR (v1<=31)) OR (v1<36)) OR (v1=47 AND v2 BETWEEN 0 AND 92 AND v3<=43));`,
		ExpectedPlan: "Filter(((((((comp_index_t1.v1 > 28) AND (comp_index_t1.v2 >= 73)) AND (comp_index_t1.v3 = 79)) AND ((comp_index_t1.v1 <= 70) AND (comp_index_t1.v2 BETWEEN 5 AND 36))) OR (comp_index_t1.v1 <= 31)) OR (comp_index_t1.v1 < 36)) OR (((comp_index_t1.v1 = 47) AND (comp_index_t1.v2 BETWEEN 0 AND 92)) AND (comp_index_t1.v3 <= 43)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 36), (-∞, ∞), (-∞, ∞)}, {[47, 47], [0, 92], (-∞, 43]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>24) AND (v1>68 AND v2 BETWEEN 1 AND 79 AND v3 BETWEEN 23 AND 44) OR (v1>78));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 > 24) AND (((comp_index_t1.v1 > 68) AND (comp_index_t1.v2 BETWEEN 1 AND 79)) AND (comp_index_t1.v3 BETWEEN 23 AND 44))) OR (comp_index_t1.v1 > 78))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(68, 78], [1, 79], [23, 44]}, {(78, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1=47 AND v2=7) OR (v1>=7 AND v2<>87)) OR (v1<>6 AND v2<=84));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 6), (-∞, 84], (-∞, ∞)}, {(6, 7), (-∞, 84], (-∞, ∞)}, {[7, ∞), (-∞, 87), (-∞, ∞)}, {[7, ∞), (87, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1>=49 AND v2>53 AND v3<>12) OR (v1=95 AND v2<1 AND v3<>89)) OR (v1=62 AND v3>=37 AND v2<=22)) OR (v1>30 AND v2>=66));`,
		ExpectedPlan: "Filter((((((comp_index_t1.v1 >= 49) AND (comp_index_t1.v2 > 53)) AND (NOT((comp_index_t1.v3 = 12)))) OR (((comp_index_t1.v1 = 95) AND (comp_index_t1.v2 < 1)) AND (NOT((comp_index_t1.v3 = 89))))) OR (((comp_index_t1.v1 = 62) AND (comp_index_t1.v3 >= 37)) AND (comp_index_t1.v2 <= 22))) OR ((comp_index_t1.v1 > 30) AND (comp_index_t1.v2 >= 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(30, ∞), [66, ∞), (-∞, ∞)}, {[49, ∞), (53, 66), (-∞, 12)}, {[49, ∞), (53, 66), (12, ∞)}, {[62, 62], (-∞, 22], [37, ∞)}, {[95, 95], (-∞, 1), (-∞, 89)}, {[95, 95], (-∞, 1), (89, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1=24 AND v2<81) OR (v1<=22 AND v2>34 AND v3<55)) OR (v1=45 AND v2>=94 AND v3>17));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 22], (34, ∞), (-∞, 55)}, {[24, 24], (-∞, 81), (-∞, ∞)}, {[45, 45], [94, ∞), (17, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1>38) OR (v1<51 AND v2>=28 AND v3=44)) OR (v1 BETWEEN 23 AND 61 AND v2 BETWEEN 54 AND 75 AND v3<>44)) OR (v1>72));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 38], [28, ∞), [44, 44]}, {[23, 38], [54, 75], (-∞, 44)}, {[23, 38], [54, 75], (44, ∞)}, {(38, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((((v1>40 AND v2 BETWEEN 26 AND 30) OR (v1<3 AND v2>=62 AND v3<=8)) OR (v1<>57)) OR (v1=16 AND v2>92 AND v3<=74));`,
		ExpectedPlan: "Filter(((((comp_index_t1.v1 > 40) AND (comp_index_t1.v2 BETWEEN 26 AND 30)) OR (((comp_index_t1.v1 < 3) AND (comp_index_t1.v2 >= 62)) AND (comp_index_t1.v3 <= 8))) OR (NOT((comp_index_t1.v1 = 57)))) OR (((comp_index_t1.v1 = 16) AND (comp_index_t1.v2 > 92)) AND (comp_index_t1.v3 <= 74)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 57), (-∞, ∞), (-∞, ∞)}, {[57, 57], [26, 30], (-∞, ∞)}, {(57, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<=34 AND v2 BETWEEN 29 AND 35 AND v3>=64) OR (v1<>47)) AND (v1>=11) OR (v1<>46 AND v2 BETWEEN 4 AND 26));`,
		ExpectedPlan: "Filter((((((comp_index_t1.v1 <= 34) AND (comp_index_t1.v2 BETWEEN 29 AND 35)) AND (comp_index_t1.v3 >= 64)) OR (NOT((comp_index_t1.v1 = 47)))) AND (comp_index_t1.v1 >= 11)) OR ((NOT((comp_index_t1.v1 = 46))) AND (comp_index_t1.v2 BETWEEN 4 AND 26)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 11), [4, 26], (-∞, ∞)}, {[11, 46], (-∞, ∞), (-∞, ∞)}, {(46, 47), (-∞, ∞), (-∞, ∞)}, {[47, 47], [4, 26], (-∞, ∞)}, {(47, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1 BETWEEN 41 AND 98 AND v2>54) OR (v1<29)) OR (v1<32));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 32), (-∞, ∞), (-∞, ∞)}, {[41, 98], (54, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=79 AND v3 BETWEEN 9 AND 95) OR (v1 BETWEEN 50 AND 50 AND v2 BETWEEN 16 AND 38 AND v3<>94));`,
		ExpectedPlan: "Filter(((comp_index_t1.v1 >= 79) AND (comp_index_t1.v3 BETWEEN 9 AND 95)) OR (((comp_index_t1.v1 BETWEEN 50 AND 50) AND (comp_index_t1.v2 BETWEEN 16 AND 38)) AND (NOT((comp_index_t1.v3 = 94)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[50, 50], [16, 38], (-∞, 94)}, {[50, 50], [16, 38], (94, ∞)}, {[79, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((((v1<>79) OR (v1 BETWEEN 9 AND 11 AND v2<48 AND v3<=73)) OR (v1<=46)) OR (v1 BETWEEN 66 AND 67)) OR (v1<=86 AND v2<4));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t1.v1 = 79))) OR (((comp_index_t1.v1 BETWEEN 9 AND 11) AND (comp_index_t1.v2 < 48)) AND (comp_index_t1.v3 <= 73))) OR (comp_index_t1.v1 <= 46)) OR (comp_index_t1.v1 BETWEEN 66 AND 67)) OR ((comp_index_t1.v1 <= 86) AND (comp_index_t1.v2 < 4)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 79), (-∞, ∞), (-∞, ∞)}, {[79, 79], (-∞, 4), (-∞, ∞)}, {(79, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1>=2 AND v2 BETWEEN 32 AND 59 AND v3 BETWEEN 50 AND 52) OR (v1<26)) OR (v1<>2 AND v2>11)) AND (v1>32 AND v2<=92) AND (v1>45 AND v2<>5 AND v3<>49);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(45, ∞), (11, 92], (-∞, 49)}, {(45, ∞), (11, 92], (49, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=19) AND (v1<=73) OR (v1=9 AND v2=5 AND v3<=5));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[9, 9], [5, 5], (-∞, 5]}, {[19, 73], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE (((v1<62) AND (v1<=57 AND v2>51 AND v3 BETWEEN 29 AND 30) OR (v1>=28 AND v2<=62 AND v3<>76)) OR (v1>=94));`,
		ExpectedPlan: "Filter((((comp_index_t1.v1 < 62) AND (((comp_index_t1.v1 <= 57) AND (comp_index_t1.v2 > 51)) AND (comp_index_t1.v3 BETWEEN 29 AND 30))) OR (((comp_index_t1.v1 >= 28) AND (comp_index_t1.v2 <= 62)) AND (NOT((comp_index_t1.v3 = 76))))) OR (comp_index_t1.v1 >= 94))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(-∞, 28), (51, ∞), [29, 30]}, {[28, 57], (62, ∞), [29, 30]}, {[28, 94), (-∞, 62], (-∞, 76)}, {[28, 94), (-∞, 62], (76, ∞)}, {[94, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>21) OR (v1>=86 AND v2>2 AND v3>=67));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{(21, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t1 WHERE ((v1>=94) OR (v1>=57 AND v2<>53 AND v3>22));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(comp_index_t1 on [comp_index_t1.v1,comp_index_t1.v2,comp_index_t1.v3] with ranges: [{[57, 94), (-∞, 53), (22, ∞)}, {[57, 94), (53, ∞), (22, ∞)}, {[94, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<40 AND v2=9) OR (v1<11 AND v2=15 AND v3<>55 AND v4<>95));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 11), [15, 15], (-∞, 55), (-∞, 95)}, {(-∞, 11), [15, 15], (-∞, 55), (95, ∞)}, {(-∞, 11), [15, 15], (55, ∞), (-∞, 95)}, {(-∞, 11), [15, 15], (55, ∞), (95, ∞)}, {(-∞, 40), [9, 9], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=82 AND v2=74 AND v3=98) OR (v1=27 AND v2 BETWEEN 16 AND 46 AND v3<>27)) OR (v1>=80 AND v2<>42 AND v3>=47));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 80), [74, 74], [98, 98], (-∞, ∞)}, {[27, 27], [16, 46], (-∞, 27), (-∞, ∞)}, {[27, 27], [16, 46], (27, ∞), (-∞, ∞)}, {[80, ∞), (-∞, 42), [47, ∞), (-∞, ∞)}, {[80, ∞), (42, ∞), [47, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>=47 AND v2<=37 AND v3<90 AND v4=25) OR (v1<42 AND v2>=96 AND v3=38)) OR (v1>26)) OR (v1>=80));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 26], [96, ∞), [38, 38], (-∞, ∞)}, {(26, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>33 AND v2>=16) OR (v1>=24));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[24, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=51 AND v4 BETWEEN 36 AND 55 AND v2>62 AND v3<43) OR (v1 BETWEEN 5 AND 60 AND v2<1)) OR (v1=51 AND v2>=98 AND v3>=94));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 = 51) AND (comp_index_t2.v4 BETWEEN 36 AND 55)) AND (comp_index_t2.v2 > 62)) AND (comp_index_t2.v3 < 43)) OR ((comp_index_t2.v1 BETWEEN 5 AND 60) AND (comp_index_t2.v2 < 1))) OR (((comp_index_t2.v1 = 51) AND (comp_index_t2.v2 >= 98)) AND (comp_index_t2.v3 >= 94)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, 60], (-∞, 1), (-∞, ∞), (-∞, ∞)}, {[51, 51], (62, ∞), (-∞, 43), [36, 55]}, {[51, 51], [98, ∞), [94, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=6 AND v4<95 AND v2<41 AND v3<=4) AND (v1>=81 AND v4>44 AND v2 BETWEEN 6 AND 11) OR (v1<=98));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 >= 6) AND (comp_index_t2.v4 < 95)) AND (comp_index_t2.v2 < 41)) AND (comp_index_t2.v3 <= 4)) AND (((comp_index_t2.v1 >= 81) AND (comp_index_t2.v4 > 44)) AND (comp_index_t2.v2 BETWEEN 6 AND 11))) OR (comp_index_t2.v1 <= 98))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 98], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(98, ∞), [6, 11], (-∞, 4], (44, 95)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=12 AND v2<=78 AND v3 BETWEEN 28 AND 63 AND v4 BETWEEN 46 AND 95) OR (v1=87 AND v2<=44)) OR (v1<14 AND v2<>37 AND v3 BETWEEN 6 AND 32));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 14), (-∞, 37), [6, 32], (-∞, ∞)}, {(-∞, 14), (37, ∞), [6, 32], (-∞, ∞)}, {[12, 14), (-∞, 37), (32, 63], [46, 95]}, {[12, 14), [37, 37], [28, 63], [46, 95]}, {[12, 14), (37, 78], (32, 63], [46, 95]}, {[14, 87), (-∞, 78], [28, 63], [46, 95]}, {[87, 87], (-∞, 44], (-∞, ∞), (-∞, ∞)}, {[87, 87], (44, 78], [28, 63], [46, 95]}, {(87, ∞), (-∞, 78], [28, 63], [46, 95]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=80 AND v2=72 AND v3>19) OR (v1<>38 AND v2>=86 AND v3=7)) OR (v1<=52 AND v2=25 AND v3 BETWEEN 7 AND 32 AND v4<=31));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 38), [86, ∞), [7, 7], (-∞, ∞)}, {(-∞, 52], [25, 25], [7, 32], (-∞, 31]}, {(-∞, 80], [72, 72], (19, ∞), (-∞, ∞)}, {(38, ∞), [86, ∞), [7, 7], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=70) OR (v1>=38 AND v3 BETWEEN 25 AND 30));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 = 70) OR ((comp_index_t2.v1 >= 38) AND (comp_index_t2.v3 BETWEEN 25 AND 30)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[38, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=33) OR (v1<=31 AND v4<>35 AND v2=38));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 <= 33) OR (((comp_index_t2.v1 <= 31) AND (NOT((comp_index_t2.v4 = 35)))) AND (comp_index_t2.v2 = 38)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 33], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>14 AND v2<51 AND v3 BETWEEN 67 AND 78 AND v4=8) OR (v1>=44 AND v2<>35 AND v3<35 AND v4>=12)) OR (v1>=63 AND v2<=3));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(14, 63), (-∞, 51), [67, 78], [8, 8]}, {[44, 63), (-∞, 35), (-∞, 35), [12, ∞)}, {[44, ∞), (35, ∞), (-∞, 35), [12, ∞)}, {[63, ∞), (-∞, 3], (-∞, ∞), (-∞, ∞)}, {[63, ∞), (3, 35), (-∞, 35), [12, ∞)}, {[63, ∞), (3, 51), [67, 78], [8, 8]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=6 AND v2<=25 AND v3>39) OR (v1 BETWEEN 17 AND 94 AND v2>96));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[6, 6], (-∞, 25], (39, ∞), (-∞, ∞)}, {[17, 94], (96, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((((v1>=91 AND v4<=47 AND v2>=43) OR (v1=75)) OR (v1<41 AND v4>=64 AND v2>83)) OR (v1 BETWEEN 72 AND 88 AND v2=48 AND v3<=10)) OR (v1<=44));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 >= 91) AND (comp_index_t2.v4 <= 47)) AND (comp_index_t2.v2 >= 43)) OR (comp_index_t2.v1 = 75)) OR (((comp_index_t2.v1 < 41) AND (comp_index_t2.v4 >= 64)) AND (comp_index_t2.v2 > 83))) OR (((comp_index_t2.v1 BETWEEN 72 AND 88) AND (comp_index_t2.v2 = 48)) AND (comp_index_t2.v3 <= 10))) OR (comp_index_t2.v1 <= 44))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 44], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[72, 75), [48, 48], (-∞, 10], (-∞, ∞)}, {[75, 75], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(75, 88], [48, 48], (-∞, 10], (-∞, ∞)}, {[91, ∞), [43, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=31) OR (v1<84 AND v2<=73 AND v3<>2 AND v4<=51));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 31), (-∞, 73], (-∞, 2), (-∞, 51]}, {(-∞, 31), (-∞, 73], (2, ∞), (-∞, 51]}, {[31, 31], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(31, 84), (-∞, 73], (-∞, 2), (-∞, 51]}, {(31, 84), (-∞, 73], (2, ∞), (-∞, 51]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=20 AND v2<=29 AND v3<52 AND v4<>34) OR (v1<>46 AND v2<>98));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 46), (-∞, 98), (-∞, ∞), (-∞, ∞)}, {(-∞, 46), (98, ∞), (-∞, ∞), (-∞, ∞)}, {(46, ∞), (-∞, 98), (-∞, ∞), (-∞, ∞)}, {(46, ∞), (98, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<52 AND v3 BETWEEN 39 AND 57 AND v4 BETWEEN 13 AND 13 AND v2 BETWEEN 76 AND 99) OR (v1>44)) OR (v1<71 AND v4>7 AND v2<98)) OR (v1<>5 AND v2 BETWEEN 35 AND 40 AND v3<=10));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 < 52) AND (comp_index_t2.v3 BETWEEN 39 AND 57)) AND (comp_index_t2.v4 BETWEEN 13 AND 13)) AND (comp_index_t2.v2 BETWEEN 76 AND 99)) OR (comp_index_t2.v1 > 44)) OR (((comp_index_t2.v1 < 71) AND (comp_index_t2.v4 > 7)) AND (comp_index_t2.v2 < 98))) OR (((NOT((comp_index_t2.v1 = 5))) AND (comp_index_t2.v2 BETWEEN 35 AND 40)) AND (comp_index_t2.v3 <= 10)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 44], (-∞, 98), (-∞, ∞), (-∞, ∞)}, {(-∞, 44], [98, 99], [39, 57], [13, 13]}, {(44, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=40) OR (v1=27)) OR (v1>90 AND v2>50 AND v3=66 AND v4<83));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[27, 27], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[40, 40], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(90, ∞), (50, ∞), [66, 66], (-∞, 83)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1<=92 AND v4 BETWEEN 8 AND 90) AND (v1 BETWEEN 39 AND 42);`,
		ExpectedPlan: "Filter(comp_index_t2.v4 BETWEEN 8 AND 90)\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[39, 42], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 23 AND 85 AND v2<=51 AND v3<>68) OR (v1 BETWEEN 30 AND 58 AND v2<>75));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[23, 30), (-∞, 51], (-∞, 68), (-∞, ∞)}, {[23, 30), (-∞, 51], (68, ∞), (-∞, ∞)}, {[30, 58], (-∞, 75), (-∞, ∞), (-∞, ∞)}, {[30, 58], (75, ∞), (-∞, ∞), (-∞, ∞)}, {(58, 85], (-∞, 51], (-∞, 68), (-∞, ∞)}, {(58, 85], (-∞, 51], (68, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=67 AND v2<=17 AND v3<>91 AND v4<82) OR (v1>28 AND v2 BETWEEN 17 AND 71 AND v3<12));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(28, ∞), [17, 71], (-∞, 12), (-∞, ∞)}, {[67, ∞), (-∞, 17), (-∞, 91), (-∞, 82)}, {[67, ∞), (-∞, 17], (91, ∞), (-∞, 82)}, {[67, ∞), [17, 17], [12, 91), (-∞, 82)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>77 AND v4>82 AND v2>=96) OR (v1 BETWEEN 41 AND 80 AND v2<>21 AND v3>60));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 77) AND (comp_index_t2.v4 > 82)) AND (comp_index_t2.v2 >= 96)) OR (((comp_index_t2.v1 BETWEEN 41 AND 80) AND (NOT((comp_index_t2.v2 = 21)))) AND (comp_index_t2.v3 > 60)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[41, 77], (21, ∞), (60, ∞), (-∞, ∞)}, {[41, 80], (-∞, 21), (60, ∞), (-∞, ∞)}, {(77, 80], (21, 96), (60, ∞), (-∞, ∞)}, {(77, ∞), [96, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1=28 AND v4 BETWEEN 44 AND 50) AND (v1>=49);`,
		ExpectedPlan: "Filter(comp_index_t2.v4 BETWEEN 44 AND 50)\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 81 AND 87 AND v3<>81 AND v4<30) AND (v1=17) OR (v1<27 AND v2<>8 AND v3>35)) OR (v1>28 AND v2<62));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 BETWEEN 81 AND 87) AND (NOT((comp_index_t2.v3 = 81)))) AND (comp_index_t2.v4 < 30)) AND (comp_index_t2.v1 = 17)) OR (((comp_index_t2.v1 < 27) AND (NOT((comp_index_t2.v2 = 8)))) AND (comp_index_t2.v3 > 35))) OR ((comp_index_t2.v1 > 28) AND (comp_index_t2.v2 < 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 27), (-∞, 8), (35, ∞), (-∞, ∞)}, {(-∞, 27), (8, ∞), (35, ∞), (-∞, ∞)}, {(28, ∞), (-∞, 62), (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>65 AND v2=64) OR (v1=82 AND v3<>99)) OR (v1>=68 AND v2=3 AND v3 BETWEEN 1 AND 51 AND v4<=73));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 65) AND (comp_index_t2.v2 = 64)) OR ((comp_index_t2.v1 = 82) AND (NOT((comp_index_t2.v3 = 99))))) OR ((((comp_index_t2.v1 >= 68) AND (comp_index_t2.v2 = 3)) AND (comp_index_t2.v3 BETWEEN 1 AND 51)) AND (comp_index_t2.v4 <= 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(65, 82), [64, 64], (-∞, ∞), (-∞, ∞)}, {[68, 82), [3, 3], [1, 51], (-∞, 73]}, {[82, 82], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(82, ∞), [3, 3], [1, 51], (-∞, 73]}, {(82, ∞), [64, 64], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=27 AND v3>23) OR (v1<70 AND v2<>43));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 <= 27) AND (comp_index_t2.v3 > 23)) OR ((comp_index_t2.v1 < 70) AND (NOT((comp_index_t2.v2 = 43)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 27], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(27, 70), (-∞, 43), (-∞, ∞), (-∞, ∞)}, {(27, 70), (43, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>34 AND v2>=89 AND v3>=14) OR (v1<=42 AND v3<1)) OR (v1<59 AND v2>=23 AND v3 BETWEEN 17 AND 37 AND v4 BETWEEN 21 AND 38));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t2.v1 = 34))) AND (comp_index_t2.v2 >= 89)) AND (comp_index_t2.v3 >= 14)) OR ((comp_index_t2.v1 <= 42) AND (comp_index_t2.v3 < 1))) OR ((((comp_index_t2.v1 < 59) AND (comp_index_t2.v2 >= 23)) AND (comp_index_t2.v3 BETWEEN 17 AND 37)) AND (comp_index_t2.v4 BETWEEN 21 AND 38)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 42], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(42, 59), [23, 89), [17, 37], [21, 38]}, {(42, ∞), [89, ∞), [14, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=52 AND v2>=55) OR (v1<73 AND v2<=1 AND v3>75 AND v4<=36)) OR (v1>=45 AND v2>=49 AND v3<=26 AND v4 BETWEEN 40 AND 83));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 73), (-∞, 1], (75, ∞), (-∞, 36]}, {[45, 52), [49, ∞), (-∞, 26], [40, 83]}, {[52, ∞), [49, 55), (-∞, 26], [40, 83]}, {[52, ∞), [55, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>0 AND v2=94 AND v3<>0) OR (v1>=83 AND v2<69 AND v3<84));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(0, ∞), [94, 94], (-∞, 0), (-∞, ∞)}, {(0, ∞), [94, 94], (0, ∞), (-∞, ∞)}, {[83, ∞), (-∞, 69), (-∞, 84), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<83 AND v4>51) OR (v1<>30));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 < 83) AND (comp_index_t2.v4 > 51)) OR (NOT((comp_index_t2.v1 = 30))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<92) OR (v1 BETWEEN 6 AND 39 AND v2=47 AND v3>=63));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 92), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=98) OR (v1<=2 AND v2<5));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 2], (-∞, 5), (-∞, ∞), (-∞, ∞)}, {[98, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>29 AND v4=40 AND v2>=63) OR (v1<70 AND v2<70 AND v3<=20)) OR (v1 BETWEEN 7 AND 61 AND v2>=33 AND v3>78)) OR (v1>=4 AND v2<=22));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 > 29) AND (comp_index_t2.v4 = 40)) AND (comp_index_t2.v2 >= 63)) OR (((comp_index_t2.v1 < 70) AND (comp_index_t2.v2 < 70)) AND (comp_index_t2.v3 <= 20))) OR (((comp_index_t2.v1 BETWEEN 7 AND 61) AND (comp_index_t2.v2 >= 33)) AND (comp_index_t2.v3 > 78))) OR ((comp_index_t2.v1 >= 4) AND (comp_index_t2.v2 <= 22)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 4), (-∞, 70), (-∞, 20], (-∞, ∞)}, {[4, 29], (22, 70), (-∞, 20], (-∞, ∞)}, {[4, ∞), (-∞, 22], (-∞, ∞), (-∞, ∞)}, {[7, 29], [33, ∞), (78, ∞), (-∞, ∞)}, {(29, 61], [33, 63), (78, ∞), (-∞, ∞)}, {(29, 70), (22, 63), (-∞, 20], (-∞, ∞)}, {(29, ∞), [63, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=12) OR (v1=28));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 12], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[28, 28], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=94 AND v2>=13 AND v3<=46 AND v4<>36) AND (v1=84) OR (v1 BETWEEN 52 AND 98 AND v2<71 AND v3<>45));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 <= 94) AND (comp_index_t2.v2 >= 13)) AND (comp_index_t2.v3 <= 46)) AND (NOT((comp_index_t2.v4 = 36)))) AND (comp_index_t2.v1 = 84)) OR (((comp_index_t2.v1 BETWEEN 52 AND 98) AND (comp_index_t2.v2 < 71)) AND (NOT((comp_index_t2.v3 = 45)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[52, 98], (-∞, 71), (-∞, 45), (-∞, ∞)}, {[52, 98], (-∞, 71), (45, ∞), (-∞, ∞)}, {[84, 84], [13, 71), [45, 45], (-∞, 36)}, {[84, 84], [13, 71), [45, 45], (36, ∞)}, {[84, 84], [71, ∞), (-∞, 46], (-∞, 36)}, {[84, 84], [71, ∞), (-∞, 46], (36, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>64) OR (v1<>55 AND v2=85 AND v3<=88));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 55), [85, 85], (-∞, 88], (-∞, ∞)}, {(55, 64], [85, 85], (-∞, 88], (-∞, ∞)}, {(64, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1 BETWEEN 54 AND 87 AND v2<78 AND v3<33) OR (v1<>52)) OR (v1 BETWEEN 3 AND 61 AND v4<=49)) OR (v1>3 AND v2<73 AND v3>59));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 BETWEEN 54 AND 87) AND (comp_index_t2.v2 < 78)) AND (comp_index_t2.v3 < 33)) OR (NOT((comp_index_t2.v1 = 52)))) OR ((comp_index_t2.v1 BETWEEN 3 AND 61) AND (comp_index_t2.v4 <= 49))) OR (((comp_index_t2.v1 > 3) AND (comp_index_t2.v2 < 73)) AND (comp_index_t2.v3 > 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 2 AND 23) OR (v1 BETWEEN 7 AND 14 AND v2<=27 AND v3<=82)) OR (v1>61));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[2, 23], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(61, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=31 AND v2>44) OR (v1<44 AND v4<>6 AND v2<>10 AND v3<>14)) AND (v1=96 AND v3>25 AND v4<>32);`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 31) AND (comp_index_t2.v2 > 44)) OR ((((comp_index_t2.v1 < 44) AND (NOT((comp_index_t2.v4 = 6)))) AND (NOT((comp_index_t2.v2 = 10)))) AND (NOT((comp_index_t2.v3 = 14))))) AND (comp_index_t2.v3 > 25)) AND (NOT((comp_index_t2.v4 = 32))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>=85 AND v2<12) AND (v1>=25);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[85, ∞), (-∞, 12), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=0) OR (v1=31)) OR (v1<>73 AND v4>9 AND v2 BETWEEN 27 AND 69 AND v3=14));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 = 0) OR (comp_index_t2.v1 = 31)) OR ((((NOT((comp_index_t2.v1 = 73))) AND (comp_index_t2.v4 > 9)) AND (comp_index_t2.v2 BETWEEN 27 AND 69)) AND (comp_index_t2.v3 = 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 0), [27, 69], [14, 14], (9, ∞)}, {[0, 0], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(0, 31), [27, 69], [14, 14], (9, ∞)}, {[31, 31], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(31, 73), [27, 69], [14, 14], (9, ∞)}, {(73, ∞), [27, 69], [14, 14], (9, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=42 AND v2=41 AND v3 BETWEEN 29 AND 94 AND v4<71) OR (v1>=71 AND v2 BETWEEN 67 AND 87 AND v3>=9)) OR (v1<2 AND v2<=1 AND v3<36 AND v4>41));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 2), (-∞, 1], (-∞, 36), (41, ∞)}, {[42, ∞), [41, 41], [29, 94], (-∞, 71)}, {[71, ∞), [67, 87], [9, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=3 AND v2<57 AND v3<>74 AND v4>=69) OR (v1<>66 AND v2=16)) OR (v1=44 AND v3=58));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 <= 3) AND (comp_index_t2.v2 < 57)) AND (NOT((comp_index_t2.v3 = 74)))) AND (comp_index_t2.v4 >= 69)) OR ((NOT((comp_index_t2.v1 = 66))) AND (comp_index_t2.v2 = 16))) OR ((comp_index_t2.v1 = 44) AND (comp_index_t2.v3 = 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 3], (-∞, 16), (-∞, 74), [69, ∞)}, {(-∞, 3], (-∞, 16), (74, ∞), [69, ∞)}, {(-∞, 3], (16, 57), (-∞, 74), [69, ∞)}, {(-∞, 3], (16, 57), (74, ∞), [69, ∞)}, {(-∞, 44), [16, 16], (-∞, ∞), (-∞, ∞)}, {[44, 44], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(44, 66), [16, 16], (-∞, ∞), (-∞, ∞)}, {(66, ∞), [16, 16], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<=22 AND v2<=41) OR (v1=61 AND v2>21)) OR (v1<>10)) OR (v1 BETWEEN 43 AND 44 AND v2>=35 AND v3<>87));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 <= 22) AND (comp_index_t2.v2 <= 41)) OR ((comp_index_t2.v1 = 61) AND (comp_index_t2.v2 > 21))) OR (NOT((comp_index_t2.v1 = 10)))) OR (((comp_index_t2.v1 BETWEEN 43 AND 44) AND (comp_index_t2.v2 >= 35)) AND (NOT((comp_index_t2.v3 = 87)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 10), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[10, 10], (-∞, 41], (-∞, ∞), (-∞, ∞)}, {(10, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<=13 AND v3>20) OR (v1 BETWEEN 18 AND 26 AND v2>11 AND v3>22)) OR (v1<18 AND v2>=47 AND v3<11)) OR (v1>19));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 <= 13) AND (comp_index_t2.v3 > 20)) OR (((comp_index_t2.v1 BETWEEN 18 AND 26) AND (comp_index_t2.v2 > 11)) AND (comp_index_t2.v3 > 22))) OR (((comp_index_t2.v1 < 18) AND (comp_index_t2.v2 >= 47)) AND (comp_index_t2.v3 < 11))) OR (comp_index_t2.v1 > 19))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 13], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(13, 18), [47, ∞), (-∞, 11), (-∞, ∞)}, {[18, 19], (11, ∞), (22, ∞), (-∞, ∞)}, {(19, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 42 AND 54 AND v2>20) OR (v1<>68 AND v3>32));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 BETWEEN 42 AND 54) AND (comp_index_t2.v2 > 20)) OR ((NOT((comp_index_t2.v1 = 68))) AND (comp_index_t2.v3 > 32)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 68), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(68, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 20 AND 93) AND (v1=66 AND v2<>21 AND v3 BETWEEN 43 AND 94);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[66, 66], (21, ∞), [43, 94], (-∞, ∞)}, {[66, 66], (-∞, 21), [43, 94], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>83 AND v2<>16 AND v3=22) AND (v1=34) AND (v1=79 AND v2<=45 AND v3=49);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=44 AND v2<=98) AND (v1>15) OR (v1<=45 AND v2=1 AND v3<>54));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 44) AND (comp_index_t2.v2 <= 98)) AND (comp_index_t2.v1 > 15)) OR (((comp_index_t2.v1 <= 45) AND (comp_index_t2.v2 = 1)) AND (NOT((comp_index_t2.v3 = 54)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 44), [1, 1], (-∞, 54), (-∞, ∞)}, {(-∞, 44), [1, 1], (54, ∞), (-∞, ∞)}, {[44, 44], (-∞, 98], (-∞, ∞), (-∞, ∞)}, {(44, 45], [1, 1], (-∞, 54), (-∞, ∞)}, {(44, 45], [1, 1], (54, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<38 AND v2>24) OR (v1<20 AND v3>=3 AND v4 BETWEEN 59 AND 81)) OR (v1<31 AND v4 BETWEEN 2 AND 16 AND v2=6 AND v3<=69));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 < 38) AND (comp_index_t2.v2 > 24)) OR (((comp_index_t2.v1 < 20) AND (comp_index_t2.v3 >= 3)) AND (comp_index_t2.v4 BETWEEN 59 AND 81))) OR ((((comp_index_t2.v1 < 31) AND (comp_index_t2.v4 BETWEEN 2 AND 16)) AND (comp_index_t2.v2 = 6)) AND (comp_index_t2.v3 <= 69)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 20), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[20, 31), [6, 6], (-∞, 69], [2, 16]}, {[20, 38), (24, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((((v1<43 AND v4<=22) OR (v1<=72 AND v2>=35 AND v3>=96)) OR (v1=63 AND v2=55 AND v3<>46)) OR (v1>=9 AND v2=52 AND v3=86 AND v4<=27)) OR (v1 BETWEEN 37 AND 62));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 < 43) AND (comp_index_t2.v4 <= 22)) OR (((comp_index_t2.v1 <= 72) AND (comp_index_t2.v2 >= 35)) AND (comp_index_t2.v3 >= 96))) OR (((comp_index_t2.v1 = 63) AND (comp_index_t2.v2 = 55)) AND (NOT((comp_index_t2.v3 = 46))))) OR ((((comp_index_t2.v1 >= 9) AND (comp_index_t2.v2 = 52)) AND (comp_index_t2.v3 = 86)) AND (comp_index_t2.v4 <= 27))) OR (comp_index_t2.v1 BETWEEN 37 AND 62))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 62], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(62, 63), [35, ∞), [96, ∞), (-∞, ∞)}, {(62, ∞), [52, 52], [86, 86], (-∞, 27]}, {[63, 63], [35, 55), [96, ∞), (-∞, ∞)}, {[63, 63], [55, 55], (-∞, 46), (-∞, ∞)}, {[63, 63], [55, 55], (46, ∞), (-∞, ∞)}, {[63, 63], (55, ∞), [96, ∞), (-∞, ∞)}, {(63, 72], [35, ∞), [96, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=52) OR (v1>=59 AND v2<=30 AND v3=98 AND v4 BETWEEN 43 AND 74));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[52, 52], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[59, ∞), (-∞, 30], [98, 98], [43, 74]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=37 AND v3>=74 AND v4=54) OR (v1>=36 AND v3<=42 AND v4<=94)) AND (v1=59 AND v2<=56) OR (v1>=83 AND v2<=11));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 >= 37) AND (comp_index_t2.v3 >= 74)) AND (comp_index_t2.v4 = 54)) OR (((comp_index_t2.v1 >= 36) AND (comp_index_t2.v3 <= 42)) AND (comp_index_t2.v4 <= 94))) AND ((comp_index_t2.v1 = 59) AND (comp_index_t2.v2 <= 56))) OR ((comp_index_t2.v1 >= 83) AND (comp_index_t2.v2 <= 11)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[59, 59], (-∞, 56], (-∞, ∞), (-∞, ∞)}, {[83, ∞), (-∞, 11], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>39 AND v3<44 AND v4 BETWEEN 3 AND 31 AND v2>16) OR (v1>72 AND v2=73 AND v3<37 AND v4<=43)) OR (v1=9 AND v2<50));`,
		ExpectedPlan: "Filter((((((NOT((comp_index_t2.v1 = 39))) AND (comp_index_t2.v3 < 44)) AND (comp_index_t2.v4 BETWEEN 3 AND 31)) AND (comp_index_t2.v2 > 16)) OR ((((comp_index_t2.v1 > 72) AND (comp_index_t2.v2 = 73)) AND (comp_index_t2.v3 < 37)) AND (comp_index_t2.v4 <= 43))) OR ((comp_index_t2.v1 = 9) AND (comp_index_t2.v2 < 50)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 9), (16, ∞), (-∞, 44), [3, 31]}, {[9, 9], (-∞, 50), (-∞, ∞), (-∞, ∞)}, {[9, 9], [50, ∞), (-∞, 44), [3, 31]}, {(9, 39), (16, ∞), (-∞, 44), [3, 31]}, {(39, 72], (16, ∞), (-∞, 44), [3, 31]}, {(72, ∞), (16, 73), (-∞, 44), [3, 31]}, {(72, ∞), [73, 73], (-∞, 37), (-∞, 43]}, {(72, ∞), [73, 73], [37, 44), [3, 31]}, {(72, ∞), (73, ∞), (-∞, 44), [3, 31]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<31 AND v2<>14 AND v3 BETWEEN 0 AND 10 AND v4>=95) OR (v1<>91)) OR (v1<>35));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 < 31) AND (NOT((comp_index_t2.v2 = 14)))) AND (comp_index_t2.v3 BETWEEN 0 AND 10)) AND (comp_index_t2.v4 >= 95)) OR (NOT((comp_index_t2.v1 = 91)))) OR (NOT((comp_index_t2.v1 = 35))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>13) OR (v1<>3 AND v4<=42 AND v2 BETWEEN 89 AND 94));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 > 13) OR (((NOT((comp_index_t2.v1 = 3))) AND (comp_index_t2.v4 <= 42)) AND (comp_index_t2.v2 BETWEEN 89 AND 94)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 3), [89, 94], (-∞, ∞), (-∞, ∞)}, {(3, 13], [89, 94], (-∞, ∞), (-∞, ∞)}, {(13, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<29 AND v2<=19) AND (v1>=26) OR (v1>=87 AND v2<=12 AND v3=36 AND v4<20)) AND (v1<=24 AND v4>85 AND v2 BETWEEN 1 AND 64) OR (v1>27 AND v2>=8 AND v3<24));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 < 29) AND (comp_index_t2.v2 <= 19)) AND (comp_index_t2.v1 >= 26)) OR ((((comp_index_t2.v1 >= 87) AND (comp_index_t2.v2 <= 12)) AND (comp_index_t2.v3 = 36)) AND (comp_index_t2.v4 < 20))) AND (((comp_index_t2.v1 <= 24) AND (comp_index_t2.v4 > 85)) AND (comp_index_t2.v2 BETWEEN 1 AND 64))) OR (((comp_index_t2.v1 > 27) AND (comp_index_t2.v2 >= 8)) AND (comp_index_t2.v3 < 24)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(27, ∞), [8, ∞), (-∞, 24), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<77 AND v2 BETWEEN 5 AND 22 AND v3<>91 AND v4<34) OR (v1=68 AND v2=50)) OR (v1<44 AND v2>84 AND v3<37 AND v4>=67));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 44), (84, ∞), (-∞, 37), [67, ∞)}, {(-∞, 77), [5, 22], (-∞, 91), (-∞, 34)}, {(-∞, 77), [5, 22], (91, ∞), (-∞, 34)}, {[68, 68], [50, 50], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<4 AND v2>=71) OR (v1<18 AND v2=57));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 4), [71, ∞), (-∞, ∞), (-∞, ∞)}, {(-∞, 18), [57, 57], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>61 AND v2 BETWEEN 46 AND 51) OR (v1 BETWEEN 32 AND 75 AND v4<=32)) AND (v1>97) OR (v1<97));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t2.v1 = 61))) AND (comp_index_t2.v2 BETWEEN 46 AND 51)) OR ((comp_index_t2.v1 BETWEEN 32 AND 75) AND (comp_index_t2.v4 <= 32))) AND (comp_index_t2.v1 > 97)) OR (comp_index_t2.v1 < 97))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 97), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(97, ∞), [46, 51], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 4 AND 71 AND v2<=70) AND (v1<>47 AND v2 BETWEEN 19 AND 65) OR (v1=59 AND v2 BETWEEN 25 AND 58));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 BETWEEN 4 AND 71) AND (comp_index_t2.v2 <= 70)) AND ((NOT((comp_index_t2.v1 = 47))) AND (comp_index_t2.v2 BETWEEN 19 AND 65))) OR ((comp_index_t2.v1 = 59) AND (comp_index_t2.v2 BETWEEN 25 AND 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[4, 47), [19, 65], (-∞, ∞), (-∞, ∞)}, {(47, 71], [19, 65], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<70 AND v2<=90) OR (v1<5 AND v2<>13 AND v3 BETWEEN 20 AND 96 AND v4>92)) OR (v1<>76)) OR (v1 BETWEEN 12 AND 88 AND v2 BETWEEN 53 AND 67 AND v3>=39));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 < 70) AND (comp_index_t2.v2 <= 90)) OR ((((comp_index_t2.v1 < 5) AND (NOT((comp_index_t2.v2 = 13)))) AND (comp_index_t2.v3 BETWEEN 20 AND 96)) AND (comp_index_t2.v4 > 92))) OR (NOT((comp_index_t2.v1 = 76)))) OR (((comp_index_t2.v1 BETWEEN 12 AND 88) AND (comp_index_t2.v2 BETWEEN 53 AND 67)) AND (comp_index_t2.v3 >= 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 76), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[76, 76], [53, 67], [39, ∞), (-∞, ∞)}, {(76, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1 BETWEEN 8 AND 38 AND v2<=31 AND v3 BETWEEN 30 AND 46 AND v4>=28) OR (v1<=22 AND v4<>40 AND v2>76 AND v3 BETWEEN 38 AND 42)) OR (v1<=52 AND v2<93 AND v3>=83)) OR (v1>=33 AND v3>13 AND v4>34));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 BETWEEN 8 AND 38) AND (comp_index_t2.v2 <= 31)) AND (comp_index_t2.v3 BETWEEN 30 AND 46)) AND (comp_index_t2.v4 >= 28)) OR ((((comp_index_t2.v1 <= 22) AND (NOT((comp_index_t2.v4 = 40)))) AND (comp_index_t2.v2 > 76)) AND (comp_index_t2.v3 BETWEEN 38 AND 42))) OR (((comp_index_t2.v1 <= 52) AND (comp_index_t2.v2 < 93)) AND (comp_index_t2.v3 >= 83))) OR (((comp_index_t2.v1 >= 33) AND (comp_index_t2.v3 > 13)) AND (comp_index_t2.v4 > 34)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 22], (76, ∞), [38, 42], (-∞, 40)}, {(-∞, 22], (76, ∞), [38, 42], (40, ∞)}, {(-∞, 33), (-∞, 93), [83, ∞), (-∞, ∞)}, {[8, 33), (-∞, 31], [30, 46], [28, ∞)}, {[33, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 13 AND 40 AND v2>=0) OR (v1<>3 AND v2>47 AND v3<44 AND v4>49)) OR (v1=23));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 3), (47, ∞), (-∞, 44), (49, ∞)}, {(3, 13), (47, ∞), (-∞, 44), (49, ∞)}, {[13, 23), [0, ∞), (-∞, ∞), (-∞, ∞)}, {[23, 23], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(23, 40], [0, ∞), (-∞, ∞), (-∞, ∞)}, {(40, ∞), (47, ∞), (-∞, 44), (49, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>35 AND v2<>26) OR (v1<=30 AND v2 BETWEEN 6 AND 61 AND v3<=95 AND v4>5)) AND (v1<>97) OR (v1>31));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 35) AND (NOT((comp_index_t2.v2 = 26)))) OR ((((comp_index_t2.v1 <= 30) AND (comp_index_t2.v2 BETWEEN 6 AND 61)) AND (comp_index_t2.v3 <= 95)) AND (comp_index_t2.v4 > 5))) AND (NOT((comp_index_t2.v1 = 97)))) OR (comp_index_t2.v1 > 31))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 30], [6, 61], (-∞, 95], (5, ∞)}, {(31, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1=43 AND v2>=64) OR (v1>6 AND v3=92 AND v4>=15)) OR (v1<=55 AND v3=6 AND v4<=77 AND v2<=3)) OR (v1=96 AND v3<=80 AND v4<=13));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 43) AND (comp_index_t2.v2 >= 64)) OR (((comp_index_t2.v1 > 6) AND (comp_index_t2.v3 = 92)) AND (comp_index_t2.v4 >= 15))) OR ((((comp_index_t2.v1 <= 55) AND (comp_index_t2.v3 = 6)) AND (comp_index_t2.v4 <= 77)) AND (comp_index_t2.v2 <= 3))) OR (((comp_index_t2.v1 = 96) AND (comp_index_t2.v3 <= 80)) AND (comp_index_t2.v4 <= 13)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 6], (-∞, 3], [6, 6], (-∞, 77]}, {(6, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>8 AND v3 BETWEEN 14 AND 75 AND v4=28) AND (v1>=95 AND v2<>72 AND v3=22) OR (v1=5));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 8) AND (comp_index_t2.v3 BETWEEN 14 AND 75)) AND (comp_index_t2.v4 = 28)) AND (((comp_index_t2.v1 >= 95) AND (NOT((comp_index_t2.v2 = 72)))) AND (comp_index_t2.v3 = 22))) OR (comp_index_t2.v1 = 5))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, 5], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[95, ∞), (-∞, 72), [22, 22], [28, 28]}, {[95, ∞), (72, ∞), [22, 22], [28, 28]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=95 AND v2<1 AND v3 BETWEEN 49 AND 61 AND v4=51) OR (v1>29 AND v2>=9 AND v3>=63 AND v4<=88));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(29, ∞), [9, ∞), [63, ∞), (-∞, 88]}, {[95, 95], (-∞, 1), [49, 61], [51, 51]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>30 AND v2 BETWEEN 20 AND 64) AND (v1<=29) AND (v1>=25 AND v2<>0);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=89 AND v2<=1 AND v3<=7 AND v4>=4) AND (v1<=87) OR (v1 BETWEEN 10 AND 46 AND v2 BETWEEN 18 AND 76));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 = 89) AND (comp_index_t2.v2 <= 1)) AND (comp_index_t2.v3 <= 7)) AND (comp_index_t2.v4 >= 4)) AND (comp_index_t2.v1 <= 87)) OR ((comp_index_t2.v1 BETWEEN 10 AND 46) AND (comp_index_t2.v2 BETWEEN 18 AND 76)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[10, 46], [18, 76], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=44 AND v2>=45 AND v3>=34 AND v4>1) OR (v1=33));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[33, 33], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[44, 44], [45, ∞), [34, ∞), (1, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<>12 AND v2<=6) OR (v1>99 AND v2<>51 AND v3=38)) OR (v1>60)) OR (v1 BETWEEN 69 AND 77 AND v2>=49 AND v3>=43));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 12), (-∞, 6], (-∞, ∞), (-∞, ∞)}, {(12, 60], (-∞, 6], (-∞, ∞), (-∞, ∞)}, {(60, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 49 AND 53 AND v4 BETWEEN 22 AND 96) OR (v1 BETWEEN 7 AND 79)) AND (v1<=45 AND v2<=11) OR (v1 BETWEEN 16 AND 65 AND v2<53 AND v3<>15 AND v4>22));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 49 AND 53) AND (comp_index_t2.v4 BETWEEN 22 AND 96)) OR (comp_index_t2.v1 BETWEEN 7 AND 79)) AND ((comp_index_t2.v1 <= 45) AND (comp_index_t2.v2 <= 11))) OR ((((comp_index_t2.v1 BETWEEN 16 AND 65) AND (comp_index_t2.v2 < 53)) AND (NOT((comp_index_t2.v3 = 15)))) AND (comp_index_t2.v4 > 22)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[7, 45], (-∞, 11], (-∞, ∞), (-∞, ∞)}, {[16, 45], (11, 53), (-∞, 15), (22, ∞)}, {[16, 45], (11, 53), (15, ∞), (22, ∞)}, {(45, 65], (-∞, 53), (-∞, 15), (22, ∞)}, {(45, 65], (-∞, 53), (15, ∞), (22, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<11) OR (v1<=38 AND v2>=93 AND v3<=34 AND v4>7));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 11), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[11, 38], [93, ∞), (-∞, 34], (7, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=97 AND v3<>2) OR (v1=49 AND v2 BETWEEN 29 AND 30 AND v3<>97));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 <= 97) AND (NOT((comp_index_t2.v3 = 2)))) OR (((comp_index_t2.v1 = 49) AND (comp_index_t2.v2 BETWEEN 29 AND 30)) AND (NOT((comp_index_t2.v3 = 97)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 97], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=64) OR (v1>21 AND v2 BETWEEN 0 AND 58)) OR (v1<15 AND v4 BETWEEN 63 AND 76 AND v2>84));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 <= 64) OR ((comp_index_t2.v1 > 21) AND (comp_index_t2.v2 BETWEEN 0 AND 58))) OR (((comp_index_t2.v1 < 15) AND (comp_index_t2.v4 BETWEEN 63 AND 76)) AND (comp_index_t2.v2 > 84)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 64], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(64, ∞), [0, 58], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 24 AND 98 AND v2>0 AND v3>=87) OR (v1 BETWEEN 2 AND 3 AND v2 BETWEEN 15 AND 78));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[2, 3], [15, 78], (-∞, ∞), (-∞, ∞)}, {[24, 98], (0, ∞), [87, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<>37) OR (v1<=94 AND v2 BETWEEN 53 AND 65 AND v3>=9)) OR (v1<10 AND v3<>26 AND v4<91)) OR (v1<>21 AND v2<>24 AND v3<46));`,
		ExpectedPlan: "Filter((((NOT((comp_index_t2.v1 = 37))) OR (((comp_index_t2.v1 <= 94) AND (comp_index_t2.v2 BETWEEN 53 AND 65)) AND (comp_index_t2.v3 >= 9))) OR (((comp_index_t2.v1 < 10) AND (NOT((comp_index_t2.v3 = 26)))) AND (comp_index_t2.v4 < 91))) OR (((NOT((comp_index_t2.v1 = 21))) AND (NOT((comp_index_t2.v2 = 24)))) AND (comp_index_t2.v3 < 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 37), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[37, 37], (-∞, 24), (-∞, 46), (-∞, ∞)}, {[37, 37], (24, 53), (-∞, 46), (-∞, ∞)}, {[37, 37], [53, 65], (-∞, ∞), (-∞, ∞)}, {[37, 37], (65, ∞), (-∞, 46), (-∞, ∞)}, {(37, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>21 AND v2>27 AND v3>=97 AND v4 BETWEEN 25 AND 67) OR (v1>=66 AND v2<=56)) OR (v1=37));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 21), (27, ∞), [97, ∞), [25, 67]}, {(21, 37), (27, ∞), [97, ∞), [25, 67]}, {[37, 37], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(37, 66), (27, ∞), [97, ∞), [25, 67]}, {[66, ∞), (-∞, 56], (-∞, ∞), (-∞, ∞)}, {[66, ∞), (56, ∞), [97, ∞), [25, 67]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=43 AND v2<48 AND v3<16 AND v4<=75) OR (v1<71));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 71), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>91 AND v2=91 AND v3>=15) OR (v1 BETWEEN 16 AND 30)) OR (v1<>27 AND v4=62));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t2.v1 = 91))) AND (comp_index_t2.v2 = 91)) AND (comp_index_t2.v3 >= 15)) OR (comp_index_t2.v1 BETWEEN 16 AND 30)) OR ((NOT((comp_index_t2.v1 = 27))) AND (comp_index_t2.v4 = 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=54 AND v3>26 AND v4>30 AND v2 BETWEEN 3 AND 8) OR (v1>8 AND v2<=43 AND v3<>97));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 54) AND (comp_index_t2.v3 > 26)) AND (comp_index_t2.v4 > 30)) AND (comp_index_t2.v2 BETWEEN 3 AND 8)) OR (((comp_index_t2.v1 > 8) AND (comp_index_t2.v2 <= 43)) AND (NOT((comp_index_t2.v3 = 97)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(8, ∞), (-∞, 43], (-∞, 97), (-∞, ∞)}, {(8, ∞), (-∞, 43], (97, ∞), (-∞, ∞)}, {[54, 54], [3, 8], [97, 97], (30, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=38 AND v2<>11 AND v3>=26) OR (v1 BETWEEN 37 AND 90 AND v4<85 AND v2<0)) OR (v1<>23));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 >= 38) AND (NOT((comp_index_t2.v2 = 11)))) AND (comp_index_t2.v3 >= 26)) OR (((comp_index_t2.v1 BETWEEN 37 AND 90) AND (comp_index_t2.v4 < 85)) AND (comp_index_t2.v2 < 0))) OR (NOT((comp_index_t2.v1 = 23))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 23), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(23, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<20 AND v2<>84 AND v3<25 AND v4>=93) OR (v1<13));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 13), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[13, 20), (-∞, 84), (-∞, 25), [93, ∞)}, {[13, 20), (84, ∞), (-∞, 25), [93, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=81 AND v2 BETWEEN 55 AND 77 AND v3=64) OR (v1=20 AND v2=21));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[20, 20], [21, 21], (-∞, ∞), (-∞, ∞)}, {[81, ∞), [55, 77], [64, 64], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>30 AND v2 BETWEEN 58 AND 72 AND v3<=35) OR (v1 BETWEEN 28 AND 28 AND v2>=76)) OR (v1=74 AND v2<26));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[28, 28], [76, ∞), (-∞, ∞), (-∞, ∞)}, {(30, ∞), [58, 72], (-∞, 35], (-∞, ∞)}, {[74, 74], (-∞, 26), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>5 AND v2>8) OR (v1>78 AND v2<=39 AND v3>=41 AND v4<=35)) AND (v1<=11 AND v2<35 AND v3<=10 AND v4<76) OR (v1>=22)) OR (v1=1 AND v4<>29 AND v2 BETWEEN 64 AND 81 AND v3>46));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 > 5) AND (comp_index_t2.v2 > 8)) OR ((((comp_index_t2.v1 > 78) AND (comp_index_t2.v2 <= 39)) AND (comp_index_t2.v3 >= 41)) AND (comp_index_t2.v4 <= 35))) AND ((((comp_index_t2.v1 <= 11) AND (comp_index_t2.v2 < 35)) AND (comp_index_t2.v3 <= 10)) AND (comp_index_t2.v4 < 76))) OR (comp_index_t2.v1 >= 22)) OR ((((comp_index_t2.v1 = 1) AND (NOT((comp_index_t2.v4 = 29)))) AND (comp_index_t2.v2 BETWEEN 64 AND 81)) AND (comp_index_t2.v3 > 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[1, 1], [64, 81], (46, ∞), (-∞, 29)}, {[1, 1], [64, 81], (46, ∞), (29, ∞)}, {(5, 11], (8, 35), (-∞, 10], (-∞, 76)}, {[22, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=49) OR (v1<43 AND v2>=34));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 43), [34, ∞), (-∞, ∞), (-∞, ∞)}, {[49, 49], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>=72) OR (v1<>17)) OR (v1=47 AND v2<>1 AND v3 BETWEEN 75 AND 78 AND v4 BETWEEN 10 AND 44)) OR (v1>=64 AND v2>=74 AND v3=10 AND v4 BETWEEN 11 AND 93));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 >= 72) OR (NOT((comp_index_t2.v1 = 17)))) OR ((((comp_index_t2.v1 = 47) AND (NOT((comp_index_t2.v2 = 1)))) AND (comp_index_t2.v3 BETWEEN 75 AND 78)) AND (comp_index_t2.v4 BETWEEN 10 AND 44))) OR ((((comp_index_t2.v1 >= 64) AND (comp_index_t2.v2 >= 74)) AND (comp_index_t2.v3 = 10)) AND (comp_index_t2.v4 BETWEEN 11 AND 93)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 17), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(17, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<2 AND v2<>94) OR (v1<>76 AND v2=27 AND v3<=31 AND v4<38));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 2), (-∞, 94), (-∞, ∞), (-∞, ∞)}, {(-∞, 2), (94, ∞), (-∞, ∞), (-∞, ∞)}, {[2, 76), [27, 27], (-∞, 31], (-∞, 38)}, {(76, ∞), [27, 27], (-∞, 31], (-∞, 38)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<>11 AND v2>47 AND v3>=67 AND v4=29) OR (v1>=59 AND v3 BETWEEN 4 AND 29 AND v4>=65 AND v2<>96)) OR (v1<=62)) OR (v1<61 AND v2<>28 AND v3<>8 AND v4<>30));`,
		ExpectedPlan: "Filter(((((((NOT((comp_index_t2.v1 = 11))) AND (comp_index_t2.v2 > 47)) AND (comp_index_t2.v3 >= 67)) AND (comp_index_t2.v4 = 29)) OR ((((comp_index_t2.v1 >= 59) AND (comp_index_t2.v3 BETWEEN 4 AND 29)) AND (comp_index_t2.v4 >= 65)) AND (NOT((comp_index_t2.v2 = 96))))) OR (comp_index_t2.v1 <= 62)) OR ((((comp_index_t2.v1 < 61) AND (NOT((comp_index_t2.v2 = 28)))) AND (NOT((comp_index_t2.v3 = 8)))) AND (NOT((comp_index_t2.v4 = 30)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 62], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(62, ∞), (-∞, 96), [4, 29], [65, ∞)}, {(62, ∞), (47, ∞), [67, ∞), [29, 29]}, {(62, ∞), (96, ∞), [4, 29], [65, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 36 AND 72) OR (v1<>48 AND v4>91 AND v2<5 AND v3>=38)) OR (v1<>17 AND v3=50));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 BETWEEN 36 AND 72) OR ((((NOT((comp_index_t2.v1 = 48))) AND (comp_index_t2.v4 > 91)) AND (comp_index_t2.v2 < 5)) AND (comp_index_t2.v3 >= 38))) OR ((NOT((comp_index_t2.v1 = 17))) AND (comp_index_t2.v3 = 50)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 17), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[17, 17], (-∞, 5), [38, ∞), (91, ∞)}, {(17, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<86) OR (v1<=5 AND v2<25 AND v3<>24)) OR (v1<32 AND v3 BETWEEN 51 AND 54 AND v4<=70));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 < 86) OR (((comp_index_t2.v1 <= 5) AND (comp_index_t2.v2 < 25)) AND (NOT((comp_index_t2.v3 = 24))))) OR (((comp_index_t2.v1 < 32) AND (comp_index_t2.v3 BETWEEN 51 AND 54)) AND (comp_index_t2.v4 <= 70)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 86), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<=6) OR (v1 BETWEEN 24 AND 89)) OR (v1<87 AND v2=35 AND v3=19)) AND (v1>94 AND v2=33 AND v3>28) OR (v1 BETWEEN 36 AND 40));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 <= 6) OR (comp_index_t2.v1 BETWEEN 24 AND 89)) OR (((comp_index_t2.v1 < 87) AND (comp_index_t2.v2 = 35)) AND (comp_index_t2.v3 = 19))) AND (((comp_index_t2.v1 > 94) AND (comp_index_t2.v2 = 33)) AND (comp_index_t2.v3 > 28))) OR (comp_index_t2.v1 BETWEEN 36 AND 40))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[36, 40], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=24 AND v2=61 AND v3<49 AND v4<82) OR (v1<4 AND v2>51 AND v3=9));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 4), (51, ∞), [9, 9], (-∞, ∞)}, {[24, ∞), [61, 61], (-∞, 49), (-∞, 82)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 0 AND 87 AND v2>=44 AND v3<>68 AND v4=50) OR (v1<1 AND v4<66 AND v2<11 AND v3<>44));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 0 AND 87) AND (comp_index_t2.v2 >= 44)) AND (NOT((comp_index_t2.v3 = 68)))) AND (comp_index_t2.v4 = 50)) OR ((((comp_index_t2.v1 < 1) AND (comp_index_t2.v4 < 66)) AND (comp_index_t2.v2 < 11)) AND (NOT((comp_index_t2.v3 = 44)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 1), (-∞, 11), (-∞, 44), (-∞, 66)}, {(-∞, 1), (-∞, 11), (44, ∞), (-∞, 66)}, {[0, 87], [44, ∞), (-∞, 68), [50, 50]}, {[0, 87], [44, ∞), (68, ∞), [50, 50]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1<17 AND v2<54) AND (v1>=70 AND v2 BETWEEN 53 AND 53 AND v3>10 AND v4=17);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1=21 AND v2>25 AND v3>=7) OR (v1 BETWEEN 23 AND 88 AND v2<=26 AND v3>=87 AND v4 BETWEEN 42 AND 95)) OR (v1<4 AND v2>=66 AND v3<=24 AND v4=10)) OR (v1>69));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 4), [66, ∞), (-∞, 24], [10, 10]}, {[21, 21], (25, ∞), [7, ∞), (-∞, ∞)}, {[23, 69], (-∞, 26], [87, ∞), [42, 95]}, {(69, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 0 AND 39) OR (v1<18 AND v4>=90));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 BETWEEN 0 AND 39) OR ((comp_index_t2.v1 < 18) AND (comp_index_t2.v4 >= 90)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 39], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<99 AND v2>1 AND v3<=56) OR (v1>36 AND v2=53 AND v3>17)) OR (v1<>71)) AND (v1 BETWEEN 2 AND 86 AND v2<>78 AND v3<>29 AND v4<>63);`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 < 99) AND (comp_index_t2.v2 > 1)) AND (comp_index_t2.v3 <= 56)) OR (((comp_index_t2.v1 > 36) AND (comp_index_t2.v2 = 53)) AND (comp_index_t2.v3 > 17))) OR (NOT((comp_index_t2.v1 = 71))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[2, 71), (-∞, 78), (-∞, 29), (-∞, 63)}, {[2, 71), (-∞, 78), (-∞, 29), (63, ∞)}, {[2, 71), (-∞, 78), (29, ∞), (-∞, 63)}, {[2, 71), (-∞, 78), (29, ∞), (63, ∞)}, {[2, 71), (78, ∞), (29, ∞), (-∞, 63)}, {[2, 71), (78, ∞), (29, ∞), (63, ∞)}, {[2, 86], (78, ∞), (-∞, 29), (-∞, 63)}, {[2, 86], (78, ∞), (-∞, 29), (63, ∞)}, {[71, 71], (1, 53), (29, 56], (-∞, 63)}, {[71, 71], (1, 53), (29, 56], (63, ∞)}, {[71, 71], (1, 78), (-∞, 29), (-∞, 63)}, {[71, 71], (1, 78), (-∞, 29), (63, ∞)}, {[71, 71], [53, 53], (29, ∞), (-∞, 63)}, {[71, 71], [53, 53], (29, ∞), (63, ∞)}, {[71, 71], (53, 78), (29, 56], (-∞, 63)}, {[71, 71], (53, 78), (29, 56], (63, ∞)}, {[71, 71], (78, ∞), (29, 56], (-∞, 63)}, {[71, 71], (78, ∞), (29, 56], (63, ∞)}, {(71, 86], (-∞, 78), (-∞, 29), (-∞, 63)}, {(71, 86], (-∞, 78), (-∞, 29), (63, ∞)}, {(71, 86], (-∞, 78), (29, ∞), (-∞, 63)}, {(71, 86], (-∞, 78), (29, ∞), (63, ∞)}, {(71, 86], (78, ∞), (29, ∞), (-∞, 63)}, {(71, 86], (78, ∞), (29, ∞), (63, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=5) OR (v1=53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 5], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[53, 53], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<>12 AND v2 BETWEEN 27 AND 46 AND v3 BETWEEN 19 AND 27 AND v4>=50) OR (v1 BETWEEN 17 AND 88)) OR (v1<=36 AND v2<=37 AND v3<64)) OR (v1<>82 AND v2>84 AND v3>=90)) AND (v1>34 AND v3>4);`,
		ExpectedPlan: "Filter(comp_index_t2.v3 > 4)\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(34, 88], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(88, ∞), [27, 46], [19, 27], [50, ∞)}, {(88, ∞), (84, ∞), [90, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=82) OR (v1<=95 AND v2<>23 AND v3<18 AND v4<>50));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 82), (-∞, 23), (-∞, 18), (-∞, 50)}, {(-∞, 82), (-∞, 23), (-∞, 18), (50, ∞)}, {(-∞, 82), (23, ∞), (-∞, 18), (-∞, 50)}, {(-∞, 82), (23, ∞), (-∞, 18), (50, ∞)}, {[82, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=51) AND (v1=55 AND v2>=59 AND v3>=49) OR (v1>5 AND v2<34));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 = 51) AND (((comp_index_t2.v1 = 55) AND (comp_index_t2.v2 >= 59)) AND (comp_index_t2.v3 >= 49))) OR ((comp_index_t2.v1 > 5) AND (comp_index_t2.v2 < 34)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(5, ∞), (-∞, 34), (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>4 AND v2<=21 AND v3>=15) OR (v1=93 AND v2>=1 AND v3<>63)) OR (v1 BETWEEN 24 AND 86 AND v3<=5));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 4) AND (comp_index_t2.v2 <= 21)) AND (comp_index_t2.v3 >= 15)) OR (((comp_index_t2.v1 = 93) AND (comp_index_t2.v2 >= 1)) AND (NOT((comp_index_t2.v3 = 63))))) OR ((comp_index_t2.v1 BETWEEN 24 AND 86) AND (comp_index_t2.v3 <= 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(4, 24), (-∞, 21], [15, ∞), (-∞, ∞)}, {[24, 86], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(86, 93), (-∞, 21], [15, ∞), (-∞, ∞)}, {[93, 93], (-∞, 1), [15, ∞), (-∞, ∞)}, {[93, 93], [1, 21], (-∞, ∞), (-∞, ∞)}, {[93, 93], (21, ∞), (-∞, 63), (-∞, ∞)}, {[93, 93], (21, ∞), (63, ∞), (-∞, ∞)}, {(93, ∞), (-∞, 21], [15, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<63 AND v2<>32 AND v3>=14) OR (v1=18 AND v3 BETWEEN 4 AND 42 AND v4>10)) OR (v1<23 AND v2>=21));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 < 63) AND (NOT((comp_index_t2.v2 = 32)))) AND (comp_index_t2.v3 >= 14)) OR (((comp_index_t2.v1 = 18) AND (comp_index_t2.v3 BETWEEN 4 AND 42)) AND (comp_index_t2.v4 > 10))) OR ((comp_index_t2.v1 < 23) AND (comp_index_t2.v2 >= 21)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 18), (-∞, 21), [14, ∞), (-∞, ∞)}, {(-∞, 18), [21, ∞), (-∞, ∞), (-∞, ∞)}, {[18, 18], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(18, 23), (-∞, 21), [14, ∞), (-∞, ∞)}, {(18, 23), [21, ∞), (-∞, ∞), (-∞, ∞)}, {[23, 63), (-∞, 32), [14, ∞), (-∞, ∞)}, {[23, 63), (32, ∞), [14, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>34 AND v3 BETWEEN 27 AND 48 AND v4<=11 AND v2>42) AND (v1<>47 AND v2<48 AND v3<=47 AND v4<>12) OR (v1<=36 AND v2<>17));`,
		ExpectedPlan: "Filter((((((NOT((comp_index_t2.v1 = 34))) AND (comp_index_t2.v3 BETWEEN 27 AND 48)) AND (comp_index_t2.v4 <= 11)) AND (comp_index_t2.v2 > 42)) AND ((((NOT((comp_index_t2.v1 = 47))) AND (comp_index_t2.v2 < 48)) AND (comp_index_t2.v3 <= 47)) AND (NOT((comp_index_t2.v4 = 12))))) OR ((comp_index_t2.v1 <= 36) AND (NOT((comp_index_t2.v2 = 17)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 36], (-∞, 17), (-∞, ∞), (-∞, ∞)}, {(-∞, 36], (17, ∞), (-∞, ∞), (-∞, ∞)}, {(36, 47), (42, 48), [27, 47], (-∞, 11]}, {(47, ∞), (42, 48), [27, 47], (-∞, 11]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=34 AND v2<=80 AND v3<=27) AND (v1 BETWEEN 0 AND 33) OR (v1<=56 AND v2=50 AND v3 BETWEEN 0 AND 5 AND v4<>31));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 34) AND (comp_index_t2.v2 <= 80)) AND (comp_index_t2.v3 <= 27)) AND (comp_index_t2.v1 BETWEEN 0 AND 33)) OR ((((comp_index_t2.v1 <= 56) AND (comp_index_t2.v2 = 50)) AND (comp_index_t2.v3 BETWEEN 0 AND 5)) AND (NOT((comp_index_t2.v4 = 31)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 56], [50, 50], [0, 5], (-∞, 31)}, {(-∞, 56], [50, 50], [0, 5], (31, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=93 AND v2<>5) OR (v1>=81 AND v4=9 AND v2>33 AND v3<99));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 <= 93) AND (NOT((comp_index_t2.v2 = 5)))) OR ((((comp_index_t2.v1 >= 81) AND (comp_index_t2.v4 = 9)) AND (comp_index_t2.v2 > 33)) AND (comp_index_t2.v3 < 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 93], (-∞, 5), (-∞, ∞), (-∞, ∞)}, {(-∞, 93], (5, ∞), (-∞, ∞), (-∞, ∞)}, {(93, ∞), (33, ∞), (-∞, 99), [9, 9]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=37 AND v2=4 AND v3=3) AND (v1=12 AND v2>9 AND v3<89 AND v4<>12) OR (v1=1 AND v2=43 AND v3<=2));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 <= 37) AND (comp_index_t2.v2 = 4)) AND (comp_index_t2.v3 = 3)) AND ((((comp_index_t2.v1 = 12) AND (comp_index_t2.v2 > 9)) AND (comp_index_t2.v3 < 89)) AND (NOT((comp_index_t2.v4 = 12))))) OR (((comp_index_t2.v1 = 1) AND (comp_index_t2.v2 = 43)) AND (comp_index_t2.v3 <= 2)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[1, 1], [43, 43], (-∞, 2], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=82) OR (v1<=4 AND v2>=51)) OR (v1=58 AND v4<86));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 = 82) OR ((comp_index_t2.v1 <= 4) AND (comp_index_t2.v2 >= 51))) OR ((comp_index_t2.v1 = 58) AND (comp_index_t2.v4 < 86)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 4], [51, ∞), (-∞, ∞), (-∞, ∞)}, {[58, 58], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[82, 82], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>=42 AND v4<85 AND v2<8 AND v3<3) OR (v1>=78 AND v2<>28 AND v3<52)) OR (v1<8 AND v2<>76 AND v3 BETWEEN 36 AND 70)) OR (v1=70));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 >= 42) AND (comp_index_t2.v4 < 85)) AND (comp_index_t2.v2 < 8)) AND (comp_index_t2.v3 < 3)) OR (((comp_index_t2.v1 >= 78) AND (NOT((comp_index_t2.v2 = 28)))) AND (comp_index_t2.v3 < 52))) OR (((comp_index_t2.v1 < 8) AND (NOT((comp_index_t2.v2 = 76)))) AND (comp_index_t2.v3 BETWEEN 36 AND 70))) OR (comp_index_t2.v1 = 70))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 8), (-∞, 76), [36, 70], (-∞, ∞)}, {(-∞, 8), (76, ∞), [36, 70], (-∞, ∞)}, {[42, 70), (-∞, 8), (-∞, 3), (-∞, 85)}, {[70, 70], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(70, 78), (-∞, 8), (-∞, 3), (-∞, 85)}, {[78, ∞), (-∞, 28), (-∞, 52), (-∞, ∞)}, {[78, ∞), (28, ∞), (-∞, 52), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>69) OR (v1>=43));`,
		ExpectedPlan: "Filter((NOT((comp_index_t2.v1 = 69))) OR (comp_index_t2.v1 >= 43))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 39 AND 76 AND v4>16 AND v2<>15 AND v3<>35) AND (v1<>50 AND v2>21 AND v3 BETWEEN 27 AND 90 AND v4>18) OR (v1<25 AND v4=58));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 BETWEEN 39 AND 76) AND (comp_index_t2.v4 > 16)) AND (NOT((comp_index_t2.v2 = 15)))) AND (NOT((comp_index_t2.v3 = 35)))) AND ((((NOT((comp_index_t2.v1 = 50))) AND (comp_index_t2.v2 > 21)) AND (comp_index_t2.v3 BETWEEN 27 AND 90)) AND (comp_index_t2.v4 > 18))) OR ((comp_index_t2.v1 < 25) AND (comp_index_t2.v4 = 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 25), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[39, 50), (21, ∞), [27, 35), (18, ∞)}, {[39, 50), (21, ∞), (35, 90], (18, ∞)}, {(50, 76], (21, ∞), [27, 35), (18, ∞)}, {(50, 76], (21, ∞), (35, 90], (18, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=62) OR (v1 BETWEEN 24 AND 36 AND v2>=94 AND v3 BETWEEN 10 AND 55 AND v4>=89));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[24, 36], [94, ∞), [10, 55], [89, ∞)}, {[62, 62], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=31) OR (v1<=95 AND v2<=26 AND v3 BETWEEN 40 AND 72)) OR (v1<51 AND v2=23));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 31), (-∞, 23), [40, 72], (-∞, ∞)}, {(-∞, 31), [23, 23], (-∞, ∞), (-∞, ∞)}, {(-∞, 31), (23, 26], [40, 72], (-∞, ∞)}, {[31, 31], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(31, 51), (-∞, 23), [40, 72], (-∞, ∞)}, {(31, 51), [23, 23], (-∞, ∞), (-∞, ∞)}, {(31, 51), (23, 26], [40, 72], (-∞, ∞)}, {[51, 95], (-∞, 26], [40, 72], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=3) OR (v1>40)) AND (v1>66 AND v2>33);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(66, ∞), (33, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=69 AND v2=61 AND v3=87 AND v4 BETWEEN 63 AND 87) OR (v1 BETWEEN 48 AND 62)) OR (v1<>81 AND v2<=67 AND v3<>43));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 48), (-∞, 67], (-∞, 43), (-∞, ∞)}, {(-∞, 48), (-∞, 67], (43, ∞), (-∞, ∞)}, {[48, 62], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(62, 81), (-∞, 67], (-∞, 43), (-∞, ∞)}, {(62, 81), (-∞, 67], (43, ∞), (-∞, ∞)}, {[81, 81], [61, 61], [87, 87], [63, 87]}, {(81, ∞), (-∞, 67], (-∞, 43), (-∞, ∞)}, {(81, ∞), (-∞, 67], (43, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=19) AND (v1<=20 AND v2>=2) OR (v1 BETWEEN 12 AND 53 AND v4>=1 AND v2<43 AND v3<59));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 = 19) AND ((comp_index_t2.v1 <= 20) AND (comp_index_t2.v2 >= 2))) OR ((((comp_index_t2.v1 BETWEEN 12 AND 53) AND (comp_index_t2.v4 >= 1)) AND (comp_index_t2.v2 < 43)) AND (comp_index_t2.v3 < 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[12, 19), (-∞, 43), (-∞, 59), [1, ∞)}, {[19, 19], (-∞, 2), (-∞, 59), [1, ∞)}, {[19, 19], [2, ∞), (-∞, ∞), (-∞, ∞)}, {(19, 53], (-∞, 43), (-∞, 59), [1, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=42 AND v2<=65) AND (v1<=21) OR (v1<=14 AND v2<>1 AND v3<62));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 42) AND (comp_index_t2.v2 <= 65)) AND (comp_index_t2.v1 <= 21)) OR (((comp_index_t2.v1 <= 14) AND (NOT((comp_index_t2.v2 = 1)))) AND (comp_index_t2.v3 < 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 14], (-∞, 1), (-∞, 62), (-∞, ∞)}, {(-∞, 14], (1, ∞), (-∞, 62), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>5) OR (v1<96 AND v2>=14)) OR (v1<>96)) AND (v1<>51 AND v3>41);`,
		ExpectedPlan: "Filter((((NOT((comp_index_t2.v1 = 5))) OR ((comp_index_t2.v1 < 96) AND (comp_index_t2.v2 >= 14))) OR (NOT((comp_index_t2.v1 = 96)))) AND (comp_index_t2.v3 > 41))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 51), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(51, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>97 AND v3<>77 AND v4=30 AND v2<>45) OR (v1=36 AND v2<77 AND v3>94)) OR (v1=26));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 > 97) AND (NOT((comp_index_t2.v3 = 77)))) AND (comp_index_t2.v4 = 30)) AND (NOT((comp_index_t2.v2 = 45)))) OR (((comp_index_t2.v1 = 36) AND (comp_index_t2.v2 < 77)) AND (comp_index_t2.v3 > 94))) OR (comp_index_t2.v1 = 26))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[26, 26], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[36, 36], (-∞, 77), (94, ∞), (-∞, ∞)}, {(97, ∞), (-∞, 45), (-∞, 77), [30, 30]}, {(97, ∞), (-∞, 45), (77, ∞), [30, 30]}, {(97, ∞), (45, ∞), (-∞, 77), [30, 30]}, {(97, ∞), (45, ∞), (77, ∞), [30, 30]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 34 AND 37 AND v3>23 AND v4>31) OR (v1 BETWEEN 43 AND 81 AND v3>=54 AND v4>=72));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 BETWEEN 34 AND 37) AND (comp_index_t2.v3 > 23)) AND (comp_index_t2.v4 > 31)) OR (((comp_index_t2.v1 BETWEEN 43 AND 81) AND (comp_index_t2.v3 >= 54)) AND (comp_index_t2.v4 >= 72)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[34, 37], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[43, 81], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=17 AND v2<>19) OR (v1>45));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[17, 45], (-∞, 19), (-∞, ∞), (-∞, ∞)}, {[17, 45], (19, ∞), (-∞, ∞), (-∞, ∞)}, {(45, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=57) OR (v1>=1 AND v2<=5 AND v3>=10 AND v4<5)) OR (v1>55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[1, 55], (-∞, 5], [10, ∞), (-∞, 5)}, {(55, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=23 AND v2<=48) OR (v1>41 AND v2>=46 AND v3 BETWEEN 11 AND 29)) AND (v1<>11) OR (v1=70 AND v3<54 AND v4<=47 AND v2<>62));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 23) AND (comp_index_t2.v2 <= 48)) OR (((comp_index_t2.v1 > 41) AND (comp_index_t2.v2 >= 46)) AND (comp_index_t2.v3 BETWEEN 11 AND 29))) AND (NOT((comp_index_t2.v1 = 11)))) OR ((((comp_index_t2.v1 = 70) AND (comp_index_t2.v3 < 54)) AND (comp_index_t2.v4 <= 47)) AND (NOT((comp_index_t2.v2 = 62)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[23, 23], (-∞, 48], (-∞, ∞), (-∞, ∞)}, {(41, ∞), [46, ∞), [11, 29], (-∞, ∞)}, {[70, 70], (-∞, 46), (-∞, 54), (-∞, 47]}, {[70, 70], [46, 62), (-∞, 11), (-∞, 47]}, {[70, 70], [46, 62), (29, 54), (-∞, 47]}, {[70, 70], (62, ∞), (-∞, 11), (-∞, 47]}, {[70, 70], (62, ∞), (29, 54), (-∞, 47]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>73) OR (v1>5 AND v2>=7 AND v3>=43 AND v4<=53)) OR (v1<34 AND v2<95 AND v3 BETWEEN 9 AND 81 AND v4<>8)) AND (v1<=68 AND v4>48 AND v2>11 AND v3 BETWEEN 17 AND 89) OR (v1=41 AND v2 BETWEEN 56 AND 93));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 73) OR ((((comp_index_t2.v1 > 5) AND (comp_index_t2.v2 >= 7)) AND (comp_index_t2.v3 >= 43)) AND (comp_index_t2.v4 <= 53))) OR ((((comp_index_t2.v1 < 34) AND (comp_index_t2.v2 < 95)) AND (comp_index_t2.v3 BETWEEN 9 AND 81)) AND (NOT((comp_index_t2.v4 = 8))))) AND ((((comp_index_t2.v1 <= 68) AND (comp_index_t2.v4 > 48)) AND (comp_index_t2.v2 > 11)) AND (comp_index_t2.v3 BETWEEN 17 AND 89))) OR ((comp_index_t2.v1 = 41) AND (comp_index_t2.v2 BETWEEN 56 AND 93)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 34), (11, 95), [17, 81], (48, ∞)}, {(5, 34), (11, 95), (81, 89], (48, 53]}, {(5, 34), [95, ∞), [43, 89], (48, 53]}, {[34, 41), (11, ∞), [43, 89], (48, 53]}, {[41, 41], (11, 56), [43, 89], (48, 53]}, {[41, 41], [56, 93], (-∞, ∞), (-∞, ∞)}, {[41, 41], (93, ∞), [43, 89], (48, 53]}, {(41, 68], (11, ∞), [43, 89], (48, 53]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>3 AND v3>=34) OR (v1<>31 AND v2<16 AND v3<8));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t2.v1 = 3))) AND (comp_index_t2.v3 >= 34)) OR (((NOT((comp_index_t2.v1 = 31))) AND (comp_index_t2.v2 < 16)) AND (comp_index_t2.v3 < 8)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 3), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[3, 3], (-∞, 16), (-∞, 8), (-∞, ∞)}, {(3, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 88 AND 97) OR (v1>67 AND v4<=27 AND v2<5 AND v3>40)) OR (v1 BETWEEN 5 AND 83 AND v2>=34 AND v3=59));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 BETWEEN 88 AND 97) OR ((((comp_index_t2.v1 > 67) AND (comp_index_t2.v4 <= 27)) AND (comp_index_t2.v2 < 5)) AND (comp_index_t2.v3 > 40))) OR (((comp_index_t2.v1 BETWEEN 5 AND 83) AND (comp_index_t2.v2 >= 34)) AND (comp_index_t2.v3 = 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, 83], [34, ∞), [59, 59], (-∞, ∞)}, {(67, 88), (-∞, 5), (40, ∞), (-∞, 27]}, {[88, 97], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(97, ∞), (-∞, 5), (40, ∞), (-∞, 27]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>96 AND v2<=2 AND v3=17 AND v4<79) OR (v1=67 AND v2=30 AND v3=38 AND v4=53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 96), (-∞, 2], [17, 17], (-∞, 79)}, {[67, 67], [30, 30], [38, 38], [53, 53]}, {(96, ∞), (-∞, 2], [17, 17], (-∞, 79)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>45 AND v2>76) OR (v1=30 AND v2=53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 45), (76, ∞), (-∞, ∞), (-∞, ∞)}, {[30, 30], [53, 53], (-∞, ∞), (-∞, ∞)}, {(45, ∞), (76, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 3 AND 34 AND v2>39) OR (v1>1 AND v2>=92 AND v3=99)) OR (v1>=36 AND v2<>65 AND v3=69));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(1, 3), [92, ∞), [99, 99], (-∞, ∞)}, {[3, 34], (39, ∞), (-∞, ∞), (-∞, ∞)}, {(34, ∞), [92, ∞), [99, 99], (-∞, ∞)}, {[36, ∞), (-∞, 65), [69, 69], (-∞, ∞)}, {[36, ∞), (65, ∞), [69, 69], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=54 AND v2=38 AND v3>=64 AND v4>36) OR (v1<=48)) OR (v1<37 AND v2=13 AND v3<20));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 48], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[54, ∞), [38, 38], [64, ∞), (36, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>70) OR (v1<>2 AND v2>79 AND v3<>6 AND v4<>42));`,
		ExpectedPlan: "Filter((NOT((comp_index_t2.v1 = 70))) OR ((((NOT((comp_index_t2.v1 = 2))) AND (comp_index_t2.v2 > 79)) AND (NOT((comp_index_t2.v3 = 6)))) AND (NOT((comp_index_t2.v4 = 42)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 70), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[70, 70], (79, ∞), (-∞, 6), (-∞, 42)}, {[70, 70], (79, ∞), (-∞, 6), (42, ∞)}, {[70, 70], (79, ∞), (6, ∞), (-∞, 42)}, {[70, 70], (79, ∞), (6, ∞), (42, ∞)}, {(70, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>46 AND v2>93 AND v3>19) AND (v1<51 AND v2=39) OR (v1<61)) AND (v1<>22);`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t2.v1 = 46))) AND (comp_index_t2.v2 > 93)) AND (comp_index_t2.v3 > 19)) AND ((comp_index_t2.v1 < 51) AND (comp_index_t2.v2 = 39))) OR (comp_index_t2.v1 < 61))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 22), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(22, 61), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=53 AND v2>0 AND v3=95 AND v4<=2) OR (v1<41 AND v4<10 AND v2 BETWEEN 11 AND 35)) OR (v1=11 AND v2<20 AND v3=51 AND v4<>30));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 <= 53) AND (comp_index_t2.v2 > 0)) AND (comp_index_t2.v3 = 95)) AND (comp_index_t2.v4 <= 2)) OR (((comp_index_t2.v1 < 41) AND (comp_index_t2.v4 < 10)) AND (comp_index_t2.v2 BETWEEN 11 AND 35))) OR ((((comp_index_t2.v1 = 11) AND (comp_index_t2.v2 < 20)) AND (comp_index_t2.v3 = 51)) AND (NOT((comp_index_t2.v4 = 30)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 41), (0, 11), [95, 95], (-∞, 2]}, {(-∞, 41), [11, 35], (-∞, ∞), (-∞, ∞)}, {(-∞, 41), (35, ∞), [95, 95], (-∞, 2]}, {[11, 11], (-∞, 11), [51, 51], (-∞, 30)}, {[11, 11], (-∞, 11), [51, 51], (30, ∞)}, {[41, 53], (0, ∞), [95, 95], (-∞, 2]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=32 AND v2>6 AND v3=55) OR (v1=87 AND v2<=80)) OR (v1=88 AND v2<=87 AND v3>=45));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 32], (6, ∞), [55, 55], (-∞, ∞)}, {[87, 87], (-∞, 80], (-∞, ∞), (-∞, ∞)}, {[88, 88], (-∞, 87], [45, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>8) OR (v1 BETWEEN 16 AND 25 AND v2<>79 AND v3>=55 AND v4<=5));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(8, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=45 AND v2>55 AND v3<90) OR (v1>26 AND v2>=2 AND v3<>85 AND v4<=74));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(26, 45), [2, ∞), (-∞, 85), (-∞, 74]}, {(26, 45), [2, ∞), (85, ∞), (-∞, 74]}, {[45, 45], [2, 55], (-∞, 85), (-∞, 74]}, {[45, 45], [2, 55], (85, ∞), (-∞, 74]}, {[45, 45], (55, ∞), (-∞, 90), (-∞, ∞)}, {[45, 45], (55, ∞), [90, ∞), (-∞, 74]}, {(45, ∞), [2, ∞), (-∞, 85), (-∞, 74]}, {(45, ∞), [2, ∞), (85, ∞), (-∞, 74]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=59) OR (v1<>85 AND v4<6 AND v2 BETWEEN 14 AND 82));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 = 59) OR (((NOT((comp_index_t2.v1 = 85))) AND (comp_index_t2.v4 < 6)) AND (comp_index_t2.v2 BETWEEN 14 AND 82)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 59), [14, 82], (-∞, ∞), (-∞, ∞)}, {[59, 59], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(59, 85), [14, 82], (-∞, ∞), (-∞, ∞)}, {(85, ∞), [14, 82], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=94 AND v2>32 AND v3>61) OR (v1>51 AND v4>84 AND v2>=46)) OR (v1=39));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 >= 94) AND (comp_index_t2.v2 > 32)) AND (comp_index_t2.v3 > 61)) OR (((comp_index_t2.v1 > 51) AND (comp_index_t2.v4 > 84)) AND (comp_index_t2.v2 >= 46))) OR (comp_index_t2.v1 = 39))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[39, 39], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(51, ∞), [46, ∞), (-∞, ∞), (-∞, ∞)}, {[94, ∞), (32, 46), (61, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=89) OR (v1<=28 AND v2=13));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 28], [13, 13], (-∞, ∞), (-∞, ∞)}, {[89, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=5 AND v2<65 AND v3<64 AND v4=81) OR (v1<=75)) AND (v1=87);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=31 AND v4>30 AND v2<>38) OR (v1<>35)) OR (v1<=8 AND v2<43 AND v3<=50 AND v4<=33));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 <= 31) AND (comp_index_t2.v4 > 30)) AND (NOT((comp_index_t2.v2 = 38)))) OR (NOT((comp_index_t2.v1 = 35)))) OR ((((comp_index_t2.v1 <= 8) AND (comp_index_t2.v2 < 43)) AND (comp_index_t2.v3 <= 50)) AND (comp_index_t2.v4 <= 33)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 35), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(35, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((((v1>65 AND v2=89 AND v3>12) OR (v1 BETWEEN 37 AND 75 AND v2=42 AND v3<=14)) OR (v1>=87 AND v2=85)) OR (v1<>48 AND v4 BETWEEN 32 AND 33 AND v2>21 AND v3<=25)) OR (v1 BETWEEN 51 AND 88 AND v2<>67));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 > 65) AND (comp_index_t2.v2 = 89)) AND (comp_index_t2.v3 > 12)) OR (((comp_index_t2.v1 BETWEEN 37 AND 75) AND (comp_index_t2.v2 = 42)) AND (comp_index_t2.v3 <= 14))) OR ((comp_index_t2.v1 >= 87) AND (comp_index_t2.v2 = 85))) OR ((((NOT((comp_index_t2.v1 = 48))) AND (comp_index_t2.v4 BETWEEN 32 AND 33)) AND (comp_index_t2.v2 > 21)) AND (comp_index_t2.v3 <= 25))) OR ((comp_index_t2.v1 BETWEEN 51 AND 88) AND (NOT((comp_index_t2.v2 = 67)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 37), (21, ∞), (-∞, 25], [32, 33]}, {[37, 48), (21, 42), (-∞, 25], [32, 33]}, {[37, 48), [42, 42], (14, 25], [32, 33]}, {[37, 48), (42, ∞), (-∞, 25], [32, 33]}, {[37, 51), [42, 42], (-∞, 14], (-∞, ∞)}, {(48, 51), (21, 42), (-∞, 25], [32, 33]}, {(48, 51), [42, 42], (14, 25], [32, 33]}, {(48, 51), (42, ∞), (-∞, 25], [32, 33]}, {[51, 88], (-∞, 67), (-∞, ∞), (-∞, ∞)}, {[51, 88], [67, 67], (-∞, 25], [32, 33]}, {[51, 88], (67, ∞), (-∞, ∞), (-∞, ∞)}, {(88, ∞), (21, 85), (-∞, 25], [32, 33]}, {(88, ∞), [85, 85], (-∞, ∞), (-∞, ∞)}, {(88, ∞), (85, 89), (-∞, 25], [32, 33]}, {(88, ∞), [89, 89], (-∞, 12], [32, 33]}, {(88, ∞), [89, 89], (12, ∞), (-∞, ∞)}, {(88, ∞), (89, ∞), (-∞, 25], [32, 33]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>82) OR (v1<1 AND v3>=22)) AND (v1=4) OR (v1>27 AND v2 BETWEEN 7 AND 79 AND v3 BETWEEN 9 AND 29 AND v4<85));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 82) OR ((comp_index_t2.v1 < 1) AND (comp_index_t2.v3 >= 22))) AND (comp_index_t2.v1 = 4)) OR ((((comp_index_t2.v1 > 27) AND (comp_index_t2.v2 BETWEEN 7 AND 79)) AND (comp_index_t2.v3 BETWEEN 9 AND 29)) AND (comp_index_t2.v4 < 85)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(27, ∞), [7, 79], [9, 29], (-∞, 85)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>=41 AND v2<13 AND v3 BETWEEN 62 AND 87) AND (v1<=67 AND v2>68 AND v3=56 AND v4>28);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 23 AND 34 AND v2 BETWEEN 4 AND 75 AND v3<91) OR (v1>=31));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[23, 31), [4, 75], (-∞, 91), (-∞, ∞)}, {[31, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<59) OR (v1 BETWEEN 6 AND 86 AND v4<97)) OR (v1<>90 AND v2=43 AND v3=29));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 < 59) OR ((comp_index_t2.v1 BETWEEN 6 AND 86) AND (comp_index_t2.v4 < 97))) OR (((NOT((comp_index_t2.v1 = 90))) AND (comp_index_t2.v2 = 43)) AND (comp_index_t2.v3 = 29)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 86], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(86, 90), [43, 43], [29, 29], (-∞, ∞)}, {(90, ∞), [43, 43], [29, 29], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=1 AND v2<34) OR (v1<78));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 78), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[78, ∞), (-∞, 34), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=10 AND v2<>64 AND v3>25 AND v4<29) OR (v1>39));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[10, 10], (-∞, 64), (25, ∞), (-∞, 29)}, {[10, 10], (64, ∞), (25, ∞), (-∞, 29)}, {(39, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>35 AND v2>=14 AND v3<65 AND v4<>9) OR (v1<>14 AND v3<51 AND v4<32)) OR (v1>=21 AND v3<>25 AND v4<>16));`,
		ExpectedPlan: "Filter((((((NOT((comp_index_t2.v1 = 35))) AND (comp_index_t2.v2 >= 14)) AND (comp_index_t2.v3 < 65)) AND (NOT((comp_index_t2.v4 = 9)))) OR (((NOT((comp_index_t2.v1 = 14))) AND (comp_index_t2.v3 < 51)) AND (comp_index_t2.v4 < 32))) OR (((comp_index_t2.v1 >= 21) AND (NOT((comp_index_t2.v3 = 25)))) AND (NOT((comp_index_t2.v4 = 16)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 14), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[14, 14], [14, ∞), (-∞, 65), (-∞, 9)}, {[14, 14], [14, ∞), (-∞, 65), (9, ∞)}, {(14, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>12 AND v2<0) OR (v1=36 AND v3<37));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 > 12) AND (comp_index_t2.v2 < 0)) OR ((comp_index_t2.v1 = 36) AND (comp_index_t2.v3 < 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(12, 36), (-∞, 0), (-∞, ∞), (-∞, ∞)}, {[36, 36], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(36, ∞), (-∞, 0), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1=83 AND v3>=72 AND v4<=74) AND (v1>61 AND v2 BETWEEN 32 AND 44);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[83, 83], [32, 44], [72, ∞), (-∞, 74]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1=78 AND v2>28 AND v3<=47) AND (v1<35 AND v2=69 AND v3>16);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 31 AND 49 AND v2=20 AND v3 BETWEEN 8 AND 46) AND (v1<>57 AND v2<5);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=39 AND v2<>3) OR (v1=97 AND v2<>37));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 39], (-∞, 3), (-∞, ∞), (-∞, ∞)}, {(-∞, 39], (3, ∞), (-∞, ∞), (-∞, ∞)}, {[97, 97], (-∞, 37), (-∞, ∞), (-∞, ∞)}, {[97, 97], (37, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=19 AND v4<>62 AND v2<>19 AND v3<>29) OR (v1 BETWEEN 37 AND 75 AND v4<23 AND v2 BETWEEN 6 AND 43));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 >= 19) AND (NOT((comp_index_t2.v4 = 62)))) AND (NOT((comp_index_t2.v2 = 19)))) AND (NOT((comp_index_t2.v3 = 29)))) OR (((comp_index_t2.v1 BETWEEN 37 AND 75) AND (comp_index_t2.v4 < 23)) AND (comp_index_t2.v2 BETWEEN 6 AND 43)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[19, 37), (-∞, 19), (-∞, 29), (-∞, 62)}, {[19, 37), (-∞, 19), (-∞, 29), (62, ∞)}, {[19, 37), (-∞, 19), (29, ∞), (-∞, 62)}, {[19, 37), (-∞, 19), (29, ∞), (62, ∞)}, {[19, 37), (19, ∞), (-∞, 29), (-∞, 62)}, {[19, 37), (19, ∞), (-∞, 29), (62, ∞)}, {[19, 37), (19, ∞), (29, ∞), (-∞, 62)}, {[19, 37), (19, ∞), (29, ∞), (62, ∞)}, {[37, 75], (-∞, 6), (-∞, 29), (-∞, 62)}, {[37, 75], (-∞, 6), (-∞, 29), (62, ∞)}, {[37, 75], (-∞, 6), (29, ∞), (-∞, 62)}, {[37, 75], (-∞, 6), (29, ∞), (62, ∞)}, {[37, 75], [6, 43], (-∞, ∞), (-∞, ∞)}, {[37, 75], (43, ∞), (-∞, 29), (-∞, 62)}, {[37, 75], (43, ∞), (-∞, 29), (62, ∞)}, {[37, 75], (43, ∞), (29, ∞), (-∞, 62)}, {[37, 75], (43, ∞), (29, ∞), (62, ∞)}, {(75, ∞), (-∞, 19), (-∞, 29), (-∞, 62)}, {(75, ∞), (-∞, 19), (-∞, 29), (62, ∞)}, {(75, ∞), (-∞, 19), (29, ∞), (-∞, 62)}, {(75, ∞), (-∞, 19), (29, ∞), (62, ∞)}, {(75, ∞), (19, ∞), (-∞, 29), (-∞, 62)}, {(75, ∞), (19, ∞), (-∞, 29), (62, ∞)}, {(75, ∞), (19, ∞), (29, ∞), (-∞, 62)}, {(75, ∞), (19, ∞), (29, ∞), (62, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<8 AND v2<=33 AND v3 BETWEEN 54 AND 85) OR (v1=46));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 8), (-∞, 33], [54, 85], (-∞, ∞)}, {[46, 46], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=69 AND v2<8) AND (v1>=34 AND v2>=99 AND v3>96 AND v4 BETWEEN 36 AND 99) OR (v1=0 AND v2>=71));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 <= 69) AND (comp_index_t2.v2 < 8)) AND ((((comp_index_t2.v1 >= 34) AND (comp_index_t2.v2 >= 99)) AND (comp_index_t2.v3 > 96)) AND (comp_index_t2.v4 BETWEEN 36 AND 99))) OR ((comp_index_t2.v1 = 0) AND (comp_index_t2.v2 >= 71)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[0, 0], [71, ∞), (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 20 AND 54 AND v2<>31 AND v3 BETWEEN 15 AND 21) OR (v1<=46 AND v3>76)) OR (v1 BETWEEN 31 AND 71));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 20 AND 54) AND (NOT((comp_index_t2.v2 = 31)))) AND (comp_index_t2.v3 BETWEEN 15 AND 21)) OR ((comp_index_t2.v1 <= 46) AND (comp_index_t2.v3 > 76))) OR (comp_index_t2.v1 BETWEEN 31 AND 71))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 71], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>37 AND v2<>5 AND v3=8 AND v4 BETWEEN 26 AND 50) OR (v1>=53)) AND (v1 BETWEEN 5 AND 80);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(37, 53), (-∞, 5), [8, 8], [26, 50]}, {(37, 53), (5, ∞), [8, 8], [26, 50]}, {[53, 80], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=25) OR (v1<=87));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 87], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=99 AND v2>=85) AND (v1<=83 AND v2=99) OR (v1<=6 AND v2 BETWEEN 36 AND 68 AND v3>62 AND v4=79));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 99) AND (comp_index_t2.v2 >= 85)) AND ((comp_index_t2.v1 <= 83) AND (comp_index_t2.v2 = 99))) OR ((((comp_index_t2.v1 <= 6) AND (comp_index_t2.v2 BETWEEN 36 AND 68)) AND (comp_index_t2.v3 > 62)) AND (comp_index_t2.v4 = 79)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 6], [36, 68], (62, ∞), [79, 79]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 30 AND 32 AND v2<68 AND v3<24) AND (v1>=32);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[32, 32], (-∞, 68), (-∞, 24), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>62 AND v2>0) OR (v1<>80 AND v2>55 AND v3=10 AND v4=91));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 62], (55, ∞), [10, 10], [91, 91]}, {(62, ∞), (0, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=7 AND v2 BETWEEN 55 AND 81) OR (v1<>56 AND v2<=76 AND v3<>36)) AND (v1<56 AND v2<>69 AND v3=25);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 7], (69, 81], [25, 25], (-∞, ∞)}, {(-∞, 56), (-∞, 69), [25, 25], (-∞, ∞)}, {(7, 56), (69, 76], [25, 25], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>18) OR (v1>=42 AND v2<=65 AND v3=87 AND v4=80));`,
		ExpectedPlan: "Filter((NOT((comp_index_t2.v1 = 18))) OR ((((comp_index_t2.v1 >= 42) AND (comp_index_t2.v2 <= 65)) AND (comp_index_t2.v3 = 87)) AND (comp_index_t2.v4 = 80)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 18), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(18, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=27) OR (v1<23 AND v2>=41));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 27], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>71 AND v4>0) OR (v1<48 AND v2=89 AND v3>=46 AND v4<=32)) OR (v1<62 AND v2>=33 AND v3>58)) OR (v1>=31 AND v3<>71));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 71) AND (comp_index_t2.v4 > 0)) OR ((((comp_index_t2.v1 < 48) AND (comp_index_t2.v2 = 89)) AND (comp_index_t2.v3 >= 46)) AND (comp_index_t2.v4 <= 32))) OR (((comp_index_t2.v1 < 62) AND (comp_index_t2.v2 >= 33)) AND (comp_index_t2.v3 > 58))) OR ((comp_index_t2.v1 >= 31) AND (NOT((comp_index_t2.v3 = 71)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 31), [33, ∞), (58, ∞), (-∞, ∞)}, {(-∞, 31), [89, 89], [46, 58], (-∞, 32]}, {[31, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 9 AND 40 AND v3<=43 AND v4=62 AND v2>=43) OR (v1=61 AND v2>12 AND v3 BETWEEN 0 AND 13 AND v4>=8));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 9 AND 40) AND (comp_index_t2.v3 <= 43)) AND (comp_index_t2.v4 = 62)) AND (comp_index_t2.v2 >= 43)) OR ((((comp_index_t2.v1 = 61) AND (comp_index_t2.v2 > 12)) AND (comp_index_t2.v3 BETWEEN 0 AND 13)) AND (comp_index_t2.v4 >= 8)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[9, 40], [43, ∞), (-∞, 43], [62, 62]}, {[61, 61], (12, ∞), [0, 13], [8, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<58) OR (v1 BETWEEN 17 AND 20 AND v2<>99 AND v3<=76)) OR (v1 BETWEEN 48 AND 87)) OR (v1<39 AND v2 BETWEEN 48 AND 94 AND v3<>0));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 87], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=33) OR (v1 BETWEEN 7 AND 41 AND v2<82 AND v3<53 AND v4<>3));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[7, 33), (-∞, 82), (-∞, 53), (-∞, 3)}, {[7, 33), (-∞, 82), (-∞, 53), (3, ∞)}, {[33, 33], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(33, 41], (-∞, 82), (-∞, 53), (-∞, 3)}, {(33, 41], (-∞, 82), (-∞, 53), (3, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=9 AND v4=22 AND v2>=95) OR (v1>96));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 <= 9) AND (comp_index_t2.v4 = 22)) AND (comp_index_t2.v2 >= 95)) OR (comp_index_t2.v1 > 96))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 9], [95, ∞), (-∞, ∞), (-∞, ∞)}, {(96, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=56) OR (v1>=31 AND v4<38 AND v2>20)) OR (v1=91 AND v2<48));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 <= 56) OR (((comp_index_t2.v1 >= 31) AND (comp_index_t2.v4 < 38)) AND (comp_index_t2.v2 > 20))) OR ((comp_index_t2.v1 = 91) AND (comp_index_t2.v2 < 48)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 56], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(56, 91), (20, ∞), (-∞, ∞), (-∞, ∞)}, {[91, 91], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(91, ∞), (20, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=75 AND v4<=30) OR (v1>=41 AND v2 BETWEEN 16 AND 25 AND v3>=99));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 <= 75) AND (comp_index_t2.v4 <= 30)) OR (((comp_index_t2.v1 >= 41) AND (comp_index_t2.v2 BETWEEN 16 AND 25)) AND (comp_index_t2.v3 >= 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 75], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(75, ∞), [16, 25], [99, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 2 AND 64) OR (v1>=23));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[2, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=26 AND v2<1 AND v3=82 AND v4<=42) OR (v1 BETWEEN 42 AND 73));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 26], (-∞, 1), [82, 82], (-∞, 42]}, {[42, 73], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>=23 AND v2<=10) AND (v1>=75 AND v4 BETWEEN 24 AND 68) AND (v1>44 AND v2>8 AND v3<=16);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[75, ∞), (8, 10], (-∞, 16], [24, 68]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((((v1>6 AND v2>61 AND v3=0 AND v4>=76) OR (v1<23)) OR (v1<>46 AND v2=29 AND v3>4)) OR (v1>=59)) OR (v1=87 AND v2<=98 AND v3>=47));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 23), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[23, 46), [29, 29], (4, ∞), (-∞, ∞)}, {[23, 59), (61, ∞), [0, 0], [76, ∞)}, {(46, 59), [29, 29], (4, ∞), (-∞, ∞)}, {[59, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=59 AND v2 BETWEEN 15 AND 53 AND v3<>17 AND v4>=10) OR (v1 BETWEEN 37 AND 95 AND v2<=32 AND v3>=81));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[37, 95], (-∞, 32], [81, ∞), (-∞, ∞)}, {[59, 59], [15, 32], (17, 81), [10, ∞)}, {[59, 59], [15, 53], (-∞, 17), [10, ∞)}, {[59, 59], (32, 53], (17, ∞), [10, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 6 AND 92 AND v2=75 AND v3>79) OR (v1>=10)) OR (v1<=35 AND v2<=42)) AND (v1<>65);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 10), (-∞, 42], (-∞, ∞), (-∞, ∞)}, {[6, 10), [75, 75], (79, ∞), (-∞, ∞)}, {[10, 65), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(65, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>84 AND v4<=53 AND v2=77 AND v3>=40) OR (v1>78 AND v2<>1 AND v3=98 AND v4>=76));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 84) AND (comp_index_t2.v4 <= 53)) AND (comp_index_t2.v2 = 77)) AND (comp_index_t2.v3 >= 40)) OR ((((comp_index_t2.v1 > 78) AND (NOT((comp_index_t2.v2 = 1)))) AND (comp_index_t2.v3 = 98)) AND (comp_index_t2.v4 >= 76)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(78, ∞), (-∞, 1), [98, 98], [76, ∞)}, {(78, ∞), (1, ∞), [98, 98], [76, ∞)}, {(84, ∞), [77, 77], [40, ∞), (-∞, 53]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>79 AND v2<=85) OR (v1<>13)) OR (v1 BETWEEN 4 AND 67));`,
		ExpectedPlan: "Filter((((NOT((comp_index_t2.v1 = 79))) AND (comp_index_t2.v2 <= 85)) OR (NOT((comp_index_t2.v1 = 13)))) OR (comp_index_t2.v1 BETWEEN 4 AND 67))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>34) OR (v1<35 AND v2>=93)) OR (v1>8));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 8], [93, ∞), (-∞, ∞), (-∞, ∞)}, {(8, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((((v1<65) OR (v1<>44)) OR (v1<=39 AND v3>=14)) OR (v1<=33 AND v2<>11)) OR (v1=75 AND v2=0 AND v3<28));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 < 65) OR (NOT((comp_index_t2.v1 = 44)))) OR ((comp_index_t2.v1 <= 39) AND (comp_index_t2.v3 >= 14))) OR ((comp_index_t2.v1 <= 33) AND (NOT((comp_index_t2.v2 = 11))))) OR (((comp_index_t2.v1 = 75) AND (comp_index_t2.v2 = 0)) AND (comp_index_t2.v3 < 28)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>50 AND v2>=46) AND (v1<>17 AND v2=45 AND v3<=79) OR (v1=10 AND v2>=35)) AND (v1=44 AND v2=38);`,
		ExpectedPlan: "Filter((((NOT((comp_index_t2.v1 = 50))) AND (comp_index_t2.v2 >= 46)) AND (((NOT((comp_index_t2.v1 = 17))) AND (comp_index_t2.v2 = 45)) AND (comp_index_t2.v3 <= 79))) OR ((comp_index_t2.v1 = 10) AND (comp_index_t2.v2 >= 35)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<34) OR (v1<=62 AND v4<>18 AND v2 BETWEEN 1 AND 41)) OR (v1>=65 AND v2>=93 AND v3 BETWEEN 34 AND 41));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 < 34) OR (((comp_index_t2.v1 <= 62) AND (NOT((comp_index_t2.v4 = 18)))) AND (comp_index_t2.v2 BETWEEN 1 AND 41))) OR (((comp_index_t2.v1 >= 65) AND (comp_index_t2.v2 >= 93)) AND (comp_index_t2.v3 BETWEEN 34 AND 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 34), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[34, 62], [1, 41], (-∞, ∞), (-∞, ∞)}, {[65, ∞), [93, ∞), [34, 41], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>8) OR (v1>20 AND v4>=99));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 > 8) OR ((comp_index_t2.v1 > 20) AND (comp_index_t2.v4 >= 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(8, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>57) OR (v1<87 AND v2<>91 AND v3 BETWEEN 47 AND 98));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 57], (-∞, 91), [47, 98], (-∞, ∞)}, {(-∞, 57], (91, ∞), [47, 98], (-∞, ∞)}, {(57, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=57) OR (v1=88 AND v2 BETWEEN 72 AND 93));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[57, 57], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[88, 88], [72, 93], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>10 AND v2=20 AND v3<=21 AND v4<>88) OR (v1<28 AND v2 BETWEEN 38 AND 59 AND v3<>98 AND v4>=26));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 10), [20, 20], (-∞, 21], (-∞, 88)}, {(-∞, 10), [20, 20], (-∞, 21], (88, ∞)}, {(-∞, 28), [38, 59], (-∞, 98), [26, ∞)}, {(-∞, 28), [38, 59], (98, ∞), [26, ∞)}, {(10, ∞), [20, 20], (-∞, 21], (-∞, 88)}, {(10, ∞), [20, 20], (-∞, 21], (88, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>5 AND v3<>53 AND v4>=49) OR (v1<18 AND v2<94));`,
		ExpectedPlan: "Filter((((NOT((comp_index_t2.v1 = 5))) AND (NOT((comp_index_t2.v3 = 53)))) AND (comp_index_t2.v4 >= 49)) OR ((comp_index_t2.v1 < 18) AND (comp_index_t2.v2 < 94)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 5), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[5, 5], (-∞, 94), (-∞, ∞), (-∞, ∞)}, {(5, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<52 AND v2 BETWEEN 33 AND 75 AND v3=32) OR (v1<=98 AND v2<=41 AND v3<>87 AND v4<>83));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 52), (-∞, 33), (-∞, 87), (-∞, 83)}, {(-∞, 52), (-∞, 33), (-∞, 87), (83, ∞)}, {(-∞, 52), [33, 41], (-∞, 32), (-∞, 83)}, {(-∞, 52), [33, 41], (-∞, 32), (83, ∞)}, {(-∞, 52), [33, 41], (32, 87), (-∞, 83)}, {(-∞, 52), [33, 41], (32, 87), (83, ∞)}, {(-∞, 52), [33, 75], [32, 32], (-∞, ∞)}, {(-∞, 98], (-∞, 41], (87, ∞), (-∞, 83)}, {(-∞, 98], (-∞, 41], (87, ∞), (83, ∞)}, {[52, 98], (-∞, 41], (-∞, 87), (-∞, 83)}, {[52, 98], (-∞, 41], (-∞, 87), (83, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>28 AND v4>57 AND v2<62 AND v3 BETWEEN 14 AND 41) AND (v1<>72 AND v2>=13 AND v3>29 AND v4>38) OR (v1<=22 AND v2>58));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 > 28) AND (comp_index_t2.v4 > 57)) AND (comp_index_t2.v2 < 62)) AND (comp_index_t2.v3 BETWEEN 14 AND 41)) AND ((((NOT((comp_index_t2.v1 = 72))) AND (comp_index_t2.v2 >= 13)) AND (comp_index_t2.v3 > 29)) AND (comp_index_t2.v4 > 38))) OR ((comp_index_t2.v1 <= 22) AND (comp_index_t2.v2 > 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 22], (58, ∞), (-∞, ∞), (-∞, ∞)}, {(28, 72), [13, 62), (29, 41], (57, ∞)}, {(72, ∞), [13, 62), (29, 41], (57, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=13 AND v2<=52 AND v3=28 AND v4>88) OR (v1<>5 AND v2<=42));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 5), (-∞, 42], (-∞, ∞), (-∞, ∞)}, {(-∞, 5), (42, 52], [28, 28], (88, ∞)}, {[5, 5], (-∞, 52], [28, 28], (88, ∞)}, {(5, 13], (42, 52], [28, 28], (88, ∞)}, {(5, ∞), (-∞, 42], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>35 AND v4<>20 AND v2<81 AND v3=27) OR (v1>13 AND v3=27));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 35) AND (NOT((comp_index_t2.v4 = 20)))) AND (comp_index_t2.v2 < 81)) AND (comp_index_t2.v3 = 27)) OR ((comp_index_t2.v1 > 13) AND (comp_index_t2.v3 = 27)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(13, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=26) OR (v1<59 AND v2 BETWEEN 2 AND 30 AND v3>=69));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 26), [2, 30], [69, ∞), (-∞, ∞)}, {[26, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<11) OR (v1<>9 AND v2 BETWEEN 51 AND 62 AND v3=98));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 11), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[11, ∞), [51, 62], [98, 98], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=92 AND v2>25) OR (v1=91 AND v2=21 AND v3<=18 AND v4<>15)) OR (v1=79 AND v2>67 AND v3<>48 AND v4<42));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[79, 79], (67, ∞), (-∞, 48), (-∞, 42)}, {[79, 79], (67, ∞), (48, ∞), (-∞, 42)}, {[91, 91], [21, 21], (-∞, 18], (-∞, 15)}, {[91, 91], [21, 21], (-∞, 18], (15, ∞)}, {[92, 92], (25, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=86 AND v2<5 AND v3<36 AND v4<81) OR (v1>=52 AND v2>24 AND v3<5)) OR (v1 BETWEEN 5 AND 80 AND v3<>80));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 = 86) AND (comp_index_t2.v2 < 5)) AND (comp_index_t2.v3 < 36)) AND (comp_index_t2.v4 < 81)) OR (((comp_index_t2.v1 >= 52) AND (comp_index_t2.v2 > 24)) AND (comp_index_t2.v3 < 5))) OR ((comp_index_t2.v1 BETWEEN 5 AND 80) AND (NOT((comp_index_t2.v3 = 80)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, 80], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(80, ∞), (24, ∞), (-∞, 5), (-∞, ∞)}, {[86, 86], (-∞, 5), (-∞, 36), (-∞, 81)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>67) OR (v1>69 AND v2>11 AND v3=13 AND v4=20));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(67, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>31) OR (v1 BETWEEN 27 AND 87 AND v2=71 AND v3=38 AND v4=1));`,
		ExpectedPlan: "Filter((NOT((comp_index_t2.v1 = 31))) OR ((((comp_index_t2.v1 BETWEEN 27 AND 87) AND (comp_index_t2.v2 = 71)) AND (comp_index_t2.v3 = 38)) AND (comp_index_t2.v4 = 1)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 31), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[31, 31], [71, 71], [38, 38], [1, 1]}, {(31, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>2 AND v4=0 AND v2 BETWEEN 6 AND 23 AND v3 BETWEEN 46 AND 52) OR (v1<=63 AND v2>=71 AND v3=28)) AND (v1<=52);`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 2) AND (comp_index_t2.v4 = 0)) AND (comp_index_t2.v2 BETWEEN 6 AND 23)) AND (comp_index_t2.v3 BETWEEN 46 AND 52)) OR (((comp_index_t2.v1 <= 63) AND (comp_index_t2.v2 >= 71)) AND (comp_index_t2.v3 = 28)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 52], [71, ∞), [28, 28], (-∞, ∞)}, {(2, 52], [6, 23], [46, 52], [0, 0]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 10 AND 90) AND (v1=86 AND v4>=4) AND (v1 BETWEEN 6 AND 58 AND v2=85);`,
		ExpectedPlan: "Filter(comp_index_t2.v4 >= 4)\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=46 AND v4>41 AND v2<>12) OR (v1>17 AND v2>=34 AND v3<>68 AND v4<=13)) OR (v1>=98 AND v4 BETWEEN 3 AND 62 AND v2=39));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 46) AND (comp_index_t2.v4 > 41)) AND (NOT((comp_index_t2.v2 = 12)))) OR ((((comp_index_t2.v1 > 17) AND (comp_index_t2.v2 >= 34)) AND (NOT((comp_index_t2.v3 = 68)))) AND (comp_index_t2.v4 <= 13))) OR (((comp_index_t2.v1 >= 98) AND (comp_index_t2.v4 BETWEEN 3 AND 62)) AND (comp_index_t2.v2 = 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(17, 46), [34, ∞), (-∞, 68), (-∞, 13]}, {(17, 46), [34, ∞), (68, ∞), (-∞, 13]}, {[46, 46], (-∞, 12), (-∞, ∞), (-∞, ∞)}, {[46, 46], (12, ∞), (-∞, ∞), (-∞, ∞)}, {(46, 98), [34, ∞), (-∞, 68), (-∞, 13]}, {(46, 98), [34, ∞), (68, ∞), (-∞, 13]}, {[98, ∞), [34, 39), (-∞, 68), (-∞, 13]}, {[98, ∞), [34, 39), (68, ∞), (-∞, 13]}, {[98, ∞), [39, 39], (-∞, ∞), (-∞, ∞)}, {[98, ∞), (39, ∞), (-∞, 68), (-∞, 13]}, {[98, ∞), (39, ∞), (68, ∞), (-∞, 13]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=12 AND v2<>4 AND v3 BETWEEN 18 AND 42) OR (v1>=73)) OR (v1<60 AND v2=93 AND v3>=79));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 12], (-∞, 4), [18, 42], (-∞, ∞)}, {(-∞, 12], (4, ∞), [18, 42], (-∞, ∞)}, {(-∞, 60), [93, 93], [79, ∞), (-∞, ∞)}, {[73, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=55 AND v2>50) OR (v1<>51 AND v2>=37));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 51), [37, ∞), (-∞, ∞), (-∞, ∞)}, {(51, ∞), [37, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 66 AND 76 AND v2>=84 AND v3>1 AND v4 BETWEEN 71 AND 95) AND (v1>36 AND v2<>41) OR (v1<44 AND v2<=50 AND v3=36 AND v4<=42));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 BETWEEN 66 AND 76) AND (comp_index_t2.v2 >= 84)) AND (comp_index_t2.v3 > 1)) AND (comp_index_t2.v4 BETWEEN 71 AND 95)) AND ((comp_index_t2.v1 > 36) AND (NOT((comp_index_t2.v2 = 41))))) OR ((((comp_index_t2.v1 < 44) AND (comp_index_t2.v2 <= 50)) AND (comp_index_t2.v3 = 36)) AND (comp_index_t2.v4 <= 42)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 44), (-∞, 50], [36, 36], (-∞, 42]}, {[66, 76], [84, ∞), (1, ∞), [71, 95]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=21 AND v2=44 AND v3>=68) OR (v1>=38 AND v2>=15));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 21], [44, 44], [68, ∞), (-∞, ∞)}, {[38, ∞), [15, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1<>37 AND v2>67 AND v3>52) AND (v1<48 AND v2<>73 AND v3=25 AND v4=22);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 57 AND 62 AND v2>=99) OR (v1>31));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(31, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>22 AND v3<>49) OR (v1>=41 AND v2<=74 AND v3<=46));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t2.v1 = 22))) AND (NOT((comp_index_t2.v3 = 49)))) OR (((comp_index_t2.v1 >= 41) AND (comp_index_t2.v2 <= 74)) AND (comp_index_t2.v3 <= 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 22), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(22, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=82 AND v4<=67 AND v2=40) OR (v1>63)) OR (v1<=16));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 >= 82) AND (comp_index_t2.v4 <= 67)) AND (comp_index_t2.v2 = 40)) OR (comp_index_t2.v1 > 63)) OR (comp_index_t2.v1 <= 16))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 16], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(63, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<=39 AND v2<>82 AND v3>=33 AND v4>=84) OR (v1=57 AND v2<25 AND v3<>55 AND v4<=82)) OR (v1>10 AND v2>28 AND v3>=65)) OR (v1<=13 AND v2=66));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 10], (-∞, 66), [33, ∞), [84, ∞)}, {(-∞, 10], (66, 82), [33, ∞), [84, ∞)}, {(-∞, 10], (82, ∞), [33, ∞), [84, ∞)}, {(-∞, 13], [66, 66], (-∞, ∞), (-∞, ∞)}, {(10, 13], (28, 66), [33, 65), [84, ∞)}, {(10, 13], (28, 66), [65, ∞), (-∞, ∞)}, {(10, 13], (66, 82), [33, 65), [84, ∞)}, {(10, 13], (66, ∞), [65, ∞), (-∞, ∞)}, {(10, 39], (-∞, 28], [33, ∞), [84, ∞)}, {(10, 39], (82, ∞), [33, 65), [84, ∞)}, {(13, 39], (28, 82), [33, 65), [84, ∞)}, {(13, ∞), (28, ∞), [65, ∞), (-∞, ∞)}, {[57, 57], (-∞, 25), (-∞, 55), (-∞, 82]}, {[57, 57], (-∞, 25), (55, ∞), (-∞, 82]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=60 AND v2<=25 AND v3<>9) OR (v1 BETWEEN 19 AND 92 AND v2>=33 AND v3<=40 AND v4=53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 60], (-∞, 25], (-∞, 9), (-∞, ∞)}, {(-∞, 60], (-∞, 25], (9, ∞), (-∞, ∞)}, {[19, 92], [33, ∞), (-∞, 40], [53, 53]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=21 AND v2<=27 AND v3>=86 AND v4>99) OR (v1<76 AND v2<>97));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 76), (-∞, 97), (-∞, ∞), (-∞, ∞)}, {(-∞, 76), (97, ∞), (-∞, ∞), (-∞, ∞)}, {[76, ∞), (-∞, 27], [86, ∞), (99, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 4 AND 8 AND v3>=12) OR (v1>=12 AND v2>=0 AND v3=18));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 BETWEEN 4 AND 8) AND (comp_index_t2.v3 >= 12)) OR (((comp_index_t2.v1 >= 12) AND (comp_index_t2.v2 >= 0)) AND (comp_index_t2.v3 = 18)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[4, 8], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[12, ∞), [0, ∞), [18, 18], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>65 AND v2<=52 AND v3>37) OR (v1>11)) OR (v1<=54 AND v2 BETWEEN 30 AND 85 AND v3 BETWEEN 14 AND 27 AND v4>=35)) OR (v1>44 AND v2<>76 AND v3>=52));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 11], [30, 85], [14, 27], [35, ∞)}, {(11, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=54) OR (v1<17 AND v2=34 AND v3>=59));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 17), [34, 34], [59, ∞), (-∞, ∞)}, {[54, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>9 AND v4<>61 AND v2=98 AND v3<1) OR (v1<2 AND v2 BETWEEN 3 AND 70));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t2.v1 = 9))) AND (NOT((comp_index_t2.v4 = 61)))) AND (comp_index_t2.v2 = 98)) AND (comp_index_t2.v3 < 1)) OR ((comp_index_t2.v1 < 2) AND (comp_index_t2.v2 BETWEEN 3 AND 70)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 2), [3, 70], (-∞, ∞), (-∞, ∞)}, {(-∞, 9), [98, 98], (-∞, 1), (-∞, 61)}, {(-∞, 9), [98, 98], (-∞, 1), (61, ∞)}, {(9, ∞), [98, 98], (-∞, 1), (-∞, 61)}, {(9, ∞), [98, 98], (-∞, 1), (61, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=6 AND v2>93) OR (v1 BETWEEN 38 AND 46));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 6], (93, ∞), (-∞, ∞), (-∞, ∞)}, {[38, 46], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1 BETWEEN 16 AND 72) OR (v1=20)) OR (v1>61 AND v2<>48 AND v3<>83 AND v4=46)) OR (v1=5 AND v2=59));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, 5], [59, 59], (-∞, ∞), (-∞, ∞)}, {[16, 72], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(72, ∞), (-∞, 48), (-∞, 83), [46, 46]}, {(72, ∞), (-∞, 48), (83, ∞), [46, 46]}, {(72, ∞), (48, ∞), (-∞, 83), [46, 46]}, {(72, ∞), (48, ∞), (83, ∞), [46, 46]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>41 AND v2>74 AND v3>37 AND v4<38) OR (v1=58 AND v2>=1)) OR (v1<=4 AND v2>0 AND v3 BETWEEN 39 AND 72 AND v4>=29));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 4], (0, ∞), [39, 72], [29, ∞)}, {(41, 58), (74, ∞), (37, ∞), (-∞, 38)}, {[58, 58], [1, ∞), (-∞, ∞), (-∞, ∞)}, {(58, ∞), (74, ∞), (37, ∞), (-∞, 38)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>53 AND v4<99 AND v2<>31) OR (v1<>5 AND v2>70 AND v3>=71));`,
		ExpectedPlan: "Filter((((NOT((comp_index_t2.v1 = 53))) AND (comp_index_t2.v4 < 99)) AND (NOT((comp_index_t2.v2 = 31)))) OR (((NOT((comp_index_t2.v1 = 5))) AND (comp_index_t2.v2 > 70)) AND (comp_index_t2.v3 >= 71)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 53), (-∞, 31), (-∞, ∞), (-∞, ∞)}, {(-∞, 53), (31, ∞), (-∞, ∞), (-∞, ∞)}, {[53, 53], (70, ∞), [71, ∞), (-∞, ∞)}, {(53, ∞), (-∞, 31), (-∞, ∞), (-∞, ∞)}, {(53, ∞), (31, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>1 AND v4=93) OR (v1<10 AND v2 BETWEEN 40 AND 74 AND v3>=27));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 > 1) AND (comp_index_t2.v4 = 93)) OR (((comp_index_t2.v1 < 10) AND (comp_index_t2.v2 BETWEEN 40 AND 74)) AND (comp_index_t2.v3 >= 27)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 1], [40, 74], [27, ∞), (-∞, ∞)}, {(1, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=92 AND v2>=64 AND v3=39 AND v4 BETWEEN 16 AND 53) OR (v1<54 AND v2 BETWEEN 8 AND 17 AND v3=21 AND v4=86));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 54), [8, 17], [21, 21], [86, 86]}, {[92, ∞), [64, ∞), [39, 39], [16, 53]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 16 AND 31 AND v4 BETWEEN 18 AND 96) OR (v1=40 AND v2<=35 AND v3>=51 AND v4>=83));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 BETWEEN 16 AND 31) AND (comp_index_t2.v4 BETWEEN 18 AND 96)) OR ((((comp_index_t2.v1 = 40) AND (comp_index_t2.v2 <= 35)) AND (comp_index_t2.v3 >= 51)) AND (comp_index_t2.v4 >= 83)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[16, 31], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[40, 40], (-∞, 35], [51, ∞), [83, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 68 AND 78 AND v2>96 AND v3<58 AND v4<14) OR (v1=71)) AND (v1>15 AND v2>=19) OR (v1>36));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 BETWEEN 68 AND 78) AND (comp_index_t2.v2 > 96)) AND (comp_index_t2.v3 < 58)) AND (comp_index_t2.v4 < 14)) OR (comp_index_t2.v1 = 71)) AND ((comp_index_t2.v1 > 15) AND (comp_index_t2.v2 >= 19))) OR (comp_index_t2.v1 > 36))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(36, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 33 AND 71 AND v2<=61 AND v3<=32 AND v4 BETWEEN 18 AND 73) AND (v1<3) AND (v1<=59 AND v2=47 AND v3<49 AND v4>36);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<77 AND v2=43 AND v3<92 AND v4=13) OR (v1=38 AND v2<=46)) OR (v1 BETWEEN 10 AND 79 AND v2>=11 AND v3 BETWEEN 14 AND 14));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 10), [43, 43], (-∞, 92), [13, 13]}, {[10, 38), [11, ∞), [14, 14], (-∞, ∞)}, {[10, 38), [43, 43], (-∞, 14), [13, 13]}, {[10, 38), [43, 43], (14, 92), [13, 13]}, {[38, 38], (-∞, 46], (-∞, ∞), (-∞, ∞)}, {[38, 38], (46, ∞), [14, 14], (-∞, ∞)}, {(38, 77), [43, 43], (-∞, 14), [13, 13]}, {(38, 77), [43, 43], (14, 92), [13, 13]}, {(38, 79], [11, ∞), [14, 14], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=47 AND v4=13) AND (v1<=27 AND v3<54 AND v4 BETWEEN 27 AND 40) OR (v1>=40 AND v4=98 AND v2=25 AND v3>66));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 >= 47) AND (comp_index_t2.v4 = 13)) AND (((comp_index_t2.v1 <= 27) AND (comp_index_t2.v3 < 54)) AND (comp_index_t2.v4 BETWEEN 27 AND 40))) OR ((((comp_index_t2.v1 >= 40) AND (comp_index_t2.v4 = 98)) AND (comp_index_t2.v2 = 25)) AND (comp_index_t2.v3 > 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[40, ∞), [25, 25], (66, ∞), [98, 98]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<98 AND v3 BETWEEN 80 AND 82) OR (v1 BETWEEN 31 AND 38 AND v2=39));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 < 98) AND (comp_index_t2.v3 BETWEEN 80 AND 82)) OR ((comp_index_t2.v1 BETWEEN 31 AND 38) AND (comp_index_t2.v2 = 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 98), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=75 AND v2 BETWEEN 45 AND 51 AND v3<15) OR (v1>=74 AND v2>=37 AND v3<76));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[74, ∞), [37, ∞), (-∞, 76), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=40) OR (v1<>32 AND v4<=37));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 >= 40) OR ((NOT((comp_index_t2.v1 = 32))) AND (comp_index_t2.v4 <= 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 32), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(32, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>63 AND v3 BETWEEN 43 AND 50 AND v4<29 AND v2>=89) OR (v1>80));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 63) AND (comp_index_t2.v3 BETWEEN 43 AND 50)) AND (comp_index_t2.v4 < 29)) AND (comp_index_t2.v2 >= 89)) OR (comp_index_t2.v1 > 80))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(63, 80], [89, ∞), [43, 50], (-∞, 29)}, {(80, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=81) OR (v1>=27 AND v2>=21 AND v3 BETWEEN 1 AND 63 AND v4>=92));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[27, 81), [21, ∞), [1, 63], [92, ∞)}, {[81, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>13) OR (v1>72 AND v2=2 AND v3<=40)) OR (v1>77 AND v2<21));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(13, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>54 AND v2>23 AND v3 BETWEEN 28 AND 48 AND v4>=37) OR (v1>93 AND v2>=51 AND v3<9 AND v4<>49)) OR (v1>=71 AND v2<>33));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 54), (23, ∞), [28, 48], [37, ∞)}, {(54, 71), (23, ∞), [28, 48], [37, ∞)}, {[71, ∞), (-∞, 33), (-∞, ∞), (-∞, ∞)}, {[71, ∞), [33, 33], [28, 48], [37, ∞)}, {[71, ∞), (33, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 15 AND 69 AND v4=83 AND v2<=43) OR (v1<51 AND v2<24 AND v3<>27 AND v4<>50)) OR (v1<>37));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 15 AND 69) AND (comp_index_t2.v4 = 83)) AND (comp_index_t2.v2 <= 43)) OR ((((comp_index_t2.v1 < 51) AND (comp_index_t2.v2 < 24)) AND (NOT((comp_index_t2.v3 = 27)))) AND (NOT((comp_index_t2.v4 = 50))))) OR (NOT((comp_index_t2.v1 = 37))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 37), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[37, 37], (-∞, 43], (-∞, ∞), (-∞, ∞)}, {(37, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 55 AND 66 AND v2<>81 AND v3=6 AND v4<=19) OR (v1<>91));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 55 AND 66) AND (NOT((comp_index_t2.v2 = 81)))) AND (comp_index_t2.v3 = 6)) AND (comp_index_t2.v4 <= 19)) OR (NOT((comp_index_t2.v1 = 91))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 91), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(91, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=21 AND v2<50 AND v3>=39) OR (v1<=79 AND v4>62 AND v2=31));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 21) AND (comp_index_t2.v2 < 50)) AND (comp_index_t2.v3 >= 39)) OR (((comp_index_t2.v1 <= 79) AND (comp_index_t2.v4 > 62)) AND (comp_index_t2.v2 = 31)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 79], [31, 31], (-∞, ∞), (-∞, ∞)}, {[21, 21], (-∞, 31), [39, ∞), (-∞, ∞)}, {[21, 21], (31, 50), [39, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>78) OR (v1>=9 AND v2<>84));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[9, 78], (-∞, 84), (-∞, ∞), (-∞, ∞)}, {[9, 78], (84, ∞), (-∞, ∞), (-∞, ∞)}, {(78, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>16 AND v3>=29) OR (v1>=47 AND v2<>63));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 > 16) AND (comp_index_t2.v3 >= 29)) OR ((comp_index_t2.v1 >= 47) AND (NOT((comp_index_t2.v2 = 63)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(16, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=16 AND v2>=9 AND v3<>48) OR (v1>=76 AND v2<>86)) OR (v1<28 AND v2=1 AND v3<=23 AND v4 BETWEEN 13 AND 55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 28), [1, 1], (-∞, 23], [13, 55]}, {[16, 16], [9, ∞), (-∞, 48), (-∞, ∞)}, {[16, 16], [9, ∞), (48, ∞), (-∞, ∞)}, {[76, ∞), (-∞, 86), (-∞, ∞), (-∞, ∞)}, {[76, ∞), (86, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=35 AND v2>67) OR (v1<>55));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 = 35) AND (comp_index_t2.v2 > 67)) OR (NOT((comp_index_t2.v1 = 55))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 55), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(55, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<89 AND v2<5 AND v3 BETWEEN 53 AND 61) OR (v1<>72 AND v3<20));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 < 89) AND (comp_index_t2.v2 < 5)) AND (comp_index_t2.v3 BETWEEN 53 AND 61)) OR ((NOT((comp_index_t2.v1 = 72))) AND (comp_index_t2.v3 < 20)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 72), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[72, 72], (-∞, 5), [53, 61], (-∞, ∞)}, {(72, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=58 AND v2<=89 AND v3=78 AND v4<=58) OR (v1>39)) AND (v1<>25 AND v2>1 AND v3<18);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(39, ∞), (1, ∞), (-∞, 18), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>94) OR (v1=33 AND v2 BETWEEN 53 AND 60 AND v3 BETWEEN 37 AND 73));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[33, 33], [53, 60], [37, 73], (-∞, ∞)}, {(94, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=40 AND v2<>8 AND v3<=69) OR (v1<=72)) OR (v1 BETWEEN 87 AND 89 AND v2 BETWEEN 52 AND 58));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 72], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[87, 89], [52, 58], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<9 AND v2=97 AND v3<>54 AND v4>71) OR (v1>48 AND v2 BETWEEN 7 AND 23 AND v3<>95 AND v4>86)) OR (v1 BETWEEN 36 AND 90));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 9), [97, 97], (-∞, 54), (71, ∞)}, {(-∞, 9), [97, 97], (54, ∞), (71, ∞)}, {[36, 90], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(90, ∞), [7, 23], (-∞, 95), (86, ∞)}, {(90, ∞), [7, 23], (95, ∞), (86, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=38 AND v2<70) OR (v1>79));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[38, 79], (-∞, 70), (-∞, ∞), (-∞, ∞)}, {(79, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<47 AND v2 BETWEEN 22 AND 85) AND (v1=73) OR (v1<42));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 < 47) AND (comp_index_t2.v2 BETWEEN 22 AND 85)) AND (comp_index_t2.v1 = 73)) OR (comp_index_t2.v1 < 42))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 42), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<29) AND (v1<41 AND v2>52 AND v3<>55) OR (v1 BETWEEN 16 AND 28 AND v2>=9 AND v3=43 AND v4<6));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 < 29) AND (((comp_index_t2.v1 < 41) AND (comp_index_t2.v2 > 52)) AND (NOT((comp_index_t2.v3 = 55))))) OR ((((comp_index_t2.v1 BETWEEN 16 AND 28) AND (comp_index_t2.v2 >= 9)) AND (comp_index_t2.v3 = 43)) AND (comp_index_t2.v4 < 6)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 29), (52, ∞), (-∞, 55), (-∞, ∞)}, {(-∞, 29), (52, ∞), (55, ∞), (-∞, ∞)}, {[16, 28], [9, 52], [43, 43], (-∞, 6)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<56 AND v2<=52) OR (v1>=30 AND v2<73 AND v3>40 AND v4>=13)) AND (v1<30 AND v4<>25 AND v2<>82 AND v3 BETWEEN 80 AND 88);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 30), (-∞, 52], [80, 88], (-∞, 25)}, {(-∞, 30), (-∞, 52], [80, 88], (25, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 9 AND 53 AND v2 BETWEEN 26 AND 56) OR (v1 BETWEEN 29 AND 72 AND v2<18 AND v3=73 AND v4<=12));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[9, 53], [26, 56], (-∞, ∞), (-∞, ∞)}, {[29, 72], (-∞, 18), [73, 73], (-∞, 12]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>96 AND v2<27) OR (v1<82)) AND (v1>=80 AND v2 BETWEEN 14 AND 53);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[80, 82), [14, 53], (-∞, ∞), (-∞, ∞)}, {(96, ∞), [14, 27), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>86) OR (v1>=48 AND v4>9));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 > 86) OR ((comp_index_t2.v1 >= 48) AND (comp_index_t2.v4 > 9)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[48, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=41 AND v2=79 AND v3<16 AND v4>=2) OR (v1<16 AND v4>59));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 41) AND (comp_index_t2.v2 = 79)) AND (comp_index_t2.v3 < 16)) AND (comp_index_t2.v4 >= 2)) OR ((comp_index_t2.v1 < 16) AND (comp_index_t2.v4 > 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 16), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[41, 41], [79, 79], (-∞, 16), [2, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>=69 AND v2 BETWEEN 38 AND 45) AND (v1<>35 AND v2<28 AND v3>14);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=93 AND v2<=10 AND v3 BETWEEN 21 AND 83) AND (v1<>5 AND v2>59 AND v3<>17) OR (v1<69 AND v3<>65 AND v4>=51 AND v2<=48)) OR (v1 BETWEEN 37 AND 57 AND v2 BETWEEN 44 AND 57 AND v3<40 AND v4=98));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 >= 93) AND (comp_index_t2.v2 <= 10)) AND (comp_index_t2.v3 BETWEEN 21 AND 83)) AND (((NOT((comp_index_t2.v1 = 5))) AND (comp_index_t2.v2 > 59)) AND (NOT((comp_index_t2.v3 = 17))))) OR ((((comp_index_t2.v1 < 69) AND (NOT((comp_index_t2.v3 = 65)))) AND (comp_index_t2.v4 >= 51)) AND (comp_index_t2.v2 <= 48))) OR ((((comp_index_t2.v1 BETWEEN 37 AND 57) AND (comp_index_t2.v2 BETWEEN 44 AND 57)) AND (comp_index_t2.v3 < 40)) AND (comp_index_t2.v4 = 98)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 69), (-∞, 48], (-∞, 65), [51, ∞)}, {(-∞, 69), (-∞, 48], (65, ∞), [51, ∞)}, {[37, 57], (48, 57], (-∞, 40), [98, 98]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<46) OR (v1<>60));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 < 46) OR (NOT((comp_index_t2.v1 = 60))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 60), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(60, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<97 AND v2<=47 AND v3=91) OR (v1=74 AND v4>72 AND v2<>44 AND v3 BETWEEN 4 AND 51));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 < 97) AND (comp_index_t2.v2 <= 47)) AND (comp_index_t2.v3 = 91)) OR ((((comp_index_t2.v1 = 74) AND (comp_index_t2.v4 > 72)) AND (NOT((comp_index_t2.v2 = 44)))) AND (comp_index_t2.v3 BETWEEN 4 AND 51)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 97), (-∞, 47], [91, 91], (-∞, ∞)}, {[74, 74], (-∞, 44), [4, 51], (72, ∞)}, {[74, 74], (44, ∞), [4, 51], (72, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 26 AND 60 AND v2>53 AND v3<=9 AND v4<8) OR (v1>0 AND v2<=69));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(0, ∞), (-∞, 69], (-∞, ∞), (-∞, ∞)}, {[26, 60], (69, ∞), (-∞, 9], (-∞, 8)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=33 AND v2<2 AND v3<>63) OR (v1 BETWEEN 20 AND 95 AND v2<>7 AND v3 BETWEEN 95 AND 96 AND v4 BETWEEN 34 AND 41)) OR (v1 BETWEEN 27 AND 44 AND v4<>28 AND v2<=43 AND v3<=64));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 33) AND (comp_index_t2.v2 < 2)) AND (NOT((comp_index_t2.v3 = 63)))) OR ((((comp_index_t2.v1 BETWEEN 20 AND 95) AND (NOT((comp_index_t2.v2 = 7)))) AND (comp_index_t2.v3 BETWEEN 95 AND 96)) AND (comp_index_t2.v4 BETWEEN 34 AND 41))) OR ((((comp_index_t2.v1 BETWEEN 27 AND 44) AND (NOT((comp_index_t2.v4 = 28)))) AND (comp_index_t2.v2 <= 43)) AND (comp_index_t2.v3 <= 64)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[20, 33), (-∞, 7), [95, 96], [34, 41]}, {[20, 95], (7, ∞), [95, 96], [34, 41]}, {[27, 33), (-∞, 43], (-∞, 64], (-∞, 28)}, {[27, 33), (-∞, 43], (-∞, 64], (28, ∞)}, {[33, 33], (-∞, 2), (-∞, 63), (-∞, ∞)}, {[33, 33], (-∞, 2), [63, 63], (-∞, 28)}, {[33, 33], (-∞, 2), [63, 63], (28, ∞)}, {[33, 33], (-∞, 2), (63, ∞), (-∞, ∞)}, {[33, 33], [2, 7), [95, 96], [34, 41]}, {[33, 33], [2, 43], (-∞, 64], (-∞, 28)}, {[33, 33], [2, 43], (-∞, 64], (28, ∞)}, {(33, 44], (-∞, 43], (-∞, 64], (-∞, 28)}, {(33, 44], (-∞, 43], (-∞, 64], (28, ∞)}, {(33, 95], (-∞, 7), [95, 96], [34, 41]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1 BETWEEN 13 AND 36 AND v2>40) OR (v1<>28 AND v2<29)) OR (v1 BETWEEN 36 AND 89 AND v2>=92 AND v3>39 AND v4<16)) OR (v1<=1));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 1], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(1, 28), (-∞, 29), (-∞, ∞), (-∞, ∞)}, {[13, 36], (40, ∞), (-∞, ∞), (-∞, ∞)}, {(28, ∞), (-∞, 29), (-∞, ∞), (-∞, ∞)}, {(36, 89], [92, ∞), (39, ∞), (-∞, 16)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=68 AND v2=49) OR (v1<=35 AND v2>=59 AND v3>=88 AND v4 BETWEEN 1 AND 62));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 35], [59, ∞), [88, ∞), [1, 62]}, {[68, 68], [49, 49], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>33) OR (v1<23 AND v4<=23 AND v2>=41));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 > 33) OR (((comp_index_t2.v1 < 23) AND (comp_index_t2.v4 <= 23)) AND (comp_index_t2.v2 >= 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 23), [41, ∞), (-∞, ∞), (-∞, ∞)}, {(33, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=78 AND v2=26 AND v3 BETWEEN 70 AND 89) OR (v1 BETWEEN 12 AND 78 AND v2>41 AND v3 BETWEEN 2 AND 11 AND v4 BETWEEN 12 AND 97)) OR (v1>16 AND v2=85 AND v3<56 AND v4<19));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[12, 16], (41, ∞), [2, 11], [12, 97]}, {(16, 78], (41, 85), [2, 11], [12, 97]}, {(16, 78], [85, 85], (-∞, 2), (-∞, 19)}, {(16, 78], [85, 85], [2, 11], (-∞, 97]}, {(16, 78], [85, 85], (11, 56), (-∞, 19)}, {(16, 78], (85, ∞), [2, 11], [12, 97]}, {[78, ∞), [26, 26], [70, 89], (-∞, ∞)}, {(78, ∞), [85, 85], (-∞, 56), (-∞, 19)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=51 AND v2=3 AND v3>48 AND v4>=49) OR (v1>25 AND v3=37));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 >= 51) AND (comp_index_t2.v2 = 3)) AND (comp_index_t2.v3 > 48)) AND (comp_index_t2.v4 >= 49)) OR ((comp_index_t2.v1 > 25) AND (comp_index_t2.v3 = 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(25, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<4 AND v2<>1 AND v3<=34) OR (v1>=63)) OR (v1<58 AND v2=33)) AND (v1<=55) OR (v1 BETWEEN 1 AND 80 AND v2<=51));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 < 4) AND (NOT((comp_index_t2.v2 = 1)))) AND (comp_index_t2.v3 <= 34)) OR (comp_index_t2.v1 >= 63)) OR ((comp_index_t2.v1 < 58) AND (comp_index_t2.v2 = 33))) AND (comp_index_t2.v1 <= 55)) OR ((comp_index_t2.v1 BETWEEN 1 AND 80) AND (comp_index_t2.v2 <= 51)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 1), (-∞, 1), (-∞, 34], (-∞, ∞)}, {(-∞, 1), (1, 33), (-∞, 34], (-∞, ∞)}, {(-∞, 1), [33, 33], (-∞, ∞), (-∞, ∞)}, {(-∞, 1), (33, ∞), (-∞, 34], (-∞, ∞)}, {[1, 4), (51, ∞), (-∞, 34], (-∞, ∞)}, {[1, 80], (-∞, 51], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 33 AND 82 AND v2<26) OR (v1>=98 AND v4>30 AND v2 BETWEEN 47 AND 67 AND v3 BETWEEN 9 AND 54)) OR (v1>=5)) AND (v1<>85 AND v4<>31);`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 33 AND 82) AND (comp_index_t2.v2 < 26)) OR ((((comp_index_t2.v1 >= 98) AND (comp_index_t2.v4 > 30)) AND (comp_index_t2.v2 BETWEEN 47 AND 67)) AND (comp_index_t2.v3 BETWEEN 9 AND 54))) OR (comp_index_t2.v1 >= 5)) AND (NOT((comp_index_t2.v4 = 31))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, 85), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(85, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=82 AND v3<>55 AND v4>26) OR (v1=35)) OR (v1 BETWEEN 18 AND 70 AND v2>=17));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 = 82) AND (NOT((comp_index_t2.v3 = 55)))) AND (comp_index_t2.v4 > 26)) OR (comp_index_t2.v1 = 35)) OR ((comp_index_t2.v1 BETWEEN 18 AND 70) AND (comp_index_t2.v2 >= 17)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[18, 35), [17, ∞), (-∞, ∞), (-∞, ∞)}, {[35, 35], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(35, 70], [17, ∞), (-∞, ∞), (-∞, ∞)}, {[82, 82], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>45 AND v2<=55 AND v3>=2 AND v4<46) OR (v1>=0 AND v2<>6));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 0), (-∞, 55], [2, ∞), (-∞, 46)}, {[0, 45), [6, 6], [2, ∞), (-∞, 46)}, {[0, ∞), (-∞, 6), (-∞, ∞), (-∞, ∞)}, {[0, ∞), (6, ∞), (-∞, ∞), (-∞, ∞)}, {(45, ∞), [6, 6], [2, ∞), (-∞, 46)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=85 AND v2>=46 AND v3=87 AND v4>3) OR (v1=52));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 52), [46, ∞), [87, 87], (3, ∞)}, {[52, 52], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(52, 85], [46, ∞), [87, 87], (3, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<90 AND v4=77) OR (v1<>32 AND v2<=17 AND v3=68)) OR (v1<41));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 < 90) AND (comp_index_t2.v4 = 77)) OR (((NOT((comp_index_t2.v1 = 32))) AND (comp_index_t2.v2 <= 17)) AND (comp_index_t2.v3 = 68))) OR (comp_index_t2.v1 < 41))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 90), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[90, ∞), (-∞, 17], [68, 68], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=2) AND (v1>=13 AND v2<=23 AND v3<=23) OR (v1 BETWEEN 18 AND 57));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 = 2) AND (((comp_index_t2.v1 >= 13) AND (comp_index_t2.v2 <= 23)) AND (comp_index_t2.v3 <= 23))) OR (comp_index_t2.v1 BETWEEN 18 AND 57))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[18, 57], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 32 AND 72 AND v2<>89 AND v3>=39) OR (v1>50 AND v4>80));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 BETWEEN 32 AND 72) AND (NOT((comp_index_t2.v2 = 89)))) AND (comp_index_t2.v3 >= 39)) OR ((comp_index_t2.v1 > 50) AND (comp_index_t2.v4 > 80)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[32, 50], (-∞, 89), [39, ∞), (-∞, ∞)}, {[32, 50], (89, ∞), [39, ∞), (-∞, ∞)}, {(50, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<44) OR (v1<>37 AND v2<=12 AND v3>65 AND v4<47)) OR (v1<>76));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 < 44) OR ((((NOT((comp_index_t2.v1 = 37))) AND (comp_index_t2.v2 <= 12)) AND (comp_index_t2.v3 > 65)) AND (comp_index_t2.v4 < 47))) OR (NOT((comp_index_t2.v1 = 76))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 76), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[76, 76], (-∞, 12], (65, ∞), (-∞, 47)}, {(76, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 29 AND 37) OR (v1<>54 AND v2<=65 AND v3<=1 AND v4<>10)) OR (v1<>55 AND v2 BETWEEN 49 AND 56 AND v3>=25 AND v4<=8));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 29), (-∞, 65], (-∞, 1], (-∞, 10)}, {(-∞, 29), (-∞, 65], (-∞, 1], (10, ∞)}, {(-∞, 29), [49, 56], [25, ∞), (-∞, 8]}, {[29, 37], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(37, 54), (-∞, 65], (-∞, 1], (-∞, 10)}, {(37, 54), (-∞, 65], (-∞, 1], (10, ∞)}, {(37, 55), [49, 56], [25, ∞), (-∞, 8]}, {(54, ∞), (-∞, 65], (-∞, 1], (-∞, 10)}, {(54, ∞), (-∞, 65], (-∞, 1], (10, ∞)}, {(55, ∞), [49, 56], [25, ∞), (-∞, 8]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=80 AND v2<95 AND v3>6) OR (v1 BETWEEN 7 AND 14 AND v2 BETWEEN 27 AND 49 AND v3>57 AND v4 BETWEEN 28 AND 60));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[7, 14], [27, 49], (57, ∞), [28, 60]}, {[80, 80], (-∞, 95), (6, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>10 AND v2<43 AND v3<>15) OR (v1<=71 AND v4<>22));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 10) AND (comp_index_t2.v2 < 43)) AND (NOT((comp_index_t2.v3 = 15)))) OR ((comp_index_t2.v1 <= 71) AND (NOT((comp_index_t2.v4 = 22)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 71], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(71, ∞), (-∞, 43), (-∞, 15), (-∞, ∞)}, {(71, ∞), (-∞, 43), (15, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 18 AND 36 AND v4<>87 AND v2>=13) OR (v1>=63 AND v3<=89)) AND (v1<76 AND v4<49 AND v2<=96);`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 18 AND 36) AND (NOT((comp_index_t2.v4 = 87)))) AND (comp_index_t2.v2 >= 13)) OR ((comp_index_t2.v1 >= 63) AND (comp_index_t2.v3 <= 89))) AND (comp_index_t2.v4 < 49))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[18, 36], [13, 96], (-∞, ∞), (-∞, ∞)}, {[63, 76), (-∞, 96], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<93 AND v2<>16) OR (v1>=23 AND v4>=19)) OR (v1<48 AND v2<=45 AND v3<>46 AND v4>76)) AND (v1=22 AND v3=41) OR (v1<=17 AND v2>=41));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 < 93) AND (NOT((comp_index_t2.v2 = 16)))) OR ((comp_index_t2.v1 >= 23) AND (comp_index_t2.v4 >= 19))) OR ((((comp_index_t2.v1 < 48) AND (comp_index_t2.v2 <= 45)) AND (NOT((comp_index_t2.v3 = 46)))) AND (comp_index_t2.v4 > 76))) AND ((comp_index_t2.v1 = 22) AND (comp_index_t2.v3 = 41))) OR ((comp_index_t2.v1 <= 17) AND (comp_index_t2.v2 >= 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 17], [41, ∞), (-∞, ∞), (-∞, ∞)}, {[22, 22], (-∞, 16), (-∞, ∞), (-∞, ∞)}, {[22, 22], [16, 16], (-∞, 46), (76, ∞)}, {[22, 22], [16, 16], (46, ∞), (76, ∞)}, {[22, 22], (16, ∞), (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>17 AND v4>50 AND v2 BETWEEN 11 AND 23 AND v3=23) OR (v1<73));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 17) AND (comp_index_t2.v4 > 50)) AND (comp_index_t2.v2 BETWEEN 11 AND 23)) AND (comp_index_t2.v3 = 23)) OR (comp_index_t2.v1 < 73))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 73), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[73, ∞), [11, 23], [23, 23], (50, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 5 AND 41 AND v3<78 AND v4<41) OR (v1>84 AND v2<>43));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 BETWEEN 5 AND 41) AND (comp_index_t2.v3 < 78)) AND (comp_index_t2.v4 < 41)) OR ((comp_index_t2.v1 > 84) AND (NOT((comp_index_t2.v2 = 43)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, 41], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(84, ∞), (-∞, 43), (-∞, ∞), (-∞, ∞)}, {(84, ∞), (43, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=24 AND v2 BETWEEN 43 AND 84) OR (v1>=90 AND v2>1 AND v3<>70)) OR (v1>=66 AND v2<95));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[24, 24], [43, 84], (-∞, ∞), (-∞, ∞)}, {[66, ∞), (-∞, 95), (-∞, ∞), (-∞, ∞)}, {[90, ∞), [95, ∞), (-∞, 70), (-∞, ∞)}, {[90, ∞), [95, ∞), (70, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<=18 AND v2<=70) OR (v1>55 AND v2>52 AND v3<>70)) OR (v1=58)) AND (v1<>22 AND v4>76) OR (v1>14 AND v2<32 AND v3>97));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 <= 18) AND (comp_index_t2.v2 <= 70)) OR (((comp_index_t2.v1 > 55) AND (comp_index_t2.v2 > 52)) AND (NOT((comp_index_t2.v3 = 70))))) OR (comp_index_t2.v1 = 58)) AND ((NOT((comp_index_t2.v1 = 22))) AND (comp_index_t2.v4 > 76))) OR (((comp_index_t2.v1 > 14) AND (comp_index_t2.v2 < 32)) AND (comp_index_t2.v3 > 97)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 18], (-∞, 70], (-∞, ∞), (-∞, ∞)}, {(18, 58), (-∞, 32), (97, ∞), (-∞, ∞)}, {(55, 58), (52, ∞), (-∞, 70), (-∞, ∞)}, {(55, 58), (52, ∞), (70, ∞), (-∞, ∞)}, {[58, 58], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(58, ∞), (-∞, 32), (97, ∞), (-∞, ∞)}, {(58, ∞), (52, ∞), (-∞, 70), (-∞, ∞)}, {(58, ∞), (52, ∞), (70, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>=9 AND v2>69) AND (v1 BETWEEN 39 AND 73);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[39, 73], (69, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<87 AND v2 BETWEEN 2 AND 34 AND v3=87 AND v4>=76) OR (v1<>77 AND v2<=44 AND v3>34));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 77), (-∞, 44], (34, ∞), (-∞, ∞)}, {[77, 77], [2, 34], [87, 87], [76, ∞)}, {(77, ∞), (-∞, 44], (34, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=9 AND v4>=68 AND v2>21) OR (v1=5 AND v2<69 AND v3<=15 AND v4>=61));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 9) AND (comp_index_t2.v4 >= 68)) AND (comp_index_t2.v2 > 21)) OR ((((comp_index_t2.v1 = 5) AND (comp_index_t2.v2 < 69)) AND (comp_index_t2.v3 <= 15)) AND (comp_index_t2.v4 >= 61)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, 5], (-∞, 69), (-∞, 15], [61, ∞)}, {[9, 9], (21, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=22) OR (v1>55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[22, 22], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(55, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 47 AND 57 AND v2>=83) OR (v1=91 AND v2>34));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[47, 57], [83, ∞), (-∞, ∞), (-∞, ∞)}, {[91, 91], (34, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 23 AND 25) AND (v1<98 AND v2>=20 AND v3>37);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[23, 25], [20, ∞), (37, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=6) OR (v1>61 AND v2<=34)) OR (v1>10 AND v2<>50 AND v3<>62 AND v4<=84));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[6, 6], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(10, 61], (-∞, 50), (-∞, 62), (-∞, 84]}, {(10, 61], (-∞, 50), (62, ∞), (-∞, 84]}, {(10, ∞), (50, ∞), (-∞, 62), (-∞, 84]}, {(10, ∞), (50, ∞), (62, ∞), (-∞, 84]}, {(61, ∞), (-∞, 34], (-∞, ∞), (-∞, ∞)}, {(61, ∞), (34, 50), (-∞, 62), (-∞, 84]}, {(61, ∞), (34, 50), (62, ∞), (-∞, 84]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>74) OR (v1<>86 AND v2<=91)) AND (v1>=8);`,
		ExpectedPlan: "Filter((NOT((comp_index_t2.v1 = 74))) OR ((NOT((comp_index_t2.v1 = 86))) AND (comp_index_t2.v2 <= 91)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[8, 74), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[74, 74], (-∞, 91], (-∞, ∞), (-∞, ∞)}, {(74, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>25 AND v2 BETWEEN 23 AND 54) OR (v1<>40 AND v3>90)) OR (v1<>7 AND v4<=78));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 25) AND (comp_index_t2.v2 BETWEEN 23 AND 54)) OR ((NOT((comp_index_t2.v1 = 40))) AND (comp_index_t2.v3 > 90))) OR ((NOT((comp_index_t2.v1 = 7))) AND (comp_index_t2.v4 <= 78)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=25) OR (v1>40 AND v2 BETWEEN 26 AND 40 AND v3<76));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[25, 25], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(40, ∞), [26, 40], (-∞, 76), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=13 AND v2<85) OR (v1=23 AND v2<>68 AND v3=33));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[13, 13], (-∞, 85), (-∞, ∞), (-∞, ∞)}, {[23, 23], (-∞, 68), [33, 33], (-∞, ∞)}, {[23, 23], (68, ∞), [33, 33], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<42 AND v2>95 AND v3>17 AND v4<>97) OR (v1>=13 AND v2<>10 AND v3 BETWEEN 73 AND 85 AND v4=48)) OR (v1>55 AND v2=85 AND v3>30));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 42), (95, ∞), (17, ∞), (-∞, 97)}, {(-∞, 42), (95, ∞), (17, ∞), (97, ∞)}, {[13, 42), (10, 95], [73, 85], [48, 48]}, {[13, ∞), (-∞, 10), [73, 85], [48, 48]}, {[42, 55], (10, ∞), [73, 85], [48, 48]}, {(55, ∞), (10, 85), [73, 85], [48, 48]}, {(55, ∞), [85, 85], (30, ∞), (-∞, ∞)}, {(55, ∞), (85, ∞), [73, 85], [48, 48]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 5 AND 32) OR (v1>7)) OR (v1=34));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[5, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>=34 AND v2<>61 AND v3<>3) AND (v1 BETWEEN 69 AND 93) AND (v1=36 AND v2>14);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>75) OR (v1<>74 AND v3 BETWEEN 29 AND 73));`,
		ExpectedPlan: "Filter((NOT((comp_index_t2.v1 = 75))) OR ((NOT((comp_index_t2.v1 = 74))) AND (comp_index_t2.v3 BETWEEN 29 AND 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1<>91 AND v3=27 AND v4=22 AND v2<>68) AND (v1<=88);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 88], (-∞, 68), [27, 27], [22, 22]}, {(-∞, 88], (68, ∞), [27, 27], [22, 22]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<>18 AND v2<>90 AND v3>95) OR (v1>=44)) OR (v1<4 AND v3<=26 AND v4<>67 AND v2>=37)) OR (v1<36 AND v2<=15 AND v3 BETWEEN 25 AND 36 AND v4<=14));`,
		ExpectedPlan: "Filter((((((NOT((comp_index_t2.v1 = 18))) AND (NOT((comp_index_t2.v2 = 90)))) AND (comp_index_t2.v3 > 95)) OR (comp_index_t2.v1 >= 44)) OR ((((comp_index_t2.v1 < 4) AND (comp_index_t2.v3 <= 26)) AND (NOT((comp_index_t2.v4 = 67)))) AND (comp_index_t2.v2 >= 37))) OR ((((comp_index_t2.v1 < 36) AND (comp_index_t2.v2 <= 15)) AND (comp_index_t2.v3 BETWEEN 25 AND 36)) AND (comp_index_t2.v4 <= 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 4), [37, ∞), (-∞, 26], (-∞, 67)}, {(-∞, 4), [37, ∞), (-∞, 26], (67, ∞)}, {(-∞, 18), (-∞, 90), (95, ∞), (-∞, ∞)}, {(-∞, 18), (90, ∞), (95, ∞), (-∞, ∞)}, {(-∞, 36), (-∞, 15], [25, 36], (-∞, 14]}, {(18, 44), (-∞, 90), (95, ∞), (-∞, ∞)}, {(18, 44), (90, ∞), (95, ∞), (-∞, ∞)}, {[44, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 44 AND 87 AND v2<52 AND v3<52 AND v4<1) OR (v1<30 AND v4 BETWEEN 8 AND 97 AND v2<=24));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 44 AND 87) AND (comp_index_t2.v2 < 52)) AND (comp_index_t2.v3 < 52)) AND (comp_index_t2.v4 < 1)) OR (((comp_index_t2.v1 < 30) AND (comp_index_t2.v4 BETWEEN 8 AND 97)) AND (comp_index_t2.v2 <= 24)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 30), (-∞, 24], (-∞, ∞), (-∞, ∞)}, {[44, 87], (-∞, 52), (-∞, 52), (-∞, 1)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>48 AND v2<=83) OR (v1>28 AND v2 BETWEEN 9 AND 87 AND v3<>73)) OR (v1>=53 AND v2>=91 AND v3 BETWEEN 33 AND 97));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(28, 48], [9, 87], (-∞, 73), (-∞, ∞)}, {(28, 48], [9, 87], (73, ∞), (-∞, ∞)}, {(48, ∞), (-∞, 83], (-∞, ∞), (-∞, ∞)}, {(48, ∞), (83, 87], (-∞, 73), (-∞, ∞)}, {(48, ∞), (83, 87], (73, ∞), (-∞, ∞)}, {[53, ∞), [91, ∞), [33, 97], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>8 AND v2 BETWEEN 34 AND 48) OR (v1<>54));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t2.v1 = 8))) AND (comp_index_t2.v2 BETWEEN 34 AND 48)) OR (NOT((comp_index_t2.v1 = 54))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 54), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[54, 54], [34, 48], (-∞, ∞), (-∞, ∞)}, {(54, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=78 AND v2<74 AND v3<42 AND v4>=34) OR (v1<=29 AND v2<=27 AND v3>31 AND v4 BETWEEN 35 AND 41));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 29], (-∞, 27], [42, ∞), [35, 41]}, {(-∞, 78], (-∞, 74), (-∞, 42), [34, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 9 AND 35 AND v4<=69 AND v2 BETWEEN 34 AND 53 AND v3<>28) AND (v1 BETWEEN 12 AND 48);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[12, 35], [34, 53], (28, ∞), (-∞, 69]}, {[12, 35], [34, 53], (-∞, 28), (-∞, 69]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 13 AND 77 AND v2>75 AND v3<73 AND v4>=6) AND (v1<=58 AND v2=48 AND v3 BETWEEN 33 AND 73);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>47 AND v3>47 AND v4 BETWEEN 51 AND 86 AND v2=26) OR (v1<82 AND v2<=17 AND v3<17 AND v4>=46));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 47) AND (comp_index_t2.v3 > 47)) AND (comp_index_t2.v4 BETWEEN 51 AND 86)) AND (comp_index_t2.v2 = 26)) OR ((((comp_index_t2.v1 < 82) AND (comp_index_t2.v2 <= 17)) AND (comp_index_t2.v3 < 17)) AND (comp_index_t2.v4 >= 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 82), (-∞, 17], (-∞, 17), [46, ∞)}, {(47, ∞), [26, 26], (47, ∞), [51, 86]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>87) OR (v1>82 AND v4>=22)) OR (v1>=52 AND v2<>47 AND v3=37)) OR (v1<=14 AND v2<57 AND v3<10));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 87) OR ((comp_index_t2.v1 > 82) AND (comp_index_t2.v4 >= 22))) OR (((comp_index_t2.v1 >= 52) AND (NOT((comp_index_t2.v2 = 47)))) AND (comp_index_t2.v3 = 37))) OR (((comp_index_t2.v1 <= 14) AND (comp_index_t2.v2 < 57)) AND (comp_index_t2.v3 < 10)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 14], (-∞, 57), (-∞, 10), (-∞, ∞)}, {[52, 82], (-∞, 47), [37, 37], (-∞, ∞)}, {[52, 82], (47, ∞), [37, 37], (-∞, ∞)}, {(82, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>=99 AND v3<=41) AND (v1<>38 AND v2<94 AND v3 BETWEEN 83 AND 95 AND v4>=86);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>78) AND (v1>32 AND v2>11 AND v3>=78);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(78, ∞), (11, ∞), [78, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1<>3 AND v2=26 AND v3=22 AND v4<=76) AND (v1 BETWEEN 59 AND 92 AND v2 BETWEEN 36 AND 80);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>10) OR (v1=12));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(10, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>=12 AND v3>=45 AND v4<98) OR (v1<>51 AND v3=79 AND v4<=24)) OR (v1 BETWEEN 4 AND 59 AND v4<82)) OR (v1>=29 AND v2<>21));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 >= 12) AND (comp_index_t2.v3 >= 45)) AND (comp_index_t2.v4 < 98)) OR (((NOT((comp_index_t2.v1 = 51))) AND (comp_index_t2.v3 = 79)) AND (comp_index_t2.v4 <= 24))) OR ((comp_index_t2.v1 BETWEEN 4 AND 59) AND (comp_index_t2.v4 < 82))) OR ((comp_index_t2.v1 >= 29) AND (NOT((comp_index_t2.v2 = 21)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>10 AND v2<=75 AND v3>=70) OR (v1<89 AND v2<=32));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 10), (32, 75], [70, ∞), (-∞, ∞)}, {(-∞, 89), (-∞, 32], (-∞, ∞), (-∞, ∞)}, {(10, 89), (32, 75], [70, ∞), (-∞, ∞)}, {[89, ∞), (-∞, 75], [70, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=15) OR (v1=15)) OR (v1 BETWEEN 14 AND 25 AND v4>55 AND v2<53 AND v3=95));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 >= 15) OR (comp_index_t2.v1 = 15)) OR ((((comp_index_t2.v1 BETWEEN 14 AND 25) AND (comp_index_t2.v4 > 55)) AND (comp_index_t2.v2 < 53)) AND (comp_index_t2.v3 = 95)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[14, 15), (-∞, 53), [95, 95], (55, ∞)}, {[15, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>48 AND v2 BETWEEN 4 AND 84 AND v3<=3 AND v4<>31) AND (v1 BETWEEN 2 AND 15 AND v3>75);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<41 AND v4=9 AND v2>77 AND v3=41) OR (v1>62 AND v2>=48 AND v3=13 AND v4>61)) OR (v1 BETWEEN 33 AND 75)) OR (v1 BETWEEN 45 AND 65 AND v4 BETWEEN 4 AND 68));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 < 41) AND (comp_index_t2.v4 = 9)) AND (comp_index_t2.v2 > 77)) AND (comp_index_t2.v3 = 41)) OR ((((comp_index_t2.v1 > 62) AND (comp_index_t2.v2 >= 48)) AND (comp_index_t2.v3 = 13)) AND (comp_index_t2.v4 > 61))) OR (comp_index_t2.v1 BETWEEN 33 AND 75)) OR ((comp_index_t2.v1 BETWEEN 45 AND 65) AND (comp_index_t2.v4 BETWEEN 4 AND 68)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 33), (77, ∞), [41, 41], [9, 9]}, {[33, 75], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(75, ∞), [48, ∞), [13, 13], (61, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>20) OR (v1>=71 AND v4 BETWEEN 12 AND 20 AND v2<=30 AND v3 BETWEEN 14 AND 44)) AND (v1>97 AND v2=91 AND v3>=5) OR (v1>7 AND v2<34 AND v3<55 AND v4 BETWEEN 88 AND 97)) AND (v1 BETWEEN 2 AND 16 AND v2<>23 AND v3=75 AND v4>99);`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 20) OR ((((comp_index_t2.v1 >= 71) AND (comp_index_t2.v4 BETWEEN 12 AND 20)) AND (comp_index_t2.v2 <= 30)) AND (comp_index_t2.v3 BETWEEN 14 AND 44))) AND (((comp_index_t2.v1 > 97) AND (comp_index_t2.v2 = 91)) AND (comp_index_t2.v3 >= 5))) OR ((((comp_index_t2.v1 > 7) AND (comp_index_t2.v2 < 34)) AND (comp_index_t2.v3 < 55)) AND (comp_index_t2.v4 BETWEEN 88 AND 97)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=55 AND v2<13 AND v3<=96 AND v4>=49) OR (v1 BETWEEN 39 AND 98 AND v2=77 AND v3>85));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[39, 98], [77, 77], (85, ∞), (-∞, ∞)}, {[55, 55], (-∞, 13), (-∞, 96], [49, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=74 AND v2<>13 AND v3<67 AND v4 BETWEEN 1 AND 70) OR (v1 BETWEEN 30 AND 50 AND v2<27 AND v3>=35));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[30, 50], (-∞, 27), [35, ∞), (-∞, ∞)}, {[74, 74], (-∞, 13), (-∞, 67), [1, 70]}, {[74, 74], (13, ∞), (-∞, 67), [1, 70]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1=76) OR (v1>22 AND v3<49 AND v4=2)) OR (v1=85 AND v4>79)) OR (v1=10 AND v2=47 AND v3 BETWEEN 6 AND 21 AND v4>97));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 76) OR (((comp_index_t2.v1 > 22) AND (comp_index_t2.v3 < 49)) AND (comp_index_t2.v4 = 2))) OR ((comp_index_t2.v1 = 85) AND (comp_index_t2.v4 > 79))) OR ((((comp_index_t2.v1 = 10) AND (comp_index_t2.v2 = 47)) AND (comp_index_t2.v3 BETWEEN 6 AND 21)) AND (comp_index_t2.v4 > 97)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[10, 10], [47, 47], [6, 21], (97, ∞)}, {(22, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>38 AND v2>98) OR (v1<>29 AND v2=75)) OR (v1>58 AND v2<>49 AND v3 BETWEEN 25 AND 58));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 29), [75, 75], (-∞, ∞), (-∞, ∞)}, {(29, ∞), [75, 75], (-∞, ∞), (-∞, ∞)}, {(38, ∞), (98, ∞), (-∞, ∞), (-∞, ∞)}, {(58, ∞), (-∞, 49), [25, 58], (-∞, ∞)}, {(58, ∞), (49, 75), [25, 58], (-∞, ∞)}, {(58, ∞), (75, 98], [25, 58], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>82 AND v4=74 AND v2=8 AND v3>=43) OR (v1=1 AND v2>=54 AND v3 BETWEEN 41 AND 91 AND v4>=0));`,
		ExpectedPlan: "Filter(((((NOT((comp_index_t2.v1 = 82))) AND (comp_index_t2.v4 = 74)) AND (comp_index_t2.v2 = 8)) AND (comp_index_t2.v3 >= 43)) OR ((((comp_index_t2.v1 = 1) AND (comp_index_t2.v2 >= 54)) AND (comp_index_t2.v3 BETWEEN 41 AND 91)) AND (comp_index_t2.v4 >= 0)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 82), [8, 8], [43, ∞), [74, 74]}, {[1, 1], [54, ∞), [41, 91], [0, ∞)}, {(82, ∞), [8, 8], [43, ∞), [74, 74]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=26 AND v2<=94 AND v3<=76) OR (v1<34 AND v2 BETWEEN 5 AND 20));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 34), [5, 20], (-∞, ∞), (-∞, ∞)}, {[26, 26], (-∞, 5), (-∞, 76], (-∞, ∞)}, {[26, 26], (20, 94], (-∞, 76], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>74 AND v2<=3 AND v3>51 AND v4<1) OR (v1>=92 AND v2<=2));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(74, 92), (-∞, 3], (51, ∞), (-∞, 1)}, {[92, ∞), (-∞, 2], (-∞, ∞), (-∞, ∞)}, {[92, ∞), (2, 3], (51, ∞), (-∞, 1)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<=10 AND v2 BETWEEN 29 AND 83 AND v3<30 AND v4=54) OR (v1=68 AND v2=9 AND v3<=31)) AND (v1=87 AND v2>=91) OR (v1<=3 AND v2<>65 AND v3<8 AND v4<54)) OR (v1<7 AND v2>=4 AND v3<=47));`,
		ExpectedPlan: "Filter((((((((comp_index_t2.v1 <= 10) AND (comp_index_t2.v2 BETWEEN 29 AND 83)) AND (comp_index_t2.v3 < 30)) AND (comp_index_t2.v4 = 54)) OR (((comp_index_t2.v1 = 68) AND (comp_index_t2.v2 = 9)) AND (comp_index_t2.v3 <= 31))) AND ((comp_index_t2.v1 = 87) AND (comp_index_t2.v2 >= 91))) OR ((((comp_index_t2.v1 <= 3) AND (NOT((comp_index_t2.v2 = 65)))) AND (comp_index_t2.v3 < 8)) AND (comp_index_t2.v4 < 54))) OR (((comp_index_t2.v1 < 7) AND (comp_index_t2.v2 >= 4)) AND (comp_index_t2.v3 <= 47)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 3], (-∞, 4), (-∞, 8), (-∞, 54)}, {(-∞, 7), [4, ∞), (-∞, 47], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<35) OR (v1>=5 AND v2>=10 AND v3=65));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 35), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[35, ∞), [10, ∞), [65, 65], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>31 AND v2<=37 AND v3>56 AND v4 BETWEEN 10 AND 31) OR (v1>8)) AND (v1>=27 AND v2<>44);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[27, ∞), (-∞, 44), (-∞, ∞), (-∞, ∞)}, {[27, ∞), (44, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>52) OR (v1<21 AND v2<61 AND v3=13)) OR (v1=89 AND v3>33));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 > 52) OR (((comp_index_t2.v1 < 21) AND (comp_index_t2.v2 < 61)) AND (comp_index_t2.v3 = 13))) OR ((comp_index_t2.v1 = 89) AND (comp_index_t2.v3 > 33)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 21), (-∞, 61), [13, 13], (-∞, ∞)}, {(52, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<30 AND v4>11 AND v2<=11) OR (v1<>19 AND v2<>47 AND v3 BETWEEN 38 AND 77 AND v4>31)) OR (v1 BETWEEN 0 AND 27 AND v2 BETWEEN 33 AND 34)) OR (v1<32)) AND (v1<9 AND v3=54 AND v4<>31 AND v2<>95);`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 < 30) AND (comp_index_t2.v4 > 11)) AND (comp_index_t2.v2 <= 11)) OR ((((NOT((comp_index_t2.v1 = 19))) AND (NOT((comp_index_t2.v2 = 47)))) AND (comp_index_t2.v3 BETWEEN 38 AND 77)) AND (comp_index_t2.v4 > 31))) OR ((comp_index_t2.v1 BETWEEN 0 AND 27) AND (comp_index_t2.v2 BETWEEN 33 AND 34))) OR (comp_index_t2.v1 < 32))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 9), (-∞, 95), [54, 54], (-∞, 31)}, {(-∞, 9), (-∞, 95), [54, 54], (31, ∞)}, {(-∞, 9), (95, ∞), [54, 54], (-∞, 31)}, {(-∞, 9), (95, ∞), [54, 54], (31, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=48) OR (v1 BETWEEN 2 AND 81));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 81], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<>36) OR (v1<>70 AND v2 BETWEEN 23 AND 39)) OR (v1>51 AND v2>=57)) OR (v1<50 AND v2<=3 AND v3 BETWEEN 1 AND 74));`,
		ExpectedPlan: "Filter((((NOT((comp_index_t2.v1 = 36))) OR ((NOT((comp_index_t2.v1 = 70))) AND (comp_index_t2.v2 BETWEEN 23 AND 39))) OR ((comp_index_t2.v1 > 51) AND (comp_index_t2.v2 >= 57))) OR (((comp_index_t2.v1 < 50) AND (comp_index_t2.v2 <= 3)) AND (comp_index_t2.v3 BETWEEN 1 AND 74)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 36), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[36, 36], (-∞, 3], [1, 74], (-∞, ∞)}, {[36, 36], [23, 39], (-∞, ∞), (-∞, ∞)}, {(36, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1>30) OR (v1>98 AND v4>43 AND v2<>80)) OR (v1 BETWEEN 2 AND 23 AND v2>=34)) OR (v1>=42));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 30) OR (((comp_index_t2.v1 > 98) AND (comp_index_t2.v4 > 43)) AND (NOT((comp_index_t2.v2 = 80))))) OR ((comp_index_t2.v1 BETWEEN 2 AND 23) AND (comp_index_t2.v2 >= 34))) OR (comp_index_t2.v1 >= 42))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[2, 23], [34, ∞), (-∞, ∞), (-∞, ∞)}, {(30, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<68 AND v2<81 AND v3<34 AND v4<>33) OR (v1<=78 AND v4 BETWEEN 34 AND 99 AND v2>=79 AND v3>=9)) OR (v1=27 AND v4 BETWEEN 20 AND 41 AND v2<98 AND v3>=15));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 < 68) AND (comp_index_t2.v2 < 81)) AND (comp_index_t2.v3 < 34)) AND (NOT((comp_index_t2.v4 = 33)))) OR ((((comp_index_t2.v1 <= 78) AND (comp_index_t2.v4 BETWEEN 34 AND 99)) AND (comp_index_t2.v2 >= 79)) AND (comp_index_t2.v3 >= 9))) OR ((((comp_index_t2.v1 = 27) AND (comp_index_t2.v4 BETWEEN 20 AND 41)) AND (comp_index_t2.v2 < 98)) AND (comp_index_t2.v3 >= 15)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 27), (-∞, 81), (-∞, 34), (-∞, 33)}, {(-∞, 27), (-∞, 81), (-∞, 34), (33, ∞)}, {(-∞, 27), [79, 81), [34, ∞), [34, 99]}, {(-∞, 27), [81, ∞), [9, ∞), [34, 99]}, {[27, 27], (-∞, 79), [34, ∞), [20, 41]}, {[27, 27], (-∞, 81), (-∞, 15), (-∞, 33)}, {[27, 27], (-∞, 81), (-∞, 15), (33, ∞)}, {[27, 27], (-∞, 81), [15, 34), (-∞, ∞)}, {[27, 27], [79, 81), [34, ∞), [20, 99]}, {[27, 27], [81, 98), [9, 15), [34, 99]}, {[27, 27], [81, 98), [15, ∞), [20, 99]}, {[27, 27], [98, ∞), [9, ∞), [34, 99]}, {(27, 68), (-∞, 81), (-∞, 34), (-∞, 33)}, {(27, 68), (-∞, 81), (-∞, 34), (33, ∞)}, {(27, 68), [79, 81), [34, ∞), [34, 99]}, {(27, 68), [81, ∞), [9, ∞), [34, 99]}, {[68, 78], [79, ∞), [9, ∞), [34, 99]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<23 AND v2<=45 AND v3<0) OR (v1>=31)) OR (v1>=50));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 23), (-∞, 45], (-∞, 0), (-∞, ∞)}, {[31, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<16) OR (v1>=19 AND v2<25 AND v3>77));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 16), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[19, ∞), (-∞, 25), (77, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<29 AND v2 BETWEEN 81 AND 92) OR (v1>20 AND v2>=53 AND v3 BETWEEN 20 AND 68));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 29), [81, 92], (-∞, ∞), (-∞, ∞)}, {(20, 29), [53, 81), [20, 68], (-∞, ∞)}, {(20, 29), (92, ∞), [20, 68], (-∞, ∞)}, {[29, ∞), [53, ∞), [20, 68], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1 BETWEEN 25 AND 59 AND v2=1 AND v3<93 AND v4<=16) OR (v1<40 AND v2 BETWEEN 14 AND 37 AND v3>62 AND v4<58)) OR (v1<>17 AND v2<>36)) OR (v1 BETWEEN 7 AND 99 AND v2<>6 AND v3=43 AND v4<89));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 17), (-∞, 36), (-∞, ∞), (-∞, ∞)}, {(-∞, 17), [36, 36], (62, ∞), (-∞, 58)}, {(-∞, 17), (36, ∞), (-∞, ∞), (-∞, ∞)}, {[7, 17), [36, 36], [43, 43], (-∞, 89)}, {[17, 17], (-∞, 6), [43, 43], (-∞, 89)}, {[17, 17], (6, ∞), [43, 43], (-∞, 89)}, {[17, 17], [14, 37], (62, ∞), (-∞, 58)}, {(17, 40), [36, 36], (62, ∞), (-∞, 58)}, {(17, 99], [36, 36], [43, 43], (-∞, 89)}, {(17, ∞), (-∞, 36), (-∞, ∞), (-∞, ∞)}, {(17, ∞), (36, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1=46) AND (v1>=93 AND v3<>51 AND v4=93 AND v2=8);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<=5 AND v2>=14 AND v3<=2) OR (v1<53 AND v4=99 AND v2=72)) OR (v1<>49 AND v2<>39 AND v3>=70 AND v4<>24)) OR (v1<79));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 <= 5) AND (comp_index_t2.v2 >= 14)) AND (comp_index_t2.v3 <= 2)) OR (((comp_index_t2.v1 < 53) AND (comp_index_t2.v4 = 99)) AND (comp_index_t2.v2 = 72))) OR ((((NOT((comp_index_t2.v1 = 49))) AND (NOT((comp_index_t2.v2 = 39)))) AND (comp_index_t2.v3 >= 70)) AND (NOT((comp_index_t2.v4 = 24))))) OR (comp_index_t2.v1 < 79))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 79), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[79, ∞), (-∞, 39), [70, ∞), (-∞, 24)}, {[79, ∞), (-∞, 39), [70, ∞), (24, ∞)}, {[79, ∞), (39, ∞), [70, ∞), (-∞, 24)}, {[79, ∞), (39, ∞), [70, ∞), (24, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1<99 AND v2<=42) OR (v1=47 AND v4 BETWEEN 33 AND 63 AND v2>=10 AND v3<=57)) OR (v1>44)) OR (v1<>87 AND v2>42 AND v3<69));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 < 99) AND (comp_index_t2.v2 <= 42)) OR ((((comp_index_t2.v1 = 47) AND (comp_index_t2.v4 BETWEEN 33 AND 63)) AND (comp_index_t2.v2 >= 10)) AND (comp_index_t2.v3 <= 57))) OR (comp_index_t2.v1 > 44)) OR (((NOT((comp_index_t2.v1 = 87))) AND (comp_index_t2.v2 > 42)) AND (comp_index_t2.v3 < 69)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 44], (-∞, 42], (-∞, ∞), (-∞, ∞)}, {(-∞, 44], (42, ∞), (-∞, 69), (-∞, ∞)}, {(44, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=29 AND v2 BETWEEN 50 AND 86 AND v3<=6 AND v4 BETWEEN 8 AND 48) OR (v1>86 AND v2 BETWEEN 62 AND 70 AND v3=33));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 29], [50, 86], (-∞, 6], [8, 48]}, {(86, ∞), [62, 70], [33, 33], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=15) OR (v1>=59 AND v2<18)) OR (v1 BETWEEN 23 AND 31 AND v3>50 AND v4 BETWEEN 15 AND 54));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 >= 15) OR ((comp_index_t2.v1 >= 59) AND (comp_index_t2.v2 < 18))) OR (((comp_index_t2.v1 BETWEEN 23 AND 31) AND (comp_index_t2.v3 > 50)) AND (comp_index_t2.v4 BETWEEN 15 AND 54)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[15, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=50 AND v2<=84 AND v3 BETWEEN 22 AND 26) OR (v1<=18 AND v2<49 AND v3>19 AND v4 BETWEEN 61 AND 75)) AND (v1>48 AND v2>=56 AND v3=6) OR (v1<=88 AND v2>=76 AND v3<40 AND v4<=18));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 >= 50) AND (comp_index_t2.v2 <= 84)) AND (comp_index_t2.v3 BETWEEN 22 AND 26)) OR ((((comp_index_t2.v1 <= 18) AND (comp_index_t2.v2 < 49)) AND (comp_index_t2.v3 > 19)) AND (comp_index_t2.v4 BETWEEN 61 AND 75))) AND (((comp_index_t2.v1 > 48) AND (comp_index_t2.v2 >= 56)) AND (comp_index_t2.v3 = 6))) OR ((((comp_index_t2.v1 <= 88) AND (comp_index_t2.v2 >= 76)) AND (comp_index_t2.v3 < 40)) AND (comp_index_t2.v4 <= 18)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 88], [76, ∞), (-∞, 40), (-∞, 18]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=27) OR (v1>=11 AND v2<97 AND v3<97 AND v4<44));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[11, 27), (-∞, 97), (-∞, 97), (-∞, 44)}, {[27, 27], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(27, ∞), (-∞, 97), (-∞, 97), (-∞, 44)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=89 AND v2<=93) OR (v1<=54));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 54], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(54, 89], (-∞, 93], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=74 AND v2<=31) OR (v1<11)) OR (v1 BETWEEN 26 AND 38));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 11), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[26, 38], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[74, 74], (-∞, 31], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=10 AND v2<12 AND v3=54 AND v4>89) OR (v1=99 AND v4=37));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 >= 10) AND (comp_index_t2.v2 < 12)) AND (comp_index_t2.v3 = 54)) AND (comp_index_t2.v4 > 89)) OR ((comp_index_t2.v1 = 99) AND (comp_index_t2.v4 = 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[10, 99), (-∞, 12), [54, 54], (89, ∞)}, {[99, 99], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(99, ∞), (-∞, 12), [54, 54], (89, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=50 AND v2<50) OR (v1<19)) OR (v1=51));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 19), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[19, 50], (-∞, 50), (-∞, ∞), (-∞, ∞)}, {[51, 51], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=62 AND v2<89) AND (v1<90 AND v2>=19) OR (v1<=1 AND v2>49));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 62) AND (comp_index_t2.v2 < 89)) AND ((comp_index_t2.v1 < 90) AND (comp_index_t2.v2 >= 19))) OR ((comp_index_t2.v1 <= 1) AND (comp_index_t2.v2 > 49)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 1], (49, ∞), (-∞, ∞), (-∞, ∞)}, {[62, 62], [19, 89), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1<=61 AND v2<=64) AND (v1>=0);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[0, 61], (-∞, 64], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 5 AND 69) OR (v1<52 AND v4<14 AND v2>=25 AND v3=63));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 BETWEEN 5 AND 69) OR ((((comp_index_t2.v1 < 52) AND (comp_index_t2.v4 < 14)) AND (comp_index_t2.v2 >= 25)) AND (comp_index_t2.v3 = 63)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 5), [25, ∞), [63, 63], (-∞, 14)}, {[5, 69], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=21 AND v2<>0 AND v3<49) OR (v1<=70 AND v2>16 AND v3<=89 AND v4>=27)) OR (v1>=14));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 14), (16, ∞), (-∞, 89], [27, ∞)}, {[14, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>14) OR (v1>=82));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(14, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=19 AND v3<72 AND v4=23) OR (v1<=36 AND v2>99));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 19) AND (comp_index_t2.v3 < 72)) AND (comp_index_t2.v4 = 23)) OR ((comp_index_t2.v1 <= 36) AND (comp_index_t2.v2 > 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 19), (99, ∞), (-∞, ∞), (-∞, ∞)}, {[19, 19], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(19, 36], (99, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>43) OR (v1>=41 AND v4=32 AND v2<=66)) AND (v1>43 AND v2 BETWEEN 83 AND 97);`,
		ExpectedPlan: "Filter((NOT((comp_index_t2.v1 = 43))) OR (((comp_index_t2.v1 >= 41) AND (comp_index_t2.v4 = 32)) AND (comp_index_t2.v2 <= 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(43, ∞), [83, 97], (-∞, ∞), (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=8 AND v4>=44) AND (v1=84 AND v2=41 AND v3 BETWEEN 5 AND 81) OR (v1<>31 AND v2<=96 AND v3<=20 AND v4<=14));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 <= 8) AND (comp_index_t2.v4 >= 44)) AND (((comp_index_t2.v1 = 84) AND (comp_index_t2.v2 = 41)) AND (comp_index_t2.v3 BETWEEN 5 AND 81))) OR ((((NOT((comp_index_t2.v1 = 31))) AND (comp_index_t2.v2 <= 96)) AND (comp_index_t2.v3 <= 20)) AND (comp_index_t2.v4 <= 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 31), (-∞, 96], (-∞, 20], (-∞, 14]}, {(31, ∞), (-∞, 96], (-∞, 20], (-∞, 14]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 52 AND 55) OR (v1>1 AND v2>36 AND v3<=47)) OR (v1 BETWEEN 0 AND 38 AND v2<=49 AND v3>=8));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[0, 1], (-∞, 49], [8, ∞), (-∞, ∞)}, {(1, 38], (-∞, 36], [8, ∞), (-∞, ∞)}, {(1, 38], (36, 49], (-∞, ∞), (-∞, ∞)}, {(1, 38], (49, ∞), (-∞, 47], (-∞, ∞)}, {(38, 52), (36, ∞), (-∞, 47], (-∞, ∞)}, {[52, 55], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(55, ∞), (36, ∞), (-∞, 47], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=11 AND v2>=41 AND v3=9) AND (v1<>41 AND v3<>69 AND v4<24) OR (v1>48 AND v4<79));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 <= 11) AND (comp_index_t2.v2 >= 41)) AND (comp_index_t2.v3 = 9)) AND (((NOT((comp_index_t2.v1 = 41))) AND (NOT((comp_index_t2.v3 = 69)))) AND (comp_index_t2.v4 < 24))) OR ((comp_index_t2.v1 > 48) AND (comp_index_t2.v4 < 79)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 11], [41, ∞), [9, 9], (-∞, 24)}, {(48, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1=23 AND v4>=52 AND v2>=61) AND (v1<>85 AND v3>2 AND v4<15);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1 BETWEEN 32 AND 51 AND v4 BETWEEN 5 AND 14 AND v2=46 AND v3>=31) OR (v1>=32 AND v2<=26 AND v3>52 AND v4>55));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 BETWEEN 32 AND 51) AND (comp_index_t2.v4 BETWEEN 5 AND 14)) AND (comp_index_t2.v2 = 46)) AND (comp_index_t2.v3 >= 31)) OR ((((comp_index_t2.v1 >= 32) AND (comp_index_t2.v2 <= 26)) AND (comp_index_t2.v3 > 52)) AND (comp_index_t2.v4 > 55)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[32, 51], [46, 46], [31, ∞), [5, 14]}, {[32, ∞), (-∞, 26], (52, ∞), (55, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=16 AND v2<59 AND v3<=43) OR (v1=17 AND v2<=4 AND v3>71));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[16, ∞), (-∞, 59), (-∞, 43], (-∞, ∞)}, {[17, 17], (-∞, 4], (71, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1=42 AND v4=47) OR (v1>=28)) AND (v1<>10) OR (v1 BETWEEN 20 AND 60 AND v2>96 AND v3<>28)) OR (v1=99 AND v2<=62 AND v3=30 AND v4 BETWEEN 92 AND 93));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 = 42) AND (comp_index_t2.v4 = 47)) OR (comp_index_t2.v1 >= 28)) AND (NOT((comp_index_t2.v1 = 10)))) OR (((comp_index_t2.v1 BETWEEN 20 AND 60) AND (comp_index_t2.v2 > 96)) AND (NOT((comp_index_t2.v3 = 28))))) OR ((((comp_index_t2.v1 = 99) AND (comp_index_t2.v2 <= 62)) AND (comp_index_t2.v3 = 30)) AND (comp_index_t2.v4 BETWEEN 92 AND 93)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[20, 28), (96, ∞), (-∞, 28), (-∞, ∞)}, {[20, 28), (96, ∞), (28, ∞), (-∞, ∞)}, {[28, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=50 AND v3=4 AND v4=53 AND v2>=80) OR (v1<54 AND v4<=76 AND v2>48)) OR (v1>=38 AND v4<76 AND v2=56));`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 = 50) AND (comp_index_t2.v3 = 4)) AND (comp_index_t2.v4 = 53)) AND (comp_index_t2.v2 >= 80)) OR (((comp_index_t2.v1 < 54) AND (comp_index_t2.v4 <= 76)) AND (comp_index_t2.v2 > 48))) OR (((comp_index_t2.v1 >= 38) AND (comp_index_t2.v4 < 76)) AND (comp_index_t2.v2 = 56)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 54), (48, ∞), (-∞, ∞), (-∞, ∞)}, {[54, ∞), [56, 56], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=79 AND v2>24) OR (v1<76 AND v3<=59 AND v4<=36 AND v2=39));`,
		ExpectedPlan: "Filter(((comp_index_t2.v1 = 79) AND (comp_index_t2.v2 > 24)) OR ((((comp_index_t2.v1 < 76) AND (comp_index_t2.v3 <= 59)) AND (comp_index_t2.v4 <= 36)) AND (comp_index_t2.v2 = 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 76), [39, 39], (-∞, 59], (-∞, 36]}, {[79, 79], (24, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<=15 AND v2 BETWEEN 21 AND 76 AND v3=23) OR (v1 BETWEEN 2 AND 55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 2), [21, 76], [23, 23], (-∞, ∞)}, {[2, 55], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=85 AND v2>37 AND v3<=57 AND v4 BETWEEN 12 AND 49) AND (v1>10) OR (v1>56)) OR (v1>=57));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 = 85) AND (comp_index_t2.v2 > 37)) AND (comp_index_t2.v3 <= 57)) AND (comp_index_t2.v4 BETWEEN 12 AND 49)) AND (comp_index_t2.v1 > 10)) OR (comp_index_t2.v1 > 56)) OR (comp_index_t2.v1 >= 57))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(56, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((((v1<>89 AND v2>=75) OR (v1<=5)) OR (v1=5 AND v2<19 AND v3>=1)) OR (v1>=18 AND v2>=17 AND v3 BETWEEN 78 AND 83)) OR (v1>=11 AND v3<=9 AND v4>39));`,
		ExpectedPlan: "Filter((((((NOT((comp_index_t2.v1 = 89))) AND (comp_index_t2.v2 >= 75)) OR (comp_index_t2.v1 <= 5)) OR (((comp_index_t2.v1 = 5) AND (comp_index_t2.v2 < 19)) AND (comp_index_t2.v3 >= 1))) OR (((comp_index_t2.v1 >= 18) AND (comp_index_t2.v2 >= 17)) AND (comp_index_t2.v3 BETWEEN 78 AND 83))) OR (((comp_index_t2.v1 >= 11) AND (comp_index_t2.v3 <= 9)) AND (comp_index_t2.v4 > 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 5], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(5, 11), [75, ∞), (-∞, ∞), (-∞, ∞)}, {[11, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1 BETWEEN 36 AND 48 AND v4<97 AND v2>=99 AND v3=3) OR (v1<>84 AND v2=46 AND v3=4)) OR (v1>73 AND v2 BETWEEN 34 AND 39 AND v3 BETWEEN 34 AND 71 AND v4>=15)) OR (v1<>82));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 BETWEEN 36 AND 48) AND (comp_index_t2.v4 < 97)) AND (comp_index_t2.v2 >= 99)) AND (comp_index_t2.v3 = 3)) OR (((NOT((comp_index_t2.v1 = 84))) AND (comp_index_t2.v2 = 46)) AND (comp_index_t2.v3 = 4))) OR ((((comp_index_t2.v1 > 73) AND (comp_index_t2.v2 BETWEEN 34 AND 39)) AND (comp_index_t2.v3 BETWEEN 34 AND 71)) AND (comp_index_t2.v4 >= 15))) OR (NOT((comp_index_t2.v1 = 82))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 82), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[82, 82], [34, 39], [34, 71], [15, ∞)}, {[82, 82], [46, 46], [4, 4], (-∞, ∞)}, {(82, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1<=50 AND v3>=51 AND v4<>69) AND (v1>1 AND v3<24);`,
		ExpectedPlan: "Filter(((comp_index_t2.v3 >= 51) AND (NOT((comp_index_t2.v4 = 69)))) AND (comp_index_t2.v3 < 24))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(1, 50], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>10 AND v2=72 AND v3<31) OR (v1<67 AND v3 BETWEEN 13 AND 70 AND v4>66 AND v2>39)) OR (v1<82)) AND (v1>=66);`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 10) AND (comp_index_t2.v2 = 72)) AND (comp_index_t2.v3 < 31)) OR ((((comp_index_t2.v1 < 67) AND (comp_index_t2.v3 BETWEEN 13 AND 70)) AND (comp_index_t2.v4 > 66)) AND (comp_index_t2.v2 > 39))) OR (comp_index_t2.v1 < 82))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[66, 82), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[82, ∞), [72, 72], (-∞, 31), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=84 AND v2<85 AND v3 BETWEEN 75 AND 86 AND v4<=34) OR (v1>=37 AND v2<59 AND v3 BETWEEN 2 AND 26 AND v4>6));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[37, ∞), (-∞, 59), [2, 26], (6, ∞)}, {[84, 84], (-∞, 85), [75, 86], (-∞, 34]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>10 AND v2=42) OR (v1>=85 AND v2<>6 AND v3=34 AND v4<=45));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(10, ∞), [42, 42], (-∞, ∞), (-∞, ∞)}, {[85, ∞), (-∞, 6), [34, 34], (-∞, 45]}, {[85, ∞), (6, 42), [34, 34], (-∞, 45]}, {[85, ∞), (42, ∞), [34, 34], (-∞, 45]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=24 AND v2<>33 AND v3=77 AND v4<>63) OR (v1<>22 AND v2<=58 AND v3>71 AND v4>=87)) OR (v1<=85 AND v2>18 AND v3<=40));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 22), (-∞, 58], (71, ∞), [87, ∞)}, {(-∞, 85], (18, ∞), (-∞, 40], (-∞, ∞)}, {(22, 24), (-∞, 58], (71, ∞), [87, ∞)}, {[24, 24], (-∞, 33), (71, 77), [87, ∞)}, {[24, 24], (-∞, 33), [77, 77], (-∞, 63)}, {[24, 24], (-∞, 33), [77, 77], (63, ∞)}, {[24, 24], (-∞, 33), (77, ∞), [87, ∞)}, {[24, 24], [33, 33], (71, ∞), [87, ∞)}, {[24, 24], (33, 58], (71, 77), [87, ∞)}, {[24, 24], (33, 58], (77, ∞), [87, ∞)}, {[24, 24], (33, ∞), [77, 77], (-∞, 63)}, {[24, 24], (33, ∞), [77, 77], (63, ∞)}, {(24, ∞), (-∞, 58], (71, ∞), [87, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<72 AND v2>=67) OR (v1<>88 AND v2<>23 AND v3=23));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 72), (23, 67), [23, 23], (-∞, ∞)}, {(-∞, 72), [67, ∞), (-∞, ∞), (-∞, ∞)}, {(-∞, 88), (-∞, 23), [23, 23], (-∞, ∞)}, {[72, 88), (23, ∞), [23, 23], (-∞, ∞)}, {(88, ∞), (-∞, 23), [23, 23], (-∞, ∞)}, {(88, ∞), (23, ∞), [23, 23], (-∞, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=11 AND v2>=99) OR (v1<18 AND v2>=34 AND v3<53)) OR (v1>68));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 11), [34, ∞), (-∞, 53), (-∞, ∞)}, {[11, 11], [34, 99), (-∞, 53), (-∞, ∞)}, {[11, 11], [99, ∞), (-∞, ∞), (-∞, ∞)}, {(11, 18), [34, ∞), (-∞, 53), (-∞, ∞)}, {(68, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<=40 AND v2<0) OR (v1>=35 AND v2<=95 AND v3<>61)) OR (v1>49));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 40], (-∞, 0), (-∞, ∞), (-∞, ∞)}, {[35, 40], [0, 95], (-∞, 61), (-∞, ∞)}, {[35, 40], [0, 95], (61, ∞), (-∞, ∞)}, {(40, 49], (-∞, 95], (-∞, 61), (-∞, ∞)}, {(40, 49], (-∞, 95], (61, ∞), (-∞, ∞)}, {(49, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1=85 AND v2<81 AND v3 BETWEEN 14 AND 61 AND v4<>99) OR (v1 BETWEEN 31 AND 86 AND v4<>43)) OR (v1 BETWEEN 15 AND 67)) AND (v1 BETWEEN 37 AND 55);`,
		ExpectedPlan: "Filter((((((comp_index_t2.v1 = 85) AND (comp_index_t2.v2 < 81)) AND (comp_index_t2.v3 BETWEEN 14 AND 61)) AND (NOT((comp_index_t2.v4 = 99)))) OR ((comp_index_t2.v1 BETWEEN 31 AND 86) AND (NOT((comp_index_t2.v4 = 43))))) OR (comp_index_t2.v1 BETWEEN 15 AND 67))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[37, 55], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>=52 AND v4>=86) OR (v1>=86 AND v3=79 AND v4=9 AND v2 BETWEEN 2 AND 6)) OR (v1>98 AND v2<=44 AND v3<>53));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 >= 52) AND (comp_index_t2.v4 >= 86)) OR ((((comp_index_t2.v1 >= 86) AND (comp_index_t2.v3 = 79)) AND (comp_index_t2.v4 = 9)) AND (comp_index_t2.v2 BETWEEN 2 AND 6))) OR (((comp_index_t2.v1 > 98) AND (comp_index_t2.v2 <= 44)) AND (NOT((comp_index_t2.v3 = 53)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[52, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>5 AND v4 BETWEEN 14 AND 43 AND v2>=62) OR (v1>=91 AND v2>=28 AND v3>=83 AND v4<>91));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 > 5) AND (comp_index_t2.v4 BETWEEN 14 AND 43)) AND (comp_index_t2.v2 >= 62)) OR ((((comp_index_t2.v1 >= 91) AND (comp_index_t2.v2 >= 28)) AND (comp_index_t2.v3 >= 83)) AND (NOT((comp_index_t2.v4 = 91)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(5, ∞), [62, ∞), (-∞, ∞), (-∞, ∞)}, {[91, ∞), [28, 62), [83, ∞), (-∞, 91)}, {[91, ∞), [28, 62), [83, ∞), (91, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1<>87) OR (v1>91 AND v2>23 AND v3<74));`,
		ExpectedPlan: "Filter((NOT((comp_index_t2.v1 = 87))) OR (((comp_index_t2.v1 > 91) AND (comp_index_t2.v2 > 23)) AND (comp_index_t2.v3 < 74)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 87), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(87, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1 BETWEEN 1 AND 19 AND v2 BETWEEN 22 AND 48) AND (v1 BETWEEN 6 AND 47 AND v2>=25 AND v3<27);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[6, 19], [25, 48], (-∞, 27), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((((v1=76 AND v2>35 AND v3<=59 AND v4>25) OR (v1 BETWEEN 35 AND 82 AND v2 BETWEEN 8 AND 37 AND v3>18 AND v4<=70)) OR (v1<=95 AND v3=70 AND v4=11)) OR (v1 BETWEEN 15 AND 23 AND v2<>24 AND v3<=50 AND v4<>84));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 = 76) AND (comp_index_t2.v2 > 35)) AND (comp_index_t2.v3 <= 59)) AND (comp_index_t2.v4 > 25)) OR ((((comp_index_t2.v1 BETWEEN 35 AND 82) AND (comp_index_t2.v2 BETWEEN 8 AND 37)) AND (comp_index_t2.v3 > 18)) AND (comp_index_t2.v4 <= 70))) OR (((comp_index_t2.v1 <= 95) AND (comp_index_t2.v3 = 70)) AND (comp_index_t2.v4 = 11))) OR ((((comp_index_t2.v1 BETWEEN 15 AND 23) AND (NOT((comp_index_t2.v2 = 24)))) AND (comp_index_t2.v3 <= 50)) AND (NOT((comp_index_t2.v4 = 84)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 95], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>42 AND v2=44 AND v3<>73) OR (v1>24 AND v2>49 AND v3>=7));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(24, ∞), (49, ∞), [7, ∞), (-∞, ∞)}, {(42, ∞), [44, 44], (-∞, 73), (-∞, ∞)}, {(42, ∞), [44, 44], (73, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=79 AND v3<89 AND v4>=3) OR (v1<63 AND v2<66));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 = 79) AND (comp_index_t2.v3 < 89)) AND (comp_index_t2.v4 >= 3)) OR ((comp_index_t2.v1 < 63) AND (comp_index_t2.v2 < 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 63), (-∞, 66), (-∞, ∞), (-∞, ∞)}, {[79, 79], (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>66) OR (v1=33)) OR (v1<>39 AND v2>53 AND v3<73 AND v4<75));`,
		ExpectedPlan: "Filter(((NOT((comp_index_t2.v1 = 66))) OR (comp_index_t2.v1 = 33)) OR ((((NOT((comp_index_t2.v1 = 39))) AND (comp_index_t2.v2 > 53)) AND (comp_index_t2.v3 < 73)) AND (comp_index_t2.v4 < 75)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 66), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[66, 66], (53, ∞), (-∞, 73), (-∞, 75)}, {(66, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1=15) OR (v1>36 AND v3=13 AND v4<=98 AND v2 BETWEEN 70 AND 85));`,
		ExpectedPlan: "Filter((comp_index_t2.v1 = 15) OR ((((comp_index_t2.v1 > 36) AND (comp_index_t2.v3 = 13)) AND (comp_index_t2.v4 <= 98)) AND (comp_index_t2.v2 BETWEEN 70 AND 85)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[15, 15], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(36, ∞), [70, 85], [13, 13], (-∞, 98]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 23 AND 45 AND v4<30) OR (v1>=36 AND v2<>6 AND v3 BETWEEN 30 AND 53)) OR (v1 BETWEEN 41 AND 95));`,
		ExpectedPlan: "Filter((((comp_index_t2.v1 BETWEEN 23 AND 45) AND (comp_index_t2.v4 < 30)) OR (((comp_index_t2.v1 >= 36) AND (NOT((comp_index_t2.v2 = 6)))) AND (comp_index_t2.v3 BETWEEN 30 AND 53))) OR (comp_index_t2.v1 BETWEEN 41 AND 95))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[23, 95], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {(95, ∞), (-∞, 6), [30, 53], (-∞, ∞)}, {(95, ∞), (6, ∞), [30, 53], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>6 AND v4<>9 AND v2<>77 AND v3>=81) OR (v1<>21 AND v2>=17 AND v3<=3));`,
		ExpectedPlan: "Filter(((((comp_index_t2.v1 > 6) AND (NOT((comp_index_t2.v4 = 9)))) AND (NOT((comp_index_t2.v2 = 77)))) AND (comp_index_t2.v3 >= 81)) OR (((NOT((comp_index_t2.v1 = 21))) AND (comp_index_t2.v2 >= 17)) AND (comp_index_t2.v3 <= 3)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 21), [17, ∞), (-∞, 3], (-∞, ∞)}, {(6, ∞), (-∞, 77), [81, ∞), (-∞, 9)}, {(6, ∞), (-∞, 77), [81, ∞), (9, ∞)}, {(6, ∞), (77, ∞), [81, ∞), (-∞, 9)}, {(6, ∞), (77, ∞), [81, ∞), (9, ∞)}, {(21, ∞), [17, ∞), (-∞, 3], (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1 BETWEEN 94 AND 99 AND v2>4 AND v3<94 AND v4<=59) OR (v1=19 AND v2 BETWEEN 47 AND 54)) AND (v1>=83) OR (v1 BETWEEN 50 AND 97 AND v2<12 AND v3>23));`,
		ExpectedPlan: "Filter(((((((comp_index_t2.v1 BETWEEN 94 AND 99) AND (comp_index_t2.v2 > 4)) AND (comp_index_t2.v3 < 94)) AND (comp_index_t2.v4 <= 59)) OR ((comp_index_t2.v1 = 19) AND (comp_index_t2.v2 BETWEEN 47 AND 54))) AND (comp_index_t2.v1 >= 83)) OR (((comp_index_t2.v1 BETWEEN 50 AND 97) AND (comp_index_t2.v2 < 12)) AND (comp_index_t2.v3 > 23)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[50, 97], (-∞, 12), (23, ∞), (-∞, ∞)}, {[94, 97], (4, 12), (-∞, 23], (-∞, 59]}, {[94, 97], [12, ∞), (-∞, 94), (-∞, 59]}, {(97, 99], (4, ∞), (-∞, 94), (-∞, 59]}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>19 AND v2>46 AND v3=26 AND v4>=47) OR (v1>18 AND v2<=79 AND v3=45 AND v4<=7)) OR (v1 BETWEEN 2 AND 21 AND v2>32));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 2), (46, ∞), [26, 26], [47, ∞)}, {[2, 21], (32, ∞), (-∞, ∞), (-∞, ∞)}, {(18, 21], (-∞, 32], [45, 45], (-∞, 7]}, {(21, ∞), (-∞, 79], [45, 45], (-∞, 7]}, {(21, ∞), (46, ∞), [26, 26], [47, ∞)}, {(-∞, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (v1>=5) AND (v1=50 AND v2<=50);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{[50, 50], (-∞, 50], (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>=82 AND v2 BETWEEN 34 AND 50 AND v3<26 AND v4 BETWEEN 48 AND 76) OR (v1<=6));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 6], (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[82, ∞), [34, 50], (-∞, 26), [48, 76]}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE ((v1>29) OR (v1<>94 AND v2>=56 AND v3=14));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 29], [56, ∞), [14, 14], (-∞, ∞)}, {(29, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1>8 AND v2<97 AND v3=51 AND v4<=26) OR (v1>87)) OR (v1<10 AND v2<=45 AND v3>=73));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 10), (-∞, 45], [73, ∞), (-∞, ∞)}, {(8, 87], (-∞, 97), [51, 51], (-∞, 26]}, {(87, ∞), (-∞, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
	{
		Query: `SELECT * FROM comp_index_t2 WHERE (((v1<>15 AND v2>1) OR (v1<46)) OR (v1>47 AND v2>=9 AND v3 BETWEEN 39 AND 87 AND v4>=10));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(comp_index_t2 on [comp_index_t2.v1,comp_index_t2.v2,comp_index_t2.v3,comp_index_t2.v4] with ranges: [{(-∞, 46), (-∞, ∞), (-∞, ∞), (-∞, ∞)}, {[46, ∞), (1, ∞), (-∞, ∞), (-∞, ∞)}])\n" +
			"",
	},
}
