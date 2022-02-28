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

package enginetest

var IndexPlanTests = []QueryPlanTest{
	{
		Query: `SELECT * FROM t0 WHERE ((v1<25) OR (v1>24));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>=99 AND v2<>83) OR (v1>=1));`,
		ExpectedPlan: "Filter(((t0.v1 >= 99) AND (NOT((t0.v2 = 83)))) OR (t0.v1 >= 1))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=38 AND v2<41) OR (v1>60)) OR (v1<22));`,
		ExpectedPlan: "Filter((((t0.v1 <= 38) AND (t0.v2 < 41)) OR (t0.v1 > 60)) OR (t0.v1 < 22))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>92 AND v2>25) OR (v1 BETWEEN 6 AND 24 AND v2=80));`,
		ExpectedPlan: "Filter(((t0.v1 > 92) AND (t0.v2 > 25)) OR ((t0.v1 BETWEEN 6 AND 24) AND (t0.v2 = 80)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=29) OR (v1=49 AND v2<48));`,
		ExpectedPlan: "Filter((t0.v1 <= 29) OR ((t0.v1 = 49) AND (t0.v2 < 48)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>75) OR (v1<=11));`,
		ExpectedPlan: "Filter((NOT((t0.v1 = 75))) OR (t0.v1 <= 11))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=86) OR (v1<>9)) AND (v1=87 AND v2<=45);`,
		ExpectedPlan: "Filter(((t0.v1 <= 86) OR (NOT((t0.v1 = 9)))) AND (t0.v1 = 87))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=5) OR (v1=71)) OR (v1<>96));`,
		ExpectedPlan: "Filter(((t0.v1 <= 5) OR (t0.v1 = 71)) OR (NOT((t0.v1 = 96))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=97) OR (v1 BETWEEN 36 AND 98));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1=86 AND v2>41) OR (v1<>6 AND v2>16));`,
		ExpectedPlan: "Filter(((t0.v1 = 86) AND (t0.v2 > 41)) OR ((NOT((t0.v1 = 6))) AND (t0.v2 > 16)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<>22 AND v2>18) OR (v1<>12)) OR (v1<=34));`,
		ExpectedPlan: "Filter((((NOT((t0.v1 = 22))) AND (t0.v2 > 18)) OR (NOT((t0.v1 = 12)))) OR (t0.v1 <= 34))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<11) OR (v1>=66 AND v2=22));`,
		ExpectedPlan: "Filter((t0.v1 < 11) OR ((t0.v1 >= 66) AND (t0.v2 = 22)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>45 AND v2>37) OR (v1<98 AND v2<=35));`,
		ExpectedPlan: "Filter(((NOT((t0.v1 = 45))) AND (t0.v2 > 37)) OR ((t0.v1 < 98) AND (t0.v2 <= 35)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>=16 AND v2>96) OR (v1<80));`,
		ExpectedPlan: "Filter(((t0.v1 >= 16) AND (t0.v2 > 96)) OR (t0.v1 < 80))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=98) OR (v1<85 AND v2>60)) OR (v1<>53 AND v2 BETWEEN 82 AND 89));`,
		ExpectedPlan: "Filter(((t0.v1 <= 98) OR ((t0.v1 < 85) AND (t0.v2 > 60))) OR ((NOT((t0.v1 = 53))) AND (t0.v2 BETWEEN 82 AND 89)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((((v1<71 AND v2<7) OR (v1<=21 AND v2<=48)) OR (v1=44 AND v2 BETWEEN 21 AND 83)) OR (v1<=72 AND v2<>27)) OR (v1=35 AND v2 BETWEEN 78 AND 89));`,
		ExpectedPlan: "Filter((((((t0.v1 < 71) AND (t0.v2 < 7)) OR ((t0.v1 <= 21) AND (t0.v2 <= 48))) OR ((t0.v1 = 44) AND (t0.v2 BETWEEN 21 AND 83))) OR ((t0.v1 <= 72) AND (NOT((t0.v2 = 27))))) OR ((t0.v1 = 35) AND (t0.v2 BETWEEN 78 AND 89)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=16) OR (v1>=77 AND v2>77)) OR (v1>19 AND v2>27));`,
		ExpectedPlan: "Filter(((t0.v1 <= 16) OR ((t0.v1 >= 77) AND (t0.v2 > 77))) OR ((t0.v1 > 19) AND (t0.v2 > 27)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>=46) AND (v1>=28 AND v2<>68) OR (v1>=33 AND v2<>39));`,
		ExpectedPlan: "Filter(((t0.v1 >= 46) AND ((t0.v1 >= 28) AND (NOT((t0.v2 = 68))))) OR ((t0.v1 >= 33) AND (NOT((t0.v2 = 39)))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<39 AND v2<10) OR (v1>64 AND v2<=15)) AND (v1>=41);`,
		ExpectedPlan: "Filter(((t0.v1 < 39) AND (t0.v2 < 10)) OR ((t0.v1 > 64) AND (t0.v2 <= 15)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=91) OR (v1<70 AND v2>=23)) OR (v1>23 AND v2<38));`,
		ExpectedPlan: "Filter(((t0.v1 <= 91) OR ((t0.v1 < 70) AND (t0.v2 >= 23))) OR ((t0.v1 > 23) AND (t0.v2 < 38)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((v1<>45 AND v2=70) OR (v1 BETWEEN 40 AND 96 AND v2 BETWEEN 48 AND 96)) OR (v1<>87 AND v2<31)) OR (v1<>62 AND v2=51)) AND (v1>=47 AND v2<29);`,
		ExpectedPlan: "Filter((((((NOT((t0.v1 = 45))) AND (t0.v2 = 70)) OR ((t0.v1 BETWEEN 40 AND 96) AND (t0.v2 BETWEEN 48 AND 96))) OR ((NOT((t0.v1 = 87))) AND (t0.v2 < 31))) OR ((NOT((t0.v1 = 62))) AND (t0.v2 = 51))) AND (t0.v1 >= 47))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<71) OR (v1 BETWEEN 46 AND 79));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>52) OR (v1<=14));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>74) OR (v1<>40 AND v2>=54));`,
		ExpectedPlan: "Filter((NOT((t0.v1 = 74))) OR ((NOT((t0.v1 = 40))) AND (t0.v2 >= 54)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=69 AND v2<24) OR (v1<77 AND v2<=53));`,
		ExpectedPlan: "Filter(((t0.v1 <= 69) AND (t0.v2 < 24)) OR ((t0.v1 < 77) AND (t0.v2 <= 53)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1=78 AND v2=87) OR (v1 BETWEEN 37 AND 58 AND v2>=30)) AND (v1=86 AND v2 BETWEEN 0 AND 70);`,
		ExpectedPlan: "Filter((((t0.v1 = 78) AND (t0.v2 = 87)) OR ((t0.v1 BETWEEN 37 AND 58) AND (t0.v2 >= 30))) AND (t0.v1 = 86))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>94) OR (v1<=52));`,
		ExpectedPlan: "Filter((NOT((t0.v1 = 94))) OR (t0.v1 <= 52))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<>23 AND v2>64) OR (v1>73 AND v2<=66)) OR (v1 BETWEEN 39 AND 69 AND v2>84));`,
		ExpectedPlan: "Filter((((NOT((t0.v1 = 23))) AND (t0.v2 > 64)) OR ((t0.v1 > 73) AND (t0.v2 <= 66))) OR ((t0.v1 BETWEEN 39 AND 69) AND (t0.v2 > 84)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>54 AND v2<16) OR (v1<74 AND v2>29)) AND (v1 BETWEEN 34 AND 48);`,
		ExpectedPlan: "Filter(((t0.v1 > 54) AND (t0.v2 < 16)) OR ((t0.v1 < 74) AND (t0.v2 > 29)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>44 AND v2>12) OR (v1<=5 AND v2>27));`,
		ExpectedPlan: "Filter(((NOT((t0.v1 = 44))) AND (t0.v2 > 12)) OR ((t0.v1 <= 5) AND (t0.v2 > 27)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=54 AND v2<>13) OR (v1>84));`,
		ExpectedPlan: "Filter(((t0.v1 <= 54) AND (NOT((t0.v2 = 13)))) OR (t0.v1 > 84))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>1 AND v2<>51) OR (v1=28));`,
		ExpectedPlan: "Filter(((t0.v1 > 1) AND (NOT((t0.v2 = 51)))) OR (t0.v1 = 28))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1>35) OR (v1 BETWEEN 11 AND 21)) OR (v1<>98));`,
		ExpectedPlan: "Filter(((t0.v1 > 35) OR (t0.v1 BETWEEN 11 AND 21)) OR (NOT((t0.v1 = 98))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1=16 AND v2=57) OR (v1<46 AND v2 BETWEEN 78 AND 89));`,
		ExpectedPlan: "Filter(((t0.v1 = 16) AND (t0.v2 = 57)) OR ((t0.v1 < 46) AND (t0.v2 BETWEEN 78 AND 89)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<53 AND v2<10) AND (v1<>37) OR (v1>23));`,
		ExpectedPlan: "Filter((((t0.v1 < 53) AND (t0.v2 < 10)) AND (NOT((t0.v1 = 37)))) OR (t0.v1 > 23))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((((v1<>30) OR (v1>=6 AND v2 BETWEEN 62 AND 65)) OR (v1<>89)) OR (v1<=40 AND v2>=73)) OR (v1<99));`,
		ExpectedPlan: "Filter(((((NOT((t0.v1 = 30))) OR ((t0.v1 >= 6) AND (t0.v2 BETWEEN 62 AND 65))) OR (NOT((t0.v1 = 89)))) OR ((t0.v1 <= 40) AND (t0.v2 >= 73))) OR (t0.v1 < 99))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1 BETWEEN 34 AND 34 AND v2 BETWEEN 0 AND 91) OR (v1 BETWEEN 54 AND 77 AND v2>92));`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 34 AND 34) AND (t0.v2 BETWEEN 0 AND 91)) OR ((t0.v1 BETWEEN 54 AND 77) AND (t0.v2 > 92)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((((v1<=55) OR (v1>=46 AND v2<=26)) OR (v1 BETWEEN 8 AND 54)) OR (v1>26 AND v2 BETWEEN 62 AND 89)) OR (v1<31 AND v2=11)) OR (v1>9 AND v2=60));`,
		ExpectedPlan: "Filter((((((t0.v1 <= 55) OR ((t0.v1 >= 46) AND (t0.v2 <= 26))) OR (t0.v1 BETWEEN 8 AND 54)) OR ((t0.v1 > 26) AND (t0.v2 BETWEEN 62 AND 89))) OR ((t0.v1 < 31) AND (t0.v2 = 11))) OR ((t0.v1 > 9) AND (t0.v2 = 60)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1 BETWEEN 17 AND 54 AND v2>=37) AND (v1<42 AND v2=96) OR (v1<>50));`,
		ExpectedPlan: "Filter((((t0.v1 BETWEEN 17 AND 54) AND (t0.v2 >= 37)) AND ((t0.v1 < 42) AND (t0.v2 = 96))) OR (NOT((t0.v1 = 50))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>39 AND v2>66) OR (v1=99));`,
		ExpectedPlan: "Filter(((t0.v1 > 39) AND (t0.v2 > 66)) OR (t0.v1 = 99))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1 BETWEEN 24 AND 66) OR (v1<=81 AND v2<>29));`,
		ExpectedPlan: "Filter((t0.v1 BETWEEN 24 AND 66) OR ((t0.v1 <= 81) AND (NOT((t0.v2 = 29)))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<>18 AND v2<>8) OR (v1>=10 AND v2>3)) OR (v1=53));`,
		ExpectedPlan: "Filter((((NOT((t0.v1 = 18))) AND (NOT((t0.v2 = 8)))) OR ((t0.v1 >= 10) AND (t0.v2 > 3))) OR (t0.v1 = 53))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>=42 AND v2>34) OR (v1<=40 AND v2<=49));`,
		ExpectedPlan: "Filter(((t0.v1 >= 42) AND (t0.v2 > 34)) OR ((t0.v1 <= 40) AND (t0.v2 <= 49)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1 BETWEEN 8 AND 38) OR (v1>=23 AND v2 BETWEEN 36 AND 49));`,
		ExpectedPlan: "Filter((t0.v1 BETWEEN 8 AND 38) OR ((t0.v1 >= 23) AND (t0.v2 BETWEEN 36 AND 49)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>57 AND v2 BETWEEN 2 AND 93) OR (v1=52));`,
		ExpectedPlan: "Filter(((NOT((t0.v1 = 57))) AND (t0.v2 BETWEEN 2 AND 93)) OR (t0.v1 = 52))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((v1<24) OR (v1<41)) OR (v1<12 AND v2=2)) OR (v1=3 AND v2<>66));`,
		ExpectedPlan: "Filter((((t0.v1 < 24) OR (t0.v1 < 41)) OR ((t0.v1 < 12) AND (t0.v2 = 2))) OR ((t0.v1 = 3) AND (NOT((t0.v2 = 66)))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=52 AND v2<40) AND (v1<30) OR (v1<=75 AND v2 BETWEEN 54 AND 54)) OR (v1<>31 AND v2<>56));`,
		ExpectedPlan: "Filter(((((t0.v1 <= 52) AND (t0.v2 < 40)) AND (t0.v1 < 30)) OR ((t0.v1 <= 75) AND (t0.v2 BETWEEN 54 AND 54))) OR ((NOT((t0.v1 = 31))) AND (NOT((t0.v2 = 56)))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>52 AND v2<90) OR (v1 BETWEEN 27 AND 77 AND v2 BETWEEN 49 AND 83));`,
		ExpectedPlan: "Filter(((NOT((t0.v1 = 52))) AND (t0.v2 < 90)) OR ((t0.v1 BETWEEN 27 AND 77) AND (t0.v2 BETWEEN 49 AND 83)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>2) OR (v1<72 AND v2>=21)) AND (v1=69 AND v2 BETWEEN 44 AND 48);`,
		ExpectedPlan: "Filter(((t0.v1 > 2) OR ((t0.v1 < 72) AND (t0.v2 >= 21))) AND (t0.v1 = 69))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((v1>77) OR (v1=57)) OR (v1>9 AND v2>80)) OR (v1=22));`,
		ExpectedPlan: "Filter((((t0.v1 > 77) OR (t0.v1 = 57)) OR ((t0.v1 > 9) AND (t0.v2 > 80))) OR (t0.v1 = 22))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((v1>28) OR (v1<=30 AND v2=30)) OR (v1<29)) OR (v1 BETWEEN 54 AND 74));`,
		ExpectedPlan: "Filter((((t0.v1 > 28) OR ((t0.v1 <= 30) AND (t0.v2 = 30))) OR (t0.v1 < 29)) OR (t0.v1 BETWEEN 54 AND 74))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>30 AND v2 BETWEEN 20 AND 41) OR (v1>=69 AND v2=51));`,
		ExpectedPlan: "Filter(((NOT((t0.v1 = 30))) AND (t0.v2 BETWEEN 20 AND 41)) OR ((t0.v1 >= 69) AND (t0.v2 = 51)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>39) OR (v1=55)) AND (v1=67);`,
		ExpectedPlan: "Filter((NOT((t0.v1 = 39))) OR (t0.v1 = 55))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<20 AND v2<=46) OR (v1<>4 AND v2=26)) OR (v1>36 AND v2<>13));`,
		ExpectedPlan: "Filter((((t0.v1 < 20) AND (t0.v2 <= 46)) OR ((NOT((t0.v1 = 4))) AND (t0.v2 = 26))) OR ((t0.v1 > 36) AND (NOT((t0.v2 = 13)))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=5 AND v2>66) OR (v1<=0)) OR (v1 BETWEEN 10 AND 87));`,
		ExpectedPlan: "Filter((((t0.v1 <= 5) AND (t0.v2 > 66)) OR (t0.v1 <= 0)) OR (t0.v1 BETWEEN 10 AND 87))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((((v1<>99 AND v2 BETWEEN 12 AND 31) OR (v1<56 AND v2<>69)) OR (v1>=37 AND v2<47)) OR (v1<=98 AND v2=50)) AND (v1 BETWEEN 15 AND 47) OR (v1>55 AND v2>85)) OR (v1>86));`,
		ExpectedPlan: "Filter((((((((NOT((t0.v1 = 99))) AND (t0.v2 BETWEEN 12 AND 31)) OR ((t0.v1 < 56) AND (NOT((t0.v2 = 69))))) OR ((t0.v1 >= 37) AND (t0.v2 < 47))) OR ((t0.v1 <= 98) AND (t0.v2 = 50))) AND (t0.v1 BETWEEN 15 AND 47)) OR ((t0.v1 > 55) AND (t0.v2 > 85))) OR (t0.v1 > 86))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<37) OR (v1<=48 AND v2<=54)) OR (v1=88));`,
		ExpectedPlan: "Filter(((t0.v1 < 37) OR ((t0.v1 <= 48) AND (t0.v2 <= 54))) OR (t0.v1 = 88))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<>31) OR (v1<>43)) OR (v1>37 AND v2>5));`,
		ExpectedPlan: "Filter(((NOT((t0.v1 = 31))) OR (NOT((t0.v1 = 43)))) OR ((t0.v1 > 37) AND (t0.v2 > 5)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=91) OR (v1<>79)) OR (v1<64));`,
		ExpectedPlan: "Filter(((t0.v1 <= 91) OR (NOT((t0.v1 = 79)))) OR (t0.v1 < 64))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>48) OR (v1>11));`,
		ExpectedPlan: "Filter((NOT((t0.v1 = 48))) OR (t0.v1 > 11))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>40) OR (v1>=49 AND v2>=92));`,
		ExpectedPlan: "Filter((t0.v1 > 40) OR ((t0.v1 >= 49) AND (t0.v2 >= 92)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((v1<40) OR (v1<=59)) OR (v1<99)) AND (v1>=83) OR (v1>9));`,
		ExpectedPlan: "Filter(((((t0.v1 < 40) OR (t0.v1 <= 59)) OR (t0.v1 < 99)) AND (t0.v1 >= 83)) OR (t0.v1 > 9))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=53 AND v2<=79) OR (v1>50 AND v2>26)) AND (v1>26) AND (v1>43 AND v2<7);`,
		ExpectedPlan: "Filter(((((t0.v1 <= 53) AND (t0.v2 <= 79)) OR ((t0.v1 > 50) AND (t0.v2 > 26))) AND (t0.v1 > 26)) AND (t0.v1 > 43))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1 BETWEEN 27 AND 84) OR (v1<98 AND v2>38)) OR (v1<>30));`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 27 AND 84) OR ((t0.v1 < 98) AND (t0.v2 > 38))) OR (NOT((t0.v1 = 30))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1=45) OR (v1=28));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (v1 BETWEEN 11 AND 18) AND (v1>31 AND v2 BETWEEN 38 AND 88);`,
		ExpectedPlan: "Filter((t0.v1 BETWEEN 11 AND 18) AND (t0.v1 > 31))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>95 AND v2>5) OR (v1>16 AND v2>=38));`,
		ExpectedPlan: "Filter(((t0.v1 > 95) AND (t0.v2 > 5)) OR ((t0.v1 > 16) AND (t0.v2 >= 38)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>=23) OR (v1=47 AND v2>23));`,
		ExpectedPlan: "Filter((t0.v1 >= 23) OR ((t0.v1 = 47) AND (t0.v2 > 23)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1=30) OR (v1<>67));`,
		ExpectedPlan: "Filter((t0.v1 = 30) OR (NOT((t0.v1 = 67))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>=30 AND v2>=67) OR (v1<=52));`,
		ExpectedPlan: "Filter(((t0.v1 >= 30) AND (t0.v2 >= 67)) OR (t0.v1 <= 52))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1 BETWEEN 48 AND 86 AND v2>=29) OR (v1<>82 AND v2<=93)) OR (v1 BETWEEN 79 AND 87 AND v2 BETWEEN 13 AND 69));`,
		ExpectedPlan: "Filter((((t0.v1 BETWEEN 48 AND 86) AND (t0.v2 >= 29)) OR ((NOT((t0.v1 = 82))) AND (t0.v2 <= 93))) OR ((t0.v1 BETWEEN 79 AND 87) AND (t0.v2 BETWEEN 13 AND 69)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1 BETWEEN 3 AND 95 AND v2>=36) OR (v1>=40 AND v2<13)) OR (v1 BETWEEN 4 AND 8 AND v2=50));`,
		ExpectedPlan: "Filter((((t0.v1 BETWEEN 3 AND 95) AND (t0.v2 >= 36)) OR ((t0.v1 >= 40) AND (t0.v2 < 13))) OR ((t0.v1 BETWEEN 4 AND 8) AND (t0.v2 = 50)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<11 AND v2<>32) OR (v1 BETWEEN 35 AND 41)) OR (v1>=76));`,
		ExpectedPlan: "Filter((((t0.v1 < 11) AND (NOT((t0.v2 = 32)))) OR (t0.v1 BETWEEN 35 AND 41)) OR (t0.v1 >= 76))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1=15 AND v2=8) AND (v1>2) OR (v1 BETWEEN 50 AND 97));`,
		ExpectedPlan: "Filter((((t0.v1 = 15) AND (t0.v2 = 8)) AND (t0.v1 > 2)) OR (t0.v1 BETWEEN 50 AND 97))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<67 AND v2<>39) OR (v1>36));`,
		ExpectedPlan: "Filter(((t0.v1 < 67) AND (NOT((t0.v2 = 39)))) OR (t0.v1 > 36))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>66) OR (v1<50));`,
		ExpectedPlan: "Filter((NOT((t0.v1 = 66))) OR (t0.v1 < 50))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1 BETWEEN 5 AND 19) OR (v1<>50 AND v2>=51)) OR (v1>55));`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 5 AND 19) OR ((NOT((t0.v1 = 50))) AND (t0.v2 >= 51))) OR (t0.v1 > 55))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1 BETWEEN 16 AND 65) OR (v1<>18 AND v2>=81)) OR (v1 BETWEEN 6 AND 48));`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 16 AND 65) OR ((NOT((t0.v1 = 18))) AND (t0.v2 >= 81))) OR (t0.v1 BETWEEN 6 AND 48))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1>=31 AND v2>=55) OR (v1 BETWEEN 1 AND 28)) OR (v1 BETWEEN 26 AND 41 AND v2<=15));`,
		ExpectedPlan: "Filter((((t0.v1 >= 31) AND (t0.v2 >= 55)) OR (t0.v1 BETWEEN 1 AND 28)) OR ((t0.v1 BETWEEN 26 AND 41) AND (t0.v2 <= 15)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<=77 AND v2 BETWEEN 4 AND 26) OR (v1<=1 AND v2<>20)) OR (v1>8 AND v2>40));`,
		ExpectedPlan: "Filter((((t0.v1 <= 77) AND (t0.v2 BETWEEN 4 AND 26)) OR ((t0.v1 <= 1) AND (NOT((t0.v2 = 20))))) OR ((t0.v1 > 8) AND (t0.v2 > 40)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((((v1=37 AND v2>32) OR (v1>13 AND v2>51)) AND (v1 BETWEEN 8 AND 19) OR (v1<>4)) OR (v1<=58 AND v2<>70)) OR (v1<87 AND v2>=24));`,
		ExpectedPlan: "Filter(((((((t0.v1 = 37) AND (t0.v2 > 32)) OR ((t0.v1 > 13) AND (t0.v2 > 51))) AND (t0.v1 BETWEEN 8 AND 19)) OR (NOT((t0.v1 = 4)))) OR ((t0.v1 <= 58) AND (NOT((t0.v2 = 70))))) OR ((t0.v1 < 87) AND (t0.v2 >= 24)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1<>50) OR (v1<=88)) OR (v1>=28 AND v2 BETWEEN 30 AND 85));`,
		ExpectedPlan: "Filter(((NOT((t0.v1 = 50))) OR (t0.v1 <= 88)) OR ((t0.v1 >= 28) AND (t0.v2 BETWEEN 30 AND 85)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=94) OR (v1<=87));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<>56 AND v2<93) OR (v1<73 AND v2<=70));`,
		ExpectedPlan: "Filter(((NOT((t0.v1 = 56))) AND (t0.v2 < 93)) OR ((t0.v1 < 73) AND (t0.v2 <= 70)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((v1>=85) OR (v1=91)) OR (v1<88 AND v2<42)) OR (v1<>42 AND v2<=10));`,
		ExpectedPlan: "Filter((((t0.v1 >= 85) OR (t0.v1 = 91)) OR ((t0.v1 < 88) AND (t0.v2 < 42))) OR ((NOT((t0.v1 = 42))) AND (t0.v2 <= 10)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>42 AND v2<=13) OR (v1=7));`,
		ExpectedPlan: "Filter(((t0.v1 > 42) AND (t0.v2 <= 13)) OR (t0.v1 = 7))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1=63) OR (v1 BETWEEN 55 AND 82 AND v2 BETWEEN 0 AND 6)) OR (v1=46));`,
		ExpectedPlan: "Filter(((t0.v1 = 63) OR ((t0.v1 BETWEEN 55 AND 82) AND (t0.v2 BETWEEN 0 AND 6))) OR (t0.v1 = 46))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1 BETWEEN 20 AND 77 AND v2>=49) OR (v1<13));`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 20 AND 77) AND (t0.v2 >= 49)) OR (t0.v1 < 13))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1>=72) OR (v1<49 AND v2<>36)) OR (v1>=10 AND v2<1));`,
		ExpectedPlan: "Filter(((t0.v1 >= 72) OR ((t0.v1 < 49) AND (NOT((t0.v2 = 36))))) OR ((t0.v1 >= 10) AND (t0.v2 < 1)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE (((v1 BETWEEN 18 AND 87) OR (v1>=42 AND v2>44)) OR (v1<26 AND v2<=55)) AND (v1<=21);`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 18 AND 87) OR ((t0.v1 >= 42) AND (t0.v2 > 44))) OR ((t0.v1 < 26) AND (t0.v2 <= 55)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>98 AND v2<75) OR (v1=47));`,
		ExpectedPlan: "Filter(((t0.v1 > 98) AND (t0.v2 < 75)) OR (t0.v1 = 47))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=57 AND v2>=43) OR (v1<27 AND v2<>3));`,
		ExpectedPlan: "Filter(((t0.v1 <= 57) AND (t0.v2 >= 43)) OR ((t0.v1 < 27) AND (NOT((t0.v2 = 3)))))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1 BETWEEN 16 AND 45 AND v2=22) OR (v1>=87 AND v2=48));`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 16 AND 45) AND (t0.v2 = 22)) OR ((t0.v1 >= 87) AND (t0.v2 = 48)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1 BETWEEN 45 AND 74 AND v2<=74) OR (v1<>48 AND v2>58));`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 45 AND 74) AND (t0.v2 <= 74)) OR ((NOT((t0.v1 = 48))) AND (t0.v2 > 58)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((((v1<32 AND v2>=79) OR (v1<=28)) OR (v1 BETWEEN 46 AND 72)) OR (v1>16));`,
		ExpectedPlan: "Filter(((((t0.v1 < 32) AND (t0.v2 >= 79)) OR (t0.v1 <= 28)) OR (t0.v1 BETWEEN 46 AND 72)) OR (t0.v1 > 16))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<10) OR (v1<89));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1>=64 AND v2>=69) OR (v1>=2));`,
		ExpectedPlan: "Filter(((t0.v1 >= 64) AND (t0.v2 >= 69)) OR (t0.v1 >= 2))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1<=65) OR (v1<64));`,
		ExpectedPlan: "Projected table access on [pk v1 v2]\n" +
			" └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1=46) OR (v1>9 AND v2>=22));`,
		ExpectedPlan: "Filter((t0.v1 = 46) OR ((t0.v1 > 9) AND (t0.v2 >= 22)))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t0 WHERE ((v1 BETWEEN 21 AND 33 AND v2>25) OR (v1<0));`,
		ExpectedPlan: "Filter(((t0.v1 BETWEEN 21 AND 33) AND (t0.v2 > 25)) OR (t0.v1 < 0))\n" +
			" └─ Projected table access on [pk v1 v2]\n" +
			"     └─ IndexedTableAccess(t0 on [t0.v1,t0.v2])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>87 AND v2 BETWEEN 8 AND 33) OR (v1 BETWEEN 39 AND 69 AND v3<4));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 87))) AND (t1.v2 BETWEEN 8 AND 33)) OR ((t1.v1 BETWEEN 39 AND 69) AND (t1.v3 < 4)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=55 AND v2>=72 AND v3=63) AND (v1<>54 AND v2 BETWEEN 3 AND 80) OR (v1=15)) AND (v1<>50);`,
		ExpectedPlan: "Filter(((((t1.v1 >= 55) AND (t1.v2 >= 72)) AND (t1.v3 = 63)) AND ((NOT((t1.v1 = 54))) AND (t1.v2 BETWEEN 3 AND 80))) OR (t1.v1 = 15))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<93 AND v2<39 AND v3 BETWEEN 30 AND 97) OR (v1>54)) OR (v1<66));`,
		ExpectedPlan: "Filter(((((t1.v1 < 93) AND (t1.v2 < 39)) AND (t1.v3 BETWEEN 30 AND 97)) OR (t1.v1 > 54)) OR (t1.v1 < 66))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>59 AND v2<=15) OR (v1 BETWEEN 2 AND 51)) OR (v1>15 AND v2 BETWEEN 31 AND 81));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 59))) AND (t1.v2 <= 15)) OR (t1.v1 BETWEEN 2 AND 51)) OR ((t1.v1 > 15) AND (t1.v2 BETWEEN 31 AND 81)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<3 AND v2<>23 AND v3<>11) OR (v1<>49)) AND (v1<=41 AND v2>40);`,
		ExpectedPlan: "Filter(((((t1.v1 < 3) AND (NOT((t1.v2 = 23)))) AND (NOT((t1.v3 = 11)))) OR (NOT((t1.v1 = 49)))) AND (t1.v1 <= 41))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1 BETWEEN 28 AND 38 AND v3<33) OR (v1 BETWEEN 75 AND 85)) AND (v1>=60) OR (v1>=53 AND v2 BETWEEN 36 AND 53 AND v3>48));`,
		ExpectedPlan: "Filter(((((t1.v1 BETWEEN 28 AND 38) AND (t1.v3 < 33)) OR (t1.v1 BETWEEN 75 AND 85)) AND (t1.v1 >= 60)) OR (((t1.v1 >= 53) AND (t1.v2 BETWEEN 36 AND 53)) AND (t1.v3 > 48)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<6 AND v2<>44) OR (v1 BETWEEN 27 AND 96)) OR (v1>22 AND v2<>30 AND v3<49));`,
		ExpectedPlan: "Filter((((t1.v1 < 6) AND (NOT((t1.v2 = 44)))) OR (t1.v1 BETWEEN 27 AND 96)) OR (((t1.v1 > 22) AND (NOT((t1.v2 = 30)))) AND (t1.v3 < 49)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>29 AND v2=40) OR (v1<=74)) OR (v1<13 AND v2 BETWEEN 27 AND 82 AND v3<82));`,
		ExpectedPlan: "Filter((((t1.v1 > 29) AND (t1.v2 = 40)) OR (t1.v1 <= 74)) OR (((t1.v1 < 13) AND (t1.v2 BETWEEN 27 AND 82)) AND (t1.v3 < 82)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>6 AND v2 BETWEEN 0 AND 97) OR (v1<>40 AND v3<10 AND v2<>10));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 6))) AND (t1.v2 BETWEEN 0 AND 97)) OR (((NOT((t1.v1 = 40))) AND (t1.v3 < 10)) AND (NOT((t1.v2 = 10)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1>=35) OR (v1=86)) OR (v1>41 AND v2>=92)) OR (v1<>28));`,
		ExpectedPlan: "Filter((((t1.v1 >= 35) OR (t1.v1 = 86)) OR ((t1.v1 > 41) AND (t1.v2 >= 92))) OR (NOT((t1.v1 = 28))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<16 AND v3=63 AND v2>=20) OR (v1<>41)) OR (v1<=74 AND v3 BETWEEN 14 AND 74 AND v2<>13));`,
		ExpectedPlan: "Filter(((((t1.v1 < 16) AND (t1.v3 = 63)) AND (t1.v2 >= 20)) OR (NOT((t1.v1 = 41)))) OR (((t1.v1 <= 74) AND (t1.v3 BETWEEN 14 AND 74)) AND (NOT((t1.v2 = 13)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1 BETWEEN 1 AND 11) OR (v1>2 AND v3<=93 AND v2 BETWEEN 28 AND 84)) OR (v1 BETWEEN 34 AND 52 AND v2=73)) OR (v1<>80 AND v2<=32 AND v3 BETWEEN 3 AND 7));`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 1 AND 11) OR (((t1.v1 > 2) AND (t1.v3 <= 93)) AND (t1.v2 BETWEEN 28 AND 84))) OR ((t1.v1 BETWEEN 34 AND 52) AND (t1.v2 = 73))) OR (((NOT((t1.v1 = 80))) AND (t1.v2 <= 32)) AND (t1.v3 BETWEEN 3 AND 7)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1<45) OR (v1<>72)) OR (v1 BETWEEN 10 AND 86 AND v2=92)) OR (v1 BETWEEN 32 AND 81 AND v2>59));`,
		ExpectedPlan: "Filter((((t1.v1 < 45) OR (NOT((t1.v1 = 72)))) OR ((t1.v1 BETWEEN 10 AND 86) AND (t1.v2 = 92))) OR ((t1.v1 BETWEEN 32 AND 81) AND (t1.v2 > 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=11 AND v2>50 AND v3 BETWEEN 5 AND 67) AND (v1>74 AND v2 BETWEEN 6 AND 63 AND v3<=1) OR (v1>=53 AND v2>69 AND v3>54));`,
		ExpectedPlan: "Filter(((((t1.v1 >= 11) AND (t1.v2 > 50)) AND (t1.v3 BETWEEN 5 AND 67)) AND (((t1.v1 > 74) AND (t1.v2 BETWEEN 6 AND 63)) AND (t1.v3 <= 1))) OR (((t1.v1 >= 53) AND (t1.v2 > 69)) AND (t1.v3 > 54)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>9) OR (v1>14 AND v2>10));`,
		ExpectedPlan: "Filter((t1.v1 > 9) OR ((t1.v1 > 14) AND (t1.v2 > 10)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<=39 AND v2 BETWEEN 17 AND 34) OR (v1=89 AND v3>49 AND v2>58)) OR (v1>97));`,
		ExpectedPlan: "Filter((((t1.v1 <= 39) AND (t1.v2 BETWEEN 17 AND 34)) OR (((t1.v1 = 89) AND (t1.v3 > 49)) AND (t1.v2 > 58))) OR (t1.v1 > 97))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<7 AND v2<>43) OR (v1<>5 AND v3<0 AND v2<1));`,
		ExpectedPlan: "Filter(((t1.v1 < 7) AND (NOT((t1.v2 = 43)))) OR (((NOT((t1.v1 = 5))) AND (t1.v3 < 0)) AND (t1.v2 < 1)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1>30 AND v2 BETWEEN 23 AND 60 AND v3=58) OR (v1<=3 AND v2 BETWEEN 68 AND 72)) OR (v1<=17)) OR (v1>6 AND v2>=24)) AND (v1<89 AND v2=73);`,
		ExpectedPlan: "Filter(((((((t1.v1 > 30) AND (t1.v2 BETWEEN 23 AND 60)) AND (t1.v3 = 58)) OR ((t1.v1 <= 3) AND (t1.v2 BETWEEN 68 AND 72))) OR (t1.v1 <= 17)) OR ((t1.v1 > 6) AND (t1.v2 >= 24))) AND (t1.v1 < 89))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>27) OR (v1>=22 AND v2>99 AND v3>=43));`,
		ExpectedPlan: "Filter((t1.v1 > 27) OR (((t1.v1 >= 22) AND (t1.v2 > 99)) AND (t1.v3 >= 43)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>25 AND v2 BETWEEN 1 AND 82) OR (v1>31 AND v2=86));`,
		ExpectedPlan: "Filter(((t1.v1 > 25) AND (t1.v2 BETWEEN 1 AND 82)) OR ((t1.v1 > 31) AND (t1.v2 = 86)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>12 AND v2<60 AND v3=91) OR (v1>63 AND v2>=8 AND v3<>32)) OR (v1>35 AND v3>=98));`,
		ExpectedPlan: "Filter(((((NOT((t1.v1 = 12))) AND (t1.v2 < 60)) AND (t1.v3 = 91)) OR (((t1.v1 > 63) AND (t1.v2 >= 8)) AND (NOT((t1.v3 = 32))))) OR ((t1.v1 > 35) AND (t1.v3 >= 98)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>27 AND v3=10) OR (v1>=25 AND v2<26)) AND (v1>=62 AND v2<=96 AND v3>28);`,
		ExpectedPlan: "Filter(((((t1.v1 > 27) AND (t1.v3 = 10)) OR ((t1.v1 >= 25) AND (t1.v2 < 26))) AND (t1.v1 >= 62)) AND (t1.v2 <= 96))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>30 AND v2=40 AND v3 BETWEEN 35 AND 35) OR (v1 BETWEEN 20 AND 77 AND v2>=56 AND v3>62));`,
		ExpectedPlan: "Filter((((t1.v1 > 30) AND (t1.v2 = 40)) AND (t1.v3 BETWEEN 35 AND 35)) OR (((t1.v1 BETWEEN 20 AND 77) AND (t1.v2 >= 56)) AND (t1.v3 > 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((((v1<=92 AND v3=0 AND v2>=9) OR (v1 BETWEEN 48 AND 79)) OR (v1>70 AND v2<=26 AND v3 BETWEEN 14 AND 82)) OR (v1>=29 AND v2<>21 AND v3 BETWEEN 37 AND 55)) OR (v1>=6 AND v3<=47));`,
		ExpectedPlan: "Filter(((((((t1.v1 <= 92) AND (t1.v3 = 0)) AND (t1.v2 >= 9)) OR (t1.v1 BETWEEN 48 AND 79)) OR (((t1.v1 > 70) AND (t1.v2 <= 26)) AND (t1.v3 BETWEEN 14 AND 82))) OR (((t1.v1 >= 29) AND (NOT((t1.v2 = 21)))) AND (t1.v3 BETWEEN 37 AND 55))) OR ((t1.v1 >= 6) AND (t1.v3 <= 47)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=15 AND v2>28) OR (v1<=84 AND v2<>91));`,
		ExpectedPlan: "Filter(((t1.v1 <= 15) AND (t1.v2 > 28)) OR ((t1.v1 <= 84) AND (NOT((t1.v2 = 91)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1=49 AND v2<=52 AND v3 BETWEEN 23 AND 38) OR (v1 BETWEEN 30 AND 84 AND v2=94));`,
		ExpectedPlan: "Filter((((t1.v1 = 49) AND (t1.v2 <= 52)) AND (t1.v3 BETWEEN 23 AND 38)) OR ((t1.v1 BETWEEN 30 AND 84) AND (t1.v2 = 94)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 8 AND 18) OR (v1=27 AND v2<=4 AND v3<14));`,
		ExpectedPlan: "Filter((t1.v1 BETWEEN 8 AND 18) OR (((t1.v1 = 27) AND (t1.v2 <= 4)) AND (t1.v3 < 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=4) OR (v1=0 AND v2<=63));`,
		ExpectedPlan: "Filter((t1.v1 >= 4) OR ((t1.v1 = 0) AND (t1.v2 <= 63)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1<=99 AND v2<>86) AND (v1>=21 AND v2>36);`,
		ExpectedPlan: "Filter((t1.v1 <= 99) AND (t1.v1 >= 21))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>43) OR (v1=14));`,
		ExpectedPlan: "Filter((NOT((t1.v1 = 43))) OR (t1.v1 = 14))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1 BETWEEN 21 AND 44 AND v2 BETWEEN 18 AND 88 AND v3=42) AND (v1>=52 AND v2>37 AND v3 BETWEEN 26 AND 91);`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 21 AND 44) AND (t1.v2 BETWEEN 18 AND 88)) AND (t1.v1 >= 52)) AND (t1.v2 > 37))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>29 AND v2>93 AND v3<64) OR (v1<>54 AND v2>35));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 29))) AND (t1.v2 > 93)) AND (t1.v3 < 64)) OR ((NOT((t1.v1 = 54))) AND (t1.v2 > 35)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<88) OR (v1<>45 AND v2<89)) AND (v1=98 AND v2<=81 AND v3 BETWEEN 34 AND 77);`,
		ExpectedPlan: "Filter((((t1.v1 < 88) OR ((NOT((t1.v1 = 45))) AND (t1.v2 < 89))) AND (t1.v1 = 98)) AND (t1.v2 <= 81))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>65 AND v2<>86 AND v3<=2) OR (v1<>37 AND v2<=96));`,
		ExpectedPlan: "Filter((((t1.v1 > 65) AND (NOT((t1.v2 = 86)))) AND (t1.v3 <= 2)) OR ((NOT((t1.v1 = 37))) AND (t1.v2 <= 96)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>79) OR (v1>66)) AND (v1<>81 AND v2<34 AND v3>=25) AND (v1<42) OR (v1<>12 AND v2<>17 AND v3<=23));`,
		ExpectedPlan: "Filter(((((NOT((t1.v1 = 79))) OR (t1.v1 > 66)) AND (((NOT((t1.v1 = 81))) AND (t1.v2 < 34)) AND (t1.v3 >= 25))) AND (t1.v1 < 42)) OR (((NOT((t1.v1 = 12))) AND (NOT((t1.v2 = 17)))) AND (t1.v3 <= 23)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<81 AND v2>=28) OR (v1=19 AND v2 BETWEEN 9 AND 57));`,
		ExpectedPlan: "Filter(((t1.v1 < 81) AND (t1.v2 >= 28)) OR ((t1.v1 = 19) AND (t1.v2 BETWEEN 9 AND 57)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<32) OR (v1>=52)) OR (v1>=98));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>47) OR (v1<>25));`,
		ExpectedPlan: "Filter((t1.v1 > 47) OR (NOT((t1.v1 = 25))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1>27 AND v2<=80 AND v3 BETWEEN 11 AND 37) AND (v1=87 AND v2<54) AND (v1>29);`,
		ExpectedPlan: "Filter(((((t1.v1 > 27) AND (t1.v2 <= 80)) AND (t1.v1 = 87)) AND (t1.v2 < 54)) AND (t1.v1 > 29))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>65 AND v2>=52) OR (v1<=85)) OR (v1<=64 AND v3=9 AND v2>=36));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 65))) AND (t1.v2 >= 52)) OR (t1.v1 <= 85)) OR (((t1.v1 <= 64) AND (t1.v3 = 9)) AND (t1.v2 >= 36)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=12 AND v2>=65) OR (v1=11 AND v2<1));`,
		ExpectedPlan: "Filter(((t1.v1 >= 12) AND (t1.v2 >= 65)) OR ((t1.v1 = 11) AND (t1.v2 < 1)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=92 AND v2<=42) OR (v1>=58));`,
		ExpectedPlan: "Filter(((t1.v1 <= 92) AND (t1.v2 <= 42)) OR (t1.v1 >= 58))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>0) OR (v1<81 AND v2>=70)) OR (v1>=52));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 0))) OR ((t1.v1 < 81) AND (t1.v2 >= 70))) OR (t1.v1 >= 52))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>5 AND v3<=32) OR (v1 BETWEEN 77 AND 85 AND v3 BETWEEN 16 AND 21 AND v2 BETWEEN 10 AND 42));`,
		ExpectedPlan: "Filter(((t1.v1 > 5) AND (t1.v3 <= 32)) OR (((t1.v1 BETWEEN 77 AND 85) AND (t1.v3 BETWEEN 16 AND 21)) AND (t1.v2 BETWEEN 10 AND 42)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>43 AND v2<53 AND v3<=20) OR (v1<7 AND v2<>79));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 43))) AND (t1.v2 < 53)) AND (t1.v3 <= 20)) OR ((t1.v1 < 7) AND (NOT((t1.v2 = 79)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1>=17 AND v2 BETWEEN 17 AND 78 AND v3=10) AND (v1<=67) AND (v1>=81 AND v2<=88 AND v3>=70);`,
		ExpectedPlan: "Filter(((((t1.v1 >= 17) AND (t1.v2 BETWEEN 17 AND 78)) AND (t1.v1 <= 67)) AND (t1.v1 >= 81)) AND (t1.v2 <= 88))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<77 AND v2<35 AND v3=73) OR (v1=85 AND v2>0 AND v3<65)) AND (v1>=20 AND v3<23 AND v2<=81) OR (v1<34 AND v2<=21 AND v3<=45));`,
		ExpectedPlan: "Filter((((((t1.v1 < 77) AND (t1.v2 < 35)) AND (t1.v3 = 73)) OR (((t1.v1 = 85) AND (t1.v2 > 0)) AND (t1.v3 < 65))) AND (((t1.v1 >= 20) AND (t1.v3 < 23)) AND (t1.v2 <= 81))) OR (((t1.v1 < 34) AND (t1.v2 <= 21)) AND (t1.v3 <= 45)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((((v1<=69) AND (v1>=60 AND v2<18 AND v3=15) OR (v1<=75)) OR (v1>=52 AND v2<10)) OR (v1<37 AND v2<=64)) OR (v1>38 AND v2=27));`,
		ExpectedPlan: "Filter((((((t1.v1 <= 69) AND (((t1.v1 >= 60) AND (t1.v2 < 18)) AND (t1.v3 = 15))) OR (t1.v1 <= 75)) OR ((t1.v1 >= 52) AND (t1.v2 < 10))) OR ((t1.v1 < 37) AND (t1.v2 <= 64))) OR ((t1.v1 > 38) AND (t1.v2 = 27)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1<=76) AND (v1<=94);`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1<>40 AND v2>1) OR (v1>3 AND v2<=42)) OR (v1=99 AND v2>62)) OR (v1<17 AND v2<>75 AND v3=6));`,
		ExpectedPlan: "Filter(((((NOT((t1.v1 = 40))) AND (t1.v2 > 1)) OR ((t1.v1 > 3) AND (t1.v2 <= 42))) OR ((t1.v1 = 99) AND (t1.v2 > 62))) OR (((t1.v1 < 17) AND (NOT((t1.v2 = 75)))) AND (t1.v3 = 6)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1=39) OR (v1=40 AND v2<>49)) OR (v1<>35 AND v2>4 AND v3>26)) OR (v1=32 AND v2<>55));`,
		ExpectedPlan: "Filter((((t1.v1 = 39) OR ((t1.v1 = 40) AND (NOT((t1.v2 = 49))))) OR (((NOT((t1.v1 = 35))) AND (t1.v2 > 4)) AND (t1.v3 > 26))) OR ((t1.v1 = 32) AND (NOT((t1.v2 = 55)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1=16 AND v2<>25 AND v3<>3) OR (v1>=4 AND v2 BETWEEN 4 AND 93 AND v3>39));`,
		ExpectedPlan: "Filter((((t1.v1 = 16) AND (NOT((t1.v2 = 25)))) AND (NOT((t1.v3 = 3)))) OR (((t1.v1 >= 4) AND (t1.v2 BETWEEN 4 AND 93)) AND (t1.v3 > 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1>=51 AND v2<83) OR (v1>=15 AND v2>=3)) OR (v1<=49)) OR (v1<69));`,
		ExpectedPlan: "Filter(((((t1.v1 >= 51) AND (t1.v2 < 83)) OR ((t1.v1 >= 15) AND (t1.v2 >= 3))) OR (t1.v1 <= 49)) OR (t1.v1 < 69))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1<>43 AND v2>10) AND (v1>30 AND v2 BETWEEN 18 AND 78 AND v3 BETWEEN 75 AND 81);`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 43))) AND (t1.v2 > 10)) AND (t1.v1 > 30)) AND (t1.v2 BETWEEN 18 AND 78))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>1) OR (v1<34 AND v2>=57 AND v3 BETWEEN 15 AND 67));`,
		ExpectedPlan: "Filter((t1.v1 > 1) OR (((t1.v1 < 34) AND (t1.v2 >= 57)) AND (t1.v3 BETWEEN 15 AND 67)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>3 AND v2>32) OR (v1<=26 AND v3>=27 AND v2>=5));`,
		ExpectedPlan: "Filter(((t1.v1 > 3) AND (t1.v2 > 32)) OR (((t1.v1 <= 26) AND (t1.v3 >= 27)) AND (t1.v2 >= 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>25 AND v2<>70 AND v3<=51) OR (v1<=71 AND v2>59));`,
		ExpectedPlan: "Filter((((t1.v1 > 25) AND (NOT((t1.v2 = 70)))) AND (t1.v3 <= 51)) OR ((t1.v1 <= 71) AND (t1.v2 > 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1 BETWEEN 0 AND 61 AND v2<0) OR (v1 BETWEEN 0 AND 38 AND v2>34)) OR (v1>=13 AND v2>=41));`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 0 AND 61) AND (t1.v2 < 0)) OR ((t1.v1 BETWEEN 0 AND 38) AND (t1.v2 > 34))) OR ((t1.v1 >= 13) AND (t1.v2 >= 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>68 AND v2<=57) AND (v1<>84 AND v3 BETWEEN 24 AND 98 AND v2 BETWEEN 28 AND 45) OR (v1>0 AND v2<>47 AND v3>=69)) OR (v1>=44));`,
		ExpectedPlan: "Filter(((((NOT((t1.v1 = 68))) AND (t1.v2 <= 57)) AND (((NOT((t1.v1 = 84))) AND (t1.v3 BETWEEN 24 AND 98)) AND (t1.v2 BETWEEN 28 AND 45))) OR (((t1.v1 > 0) AND (NOT((t1.v2 = 47)))) AND (t1.v3 >= 69))) OR (t1.v1 >= 44))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=48 AND v2 BETWEEN 33 AND 66) OR (v1>=91));`,
		ExpectedPlan: "Filter(((t1.v1 <= 48) AND (t1.v2 BETWEEN 33 AND 66)) OR (t1.v1 >= 91))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1 BETWEEN 17 AND 52 AND v2<96) OR (v1<=12 AND v2<>4 AND v3>53)) OR (v1<98 AND v3<94 AND v2=5));`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 17 AND 52) AND (t1.v2 < 96)) OR (((t1.v1 <= 12) AND (NOT((t1.v2 = 4)))) AND (t1.v3 > 53))) OR (((t1.v1 < 98) AND (t1.v3 < 94)) AND (t1.v2 = 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>26 AND v2 BETWEEN 66 AND 79 AND v3<=94) OR (v1 BETWEEN 16 AND 55));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 26))) AND (t1.v2 BETWEEN 66 AND 79)) AND (t1.v3 <= 94)) OR (t1.v1 BETWEEN 16 AND 55))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1 BETWEEN 36 AND 67 AND v3<74 AND v2=26) AND (v1 BETWEEN 9 AND 10 AND v2=96) AND (v1<=11 AND v2<>63 AND v3>=62);`,
		ExpectedPlan: "Filter((((((t1.v1 BETWEEN 36 AND 67) AND (t1.v2 = 26)) AND (t1.v1 BETWEEN 9 AND 10)) AND (t1.v2 = 96)) AND (t1.v1 <= 11)) AND (NOT((t1.v2 = 63))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 28 AND 49 AND v2<47) OR (v1>37 AND v2 BETWEEN 45 AND 61 AND v3<73));`,
		ExpectedPlan: "Filter(((t1.v1 BETWEEN 28 AND 49) AND (t1.v2 < 47)) OR (((t1.v1 > 37) AND (t1.v2 BETWEEN 45 AND 61)) AND (t1.v3 < 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<37 AND v2>=26 AND v3<=14) OR (v1<64)) OR (v1 BETWEEN 31 AND 53 AND v2>55 AND v3<=55));`,
		ExpectedPlan: "Filter(((((t1.v1 < 37) AND (t1.v2 >= 26)) AND (t1.v3 <= 14)) OR (t1.v1 < 64)) OR (((t1.v1 BETWEEN 31 AND 53) AND (t1.v2 > 55)) AND (t1.v3 <= 55)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=77) OR (v1<50)) AND (v1<=53 AND v2>35 AND v3<>98);`,
		ExpectedPlan: "Filter((t1.v1 <= 53) AND (t1.v2 > 35))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1=2 AND v2=40 AND v3 BETWEEN 18 AND 67) OR (v1=14 AND v2<=24 AND v3<=87)) OR (v1 BETWEEN 8 AND 31 AND v2>86)) OR (v1>30));`,
		ExpectedPlan: "Filter((((((t1.v1 = 2) AND (t1.v2 = 40)) AND (t1.v3 BETWEEN 18 AND 67)) OR (((t1.v1 = 14) AND (t1.v2 <= 24)) AND (t1.v3 <= 87))) OR ((t1.v1 BETWEEN 8 AND 31) AND (t1.v2 > 86))) OR (t1.v1 > 30))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>46 AND v2<>49 AND v3<=44) OR (v1 BETWEEN 64 AND 80 AND v2=41 AND v3<=68));`,
		ExpectedPlan: "Filter((((t1.v1 > 46) AND (NOT((t1.v2 = 49)))) AND (t1.v3 <= 44)) OR (((t1.v1 BETWEEN 64 AND 80) AND (t1.v2 = 41)) AND (t1.v3 <= 68)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1=95 AND v3<47 AND v2>=97) OR (v1 BETWEEN 11 AND 36 AND v2<=83));`,
		ExpectedPlan: "Filter((((t1.v1 = 95) AND (t1.v3 < 47)) AND (t1.v2 >= 97)) OR ((t1.v1 BETWEEN 11 AND 36) AND (t1.v2 <= 83)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=65 AND v2=39 AND v3 BETWEEN 49 AND 67) OR (v1<57 AND v2>35));`,
		ExpectedPlan: "Filter((((t1.v1 >= 65) AND (t1.v2 = 39)) AND (t1.v3 BETWEEN 49 AND 67)) OR ((t1.v1 < 57) AND (t1.v2 > 35)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1>71 AND v2=33) OR (v1<>85 AND v2<>50 AND v3 BETWEEN 34 AND 67)) OR (v1 BETWEEN 5 AND 47 AND v3 BETWEEN 13 AND 76 AND v2=4)) OR (v1=16 AND v2>=29 AND v3<>80));`,
		ExpectedPlan: "Filter(((((t1.v1 > 71) AND (t1.v2 = 33)) OR (((NOT((t1.v1 = 85))) AND (NOT((t1.v2 = 50)))) AND (t1.v3 BETWEEN 34 AND 67))) OR (((t1.v1 BETWEEN 5 AND 47) AND (t1.v3 BETWEEN 13 AND 76)) AND (t1.v2 = 4))) OR (((t1.v1 = 16) AND (t1.v2 >= 29)) AND (NOT((t1.v3 = 80)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=17 AND v2>38) AND (v1>=79) OR (v1<>38));`,
		ExpectedPlan: "Filter((((t1.v1 <= 17) AND (t1.v2 > 38)) AND (t1.v1 >= 79)) OR (NOT((t1.v1 = 38))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=4 AND v2=26) OR (v1>21 AND v2 BETWEEN 14 AND 64));`,
		ExpectedPlan: "Filter(((t1.v1 >= 4) AND (t1.v2 = 26)) OR ((t1.v1 > 21) AND (t1.v2 BETWEEN 14 AND 64)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>50) OR (v1<=58 AND v2<=95)) OR (v1=10));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 50))) OR ((t1.v1 <= 58) AND (t1.v2 <= 95))) OR (t1.v1 = 10))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1<=21 AND v2<>95) OR (v1<>23 AND v2 BETWEEN 15 AND 22)) OR (v1<=53 AND v2>=6)) OR (v1<=13 AND v2<>93 AND v3<15));`,
		ExpectedPlan: "Filter(((((t1.v1 <= 21) AND (NOT((t1.v2 = 95)))) OR ((NOT((t1.v1 = 23))) AND (t1.v2 BETWEEN 15 AND 22))) OR ((t1.v1 <= 53) AND (t1.v2 >= 6))) OR (((t1.v1 <= 13) AND (NOT((t1.v2 = 93)))) AND (t1.v3 < 15)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1<64 AND v2>=90 AND v3>41) AND (v1>=14 AND v2 BETWEEN 30 AND 70 AND v3>=25);`,
		ExpectedPlan: "Filter((((t1.v1 < 64) AND (t1.v2 >= 90)) AND (t1.v1 >= 14)) AND (t1.v2 BETWEEN 30 AND 70))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<27 AND v2<=43) OR (v1<62 AND v2<=99)) OR (v1<>48 AND v2<29 AND v3<>69));`,
		ExpectedPlan: "Filter((((t1.v1 < 27) AND (t1.v2 <= 43)) OR ((t1.v1 < 62) AND (t1.v2 <= 99))) OR (((NOT((t1.v1 = 48))) AND (t1.v2 < 29)) AND (NOT((t1.v3 = 69)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<11 AND v2<70 AND v3>27) OR (v1>=80 AND v2<31 AND v3<65)) OR (v1>=98 AND v2 BETWEEN 30 AND 85 AND v3>=30));`,
		ExpectedPlan: "Filter(((((t1.v1 < 11) AND (t1.v2 < 70)) AND (t1.v3 > 27)) OR (((t1.v1 >= 80) AND (t1.v2 < 31)) AND (t1.v3 < 65))) OR (((t1.v1 >= 98) AND (t1.v2 BETWEEN 30 AND 85)) AND (t1.v3 >= 30)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1<>44 AND v2>=10) AND (v1=47 AND v2=14 AND v3<30);`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 44))) AND (t1.v2 >= 10)) AND (t1.v1 = 47)) AND (t1.v2 = 14))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>6 AND v2=50) OR (v1>=16));`,
		ExpectedPlan: "Filter(((t1.v1 > 6) AND (t1.v2 = 50)) OR (t1.v1 >= 16))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=31) OR (v1>53 AND v2<>11 AND v3<>94)) OR (v1>48 AND v2 BETWEEN 11 AND 29 AND v3 BETWEEN 68 AND 72));`,
		ExpectedPlan: "Filter(((t1.v1 >= 31) OR (((t1.v1 > 53) AND (NOT((t1.v2 = 11)))) AND (NOT((t1.v3 = 94))))) OR (((t1.v1 > 48) AND (t1.v2 BETWEEN 11 AND 29)) AND (t1.v3 BETWEEN 68 AND 72)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 55 AND 59) OR (v1<=10 AND v2>=24)) AND (v1>93 AND v3<70 AND v2 BETWEEN 44 AND 79) AND (v1>=22 AND v2=27);`,
		ExpectedPlan: "Filter((((((t1.v1 BETWEEN 55 AND 59) OR ((t1.v1 <= 10) AND (t1.v2 >= 24))) AND (t1.v1 > 93)) AND (t1.v2 BETWEEN 44 AND 79)) AND (t1.v1 >= 22)) AND (t1.v2 = 27))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=43 AND v2<28 AND v3<>24) OR (v1<36 AND v2=14 AND v3 BETWEEN 16 AND 55));`,
		ExpectedPlan: "Filter((((t1.v1 >= 43) AND (t1.v2 < 28)) AND (NOT((t1.v3 = 24)))) OR (((t1.v1 < 36) AND (t1.v2 = 14)) AND (t1.v3 BETWEEN 16 AND 55)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>48 AND v2<=80) OR (v1=72 AND v3 BETWEEN 45 AND 52 AND v2=98));`,
		ExpectedPlan: "Filter(((t1.v1 > 48) AND (t1.v2 <= 80)) OR (((t1.v1 = 72) AND (t1.v3 BETWEEN 45 AND 52)) AND (t1.v2 = 98)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1>=98 AND v2=51) AND (v1>34);`,
		ExpectedPlan: "Filter((t1.v1 >= 98) AND (t1.v1 > 34))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>2) OR (v1<=30)) OR (v1<>35 AND v2 BETWEEN 6 AND 61 AND v3>=16));`,
		ExpectedPlan: "Filter(((t1.v1 > 2) OR (t1.v1 <= 30)) OR (((NOT((t1.v1 = 35))) AND (t1.v2 BETWEEN 6 AND 61)) AND (t1.v3 >= 16)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>19) OR (v1<>48));`,
		ExpectedPlan: "Filter((NOT((t1.v1 = 19))) OR (NOT((t1.v1 = 48))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 12 AND 42 AND v2<=12) OR (v1<34 AND v2 BETWEEN 30 AND 47 AND v3<>50));`,
		ExpectedPlan: "Filter(((t1.v1 BETWEEN 12 AND 42) AND (t1.v2 <= 12)) OR (((t1.v1 < 34) AND (t1.v2 BETWEEN 30 AND 47)) AND (NOT((t1.v3 = 50)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((((v1>=6) OR (v1>7)) OR (v1<88 AND v2<=34 AND v3<=47)) OR (v1>=10)) OR (v1=10));`,
		ExpectedPlan: "Filter(((((t1.v1 >= 6) OR (t1.v1 > 7)) OR (((t1.v1 < 88) AND (t1.v2 <= 34)) AND (t1.v3 <= 47))) OR (t1.v1 >= 10)) OR (t1.v1 = 10))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=74) OR (v1>=1)) OR (v1=54 AND v2>=38 AND v3>2)) AND (v1>5);`,
		ExpectedPlan: "Filter(((t1.v1 >= 74) OR (t1.v1 >= 1)) OR (((t1.v1 = 54) AND (t1.v2 >= 38)) AND (t1.v3 > 2)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=45 AND v2>18) OR (v1<64 AND v2=25 AND v3>97));`,
		ExpectedPlan: "Filter(((t1.v1 >= 45) AND (t1.v2 > 18)) OR (((t1.v1 < 64) AND (t1.v2 = 25)) AND (t1.v3 > 97)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<37 AND v3>77) OR (v1>38 AND v3<>57 AND v2=87));`,
		ExpectedPlan: "Filter(((t1.v1 < 37) AND (t1.v3 > 77)) OR (((t1.v1 > 38) AND (NOT((t1.v3 = 57)))) AND (t1.v2 = 87)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1<96 AND v2>11 AND v3<76) OR (v1<=14 AND v2=23)) OR (v1<=15 AND v2<21 AND v3<91)) OR (v1=45 AND v2<11 AND v3=1));`,
		ExpectedPlan: "Filter((((((t1.v1 < 96) AND (t1.v2 > 11)) AND (t1.v3 < 76)) OR ((t1.v1 <= 14) AND (t1.v2 = 23))) OR (((t1.v1 <= 15) AND (t1.v2 < 21)) AND (t1.v3 < 91))) OR (((t1.v1 = 45) AND (t1.v2 < 11)) AND (t1.v3 = 1)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>23 AND v3<=52) OR (v1<>19 AND v2=25));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 23))) AND (t1.v3 <= 52)) OR ((NOT((t1.v1 = 19))) AND (t1.v2 = 25)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1<=12 AND v2>=65) AND (v1<6 AND v2>=92);`,
		ExpectedPlan: "Filter((t1.v1 <= 12) AND (t1.v1 < 6))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1=62 AND v2<>32) OR (v1>=55 AND v2=41 AND v3>73));`,
		ExpectedPlan: "Filter(((t1.v1 = 62) AND (NOT((t1.v2 = 32)))) OR (((t1.v1 >= 55) AND (t1.v2 = 41)) AND (t1.v3 > 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>34 AND v2<=62) OR (v1>5 AND v2 BETWEEN 59 AND 98 AND v3<69)) OR (v1>34));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 34))) AND (t1.v2 <= 62)) OR (((t1.v1 > 5) AND (t1.v2 BETWEEN 59 AND 98)) AND (t1.v3 < 69))) OR (t1.v1 > 34))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1=61 AND v2 BETWEEN 10 AND 22 AND v3<34) OR (v1=68)) OR (v1<=97 AND v3 BETWEEN 7 AND 63 AND v2<67));`,
		ExpectedPlan: "Filter(((((t1.v1 = 61) AND (t1.v2 BETWEEN 10 AND 22)) AND (t1.v3 < 34)) OR (t1.v1 = 68)) OR (((t1.v1 <= 97) AND (t1.v3 BETWEEN 7 AND 63)) AND (t1.v2 < 67)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=42) OR (v1 BETWEEN 13 AND 30 AND v2<50));`,
		ExpectedPlan: "Filter((t1.v1 <= 42) OR ((t1.v1 BETWEEN 13 AND 30) AND (t1.v2 < 50)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 16 AND 49) OR (v1<=69 AND v2>9 AND v3<=8));`,
		ExpectedPlan: "Filter((t1.v1 BETWEEN 16 AND 49) OR (((t1.v1 <= 69) AND (t1.v2 > 9)) AND (t1.v3 <= 8)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>71 AND v2>44) OR (v1<76 AND v2>=10)) OR (v1>=44 AND v2=66));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 71))) AND (t1.v2 > 44)) OR ((t1.v1 < 76) AND (t1.v2 >= 10))) OR ((t1.v1 >= 44) AND (t1.v2 = 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((((v1>=26) OR (v1>=13 AND v2 BETWEEN 35 AND 95 AND v3>=29)) OR (v1<>54 AND v2 BETWEEN 0 AND 54)) OR (v1 BETWEEN 17 AND 17 AND v2<=71)) OR (v1>50 AND v3>=42)) OR (v1<>0));`,
		ExpectedPlan: "Filter((((((t1.v1 >= 26) OR (((t1.v1 >= 13) AND (t1.v2 BETWEEN 35 AND 95)) AND (t1.v3 >= 29))) OR ((NOT((t1.v1 = 54))) AND (t1.v2 BETWEEN 0 AND 54))) OR ((t1.v1 BETWEEN 17 AND 17) AND (t1.v2 <= 71))) OR ((t1.v1 > 50) AND (t1.v3 >= 42))) OR (NOT((t1.v1 = 0))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=99 AND v2<66) OR (v1 BETWEEN 1 AND 47)) OR (v1<>2 AND v2<30));`,
		ExpectedPlan: "Filter((((t1.v1 >= 99) AND (t1.v2 < 66)) OR (t1.v1 BETWEEN 1 AND 47)) OR ((NOT((t1.v1 = 2))) AND (t1.v2 < 30)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>9 AND v2<74) AND (v1<=63 AND v2=18) OR (v1<46));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 9))) AND (t1.v2 < 74)) AND ((t1.v1 <= 63) AND (t1.v2 = 18))) OR (t1.v1 < 46))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<=20 AND v2<=62) OR (v1>45 AND v2=33 AND v3<=4)) OR (v1>29));`,
		ExpectedPlan: "Filter((((t1.v1 <= 20) AND (t1.v2 <= 62)) OR (((t1.v1 > 45) AND (t1.v2 = 33)) AND (t1.v3 <= 4))) OR (t1.v1 > 29))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1<=55 AND v2 BETWEEN 82 AND 96 AND v3>=13) OR (v1>=89 AND v2<18 AND v3<19)) OR (v1=98 AND v3>=40)) OR (v1 BETWEEN 7 AND 74 AND v2<=73));`,
		ExpectedPlan: "Filter((((((t1.v1 <= 55) AND (t1.v2 BETWEEN 82 AND 96)) AND (t1.v3 >= 13)) OR (((t1.v1 >= 89) AND (t1.v2 < 18)) AND (t1.v3 < 19))) OR ((t1.v1 = 98) AND (t1.v3 >= 40))) OR ((t1.v1 BETWEEN 7 AND 74) AND (t1.v2 <= 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=26 AND v2 BETWEEN 6 AND 80) AND (v1=47 AND v2<67 AND v3<7) OR (v1>63));`,
		ExpectedPlan: "Filter((((t1.v1 >= 26) AND (t1.v2 BETWEEN 6 AND 80)) AND (((t1.v1 = 47) AND (t1.v2 < 67)) AND (t1.v3 < 7))) OR (t1.v1 > 63))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<11) OR (v1<>33));`,
		ExpectedPlan: "Filter((t1.v1 < 11) OR (NOT((t1.v1 = 33))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1<=35) AND (v1=44 AND v2<78 AND v3>=40) OR (v1<>88 AND v2=8)) AND (v1>=99 AND v2=62) OR (v1<=94)) OR (v1 BETWEEN 22 AND 23 AND v2 BETWEEN 14 AND 46));`,
		ExpectedPlan: "Filter((((((t1.v1 <= 35) AND (((t1.v1 = 44) AND (t1.v2 < 78)) AND (t1.v3 >= 40))) OR ((NOT((t1.v1 = 88))) AND (t1.v2 = 8))) AND ((t1.v1 >= 99) AND (t1.v2 = 62))) OR (t1.v1 <= 94)) OR ((t1.v1 BETWEEN 22 AND 23) AND (t1.v2 BETWEEN 14 AND 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<9 AND v2=94 AND v3>8) OR (v1>=63));`,
		ExpectedPlan: "Filter((((t1.v1 < 9) AND (t1.v2 = 94)) AND (t1.v3 > 8)) OR (t1.v1 >= 63))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<43) OR (v1 BETWEEN 40 AND 49 AND v2>26 AND v3 BETWEEN 22 AND 80));`,
		ExpectedPlan: "Filter((t1.v1 < 43) OR (((t1.v1 BETWEEN 40 AND 49) AND (t1.v2 > 26)) AND (t1.v3 BETWEEN 22 AND 80)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 4 AND 85 AND v2<>45 AND v3<=41) OR (v1>67 AND v2<25));`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 4 AND 85) AND (NOT((t1.v2 = 45)))) AND (t1.v3 <= 41)) OR ((t1.v1 > 67) AND (t1.v2 < 25)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>77) OR (v1<=54 AND v2<=71 AND v3>=49)) OR (v1>54 AND v2<30 AND v3=6));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 77))) OR (((t1.v1 <= 54) AND (t1.v2 <= 71)) AND (t1.v3 >= 49))) OR (((t1.v1 > 54) AND (t1.v2 < 30)) AND (t1.v3 = 6)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1 BETWEEN 21 AND 53 AND v2=0 AND v3>32) OR (v1=93 AND v2>=94 AND v3<1)) OR (v1<26)) OR (v1<>11 AND v2<>32 AND v3=6)) AND (v1>=45);`,
		ExpectedPlan: "Filter((((((t1.v1 BETWEEN 21 AND 53) AND (t1.v2 = 0)) AND (t1.v3 > 32)) OR (((t1.v1 = 93) AND (t1.v2 >= 94)) AND (t1.v3 < 1))) OR (t1.v1 < 26)) OR (((NOT((t1.v1 = 11))) AND (NOT((t1.v2 = 32)))) AND (t1.v3 = 6)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>50) OR (v1<=71));`,
		ExpectedPlan: "Filter((NOT((t1.v1 = 50))) OR (t1.v1 <= 71))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1=41) OR (v1>29 AND v2<>31));`,
		ExpectedPlan: "Filter((t1.v1 = 41) OR ((t1.v1 > 29) AND (NOT((t1.v2 = 31)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<88 AND v2<91 AND v3>9) AND (v1>=5 AND v2 BETWEEN 21 AND 29 AND v3>18) OR (v1>=40));`,
		ExpectedPlan: "Filter(((((t1.v1 < 88) AND (t1.v2 < 91)) AND (t1.v3 > 9)) AND (((t1.v1 >= 5) AND (t1.v2 BETWEEN 21 AND 29)) AND (t1.v3 > 18))) OR (t1.v1 >= 40))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>2 AND v2<76 AND v3<=35) OR (v1<=12 AND v3 BETWEEN 25 AND 30));`,
		ExpectedPlan: "Filter((((t1.v1 > 2) AND (t1.v2 < 76)) AND (t1.v3 <= 35)) OR ((t1.v1 <= 12) AND (t1.v3 BETWEEN 25 AND 30)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1 BETWEEN 25 AND 84 AND v2<=94) OR (v1>66 AND v2>4 AND v3>=57)) OR (v1=78 AND v2>66 AND v3=19)) OR (v1<>48));`,
		ExpectedPlan: "Filter(((((t1.v1 BETWEEN 25 AND 84) AND (t1.v2 <= 94)) OR (((t1.v1 > 66) AND (t1.v2 > 4)) AND (t1.v3 >= 57))) OR (((t1.v1 = 78) AND (t1.v2 > 66)) AND (t1.v3 = 19))) OR (NOT((t1.v1 = 48))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=24) OR (v1>=47 AND v2<=75 AND v3<=52));`,
		ExpectedPlan: "Filter((t1.v1 >= 24) OR (((t1.v1 >= 47) AND (t1.v2 <= 75)) AND (t1.v3 <= 52)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=21 AND v2<>70) OR (v1<=77 AND v2>4)) OR (v1<28 AND v2<=3 AND v3<>21));`,
		ExpectedPlan: "Filter((((t1.v1 >= 21) AND (NOT((t1.v2 = 70)))) OR ((t1.v1 <= 77) AND (t1.v2 > 4))) OR (((t1.v1 < 28) AND (t1.v2 <= 3)) AND (NOT((t1.v3 = 21)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=60 AND v2>91) OR (v1<=10));`,
		ExpectedPlan: "Filter(((t1.v1 >= 60) AND (t1.v2 > 91)) OR (t1.v1 <= 10))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>98 AND v2<52) OR (v1 BETWEEN 65 AND 67)) OR (v1 BETWEEN 18 AND 54)) AND (v1>=14 AND v2=27);`,
		ExpectedPlan: "Filter(((((NOT((t1.v1 = 98))) AND (t1.v2 < 52)) OR (t1.v1 BETWEEN 65 AND 67)) OR (t1.v1 BETWEEN 18 AND 54)) AND (t1.v1 >= 14))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=43 AND v2<>39) AND (v1<=32 AND v2<=15 AND v3>=54) OR (v1<>68 AND v2 BETWEEN 42 AND 46));`,
		ExpectedPlan: "Filter((((t1.v1 >= 43) AND (NOT((t1.v2 = 39)))) AND (((t1.v1 <= 32) AND (t1.v2 <= 15)) AND (t1.v3 >= 54))) OR ((NOT((t1.v1 = 68))) AND (t1.v2 BETWEEN 42 AND 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1>=19 AND v2<2) AND (v1<4 AND v3>23 AND v2<>53);`,
		ExpectedPlan: "Filter((((t1.v1 >= 19) AND (t1.v2 < 2)) AND (t1.v1 < 4)) AND (NOT((t1.v2 = 53))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 34 AND 40) OR (v1<=80 AND v2<>53)) AND (v1=81 AND v2=17 AND v3<>12);`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 34 AND 40) OR ((t1.v1 <= 80) AND (NOT((t1.v2 = 53))))) AND (t1.v1 = 81)) AND (t1.v2 = 17))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>34 AND v2 BETWEEN 18 AND 67 AND v3<67) OR (v1>21));`,
		ExpectedPlan: "Filter((((t1.v1 > 34) AND (t1.v2 BETWEEN 18 AND 67)) AND (t1.v3 < 67)) OR (t1.v1 > 21))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>45) OR (v1>=91 AND v2>=8 AND v3<=38)) OR (v1<>58 AND v3<=32 AND v2<>45));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 45))) OR (((t1.v1 >= 91) AND (t1.v2 >= 8)) AND (t1.v3 <= 38))) OR (((NOT((t1.v1 = 58))) AND (t1.v3 <= 32)) AND (NOT((t1.v2 = 45)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=48) OR (v1<38 AND v2>=26)) AND (v1<=45 AND v2>21) AND (v1=83 AND v2=20);`,
		ExpectedPlan: "Filter((((t1.v1 <= 48) OR ((t1.v1 < 38) AND (t1.v2 >= 26))) AND (t1.v1 <= 45)) AND (t1.v1 = 83))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>25) OR (v1<53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<95 AND v2>=12) OR (v1 BETWEEN 41 AND 55 AND v2<=81 AND v3<46));`,
		ExpectedPlan: "Filter(((t1.v1 < 95) AND (t1.v2 >= 12)) OR (((t1.v1 BETWEEN 41 AND 55) AND (t1.v2 <= 81)) AND (t1.v3 < 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>39 AND v2 BETWEEN 53 AND 73 AND v3<=11) OR (v1<=31 AND v2=68 AND v3>=71)) OR (v1<>18 AND v2<=51));`,
		ExpectedPlan: "Filter(((((t1.v1 > 39) AND (t1.v2 BETWEEN 53 AND 73)) AND (t1.v3 <= 11)) OR (((t1.v1 <= 31) AND (t1.v2 = 68)) AND (t1.v3 >= 71))) OR ((NOT((t1.v1 = 18))) AND (t1.v2 <= 51)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1>4) AND (v1=3 AND v2 BETWEEN 4 AND 34 AND v3<=40);`,
		ExpectedPlan: "Filter(((t1.v1 > 4) AND (t1.v1 = 3)) AND (t1.v2 BETWEEN 4 AND 34))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>36 AND v2>82) OR (v1 BETWEEN 22 AND 59));`,
		ExpectedPlan: "Filter(((t1.v1 > 36) AND (t1.v2 > 82)) OR (t1.v1 BETWEEN 22 AND 59))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=0) OR (v1 BETWEEN 17 AND 45));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<1 AND v3<=34) OR (v1 BETWEEN 2 AND 57 AND v2<>70));`,
		ExpectedPlan: "Filter(((t1.v1 < 1) AND (t1.v3 <= 34)) OR ((t1.v1 BETWEEN 2 AND 57) AND (NOT((t1.v2 = 70)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1>4) AND (v1 BETWEEN 8 AND 35 AND v2>=94 AND v3=32) AND (v1>=12);`,
		ExpectedPlan: "Filter((((t1.v1 > 4) AND (t1.v1 BETWEEN 8 AND 35)) AND (t1.v2 >= 94)) AND (t1.v1 >= 12))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1<=93 AND v3<>47) OR (v1>=93 AND v2 BETWEEN 15 AND 42 AND v3<=6)) OR (v1>15)) OR (v1 BETWEEN 0 AND 1 AND v2>33));`,
		ExpectedPlan: "Filter(((((t1.v1 <= 93) AND (NOT((t1.v3 = 47)))) OR (((t1.v1 >= 93) AND (t1.v2 BETWEEN 15 AND 42)) AND (t1.v3 <= 6))) OR (t1.v1 > 15)) OR ((t1.v1 BETWEEN 0 AND 1) AND (t1.v2 > 33)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>12) OR (v1>=26 AND v2 BETWEEN 77 AND 87 AND v3<19)) OR (v1<=89));`,
		ExpectedPlan: "Filter(((t1.v1 > 12) OR (((t1.v1 >= 26) AND (t1.v2 BETWEEN 77 AND 87)) AND (t1.v3 < 19))) OR (t1.v1 <= 89))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1=27 AND v2=16 AND v3>=8) OR (v1<20 AND v2>=1 AND v3 BETWEEN 28 AND 47)) OR (v1 BETWEEN 15 AND 43 AND v2>30));`,
		ExpectedPlan: "Filter(((((t1.v1 = 27) AND (t1.v2 = 16)) AND (t1.v3 >= 8)) OR (((t1.v1 < 20) AND (t1.v2 >= 1)) AND (t1.v3 BETWEEN 28 AND 47))) OR ((t1.v1 BETWEEN 15 AND 43) AND (t1.v2 > 30)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1=67 AND v2<>69) OR (v1<28 AND v2<62 AND v3>=99));`,
		ExpectedPlan: "Filter(((t1.v1 = 67) AND (NOT((t1.v2 = 69)))) OR (((t1.v1 < 28) AND (t1.v2 < 62)) AND (t1.v3 >= 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<45 AND v2>5 AND v3>20) OR (v1<17));`,
		ExpectedPlan: "Filter((((t1.v1 < 45) AND (t1.v2 > 5)) AND (t1.v3 > 20)) OR (t1.v1 < 17))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1=40 AND v2<>18) OR (v1<>97 AND v2<>17 AND v3<>48));`,
		ExpectedPlan: "Filter(((t1.v1 = 40) AND (NOT((t1.v2 = 18)))) OR (((NOT((t1.v1 = 97))) AND (NOT((t1.v2 = 17)))) AND (NOT((t1.v3 = 48)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>63) AND (v1<=44 AND v2<>43 AND v3=29) OR (v1=38 AND v2>45));`,
		ExpectedPlan: "Filter(((t1.v1 > 63) AND (((t1.v1 <= 44) AND (NOT((t1.v2 = 43)))) AND (t1.v3 = 29))) OR ((t1.v1 = 38) AND (t1.v2 > 45)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=6) OR (v1>0 AND v2 BETWEEN 3 AND 50));`,
		ExpectedPlan: "Filter((t1.v1 <= 6) OR ((t1.v1 > 0) AND (t1.v2 BETWEEN 3 AND 50)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 5 AND 35 AND v2<=3 AND v3<>14) OR (v1>11));`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 5 AND 35) AND (t1.v2 <= 3)) AND (NOT((t1.v3 = 14)))) OR (t1.v1 > 11))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<50) AND (v1<19 AND v2>=10) OR (v1<36 AND v2>10 AND v3<>65));`,
		ExpectedPlan: "Filter(((t1.v1 < 50) AND ((t1.v1 < 19) AND (t1.v2 >= 10))) OR (((t1.v1 < 36) AND (t1.v2 > 10)) AND (NOT((t1.v3 = 65)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1=56 AND v3<=4 AND v2=46) OR (v1 BETWEEN 21 AND 53 AND v2<>63)) OR (v1 BETWEEN 10 AND 62 AND v2>=62)) OR (v1>31));`,
		ExpectedPlan: "Filter((((((t1.v1 = 56) AND (t1.v3 <= 4)) AND (t1.v2 = 46)) OR ((t1.v1 BETWEEN 21 AND 53) AND (NOT((t1.v2 = 63))))) OR ((t1.v1 BETWEEN 10 AND 62) AND (t1.v2 >= 62))) OR (t1.v1 > 31))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<20 AND v2>=1 AND v3=26) OR (v1=12));`,
		ExpectedPlan: "Filter((((t1.v1 < 20) AND (t1.v2 >= 1)) AND (t1.v3 = 26)) OR (t1.v1 = 12))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>51) AND (v1<>4 AND v2<47 AND v3>=77) OR (v1>41 AND v3>62));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 51))) AND (((NOT((t1.v1 = 4))) AND (t1.v2 < 47)) AND (t1.v3 >= 77))) OR ((t1.v1 > 41) AND (t1.v3 > 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<35) OR (v1>=58 AND v2>=0));`,
		ExpectedPlan: "Filter((t1.v1 < 35) OR ((t1.v1 >= 58) AND (t1.v2 >= 0)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>28 AND v2<95) OR (v1<91));`,
		ExpectedPlan: "Filter(((t1.v1 > 28) AND (t1.v2 < 95)) OR (t1.v1 < 91))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (v1=99 AND v2<=41 AND v3>=61) AND (v1=34 AND v2>68 AND v3<=42);`,
		ExpectedPlan: "Filter((((t1.v1 = 99) AND (t1.v2 <= 41)) AND (t1.v1 = 34)) AND (t1.v2 > 68))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=74 AND v2<=18) OR (v1>=72)) AND (v1=95 AND v2=31 AND v3 BETWEEN 5 AND 19);`,
		ExpectedPlan: "Filter(((((t1.v1 >= 74) AND (t1.v2 <= 18)) OR (t1.v1 >= 72)) AND (t1.v1 = 95)) AND (t1.v2 = 31))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1=64) OR (v1>=49 AND v2<9 AND v3<=49));`,
		ExpectedPlan: "Filter((t1.v1 = 64) OR (((t1.v1 >= 49) AND (t1.v2 < 9)) AND (t1.v3 <= 49)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=46) AND (v1<22 AND v2<>42 AND v3<>54) OR (v1>=55 AND v2 BETWEEN 11 AND 84));`,
		ExpectedPlan: "Filter(((t1.v1 >= 46) AND (((t1.v1 < 22) AND (NOT((t1.v2 = 42)))) AND (NOT((t1.v3 = 54))))) OR ((t1.v1 >= 55) AND (t1.v2 BETWEEN 11 AND 84)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=7) OR (v1<54));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<=95 AND v2=55 AND v3>34) OR (v1=19));`,
		ExpectedPlan: "Filter((((t1.v1 <= 95) AND (t1.v2 = 55)) AND (t1.v3 > 34)) OR (t1.v1 = 19))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1=51 AND v2<=9) OR (v1<>50)) OR (v1<>4 AND v2>56)) OR (v1 BETWEEN 3 AND 18 AND v2>10 AND v3=12));`,
		ExpectedPlan: "Filter(((((t1.v1 = 51) AND (t1.v2 <= 9)) OR (NOT((t1.v1 = 50)))) OR ((NOT((t1.v1 = 4))) AND (t1.v2 > 56))) OR (((t1.v1 BETWEEN 3 AND 18) AND (t1.v2 > 10)) AND (t1.v3 = 12)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1<=90 AND v2<=17) OR (v1=2)) OR (v1<>70 AND v2>=84 AND v3<>42)) OR (v1<11 AND v2<>47 AND v3<55));`,
		ExpectedPlan: "Filter(((((t1.v1 <= 90) AND (t1.v2 <= 17)) OR (t1.v1 = 2)) OR (((NOT((t1.v1 = 70))) AND (t1.v2 >= 84)) AND (NOT((t1.v3 = 42))))) OR (((t1.v1 < 11) AND (NOT((t1.v2 = 47)))) AND (t1.v3 < 55)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 50 AND 59) OR (v1>=23 AND v3>=87 AND v2<>46));`,
		ExpectedPlan: "Filter((t1.v1 BETWEEN 50 AND 59) OR (((t1.v1 >= 23) AND (t1.v3 >= 87)) AND (NOT((t1.v2 = 46)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<53) OR (v1<=3));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=16 AND v2 BETWEEN 66 AND 94) OR (v1>70 AND v2<=3)) AND (v1<>91) OR (v1=17 AND v2>=7));`,
		ExpectedPlan: "Filter(((((t1.v1 >= 16) AND (t1.v2 BETWEEN 66 AND 94)) OR ((t1.v1 > 70) AND (t1.v2 <= 3))) AND (NOT((t1.v1 = 91)))) OR ((t1.v1 = 17) AND (t1.v2 >= 7)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<29 AND v3>=33 AND v2=43) OR (v1<59));`,
		ExpectedPlan: "Filter((((t1.v1 < 29) AND (t1.v3 >= 33)) AND (t1.v2 = 43)) OR (t1.v1 < 59))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>19 AND v2>84 AND v3>94) OR (v1>=42 AND v3=41));`,
		ExpectedPlan: "Filter((((t1.v1 > 19) AND (t1.v2 > 84)) AND (t1.v3 > 94)) OR ((t1.v1 >= 42) AND (t1.v3 = 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=8 AND v2<=97 AND v3>=77) OR (v1<>4)) OR (v1<=41));`,
		ExpectedPlan: "Filter(((((t1.v1 >= 8) AND (t1.v2 <= 97)) AND (t1.v3 >= 77)) OR (NOT((t1.v1 = 4)))) OR (t1.v1 <= 41))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>33) OR (v1<=28)) OR (v1<>68));`,
		ExpectedPlan: "Filter(((NOT((t1.v1 = 33))) OR (t1.v1 <= 28)) OR (NOT((t1.v1 = 68))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<>15 AND v2>=22 AND v3<=51) OR (v1<>40 AND v2>26 AND v3<95));`,
		ExpectedPlan: "Filter((((NOT((t1.v1 = 15))) AND (t1.v2 >= 22)) AND (t1.v3 <= 51)) OR (((NOT((t1.v1 = 40))) AND (t1.v2 > 26)) AND (t1.v3 < 95)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>6) OR (v1<=67 AND v2<>67 AND v3>=88));`,
		ExpectedPlan: "Filter((t1.v1 > 6) OR (((t1.v1 <= 67) AND (NOT((t1.v2 = 67)))) AND (t1.v3 >= 88)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<=0) OR (v1<=53)) OR (v1<=38));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3]\n" +
			" └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1=60 AND v3 BETWEEN 2 AND 13 AND v2 BETWEEN 10 AND 69) OR (v1 BETWEEN 1 AND 49)) OR (v1=8 AND v2<26));`,
		ExpectedPlan: "Filter(((((t1.v1 = 60) AND (t1.v3 BETWEEN 2 AND 13)) AND (t1.v2 BETWEEN 10 AND 69)) OR (t1.v1 BETWEEN 1 AND 49)) OR ((t1.v1 = 8) AND (t1.v2 < 26)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1 BETWEEN 14 AND 20 AND v2<>70) OR (v1>78 AND v2 BETWEEN 31 AND 52 AND v3>16)) OR (v1 BETWEEN 77 AND 78));`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 14 AND 20) AND (NOT((t1.v2 = 70)))) OR (((t1.v1 > 78) AND (t1.v2 BETWEEN 31 AND 52)) AND (t1.v3 > 16))) OR (t1.v1 BETWEEN 77 AND 78))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<80 AND v2 BETWEEN 41 AND 74) OR (v1>=36 AND v2=32));`,
		ExpectedPlan: "Filter(((t1.v1 < 80) AND (t1.v2 BETWEEN 41 AND 74)) OR ((t1.v1 >= 36) AND (t1.v2 = 32)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=24 AND v2=62) OR (v1<=24 AND v3<>22 AND v2 BETWEEN 12 AND 25)) OR (v1 BETWEEN 48 AND 49 AND v3>=90)) AND (v1<15 AND v2<>55 AND v3=51);`,
		ExpectedPlan: "Filter((((((t1.v1 >= 24) AND (t1.v2 = 62)) OR (((t1.v1 <= 24) AND (NOT((t1.v3 = 22)))) AND (t1.v2 BETWEEN 12 AND 25))) OR ((t1.v1 BETWEEN 48 AND 49) AND (t1.v3 >= 90))) AND (t1.v1 < 15)) AND (NOT((t1.v2 = 55))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<66 AND v2>=11 AND v3<90) OR (v1<>90)) OR (v1<=7 AND v2=52));`,
		ExpectedPlan: "Filter(((((t1.v1 < 66) AND (t1.v2 >= 11)) AND (t1.v3 < 90)) OR (NOT((t1.v1 = 90)))) OR ((t1.v1 <= 7) AND (t1.v2 = 52)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1 BETWEEN 6 AND 74 AND v2=52) OR (v1>44 AND v3>=15 AND v2 BETWEEN 17 AND 94)) OR (v1>84));`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 6 AND 74) AND (t1.v2 = 52)) OR (((t1.v1 > 44) AND (t1.v3 >= 15)) AND (t1.v2 BETWEEN 17 AND 94))) OR (t1.v1 > 84))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=38) OR (v1=13)) OR (v1=25 AND v2<=32 AND v3 BETWEEN 12 AND 92));`,
		ExpectedPlan: "Filter(((t1.v1 >= 38) OR (t1.v1 = 13)) OR (((t1.v1 = 25) AND (t1.v2 <= 32)) AND (t1.v3 BETWEEN 12 AND 92)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<=84) OR (v1=41)) OR (v1<83 AND v2=13 AND v3=58));`,
		ExpectedPlan: "Filter(((t1.v1 <= 84) OR (t1.v1 = 41)) OR (((t1.v1 < 83) AND (t1.v2 = 13)) AND (t1.v3 = 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<36 AND v2<=79 AND v3>47) OR (v1 BETWEEN 24 AND 89 AND v2<29));`,
		ExpectedPlan: "Filter((((t1.v1 < 36) AND (t1.v2 <= 79)) AND (t1.v3 > 47)) OR ((t1.v1 BETWEEN 24 AND 89) AND (t1.v2 < 29)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 3 AND 19 AND v2<=57 AND v3>61) OR (v1<=58 AND v2>=36 AND v3=31)) AND (v1>94);`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 3 AND 19) AND (t1.v2 <= 57)) AND (t1.v3 > 61)) OR (((t1.v1 <= 58) AND (t1.v2 >= 36)) AND (t1.v3 = 31)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1<78 AND v2 BETWEEN 55 AND 64 AND v3>=0) OR (v1<74));`,
		ExpectedPlan: "Filter((((t1.v1 < 78) AND (t1.v2 BETWEEN 55 AND 64)) AND (t1.v3 >= 0)) OR (t1.v1 < 74))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<>1 AND v2=88 AND v3<33) OR (v1<=38)) OR (v1>74 AND v3<>55 AND v2>=9));`,
		ExpectedPlan: "Filter(((((NOT((t1.v1 = 1))) AND (t1.v2 = 88)) AND (t1.v3 < 33)) OR (t1.v1 <= 38)) OR (((t1.v1 > 74) AND (NOT((t1.v3 = 55)))) AND (t1.v2 >= 9)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1 BETWEEN 15 AND 96 AND v2<>73) OR (v1>=16));`,
		ExpectedPlan: "Filter(((t1.v1 BETWEEN 15 AND 96) AND (NOT((t1.v2 = 73)))) OR (t1.v1 >= 16))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=72 AND v2<>19 AND v3 BETWEEN 9 AND 12) OR (v1<=77 AND v2=30 AND v3<=10));`,
		ExpectedPlan: "Filter((((t1.v1 >= 72) AND (NOT((t1.v2 = 19)))) AND (t1.v3 BETWEEN 9 AND 12)) OR (((t1.v1 <= 77) AND (t1.v2 = 30)) AND (t1.v3 <= 10)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1>28 AND v2>=73 AND v3=79) AND (v1<=70 AND v2 BETWEEN 5 AND 36) OR (v1<=31)) OR (v1<36)) OR (v1=47 AND v2 BETWEEN 0 AND 92 AND v3<=43));`,
		ExpectedPlan: "Filter(((((((t1.v1 > 28) AND (t1.v2 >= 73)) AND (t1.v3 = 79)) AND ((t1.v1 <= 70) AND (t1.v2 BETWEEN 5 AND 36))) OR (t1.v1 <= 31)) OR (t1.v1 < 36)) OR (((t1.v1 = 47) AND (t1.v2 BETWEEN 0 AND 92)) AND (t1.v3 <= 43)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>24) AND (v1>68 AND v2 BETWEEN 1 AND 79 AND v3 BETWEEN 23 AND 44) OR (v1>78));`,
		ExpectedPlan: "Filter(((t1.v1 > 24) AND (((t1.v1 > 68) AND (t1.v2 BETWEEN 1 AND 79)) AND (t1.v3 BETWEEN 23 AND 44))) OR (t1.v1 > 78))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1=47 AND v2=7) OR (v1>=7 AND v2<>87)) OR (v1<>6 AND v2<=84));`,
		ExpectedPlan: "Filter((((t1.v1 = 47) AND (t1.v2 = 7)) OR ((t1.v1 >= 7) AND (NOT((t1.v2 = 87))))) OR ((NOT((t1.v1 = 6))) AND (t1.v2 <= 84)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1>=49 AND v2>53 AND v3<>12) OR (v1=95 AND v2<1 AND v3<>89)) OR (v1=62 AND v3>=37 AND v2<=22)) OR (v1>30 AND v2>=66));`,
		ExpectedPlan: "Filter((((((t1.v1 >= 49) AND (t1.v2 > 53)) AND (NOT((t1.v3 = 12)))) OR (((t1.v1 = 95) AND (t1.v2 < 1)) AND (NOT((t1.v3 = 89))))) OR (((t1.v1 = 62) AND (t1.v3 >= 37)) AND (t1.v2 <= 22))) OR ((t1.v1 > 30) AND (t1.v2 >= 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1=24 AND v2<81) OR (v1<=22 AND v2>34 AND v3<55)) OR (v1=45 AND v2>=94 AND v3>17));`,
		ExpectedPlan: "Filter((((t1.v1 = 24) AND (t1.v2 < 81)) OR (((t1.v1 <= 22) AND (t1.v2 > 34)) AND (t1.v3 < 55))) OR (((t1.v1 = 45) AND (t1.v2 >= 94)) AND (t1.v3 > 17)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1>38) OR (v1<51 AND v2>=28 AND v3=44)) OR (v1 BETWEEN 23 AND 61 AND v2 BETWEEN 54 AND 75 AND v3<>44)) OR (v1>72));`,
		ExpectedPlan: "Filter((((t1.v1 > 38) OR (((t1.v1 < 51) AND (t1.v2 >= 28)) AND (t1.v3 = 44))) OR (((t1.v1 BETWEEN 23 AND 61) AND (t1.v2 BETWEEN 54 AND 75)) AND (NOT((t1.v3 = 44))))) OR (t1.v1 > 72))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((((v1>40 AND v2 BETWEEN 26 AND 30) OR (v1<3 AND v2>=62 AND v3<=8)) OR (v1<>57)) OR (v1=16 AND v2>92 AND v3<=74));`,
		ExpectedPlan: "Filter(((((t1.v1 > 40) AND (t1.v2 BETWEEN 26 AND 30)) OR (((t1.v1 < 3) AND (t1.v2 >= 62)) AND (t1.v3 <= 8))) OR (NOT((t1.v1 = 57)))) OR (((t1.v1 = 16) AND (t1.v2 > 92)) AND (t1.v3 <= 74)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<=34 AND v2 BETWEEN 29 AND 35 AND v3>=64) OR (v1<>47)) AND (v1>=11) OR (v1<>46 AND v2 BETWEEN 4 AND 26));`,
		ExpectedPlan: "Filter((((((t1.v1 <= 34) AND (t1.v2 BETWEEN 29 AND 35)) AND (t1.v3 >= 64)) OR (NOT((t1.v1 = 47)))) AND (t1.v1 >= 11)) OR ((NOT((t1.v1 = 46))) AND (t1.v2 BETWEEN 4 AND 26)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1 BETWEEN 41 AND 98 AND v2>54) OR (v1<29)) OR (v1<32));`,
		ExpectedPlan: "Filter((((t1.v1 BETWEEN 41 AND 98) AND (t1.v2 > 54)) OR (t1.v1 < 29)) OR (t1.v1 < 32))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=79 AND v3 BETWEEN 9 AND 95) OR (v1 BETWEEN 50 AND 50 AND v2 BETWEEN 16 AND 38 AND v3<>94));`,
		ExpectedPlan: "Filter(((t1.v1 >= 79) AND (t1.v3 BETWEEN 9 AND 95)) OR (((t1.v1 BETWEEN 50 AND 50) AND (t1.v2 BETWEEN 16 AND 38)) AND (NOT((t1.v3 = 94)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((((v1<>79) OR (v1 BETWEEN 9 AND 11 AND v2<48 AND v3<=73)) OR (v1<=46)) OR (v1 BETWEEN 66 AND 67)) OR (v1<=86 AND v2<4));`,
		ExpectedPlan: "Filter(((((NOT((t1.v1 = 79))) OR (((t1.v1 BETWEEN 9 AND 11) AND (t1.v2 < 48)) AND (t1.v3 <= 73))) OR (t1.v1 <= 46)) OR (t1.v1 BETWEEN 66 AND 67)) OR ((t1.v1 <= 86) AND (t1.v2 < 4)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1>=2 AND v2 BETWEEN 32 AND 59 AND v3 BETWEEN 50 AND 52) OR (v1<26)) OR (v1<>2 AND v2>11)) AND (v1>32 AND v2<=92) AND (v1>45 AND v2<>5 AND v3<>49);`,
		ExpectedPlan: "Filter(((((((((t1.v1 >= 2) AND (t1.v2 BETWEEN 32 AND 59)) AND (t1.v3 BETWEEN 50 AND 52)) OR (t1.v1 < 26)) OR ((NOT((t1.v1 = 2))) AND (t1.v2 > 11))) AND (t1.v1 > 32)) AND (t1.v2 <= 92)) AND (t1.v1 > 45)) AND (NOT((t1.v2 = 5))))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=19) AND (v1<=73) OR (v1=9 AND v2=5 AND v3<=5));`,
		ExpectedPlan: "Filter(((t1.v1 >= 19) AND (t1.v1 <= 73)) OR (((t1.v1 = 9) AND (t1.v2 = 5)) AND (t1.v3 <= 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE (((v1<62) AND (v1<=57 AND v2>51 AND v3 BETWEEN 29 AND 30) OR (v1>=28 AND v2<=62 AND v3<>76)) OR (v1>=94));`,
		ExpectedPlan: "Filter((((t1.v1 < 62) AND (((t1.v1 <= 57) AND (t1.v2 > 51)) AND (t1.v3 BETWEEN 29 AND 30))) OR (((t1.v1 >= 28) AND (t1.v2 <= 62)) AND (NOT((t1.v3 = 76))))) OR (t1.v1 >= 94))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>21) OR (v1>=86 AND v2>2 AND v3>=67));`,
		ExpectedPlan: "Filter((t1.v1 > 21) OR (((t1.v1 >= 86) AND (t1.v2 > 2)) AND (t1.v3 >= 67)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t1 WHERE ((v1>=94) OR (v1>=57 AND v2<>53 AND v3>22));`,
		ExpectedPlan: "Filter((t1.v1 >= 94) OR (((t1.v1 >= 57) AND (NOT((t1.v2 = 53)))) AND (t1.v3 > 22)))\n" +
			" └─ Projected table access on [pk v1 v2 v3]\n" +
			"     └─ IndexedTableAccess(t1 on [t1.v1,t1.v2,t1.v3])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<40 AND v2=9) OR (v1<11 AND v2=15 AND v3<>55 AND v4<>95));`,
		ExpectedPlan: "Filter(((t2.v1 < 40) AND (t2.v2 = 9)) OR ((((t2.v1 < 11) AND (t2.v2 = 15)) AND (NOT((t2.v3 = 55)))) AND (NOT((t2.v4 = 95)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=82 AND v2=74 AND v3=98) OR (v1=27 AND v2 BETWEEN 16 AND 46 AND v3<>27)) OR (v1>=80 AND v2<>42 AND v3>=47));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 82) AND (t2.v2 = 74)) AND (t2.v3 = 98)) OR (((t2.v1 = 27) AND (t2.v2 BETWEEN 16 AND 46)) AND (NOT((t2.v3 = 27))))) OR (((t2.v1 >= 80) AND (NOT((t2.v2 = 42)))) AND (t2.v3 >= 47)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>=47 AND v2<=37 AND v3<90 AND v4=25) OR (v1<42 AND v2>=96 AND v3=38)) OR (v1>26)) OR (v1>=80));`,
		ExpectedPlan: "Filter(((((((t2.v1 >= 47) AND (t2.v2 <= 37)) AND (t2.v3 < 90)) AND (t2.v4 = 25)) OR (((t2.v1 < 42) AND (t2.v2 >= 96)) AND (t2.v3 = 38))) OR (t2.v1 > 26)) OR (t2.v1 >= 80))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>33 AND v2>=16) OR (v1>=24));`,
		ExpectedPlan: "Filter(((t2.v1 > 33) AND (t2.v2 >= 16)) OR (t2.v1 >= 24))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=51 AND v4 BETWEEN 36 AND 55 AND v2>62 AND v3<43) OR (v1 BETWEEN 5 AND 60 AND v2<1)) OR (v1=51 AND v2>=98 AND v3>=94));`,
		ExpectedPlan: "Filter((((((t2.v1 = 51) AND (t2.v4 BETWEEN 36 AND 55)) AND (t2.v2 > 62)) AND (t2.v3 < 43)) OR ((t2.v1 BETWEEN 5 AND 60) AND (t2.v2 < 1))) OR (((t2.v1 = 51) AND (t2.v2 >= 98)) AND (t2.v3 >= 94)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=6 AND v4<95 AND v2<41 AND v3<=4) AND (v1>=81 AND v4>44 AND v2 BETWEEN 6 AND 11) OR (v1<=98));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 6) AND (t2.v4 < 95)) AND (t2.v2 < 41)) AND (t2.v3 <= 4)) AND (((t2.v1 >= 81) AND (t2.v4 > 44)) AND (t2.v2 BETWEEN 6 AND 11))) OR (t2.v1 <= 98))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=12 AND v2<=78 AND v3 BETWEEN 28 AND 63 AND v4 BETWEEN 46 AND 95) OR (v1=87 AND v2<=44)) OR (v1<14 AND v2<>37 AND v3 BETWEEN 6 AND 32));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 12) AND (t2.v2 <= 78)) AND (t2.v3 BETWEEN 28 AND 63)) AND (t2.v4 BETWEEN 46 AND 95)) OR ((t2.v1 = 87) AND (t2.v2 <= 44))) OR (((t2.v1 < 14) AND (NOT((t2.v2 = 37)))) AND (t2.v3 BETWEEN 6 AND 32)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=80 AND v2=72 AND v3>19) OR (v1<>38 AND v2>=86 AND v3=7)) OR (v1<=52 AND v2=25 AND v3 BETWEEN 7 AND 32 AND v4<=31));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 80) AND (t2.v2 = 72)) AND (t2.v3 > 19)) OR (((NOT((t2.v1 = 38))) AND (t2.v2 >= 86)) AND (t2.v3 = 7))) OR ((((t2.v1 <= 52) AND (t2.v2 = 25)) AND (t2.v3 BETWEEN 7 AND 32)) AND (t2.v4 <= 31)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=70) OR (v1>=38 AND v3 BETWEEN 25 AND 30));`,
		ExpectedPlan: "Filter((t2.v1 = 70) OR ((t2.v1 >= 38) AND (t2.v3 BETWEEN 25 AND 30)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=33) OR (v1<=31 AND v4<>35 AND v2=38));`,
		ExpectedPlan: "Filter((t2.v1 <= 33) OR (((t2.v1 <= 31) AND (NOT((t2.v4 = 35)))) AND (t2.v2 = 38)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>14 AND v2<51 AND v3 BETWEEN 67 AND 78 AND v4=8) OR (v1>=44 AND v2<>35 AND v3<35 AND v4>=12)) OR (v1>=63 AND v2<=3));`,
		ExpectedPlan: "Filter((((((t2.v1 > 14) AND (t2.v2 < 51)) AND (t2.v3 BETWEEN 67 AND 78)) AND (t2.v4 = 8)) OR ((((t2.v1 >= 44) AND (NOT((t2.v2 = 35)))) AND (t2.v3 < 35)) AND (t2.v4 >= 12))) OR ((t2.v1 >= 63) AND (t2.v2 <= 3)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=6 AND v2<=25 AND v3>39) OR (v1 BETWEEN 17 AND 94 AND v2>96));`,
		ExpectedPlan: "Filter((((t2.v1 = 6) AND (t2.v2 <= 25)) AND (t2.v3 > 39)) OR ((t2.v1 BETWEEN 17 AND 94) AND (t2.v2 > 96)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((((v1>=91 AND v4<=47 AND v2>=43) OR (v1=75)) OR (v1<41 AND v4>=64 AND v2>83)) OR (v1 BETWEEN 72 AND 88 AND v2=48 AND v3<=10)) OR (v1<=44));`,
		ExpectedPlan: "Filter(((((((t2.v1 >= 91) AND (t2.v4 <= 47)) AND (t2.v2 >= 43)) OR (t2.v1 = 75)) OR (((t2.v1 < 41) AND (t2.v4 >= 64)) AND (t2.v2 > 83))) OR (((t2.v1 BETWEEN 72 AND 88) AND (t2.v2 = 48)) AND (t2.v3 <= 10))) OR (t2.v1 <= 44))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=31) OR (v1<84 AND v2<=73 AND v3<>2 AND v4<=51));`,
		ExpectedPlan: "Filter((t2.v1 = 31) OR ((((t2.v1 < 84) AND (t2.v2 <= 73)) AND (NOT((t2.v3 = 2)))) AND (t2.v4 <= 51)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=20 AND v2<=29 AND v3<52 AND v4<>34) OR (v1<>46 AND v2<>98));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 20) AND (t2.v2 <= 29)) AND (t2.v3 < 52)) AND (NOT((t2.v4 = 34)))) OR ((NOT((t2.v1 = 46))) AND (NOT((t2.v2 = 98)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<52 AND v3 BETWEEN 39 AND 57 AND v4 BETWEEN 13 AND 13 AND v2 BETWEEN 76 AND 99) OR (v1>44)) OR (v1<71 AND v4>7 AND v2<98)) OR (v1<>5 AND v2 BETWEEN 35 AND 40 AND v3<=10));`,
		ExpectedPlan: "Filter(((((((t2.v1 < 52) AND (t2.v3 BETWEEN 39 AND 57)) AND (t2.v4 BETWEEN 13 AND 13)) AND (t2.v2 BETWEEN 76 AND 99)) OR (t2.v1 > 44)) OR (((t2.v1 < 71) AND (t2.v4 > 7)) AND (t2.v2 < 98))) OR (((NOT((t2.v1 = 5))) AND (t2.v2 BETWEEN 35 AND 40)) AND (t2.v3 <= 10)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=40) OR (v1=27)) OR (v1>90 AND v2>50 AND v3=66 AND v4<83));`,
		ExpectedPlan: "Filter(((t2.v1 = 40) OR (t2.v1 = 27)) OR ((((t2.v1 > 90) AND (t2.v2 > 50)) AND (t2.v3 = 66)) AND (t2.v4 < 83)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1<=92 AND v4 BETWEEN 8 AND 90) AND (v1 BETWEEN 39 AND 42);`,
		ExpectedPlan: "Filter(t2.v4 BETWEEN 8 AND 90)\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 23 AND 85 AND v2<=51 AND v3<>68) OR (v1 BETWEEN 30 AND 58 AND v2<>75));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 23 AND 85) AND (t2.v2 <= 51)) AND (NOT((t2.v3 = 68)))) OR ((t2.v1 BETWEEN 30 AND 58) AND (NOT((t2.v2 = 75)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=67 AND v2<=17 AND v3<>91 AND v4<82) OR (v1>28 AND v2 BETWEEN 17 AND 71 AND v3<12));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 67) AND (t2.v2 <= 17)) AND (NOT((t2.v3 = 91)))) AND (t2.v4 < 82)) OR (((t2.v1 > 28) AND (t2.v2 BETWEEN 17 AND 71)) AND (t2.v3 < 12)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>77 AND v4>82 AND v2>=96) OR (v1 BETWEEN 41 AND 80 AND v2<>21 AND v3>60));`,
		ExpectedPlan: "Filter((((t2.v1 > 77) AND (t2.v4 > 82)) AND (t2.v2 >= 96)) OR (((t2.v1 BETWEEN 41 AND 80) AND (NOT((t2.v2 = 21)))) AND (t2.v3 > 60)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1=28 AND v4 BETWEEN 44 AND 50) AND (v1>=49);`,
		ExpectedPlan: "Filter(t2.v4 BETWEEN 44 AND 50)\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 81 AND 87 AND v3<>81 AND v4<30) AND (v1=17) OR (v1<27 AND v2<>8 AND v3>35)) OR (v1>28 AND v2<62));`,
		ExpectedPlan: "Filter((((((t2.v1 BETWEEN 81 AND 87) AND (NOT((t2.v3 = 81)))) AND (t2.v4 < 30)) AND (t2.v1 = 17)) OR (((t2.v1 < 27) AND (NOT((t2.v2 = 8)))) AND (t2.v3 > 35))) OR ((t2.v1 > 28) AND (t2.v2 < 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>65 AND v2=64) OR (v1=82 AND v3<>99)) OR (v1>=68 AND v2=3 AND v3 BETWEEN 1 AND 51 AND v4<=73));`,
		ExpectedPlan: "Filter((((t2.v1 > 65) AND (t2.v2 = 64)) OR ((t2.v1 = 82) AND (NOT((t2.v3 = 99))))) OR ((((t2.v1 >= 68) AND (t2.v2 = 3)) AND (t2.v3 BETWEEN 1 AND 51)) AND (t2.v4 <= 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=27 AND v3>23) OR (v1<70 AND v2<>43));`,
		ExpectedPlan: "Filter(((t2.v1 <= 27) AND (t2.v3 > 23)) OR ((t2.v1 < 70) AND (NOT((t2.v2 = 43)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>34 AND v2>=89 AND v3>=14) OR (v1<=42 AND v3<1)) OR (v1<59 AND v2>=23 AND v3 BETWEEN 17 AND 37 AND v4 BETWEEN 21 AND 38));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 34))) AND (t2.v2 >= 89)) AND (t2.v3 >= 14)) OR ((t2.v1 <= 42) AND (t2.v3 < 1))) OR ((((t2.v1 < 59) AND (t2.v2 >= 23)) AND (t2.v3 BETWEEN 17 AND 37)) AND (t2.v4 BETWEEN 21 AND 38)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=52 AND v2>=55) OR (v1<73 AND v2<=1 AND v3>75 AND v4<=36)) OR (v1>=45 AND v2>=49 AND v3<=26 AND v4 BETWEEN 40 AND 83));`,
		ExpectedPlan: "Filter((((t2.v1 >= 52) AND (t2.v2 >= 55)) OR ((((t2.v1 < 73) AND (t2.v2 <= 1)) AND (t2.v3 > 75)) AND (t2.v4 <= 36))) OR ((((t2.v1 >= 45) AND (t2.v2 >= 49)) AND (t2.v3 <= 26)) AND (t2.v4 BETWEEN 40 AND 83)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>0 AND v2=94 AND v3<>0) OR (v1>=83 AND v2<69 AND v3<84));`,
		ExpectedPlan: "Filter((((t2.v1 > 0) AND (t2.v2 = 94)) AND (NOT((t2.v3 = 0)))) OR (((t2.v1 >= 83) AND (t2.v2 < 69)) AND (t2.v3 < 84)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<83 AND v4>51) OR (v1<>30));`,
		ExpectedPlan: "Filter(((t2.v1 < 83) AND (t2.v4 > 51)) OR (NOT((t2.v1 = 30))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<92) OR (v1 BETWEEN 6 AND 39 AND v2=47 AND v3>=63));`,
		ExpectedPlan: "Filter((t2.v1 < 92) OR (((t2.v1 BETWEEN 6 AND 39) AND (t2.v2 = 47)) AND (t2.v3 >= 63)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=98) OR (v1<=2 AND v2<5));`,
		ExpectedPlan: "Filter((t2.v1 >= 98) OR ((t2.v1 <= 2) AND (t2.v2 < 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>29 AND v4=40 AND v2>=63) OR (v1<70 AND v2<70 AND v3<=20)) OR (v1 BETWEEN 7 AND 61 AND v2>=33 AND v3>78)) OR (v1>=4 AND v2<=22));`,
		ExpectedPlan: "Filter((((((t2.v1 > 29) AND (t2.v4 = 40)) AND (t2.v2 >= 63)) OR (((t2.v1 < 70) AND (t2.v2 < 70)) AND (t2.v3 <= 20))) OR (((t2.v1 BETWEEN 7 AND 61) AND (t2.v2 >= 33)) AND (t2.v3 > 78))) OR ((t2.v1 >= 4) AND (t2.v2 <= 22)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=12) OR (v1=28));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=94 AND v2>=13 AND v3<=46 AND v4<>36) AND (v1=84) OR (v1 BETWEEN 52 AND 98 AND v2<71 AND v3<>45));`,
		ExpectedPlan: "Filter((((((t2.v1 <= 94) AND (t2.v2 >= 13)) AND (t2.v3 <= 46)) AND (NOT((t2.v4 = 36)))) AND (t2.v1 = 84)) OR (((t2.v1 BETWEEN 52 AND 98) AND (t2.v2 < 71)) AND (NOT((t2.v3 = 45)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>64) OR (v1<>55 AND v2=85 AND v3<=88));`,
		ExpectedPlan: "Filter((t2.v1 > 64) OR (((NOT((t2.v1 = 55))) AND (t2.v2 = 85)) AND (t2.v3 <= 88)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1 BETWEEN 54 AND 87 AND v2<78 AND v3<33) OR (v1<>52)) OR (v1 BETWEEN 3 AND 61 AND v4<=49)) OR (v1>3 AND v2<73 AND v3>59));`,
		ExpectedPlan: "Filter((((((t2.v1 BETWEEN 54 AND 87) AND (t2.v2 < 78)) AND (t2.v3 < 33)) OR (NOT((t2.v1 = 52)))) OR ((t2.v1 BETWEEN 3 AND 61) AND (t2.v4 <= 49))) OR (((t2.v1 > 3) AND (t2.v2 < 73)) AND (t2.v3 > 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 2 AND 23) OR (v1 BETWEEN 7 AND 14 AND v2<=27 AND v3<=82)) OR (v1>61));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 2 AND 23) OR (((t2.v1 BETWEEN 7 AND 14) AND (t2.v2 <= 27)) AND (t2.v3 <= 82))) OR (t2.v1 > 61))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=31 AND v2>44) OR (v1<44 AND v4<>6 AND v2<>10 AND v3<>14)) AND (v1=96 AND v3>25 AND v4<>32);`,
		ExpectedPlan: "Filter(((((t2.v1 = 31) AND (t2.v2 > 44)) OR ((((t2.v1 < 44) AND (NOT((t2.v4 = 6)))) AND (NOT((t2.v2 = 10)))) AND (NOT((t2.v3 = 14))))) AND (t2.v3 > 25)) AND (NOT((t2.v4 = 32))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>=85 AND v2<12) AND (v1>=25);`,
		ExpectedPlan: "Filter((t2.v1 >= 85) AND (t2.v1 >= 25))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=0) OR (v1=31)) OR (v1<>73 AND v4>9 AND v2 BETWEEN 27 AND 69 AND v3=14));`,
		ExpectedPlan: "Filter(((t2.v1 = 0) OR (t2.v1 = 31)) OR ((((NOT((t2.v1 = 73))) AND (t2.v4 > 9)) AND (t2.v2 BETWEEN 27 AND 69)) AND (t2.v3 = 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=42 AND v2=41 AND v3 BETWEEN 29 AND 94 AND v4<71) OR (v1>=71 AND v2 BETWEEN 67 AND 87 AND v3>=9)) OR (v1<2 AND v2<=1 AND v3<36 AND v4>41));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 42) AND (t2.v2 = 41)) AND (t2.v3 BETWEEN 29 AND 94)) AND (t2.v4 < 71)) OR (((t2.v1 >= 71) AND (t2.v2 BETWEEN 67 AND 87)) AND (t2.v3 >= 9))) OR ((((t2.v1 < 2) AND (t2.v2 <= 1)) AND (t2.v3 < 36)) AND (t2.v4 > 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=3 AND v2<57 AND v3<>74 AND v4>=69) OR (v1<>66 AND v2=16)) OR (v1=44 AND v3=58));`,
		ExpectedPlan: "Filter((((((t2.v1 <= 3) AND (t2.v2 < 57)) AND (NOT((t2.v3 = 74)))) AND (t2.v4 >= 69)) OR ((NOT((t2.v1 = 66))) AND (t2.v2 = 16))) OR ((t2.v1 = 44) AND (t2.v3 = 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<=22 AND v2<=41) OR (v1=61 AND v2>21)) OR (v1<>10)) OR (v1 BETWEEN 43 AND 44 AND v2>=35 AND v3<>87));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 22) AND (t2.v2 <= 41)) OR ((t2.v1 = 61) AND (t2.v2 > 21))) OR (NOT((t2.v1 = 10)))) OR (((t2.v1 BETWEEN 43 AND 44) AND (t2.v2 >= 35)) AND (NOT((t2.v3 = 87)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<=13 AND v3>20) OR (v1 BETWEEN 18 AND 26 AND v2>11 AND v3>22)) OR (v1<18 AND v2>=47 AND v3<11)) OR (v1>19));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 13) AND (t2.v3 > 20)) OR (((t2.v1 BETWEEN 18 AND 26) AND (t2.v2 > 11)) AND (t2.v3 > 22))) OR (((t2.v1 < 18) AND (t2.v2 >= 47)) AND (t2.v3 < 11))) OR (t2.v1 > 19))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 42 AND 54 AND v2>20) OR (v1<>68 AND v3>32));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 42 AND 54) AND (t2.v2 > 20)) OR ((NOT((t2.v1 = 68))) AND (t2.v3 > 32)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 20 AND 93) AND (v1=66 AND v2<>21 AND v3 BETWEEN 43 AND 94);`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 20 AND 93) AND (t2.v1 = 66)) AND (NOT((t2.v2 = 21))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>83 AND v2<>16 AND v3=22) AND (v1=34) AND (v1=79 AND v2<=45 AND v3=49);`,
		ExpectedPlan: "Filter(((((t2.v1 > 83) AND (NOT((t2.v2 = 16)))) AND (t2.v1 = 34)) AND (t2.v1 = 79)) AND (t2.v2 <= 45))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=44 AND v2<=98) AND (v1>15) OR (v1<=45 AND v2=1 AND v3<>54));`,
		ExpectedPlan: "Filter((((t2.v1 = 44) AND (t2.v2 <= 98)) AND (t2.v1 > 15)) OR (((t2.v1 <= 45) AND (t2.v2 = 1)) AND (NOT((t2.v3 = 54)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<38 AND v2>24) OR (v1<20 AND v3>=3 AND v4 BETWEEN 59 AND 81)) OR (v1<31 AND v4 BETWEEN 2 AND 16 AND v2=6 AND v3<=69));`,
		ExpectedPlan: "Filter((((t2.v1 < 38) AND (t2.v2 > 24)) OR (((t2.v1 < 20) AND (t2.v3 >= 3)) AND (t2.v4 BETWEEN 59 AND 81))) OR ((((t2.v1 < 31) AND (t2.v4 BETWEEN 2 AND 16)) AND (t2.v2 = 6)) AND (t2.v3 <= 69)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((((v1<43 AND v4<=22) OR (v1<=72 AND v2>=35 AND v3>=96)) OR (v1=63 AND v2=55 AND v3<>46)) OR (v1>=9 AND v2=52 AND v3=86 AND v4<=27)) OR (v1 BETWEEN 37 AND 62));`,
		ExpectedPlan: "Filter((((((t2.v1 < 43) AND (t2.v4 <= 22)) OR (((t2.v1 <= 72) AND (t2.v2 >= 35)) AND (t2.v3 >= 96))) OR (((t2.v1 = 63) AND (t2.v2 = 55)) AND (NOT((t2.v3 = 46))))) OR ((((t2.v1 >= 9) AND (t2.v2 = 52)) AND (t2.v3 = 86)) AND (t2.v4 <= 27))) OR (t2.v1 BETWEEN 37 AND 62))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=52) OR (v1>=59 AND v2<=30 AND v3=98 AND v4 BETWEEN 43 AND 74));`,
		ExpectedPlan: "Filter((t2.v1 = 52) OR ((((t2.v1 >= 59) AND (t2.v2 <= 30)) AND (t2.v3 = 98)) AND (t2.v4 BETWEEN 43 AND 74)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=37 AND v3>=74 AND v4=54) OR (v1>=36 AND v3<=42 AND v4<=94)) AND (v1=59 AND v2<=56) OR (v1>=83 AND v2<=11));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 37) AND (t2.v3 >= 74)) AND (t2.v4 = 54)) OR (((t2.v1 >= 36) AND (t2.v3 <= 42)) AND (t2.v4 <= 94))) AND ((t2.v1 = 59) AND (t2.v2 <= 56))) OR ((t2.v1 >= 83) AND (t2.v2 <= 11)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>39 AND v3<44 AND v4 BETWEEN 3 AND 31 AND v2>16) OR (v1>72 AND v2=73 AND v3<37 AND v4<=43)) OR (v1=9 AND v2<50));`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 39))) AND (t2.v3 < 44)) AND (t2.v4 BETWEEN 3 AND 31)) AND (t2.v2 > 16)) OR ((((t2.v1 > 72) AND (t2.v2 = 73)) AND (t2.v3 < 37)) AND (t2.v4 <= 43))) OR ((t2.v1 = 9) AND (t2.v2 < 50)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<31 AND v2<>14 AND v3 BETWEEN 0 AND 10 AND v4>=95) OR (v1<>91)) OR (v1<>35));`,
		ExpectedPlan: "Filter((((((t2.v1 < 31) AND (NOT((t2.v2 = 14)))) AND (t2.v3 BETWEEN 0 AND 10)) AND (t2.v4 >= 95)) OR (NOT((t2.v1 = 91)))) OR (NOT((t2.v1 = 35))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>13) OR (v1<>3 AND v4<=42 AND v2 BETWEEN 89 AND 94));`,
		ExpectedPlan: "Filter((t2.v1 > 13) OR (((NOT((t2.v1 = 3))) AND (t2.v4 <= 42)) AND (t2.v2 BETWEEN 89 AND 94)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<29 AND v2<=19) AND (v1>=26) OR (v1>=87 AND v2<=12 AND v3=36 AND v4<20)) AND (v1<=24 AND v4>85 AND v2 BETWEEN 1 AND 64) OR (v1>27 AND v2>=8 AND v3<24));`,
		ExpectedPlan: "Filter((((((t2.v1 < 29) AND (t2.v2 <= 19)) AND (t2.v1 >= 26)) OR ((((t2.v1 >= 87) AND (t2.v2 <= 12)) AND (t2.v3 = 36)) AND (t2.v4 < 20))) AND (((t2.v1 <= 24) AND (t2.v4 > 85)) AND (t2.v2 BETWEEN 1 AND 64))) OR (((t2.v1 > 27) AND (t2.v2 >= 8)) AND (t2.v3 < 24)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<77 AND v2 BETWEEN 5 AND 22 AND v3<>91 AND v4<34) OR (v1=68 AND v2=50)) OR (v1<44 AND v2>84 AND v3<37 AND v4>=67));`,
		ExpectedPlan: "Filter((((((t2.v1 < 77) AND (t2.v2 BETWEEN 5 AND 22)) AND (NOT((t2.v3 = 91)))) AND (t2.v4 < 34)) OR ((t2.v1 = 68) AND (t2.v2 = 50))) OR ((((t2.v1 < 44) AND (t2.v2 > 84)) AND (t2.v3 < 37)) AND (t2.v4 >= 67)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<4 AND v2>=71) OR (v1<18 AND v2=57));`,
		ExpectedPlan: "Filter(((t2.v1 < 4) AND (t2.v2 >= 71)) OR ((t2.v1 < 18) AND (t2.v2 = 57)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>61 AND v2 BETWEEN 46 AND 51) OR (v1 BETWEEN 32 AND 75 AND v4<=32)) AND (v1>97) OR (v1<97));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 61))) AND (t2.v2 BETWEEN 46 AND 51)) OR ((t2.v1 BETWEEN 32 AND 75) AND (t2.v4 <= 32))) AND (t2.v1 > 97)) OR (t2.v1 < 97))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 4 AND 71 AND v2<=70) AND (v1<>47 AND v2 BETWEEN 19 AND 65) OR (v1=59 AND v2 BETWEEN 25 AND 58));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 4 AND 71) AND (t2.v2 <= 70)) AND ((NOT((t2.v1 = 47))) AND (t2.v2 BETWEEN 19 AND 65))) OR ((t2.v1 = 59) AND (t2.v2 BETWEEN 25 AND 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<70 AND v2<=90) OR (v1<5 AND v2<>13 AND v3 BETWEEN 20 AND 96 AND v4>92)) OR (v1<>76)) OR (v1 BETWEEN 12 AND 88 AND v2 BETWEEN 53 AND 67 AND v3>=39));`,
		ExpectedPlan: "Filter(((((t2.v1 < 70) AND (t2.v2 <= 90)) OR ((((t2.v1 < 5) AND (NOT((t2.v2 = 13)))) AND (t2.v3 BETWEEN 20 AND 96)) AND (t2.v4 > 92))) OR (NOT((t2.v1 = 76)))) OR (((t2.v1 BETWEEN 12 AND 88) AND (t2.v2 BETWEEN 53 AND 67)) AND (t2.v3 >= 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1 BETWEEN 8 AND 38 AND v2<=31 AND v3 BETWEEN 30 AND 46 AND v4>=28) OR (v1<=22 AND v4<>40 AND v2>76 AND v3 BETWEEN 38 AND 42)) OR (v1<=52 AND v2<93 AND v3>=83)) OR (v1>=33 AND v3>13 AND v4>34));`,
		ExpectedPlan: "Filter(((((((t2.v1 BETWEEN 8 AND 38) AND (t2.v2 <= 31)) AND (t2.v3 BETWEEN 30 AND 46)) AND (t2.v4 >= 28)) OR ((((t2.v1 <= 22) AND (NOT((t2.v4 = 40)))) AND (t2.v2 > 76)) AND (t2.v3 BETWEEN 38 AND 42))) OR (((t2.v1 <= 52) AND (t2.v2 < 93)) AND (t2.v3 >= 83))) OR (((t2.v1 >= 33) AND (t2.v3 > 13)) AND (t2.v4 > 34)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 13 AND 40 AND v2>=0) OR (v1<>3 AND v2>47 AND v3<44 AND v4>49)) OR (v1=23));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 13 AND 40) AND (t2.v2 >= 0)) OR ((((NOT((t2.v1 = 3))) AND (t2.v2 > 47)) AND (t2.v3 < 44)) AND (t2.v4 > 49))) OR (t2.v1 = 23))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>35 AND v2<>26) OR (v1<=30 AND v2 BETWEEN 6 AND 61 AND v3<=95 AND v4>5)) AND (v1<>97) OR (v1>31));`,
		ExpectedPlan: "Filter(((((t2.v1 > 35) AND (NOT((t2.v2 = 26)))) OR ((((t2.v1 <= 30) AND (t2.v2 BETWEEN 6 AND 61)) AND (t2.v3 <= 95)) AND (t2.v4 > 5))) AND (NOT((t2.v1 = 97)))) OR (t2.v1 > 31))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1=43 AND v2>=64) OR (v1>6 AND v3=92 AND v4>=15)) OR (v1<=55 AND v3=6 AND v4<=77 AND v2<=3)) OR (v1=96 AND v3<=80 AND v4<=13));`,
		ExpectedPlan: "Filter(((((t2.v1 = 43) AND (t2.v2 >= 64)) OR (((t2.v1 > 6) AND (t2.v3 = 92)) AND (t2.v4 >= 15))) OR ((((t2.v1 <= 55) AND (t2.v3 = 6)) AND (t2.v4 <= 77)) AND (t2.v2 <= 3))) OR (((t2.v1 = 96) AND (t2.v3 <= 80)) AND (t2.v4 <= 13)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>8 AND v3 BETWEEN 14 AND 75 AND v4=28) AND (v1>=95 AND v2<>72 AND v3=22) OR (v1=5));`,
		ExpectedPlan: "Filter(((((t2.v1 > 8) AND (t2.v3 BETWEEN 14 AND 75)) AND (t2.v4 = 28)) AND (((t2.v1 >= 95) AND (NOT((t2.v2 = 72)))) AND (t2.v3 = 22))) OR (t2.v1 = 5))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=95 AND v2<1 AND v3 BETWEEN 49 AND 61 AND v4=51) OR (v1>29 AND v2>=9 AND v3>=63 AND v4<=88));`,
		ExpectedPlan: "Filter(((((t2.v1 = 95) AND (t2.v2 < 1)) AND (t2.v3 BETWEEN 49 AND 61)) AND (t2.v4 = 51)) OR ((((t2.v1 > 29) AND (t2.v2 >= 9)) AND (t2.v3 >= 63)) AND (t2.v4 <= 88)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>30 AND v2 BETWEEN 20 AND 64) AND (v1<=29) AND (v1>=25 AND v2<>0);`,
		ExpectedPlan: "Filter(((t2.v1 > 30) AND (t2.v1 <= 29)) AND (t2.v1 >= 25))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=89 AND v2<=1 AND v3<=7 AND v4>=4) AND (v1<=87) OR (v1 BETWEEN 10 AND 46 AND v2 BETWEEN 18 AND 76));`,
		ExpectedPlan: "Filter((((((t2.v1 = 89) AND (t2.v2 <= 1)) AND (t2.v3 <= 7)) AND (t2.v4 >= 4)) AND (t2.v1 <= 87)) OR ((t2.v1 BETWEEN 10 AND 46) AND (t2.v2 BETWEEN 18 AND 76)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=44 AND v2>=45 AND v3>=34 AND v4>1) OR (v1=33));`,
		ExpectedPlan: "Filter(((((t2.v1 = 44) AND (t2.v2 >= 45)) AND (t2.v3 >= 34)) AND (t2.v4 > 1)) OR (t2.v1 = 33))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<>12 AND v2<=6) OR (v1>99 AND v2<>51 AND v3=38)) OR (v1>60)) OR (v1 BETWEEN 69 AND 77 AND v2>=49 AND v3>=43));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 12))) AND (t2.v2 <= 6)) OR (((t2.v1 > 99) AND (NOT((t2.v2 = 51)))) AND (t2.v3 = 38))) OR (t2.v1 > 60)) OR (((t2.v1 BETWEEN 69 AND 77) AND (t2.v2 >= 49)) AND (t2.v3 >= 43)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 49 AND 53 AND v4 BETWEEN 22 AND 96) OR (v1 BETWEEN 7 AND 79)) AND (v1<=45 AND v2<=11) OR (v1 BETWEEN 16 AND 65 AND v2<53 AND v3<>15 AND v4>22));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 49 AND 53) AND (t2.v4 BETWEEN 22 AND 96)) OR (t2.v1 BETWEEN 7 AND 79)) AND ((t2.v1 <= 45) AND (t2.v2 <= 11))) OR ((((t2.v1 BETWEEN 16 AND 65) AND (t2.v2 < 53)) AND (NOT((t2.v3 = 15)))) AND (t2.v4 > 22)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<11) OR (v1<=38 AND v2>=93 AND v3<=34 AND v4>7));`,
		ExpectedPlan: "Filter((t2.v1 < 11) OR ((((t2.v1 <= 38) AND (t2.v2 >= 93)) AND (t2.v3 <= 34)) AND (t2.v4 > 7)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=97 AND v3<>2) OR (v1=49 AND v2 BETWEEN 29 AND 30 AND v3<>97));`,
		ExpectedPlan: "Filter(((t2.v1 <= 97) AND (NOT((t2.v3 = 2)))) OR (((t2.v1 = 49) AND (t2.v2 BETWEEN 29 AND 30)) AND (NOT((t2.v3 = 97)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=64) OR (v1>21 AND v2 BETWEEN 0 AND 58)) OR (v1<15 AND v4 BETWEEN 63 AND 76 AND v2>84));`,
		ExpectedPlan: "Filter(((t2.v1 <= 64) OR ((t2.v1 > 21) AND (t2.v2 BETWEEN 0 AND 58))) OR (((t2.v1 < 15) AND (t2.v4 BETWEEN 63 AND 76)) AND (t2.v2 > 84)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 24 AND 98 AND v2>0 AND v3>=87) OR (v1 BETWEEN 2 AND 3 AND v2 BETWEEN 15 AND 78));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 24 AND 98) AND (t2.v2 > 0)) AND (t2.v3 >= 87)) OR ((t2.v1 BETWEEN 2 AND 3) AND (t2.v2 BETWEEN 15 AND 78)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<>37) OR (v1<=94 AND v2 BETWEEN 53 AND 65 AND v3>=9)) OR (v1<10 AND v3<>26 AND v4<91)) OR (v1<>21 AND v2<>24 AND v3<46));`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 37))) OR (((t2.v1 <= 94) AND (t2.v2 BETWEEN 53 AND 65)) AND (t2.v3 >= 9))) OR (((t2.v1 < 10) AND (NOT((t2.v3 = 26)))) AND (t2.v4 < 91))) OR (((NOT((t2.v1 = 21))) AND (NOT((t2.v2 = 24)))) AND (t2.v3 < 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>21 AND v2>27 AND v3>=97 AND v4 BETWEEN 25 AND 67) OR (v1>=66 AND v2<=56)) OR (v1=37));`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 21))) AND (t2.v2 > 27)) AND (t2.v3 >= 97)) AND (t2.v4 BETWEEN 25 AND 67)) OR ((t2.v1 >= 66) AND (t2.v2 <= 56))) OR (t2.v1 = 37))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=43 AND v2<48 AND v3<16 AND v4<=75) OR (v1<71));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 43) AND (t2.v2 < 48)) AND (t2.v3 < 16)) AND (t2.v4 <= 75)) OR (t2.v1 < 71))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>91 AND v2=91 AND v3>=15) OR (v1 BETWEEN 16 AND 30)) OR (v1<>27 AND v4=62));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 91))) AND (t2.v2 = 91)) AND (t2.v3 >= 15)) OR (t2.v1 BETWEEN 16 AND 30)) OR ((NOT((t2.v1 = 27))) AND (t2.v4 = 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=54 AND v3>26 AND v4>30 AND v2 BETWEEN 3 AND 8) OR (v1>8 AND v2<=43 AND v3<>97));`,
		ExpectedPlan: "Filter(((((t2.v1 = 54) AND (t2.v3 > 26)) AND (t2.v4 > 30)) AND (t2.v2 BETWEEN 3 AND 8)) OR (((t2.v1 > 8) AND (t2.v2 <= 43)) AND (NOT((t2.v3 = 97)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=38 AND v2<>11 AND v3>=26) OR (v1 BETWEEN 37 AND 90 AND v4<85 AND v2<0)) OR (v1<>23));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 38) AND (NOT((t2.v2 = 11)))) AND (t2.v3 >= 26)) OR (((t2.v1 BETWEEN 37 AND 90) AND (t2.v4 < 85)) AND (t2.v2 < 0))) OR (NOT((t2.v1 = 23))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<20 AND v2<>84 AND v3<25 AND v4>=93) OR (v1<13));`,
		ExpectedPlan: "Filter(((((t2.v1 < 20) AND (NOT((t2.v2 = 84)))) AND (t2.v3 < 25)) AND (t2.v4 >= 93)) OR (t2.v1 < 13))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=81 AND v2 BETWEEN 55 AND 77 AND v3=64) OR (v1=20 AND v2=21));`,
		ExpectedPlan: "Filter((((t2.v1 >= 81) AND (t2.v2 BETWEEN 55 AND 77)) AND (t2.v3 = 64)) OR ((t2.v1 = 20) AND (t2.v2 = 21)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>30 AND v2 BETWEEN 58 AND 72 AND v3<=35) OR (v1 BETWEEN 28 AND 28 AND v2>=76)) OR (v1=74 AND v2<26));`,
		ExpectedPlan: "Filter(((((t2.v1 > 30) AND (t2.v2 BETWEEN 58 AND 72)) AND (t2.v3 <= 35)) OR ((t2.v1 BETWEEN 28 AND 28) AND (t2.v2 >= 76))) OR ((t2.v1 = 74) AND (t2.v2 < 26)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>5 AND v2>8) OR (v1>78 AND v2<=39 AND v3>=41 AND v4<=35)) AND (v1<=11 AND v2<35 AND v3<=10 AND v4<76) OR (v1>=22)) OR (v1=1 AND v4<>29 AND v2 BETWEEN 64 AND 81 AND v3>46));`,
		ExpectedPlan: "Filter((((((t2.v1 > 5) AND (t2.v2 > 8)) OR ((((t2.v1 > 78) AND (t2.v2 <= 39)) AND (t2.v3 >= 41)) AND (t2.v4 <= 35))) AND ((((t2.v1 <= 11) AND (t2.v2 < 35)) AND (t2.v3 <= 10)) AND (t2.v4 < 76))) OR (t2.v1 >= 22)) OR ((((t2.v1 = 1) AND (NOT((t2.v4 = 29)))) AND (t2.v2 BETWEEN 64 AND 81)) AND (t2.v3 > 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=49) OR (v1<43 AND v2>=34));`,
		ExpectedPlan: "Filter((t2.v1 = 49) OR ((t2.v1 < 43) AND (t2.v2 >= 34)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>=72) OR (v1<>17)) OR (v1=47 AND v2<>1 AND v3 BETWEEN 75 AND 78 AND v4 BETWEEN 10 AND 44)) OR (v1>=64 AND v2>=74 AND v3=10 AND v4 BETWEEN 11 AND 93));`,
		ExpectedPlan: "Filter((((t2.v1 >= 72) OR (NOT((t2.v1 = 17)))) OR ((((t2.v1 = 47) AND (NOT((t2.v2 = 1)))) AND (t2.v3 BETWEEN 75 AND 78)) AND (t2.v4 BETWEEN 10 AND 44))) OR ((((t2.v1 >= 64) AND (t2.v2 >= 74)) AND (t2.v3 = 10)) AND (t2.v4 BETWEEN 11 AND 93)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<2 AND v2<>94) OR (v1<>76 AND v2=27 AND v3<=31 AND v4<38));`,
		ExpectedPlan: "Filter(((t2.v1 < 2) AND (NOT((t2.v2 = 94)))) OR ((((NOT((t2.v1 = 76))) AND (t2.v2 = 27)) AND (t2.v3 <= 31)) AND (t2.v4 < 38)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<>11 AND v2>47 AND v3>=67 AND v4=29) OR (v1>=59 AND v3 BETWEEN 4 AND 29 AND v4>=65 AND v2<>96)) OR (v1<=62)) OR (v1<61 AND v2<>28 AND v3<>8 AND v4<>30));`,
		ExpectedPlan: "Filter(((((((NOT((t2.v1 = 11))) AND (t2.v2 > 47)) AND (t2.v3 >= 67)) AND (t2.v4 = 29)) OR ((((t2.v1 >= 59) AND (t2.v3 BETWEEN 4 AND 29)) AND (t2.v4 >= 65)) AND (NOT((t2.v2 = 96))))) OR (t2.v1 <= 62)) OR ((((t2.v1 < 61) AND (NOT((t2.v2 = 28)))) AND (NOT((t2.v3 = 8)))) AND (NOT((t2.v4 = 30)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 36 AND 72) OR (v1<>48 AND v4>91 AND v2<5 AND v3>=38)) OR (v1<>17 AND v3=50));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 36 AND 72) OR ((((NOT((t2.v1 = 48))) AND (t2.v4 > 91)) AND (t2.v2 < 5)) AND (t2.v3 >= 38))) OR ((NOT((t2.v1 = 17))) AND (t2.v3 = 50)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<86) OR (v1<=5 AND v2<25 AND v3<>24)) OR (v1<32 AND v3 BETWEEN 51 AND 54 AND v4<=70));`,
		ExpectedPlan: "Filter(((t2.v1 < 86) OR (((t2.v1 <= 5) AND (t2.v2 < 25)) AND (NOT((t2.v3 = 24))))) OR (((t2.v1 < 32) AND (t2.v3 BETWEEN 51 AND 54)) AND (t2.v4 <= 70)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<=6) OR (v1 BETWEEN 24 AND 89)) OR (v1<87 AND v2=35 AND v3=19)) AND (v1>94 AND v2=33 AND v3>28) OR (v1 BETWEEN 36 AND 40));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 6) OR (t2.v1 BETWEEN 24 AND 89)) OR (((t2.v1 < 87) AND (t2.v2 = 35)) AND (t2.v3 = 19))) AND (((t2.v1 > 94) AND (t2.v2 = 33)) AND (t2.v3 > 28))) OR (t2.v1 BETWEEN 36 AND 40))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=24 AND v2=61 AND v3<49 AND v4<82) OR (v1<4 AND v2>51 AND v3=9));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 24) AND (t2.v2 = 61)) AND (t2.v3 < 49)) AND (t2.v4 < 82)) OR (((t2.v1 < 4) AND (t2.v2 > 51)) AND (t2.v3 = 9)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 0 AND 87 AND v2>=44 AND v3<>68 AND v4=50) OR (v1<1 AND v4<66 AND v2<11 AND v3<>44));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 0 AND 87) AND (t2.v2 >= 44)) AND (NOT((t2.v3 = 68)))) AND (t2.v4 = 50)) OR ((((t2.v1 < 1) AND (t2.v4 < 66)) AND (t2.v2 < 11)) AND (NOT((t2.v3 = 44)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1<17 AND v2<54) AND (v1>=70 AND v2 BETWEEN 53 AND 53 AND v3>10 AND v4=17);`,
		ExpectedPlan: "Filter(((((t2.v1 < 17) AND (t2.v2 < 54)) AND (t2.v1 >= 70)) AND (t2.v2 BETWEEN 53 AND 53)) AND (t2.v3 > 10))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1=21 AND v2>25 AND v3>=7) OR (v1 BETWEEN 23 AND 88 AND v2<=26 AND v3>=87 AND v4 BETWEEN 42 AND 95)) OR (v1<4 AND v2>=66 AND v3<=24 AND v4=10)) OR (v1>69));`,
		ExpectedPlan: "Filter((((((t2.v1 = 21) AND (t2.v2 > 25)) AND (t2.v3 >= 7)) OR ((((t2.v1 BETWEEN 23 AND 88) AND (t2.v2 <= 26)) AND (t2.v3 >= 87)) AND (t2.v4 BETWEEN 42 AND 95))) OR ((((t2.v1 < 4) AND (t2.v2 >= 66)) AND (t2.v3 <= 24)) AND (t2.v4 = 10))) OR (t2.v1 > 69))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 0 AND 39) OR (v1<18 AND v4>=90));`,
		ExpectedPlan: "Filter((t2.v1 BETWEEN 0 AND 39) OR ((t2.v1 < 18) AND (t2.v4 >= 90)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<99 AND v2>1 AND v3<=56) OR (v1>36 AND v2=53 AND v3>17)) OR (v1<>71)) AND (v1 BETWEEN 2 AND 86 AND v2<>78 AND v3<>29 AND v4<>63);`,
		ExpectedPlan: "Filter((((((((t2.v1 < 99) AND (t2.v2 > 1)) AND (t2.v3 <= 56)) OR (((t2.v1 > 36) AND (t2.v2 = 53)) AND (t2.v3 > 17))) OR (NOT((t2.v1 = 71)))) AND (t2.v1 BETWEEN 2 AND 86)) AND (NOT((t2.v2 = 78)))) AND (NOT((t2.v3 = 29))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=5) OR (v1=53));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<>12 AND v2 BETWEEN 27 AND 46 AND v3 BETWEEN 19 AND 27 AND v4>=50) OR (v1 BETWEEN 17 AND 88)) OR (v1<=36 AND v2<=37 AND v3<64)) OR (v1<>82 AND v2>84 AND v3>=90)) AND (v1>34 AND v3>4);`,
		ExpectedPlan: "Filter((((((((NOT((t2.v1 = 12))) AND (t2.v2 BETWEEN 27 AND 46)) AND (t2.v3 BETWEEN 19 AND 27)) AND (t2.v4 >= 50)) OR (t2.v1 BETWEEN 17 AND 88)) OR (((t2.v1 <= 36) AND (t2.v2 <= 37)) AND (t2.v3 < 64))) OR (((NOT((t2.v1 = 82))) AND (t2.v2 > 84)) AND (t2.v3 >= 90))) AND (t2.v3 > 4))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=82) OR (v1<=95 AND v2<>23 AND v3<18 AND v4<>50));`,
		ExpectedPlan: "Filter((t2.v1 >= 82) OR ((((t2.v1 <= 95) AND (NOT((t2.v2 = 23)))) AND (t2.v3 < 18)) AND (NOT((t2.v4 = 50)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=51) AND (v1=55 AND v2>=59 AND v3>=49) OR (v1>5 AND v2<34));`,
		ExpectedPlan: "Filter(((t2.v1 = 51) AND (((t2.v1 = 55) AND (t2.v2 >= 59)) AND (t2.v3 >= 49))) OR ((t2.v1 > 5) AND (t2.v2 < 34)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>4 AND v2<=21 AND v3>=15) OR (v1=93 AND v2>=1 AND v3<>63)) OR (v1 BETWEEN 24 AND 86 AND v3<=5));`,
		ExpectedPlan: "Filter(((((t2.v1 > 4) AND (t2.v2 <= 21)) AND (t2.v3 >= 15)) OR (((t2.v1 = 93) AND (t2.v2 >= 1)) AND (NOT((t2.v3 = 63))))) OR ((t2.v1 BETWEEN 24 AND 86) AND (t2.v3 <= 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<63 AND v2<>32 AND v3>=14) OR (v1=18 AND v3 BETWEEN 4 AND 42 AND v4>10)) OR (v1<23 AND v2>=21));`,
		ExpectedPlan: "Filter(((((t2.v1 < 63) AND (NOT((t2.v2 = 32)))) AND (t2.v3 >= 14)) OR (((t2.v1 = 18) AND (t2.v3 BETWEEN 4 AND 42)) AND (t2.v4 > 10))) OR ((t2.v1 < 23) AND (t2.v2 >= 21)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>34 AND v3 BETWEEN 27 AND 48 AND v4<=11 AND v2>42) AND (v1<>47 AND v2<48 AND v3<=47 AND v4<>12) OR (v1<=36 AND v2<>17));`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 34))) AND (t2.v3 BETWEEN 27 AND 48)) AND (t2.v4 <= 11)) AND (t2.v2 > 42)) AND ((((NOT((t2.v1 = 47))) AND (t2.v2 < 48)) AND (t2.v3 <= 47)) AND (NOT((t2.v4 = 12))))) OR ((t2.v1 <= 36) AND (NOT((t2.v2 = 17)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=34 AND v2<=80 AND v3<=27) AND (v1 BETWEEN 0 AND 33) OR (v1<=56 AND v2=50 AND v3 BETWEEN 0 AND 5 AND v4<>31));`,
		ExpectedPlan: "Filter(((((t2.v1 = 34) AND (t2.v2 <= 80)) AND (t2.v3 <= 27)) AND (t2.v1 BETWEEN 0 AND 33)) OR ((((t2.v1 <= 56) AND (t2.v2 = 50)) AND (t2.v3 BETWEEN 0 AND 5)) AND (NOT((t2.v4 = 31)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=93 AND v2<>5) OR (v1>=81 AND v4=9 AND v2>33 AND v3<99));`,
		ExpectedPlan: "Filter(((t2.v1 <= 93) AND (NOT((t2.v2 = 5)))) OR ((((t2.v1 >= 81) AND (t2.v4 = 9)) AND (t2.v2 > 33)) AND (t2.v3 < 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=37 AND v2=4 AND v3=3) AND (v1=12 AND v2>9 AND v3<89 AND v4<>12) OR (v1=1 AND v2=43 AND v3<=2));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 37) AND (t2.v2 = 4)) AND (t2.v3 = 3)) AND ((((t2.v1 = 12) AND (t2.v2 > 9)) AND (t2.v3 < 89)) AND (NOT((t2.v4 = 12))))) OR (((t2.v1 = 1) AND (t2.v2 = 43)) AND (t2.v3 <= 2)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=82) OR (v1<=4 AND v2>=51)) OR (v1=58 AND v4<86));`,
		ExpectedPlan: "Filter(((t2.v1 = 82) OR ((t2.v1 <= 4) AND (t2.v2 >= 51))) OR ((t2.v1 = 58) AND (t2.v4 < 86)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>=42 AND v4<85 AND v2<8 AND v3<3) OR (v1>=78 AND v2<>28 AND v3<52)) OR (v1<8 AND v2<>76 AND v3 BETWEEN 36 AND 70)) OR (v1=70));`,
		ExpectedPlan: "Filter(((((((t2.v1 >= 42) AND (t2.v4 < 85)) AND (t2.v2 < 8)) AND (t2.v3 < 3)) OR (((t2.v1 >= 78) AND (NOT((t2.v2 = 28)))) AND (t2.v3 < 52))) OR (((t2.v1 < 8) AND (NOT((t2.v2 = 76)))) AND (t2.v3 BETWEEN 36 AND 70))) OR (t2.v1 = 70))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>69) OR (v1>=43));`,
		ExpectedPlan: "Filter((NOT((t2.v1 = 69))) OR (t2.v1 >= 43))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 39 AND 76 AND v4>16 AND v2<>15 AND v3<>35) AND (v1<>50 AND v2>21 AND v3 BETWEEN 27 AND 90 AND v4>18) OR (v1<25 AND v4=58));`,
		ExpectedPlan: "Filter((((((t2.v1 BETWEEN 39 AND 76) AND (t2.v4 > 16)) AND (NOT((t2.v2 = 15)))) AND (NOT((t2.v3 = 35)))) AND ((((NOT((t2.v1 = 50))) AND (t2.v2 > 21)) AND (t2.v3 BETWEEN 27 AND 90)) AND (t2.v4 > 18))) OR ((t2.v1 < 25) AND (t2.v4 = 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=62) OR (v1 BETWEEN 24 AND 36 AND v2>=94 AND v3 BETWEEN 10 AND 55 AND v4>=89));`,
		ExpectedPlan: "Filter((t2.v1 = 62) OR ((((t2.v1 BETWEEN 24 AND 36) AND (t2.v2 >= 94)) AND (t2.v3 BETWEEN 10 AND 55)) AND (t2.v4 >= 89)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=31) OR (v1<=95 AND v2<=26 AND v3 BETWEEN 40 AND 72)) OR (v1<51 AND v2=23));`,
		ExpectedPlan: "Filter(((t2.v1 = 31) OR (((t2.v1 <= 95) AND (t2.v2 <= 26)) AND (t2.v3 BETWEEN 40 AND 72))) OR ((t2.v1 < 51) AND (t2.v2 = 23)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=3) OR (v1>40)) AND (v1>66 AND v2>33);`,
		ExpectedPlan: "Filter(t2.v1 > 66)\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=69 AND v2=61 AND v3=87 AND v4 BETWEEN 63 AND 87) OR (v1 BETWEEN 48 AND 62)) OR (v1<>81 AND v2<=67 AND v3<>43));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 69) AND (t2.v2 = 61)) AND (t2.v3 = 87)) AND (t2.v4 BETWEEN 63 AND 87)) OR (t2.v1 BETWEEN 48 AND 62)) OR (((NOT((t2.v1 = 81))) AND (t2.v2 <= 67)) AND (NOT((t2.v3 = 43)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=19) AND (v1<=20 AND v2>=2) OR (v1 BETWEEN 12 AND 53 AND v4>=1 AND v2<43 AND v3<59));`,
		ExpectedPlan: "Filter(((t2.v1 = 19) AND ((t2.v1 <= 20) AND (t2.v2 >= 2))) OR ((((t2.v1 BETWEEN 12 AND 53) AND (t2.v4 >= 1)) AND (t2.v2 < 43)) AND (t2.v3 < 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=42 AND v2<=65) AND (v1<=21) OR (v1<=14 AND v2<>1 AND v3<62));`,
		ExpectedPlan: "Filter((((t2.v1 = 42) AND (t2.v2 <= 65)) AND (t2.v1 <= 21)) OR (((t2.v1 <= 14) AND (NOT((t2.v2 = 1)))) AND (t2.v3 < 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>5) OR (v1<96 AND v2>=14)) OR (v1<>96)) AND (v1<>51 AND v3>41);`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 5))) OR ((t2.v1 < 96) AND (t2.v2 >= 14))) OR (NOT((t2.v1 = 96)))) AND (t2.v3 > 41))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>97 AND v3<>77 AND v4=30 AND v2<>45) OR (v1=36 AND v2<77 AND v3>94)) OR (v1=26));`,
		ExpectedPlan: "Filter((((((t2.v1 > 97) AND (NOT((t2.v3 = 77)))) AND (t2.v4 = 30)) AND (NOT((t2.v2 = 45)))) OR (((t2.v1 = 36) AND (t2.v2 < 77)) AND (t2.v3 > 94))) OR (t2.v1 = 26))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 34 AND 37 AND v3>23 AND v4>31) OR (v1 BETWEEN 43 AND 81 AND v3>=54 AND v4>=72));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 34 AND 37) AND (t2.v3 > 23)) AND (t2.v4 > 31)) OR (((t2.v1 BETWEEN 43 AND 81) AND (t2.v3 >= 54)) AND (t2.v4 >= 72)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=17 AND v2<>19) OR (v1>45));`,
		ExpectedPlan: "Filter(((t2.v1 >= 17) AND (NOT((t2.v2 = 19)))) OR (t2.v1 > 45))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=57) OR (v1>=1 AND v2<=5 AND v3>=10 AND v4<5)) OR (v1>55));`,
		ExpectedPlan: "Filter(((t2.v1 = 57) OR ((((t2.v1 >= 1) AND (t2.v2 <= 5)) AND (t2.v3 >= 10)) AND (t2.v4 < 5))) OR (t2.v1 > 55))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=23 AND v2<=48) OR (v1>41 AND v2>=46 AND v3 BETWEEN 11 AND 29)) AND (v1<>11) OR (v1=70 AND v3<54 AND v4<=47 AND v2<>62));`,
		ExpectedPlan: "Filter(((((t2.v1 = 23) AND (t2.v2 <= 48)) OR (((t2.v1 > 41) AND (t2.v2 >= 46)) AND (t2.v3 BETWEEN 11 AND 29))) AND (NOT((t2.v1 = 11)))) OR ((((t2.v1 = 70) AND (t2.v3 < 54)) AND (t2.v4 <= 47)) AND (NOT((t2.v2 = 62)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>73) OR (v1>5 AND v2>=7 AND v3>=43 AND v4<=53)) OR (v1<34 AND v2<95 AND v3 BETWEEN 9 AND 81 AND v4<>8)) AND (v1<=68 AND v4>48 AND v2>11 AND v3 BETWEEN 17 AND 89) OR (v1=41 AND v2 BETWEEN 56 AND 93));`,
		ExpectedPlan: "Filter(((((t2.v1 > 73) OR ((((t2.v1 > 5) AND (t2.v2 >= 7)) AND (t2.v3 >= 43)) AND (t2.v4 <= 53))) OR ((((t2.v1 < 34) AND (t2.v2 < 95)) AND (t2.v3 BETWEEN 9 AND 81)) AND (NOT((t2.v4 = 8))))) AND ((((t2.v1 <= 68) AND (t2.v4 > 48)) AND (t2.v2 > 11)) AND (t2.v3 BETWEEN 17 AND 89))) OR ((t2.v1 = 41) AND (t2.v2 BETWEEN 56 AND 93)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>3 AND v3>=34) OR (v1<>31 AND v2<16 AND v3<8));`,
		ExpectedPlan: "Filter(((NOT((t2.v1 = 3))) AND (t2.v3 >= 34)) OR (((NOT((t2.v1 = 31))) AND (t2.v2 < 16)) AND (t2.v3 < 8)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 88 AND 97) OR (v1>67 AND v4<=27 AND v2<5 AND v3>40)) OR (v1 BETWEEN 5 AND 83 AND v2>=34 AND v3=59));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 88 AND 97) OR ((((t2.v1 > 67) AND (t2.v4 <= 27)) AND (t2.v2 < 5)) AND (t2.v3 > 40))) OR (((t2.v1 BETWEEN 5 AND 83) AND (t2.v2 >= 34)) AND (t2.v3 = 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>96 AND v2<=2 AND v3=17 AND v4<79) OR (v1=67 AND v2=30 AND v3=38 AND v4=53));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 96))) AND (t2.v2 <= 2)) AND (t2.v3 = 17)) AND (t2.v4 < 79)) OR ((((t2.v1 = 67) AND (t2.v2 = 30)) AND (t2.v3 = 38)) AND (t2.v4 = 53)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>45 AND v2>76) OR (v1=30 AND v2=53));`,
		ExpectedPlan: "Filter(((NOT((t2.v1 = 45))) AND (t2.v2 > 76)) OR ((t2.v1 = 30) AND (t2.v2 = 53)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 3 AND 34 AND v2>39) OR (v1>1 AND v2>=92 AND v3=99)) OR (v1>=36 AND v2<>65 AND v3=69));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 3 AND 34) AND (t2.v2 > 39)) OR (((t2.v1 > 1) AND (t2.v2 >= 92)) AND (t2.v3 = 99))) OR (((t2.v1 >= 36) AND (NOT((t2.v2 = 65)))) AND (t2.v3 = 69)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=54 AND v2=38 AND v3>=64 AND v4>36) OR (v1<=48)) OR (v1<37 AND v2=13 AND v3<20));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 54) AND (t2.v2 = 38)) AND (t2.v3 >= 64)) AND (t2.v4 > 36)) OR (t2.v1 <= 48)) OR (((t2.v1 < 37) AND (t2.v2 = 13)) AND (t2.v3 < 20)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>70) OR (v1<>2 AND v2>79 AND v3<>6 AND v4<>42));`,
		ExpectedPlan: "Filter((NOT((t2.v1 = 70))) OR ((((NOT((t2.v1 = 2))) AND (t2.v2 > 79)) AND (NOT((t2.v3 = 6)))) AND (NOT((t2.v4 = 42)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>46 AND v2>93 AND v3>19) AND (v1<51 AND v2=39) OR (v1<61)) AND (v1<>22);`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 46))) AND (t2.v2 > 93)) AND (t2.v3 > 19)) AND ((t2.v1 < 51) AND (t2.v2 = 39))) OR (t2.v1 < 61))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=53 AND v2>0 AND v3=95 AND v4<=2) OR (v1<41 AND v4<10 AND v2 BETWEEN 11 AND 35)) OR (v1=11 AND v2<20 AND v3=51 AND v4<>30));`,
		ExpectedPlan: "Filter((((((t2.v1 <= 53) AND (t2.v2 > 0)) AND (t2.v3 = 95)) AND (t2.v4 <= 2)) OR (((t2.v1 < 41) AND (t2.v4 < 10)) AND (t2.v2 BETWEEN 11 AND 35))) OR ((((t2.v1 = 11) AND (t2.v2 < 20)) AND (t2.v3 = 51)) AND (NOT((t2.v4 = 30)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=32 AND v2>6 AND v3=55) OR (v1=87 AND v2<=80)) OR (v1=88 AND v2<=87 AND v3>=45));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 32) AND (t2.v2 > 6)) AND (t2.v3 = 55)) OR ((t2.v1 = 87) AND (t2.v2 <= 80))) OR (((t2.v1 = 88) AND (t2.v2 <= 87)) AND (t2.v3 >= 45)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>8) OR (v1 BETWEEN 16 AND 25 AND v2<>79 AND v3>=55 AND v4<=5));`,
		ExpectedPlan: "Filter((t2.v1 > 8) OR ((((t2.v1 BETWEEN 16 AND 25) AND (NOT((t2.v2 = 79)))) AND (t2.v3 >= 55)) AND (t2.v4 <= 5)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=45 AND v2>55 AND v3<90) OR (v1>26 AND v2>=2 AND v3<>85 AND v4<=74));`,
		ExpectedPlan: "Filter((((t2.v1 = 45) AND (t2.v2 > 55)) AND (t2.v3 < 90)) OR ((((t2.v1 > 26) AND (t2.v2 >= 2)) AND (NOT((t2.v3 = 85)))) AND (t2.v4 <= 74)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=59) OR (v1<>85 AND v4<6 AND v2 BETWEEN 14 AND 82));`,
		ExpectedPlan: "Filter((t2.v1 = 59) OR (((NOT((t2.v1 = 85))) AND (t2.v4 < 6)) AND (t2.v2 BETWEEN 14 AND 82)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=94 AND v2>32 AND v3>61) OR (v1>51 AND v4>84 AND v2>=46)) OR (v1=39));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 94) AND (t2.v2 > 32)) AND (t2.v3 > 61)) OR (((t2.v1 > 51) AND (t2.v4 > 84)) AND (t2.v2 >= 46))) OR (t2.v1 = 39))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=89) OR (v1<=28 AND v2=13));`,
		ExpectedPlan: "Filter((t2.v1 >= 89) OR ((t2.v1 <= 28) AND (t2.v2 = 13)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=5 AND v2<65 AND v3<64 AND v4=81) OR (v1<=75)) AND (v1=87);`,
		ExpectedPlan: "Filter(((((t2.v1 <= 5) AND (t2.v2 < 65)) AND (t2.v3 < 64)) AND (t2.v4 = 81)) OR (t2.v1 <= 75))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=31 AND v4>30 AND v2<>38) OR (v1<>35)) OR (v1<=8 AND v2<43 AND v3<=50 AND v4<=33));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 31) AND (t2.v4 > 30)) AND (NOT((t2.v2 = 38)))) OR (NOT((t2.v1 = 35)))) OR ((((t2.v1 <= 8) AND (t2.v2 < 43)) AND (t2.v3 <= 50)) AND (t2.v4 <= 33)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((((v1>65 AND v2=89 AND v3>12) OR (v1 BETWEEN 37 AND 75 AND v2=42 AND v3<=14)) OR (v1>=87 AND v2=85)) OR (v1<>48 AND v4 BETWEEN 32 AND 33 AND v2>21 AND v3<=25)) OR (v1 BETWEEN 51 AND 88 AND v2<>67));`,
		ExpectedPlan: "Filter(((((((t2.v1 > 65) AND (t2.v2 = 89)) AND (t2.v3 > 12)) OR (((t2.v1 BETWEEN 37 AND 75) AND (t2.v2 = 42)) AND (t2.v3 <= 14))) OR ((t2.v1 >= 87) AND (t2.v2 = 85))) OR ((((NOT((t2.v1 = 48))) AND (t2.v4 BETWEEN 32 AND 33)) AND (t2.v2 > 21)) AND (t2.v3 <= 25))) OR ((t2.v1 BETWEEN 51 AND 88) AND (NOT((t2.v2 = 67)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>82) OR (v1<1 AND v3>=22)) AND (v1=4) OR (v1>27 AND v2 BETWEEN 7 AND 79 AND v3 BETWEEN 9 AND 29 AND v4<85));`,
		ExpectedPlan: "Filter((((t2.v1 > 82) OR ((t2.v1 < 1) AND (t2.v3 >= 22))) AND (t2.v1 = 4)) OR ((((t2.v1 > 27) AND (t2.v2 BETWEEN 7 AND 79)) AND (t2.v3 BETWEEN 9 AND 29)) AND (t2.v4 < 85)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>=41 AND v2<13 AND v3 BETWEEN 62 AND 87) AND (v1<=67 AND v2>68 AND v3=56 AND v4>28);`,
		ExpectedPlan: "Filter((((((t2.v1 >= 41) AND (t2.v2 < 13)) AND (t2.v3 BETWEEN 62 AND 87)) AND (t2.v1 <= 67)) AND (t2.v2 > 68)) AND (t2.v3 = 56))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 23 AND 34 AND v2 BETWEEN 4 AND 75 AND v3<91) OR (v1>=31));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 23 AND 34) AND (t2.v2 BETWEEN 4 AND 75)) AND (t2.v3 < 91)) OR (t2.v1 >= 31))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<59) OR (v1 BETWEEN 6 AND 86 AND v4<97)) OR (v1<>90 AND v2=43 AND v3=29));`,
		ExpectedPlan: "Filter(((t2.v1 < 59) OR ((t2.v1 BETWEEN 6 AND 86) AND (t2.v4 < 97))) OR (((NOT((t2.v1 = 90))) AND (t2.v2 = 43)) AND (t2.v3 = 29)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=1 AND v2<34) OR (v1<78));`,
		ExpectedPlan: "Filter(((t2.v1 >= 1) AND (t2.v2 < 34)) OR (t2.v1 < 78))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=10 AND v2<>64 AND v3>25 AND v4<29) OR (v1>39));`,
		ExpectedPlan: "Filter(((((t2.v1 = 10) AND (NOT((t2.v2 = 64)))) AND (t2.v3 > 25)) AND (t2.v4 < 29)) OR (t2.v1 > 39))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>35 AND v2>=14 AND v3<65 AND v4<>9) OR (v1<>14 AND v3<51 AND v4<32)) OR (v1>=21 AND v3<>25 AND v4<>16));`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 35))) AND (t2.v2 >= 14)) AND (t2.v3 < 65)) AND (NOT((t2.v4 = 9)))) OR (((NOT((t2.v1 = 14))) AND (t2.v3 < 51)) AND (t2.v4 < 32))) OR (((t2.v1 >= 21) AND (NOT((t2.v3 = 25)))) AND (NOT((t2.v4 = 16)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>12 AND v2<0) OR (v1=36 AND v3<37));`,
		ExpectedPlan: "Filter(((t2.v1 > 12) AND (t2.v2 < 0)) OR ((t2.v1 = 36) AND (t2.v3 < 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1=83 AND v3>=72 AND v4<=74) AND (v1>61 AND v2 BETWEEN 32 AND 44);`,
		ExpectedPlan: "Filter((((t2.v1 = 83) AND (t2.v3 >= 72)) AND (t2.v1 > 61)) AND (t2.v2 BETWEEN 32 AND 44))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1=78 AND v2>28 AND v3<=47) AND (v1<35 AND v2=69 AND v3>16);`,
		ExpectedPlan: "Filter((((t2.v1 = 78) AND (t2.v2 > 28)) AND (t2.v1 < 35)) AND (t2.v2 = 69))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 31 AND 49 AND v2=20 AND v3 BETWEEN 8 AND 46) AND (v1<>57 AND v2<5);`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 31 AND 49) AND (t2.v2 = 20)) AND (NOT((t2.v1 = 57)))) AND (t2.v2 < 5))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=39 AND v2<>3) OR (v1=97 AND v2<>37));`,
		ExpectedPlan: "Filter(((t2.v1 <= 39) AND (NOT((t2.v2 = 3)))) OR ((t2.v1 = 97) AND (NOT((t2.v2 = 37)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=19 AND v4<>62 AND v2<>19 AND v3<>29) OR (v1 BETWEEN 37 AND 75 AND v4<23 AND v2 BETWEEN 6 AND 43));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 19) AND (NOT((t2.v4 = 62)))) AND (NOT((t2.v2 = 19)))) AND (NOT((t2.v3 = 29)))) OR (((t2.v1 BETWEEN 37 AND 75) AND (t2.v4 < 23)) AND (t2.v2 BETWEEN 6 AND 43)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<8 AND v2<=33 AND v3 BETWEEN 54 AND 85) OR (v1=46));`,
		ExpectedPlan: "Filter((((t2.v1 < 8) AND (t2.v2 <= 33)) AND (t2.v3 BETWEEN 54 AND 85)) OR (t2.v1 = 46))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=69 AND v2<8) AND (v1>=34 AND v2>=99 AND v3>96 AND v4 BETWEEN 36 AND 99) OR (v1=0 AND v2>=71));`,
		ExpectedPlan: "Filter((((t2.v1 <= 69) AND (t2.v2 < 8)) AND ((((t2.v1 >= 34) AND (t2.v2 >= 99)) AND (t2.v3 > 96)) AND (t2.v4 BETWEEN 36 AND 99))) OR ((t2.v1 = 0) AND (t2.v2 >= 71)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 20 AND 54 AND v2<>31 AND v3 BETWEEN 15 AND 21) OR (v1<=46 AND v3>76)) OR (v1 BETWEEN 31 AND 71));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 20 AND 54) AND (NOT((t2.v2 = 31)))) AND (t2.v3 BETWEEN 15 AND 21)) OR ((t2.v1 <= 46) AND (t2.v3 > 76))) OR (t2.v1 BETWEEN 31 AND 71))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>37 AND v2<>5 AND v3=8 AND v4 BETWEEN 26 AND 50) OR (v1>=53)) AND (v1 BETWEEN 5 AND 80);`,
		ExpectedPlan: "Filter(((((t2.v1 > 37) AND (NOT((t2.v2 = 5)))) AND (t2.v3 = 8)) AND (t2.v4 BETWEEN 26 AND 50)) OR (t2.v1 >= 53))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=25) OR (v1<=87));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=99 AND v2>=85) AND (v1<=83 AND v2=99) OR (v1<=6 AND v2 BETWEEN 36 AND 68 AND v3>62 AND v4=79));`,
		ExpectedPlan: "Filter((((t2.v1 = 99) AND (t2.v2 >= 85)) AND ((t2.v1 <= 83) AND (t2.v2 = 99))) OR ((((t2.v1 <= 6) AND (t2.v2 BETWEEN 36 AND 68)) AND (t2.v3 > 62)) AND (t2.v4 = 79)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 30 AND 32 AND v2<68 AND v3<24) AND (v1>=32);`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 30 AND 32) AND (t2.v2 < 68)) AND (t2.v1 >= 32))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>62 AND v2>0) OR (v1<>80 AND v2>55 AND v3=10 AND v4=91));`,
		ExpectedPlan: "Filter(((t2.v1 > 62) AND (t2.v2 > 0)) OR ((((NOT((t2.v1 = 80))) AND (t2.v2 > 55)) AND (t2.v3 = 10)) AND (t2.v4 = 91)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=7 AND v2 BETWEEN 55 AND 81) OR (v1<>56 AND v2<=76 AND v3<>36)) AND (v1<56 AND v2<>69 AND v3=25);`,
		ExpectedPlan: "Filter(((((t2.v1 <= 7) AND (t2.v2 BETWEEN 55 AND 81)) OR (((NOT((t2.v1 = 56))) AND (t2.v2 <= 76)) AND (NOT((t2.v3 = 36))))) AND (t2.v1 < 56)) AND (NOT((t2.v2 = 69))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>18) OR (v1>=42 AND v2<=65 AND v3=87 AND v4=80));`,
		ExpectedPlan: "Filter((NOT((t2.v1 = 18))) OR ((((t2.v1 >= 42) AND (t2.v2 <= 65)) AND (t2.v3 = 87)) AND (t2.v4 = 80)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=27) OR (v1<23 AND v2>=41));`,
		ExpectedPlan: "Filter((t2.v1 <= 27) OR ((t2.v1 < 23) AND (t2.v2 >= 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>71 AND v4>0) OR (v1<48 AND v2=89 AND v3>=46 AND v4<=32)) OR (v1<62 AND v2>=33 AND v3>58)) OR (v1>=31 AND v3<>71));`,
		ExpectedPlan: "Filter(((((t2.v1 > 71) AND (t2.v4 > 0)) OR ((((t2.v1 < 48) AND (t2.v2 = 89)) AND (t2.v3 >= 46)) AND (t2.v4 <= 32))) OR (((t2.v1 < 62) AND (t2.v2 >= 33)) AND (t2.v3 > 58))) OR ((t2.v1 >= 31) AND (NOT((t2.v3 = 71)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 9 AND 40 AND v3<=43 AND v4=62 AND v2>=43) OR (v1=61 AND v2>12 AND v3 BETWEEN 0 AND 13 AND v4>=8));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 9 AND 40) AND (t2.v3 <= 43)) AND (t2.v4 = 62)) AND (t2.v2 >= 43)) OR ((((t2.v1 = 61) AND (t2.v2 > 12)) AND (t2.v3 BETWEEN 0 AND 13)) AND (t2.v4 >= 8)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<58) OR (v1 BETWEEN 17 AND 20 AND v2<>99 AND v3<=76)) OR (v1 BETWEEN 48 AND 87)) OR (v1<39 AND v2 BETWEEN 48 AND 94 AND v3<>0));`,
		ExpectedPlan: "Filter((((t2.v1 < 58) OR (((t2.v1 BETWEEN 17 AND 20) AND (NOT((t2.v2 = 99)))) AND (t2.v3 <= 76))) OR (t2.v1 BETWEEN 48 AND 87)) OR (((t2.v1 < 39) AND (t2.v2 BETWEEN 48 AND 94)) AND (NOT((t2.v3 = 0)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=33) OR (v1 BETWEEN 7 AND 41 AND v2<82 AND v3<53 AND v4<>3));`,
		ExpectedPlan: "Filter((t2.v1 = 33) OR ((((t2.v1 BETWEEN 7 AND 41) AND (t2.v2 < 82)) AND (t2.v3 < 53)) AND (NOT((t2.v4 = 3)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=9 AND v4=22 AND v2>=95) OR (v1>96));`,
		ExpectedPlan: "Filter((((t2.v1 <= 9) AND (t2.v4 = 22)) AND (t2.v2 >= 95)) OR (t2.v1 > 96))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=56) OR (v1>=31 AND v4<38 AND v2>20)) OR (v1=91 AND v2<48));`,
		ExpectedPlan: "Filter(((t2.v1 <= 56) OR (((t2.v1 >= 31) AND (t2.v4 < 38)) AND (t2.v2 > 20))) OR ((t2.v1 = 91) AND (t2.v2 < 48)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=75 AND v4<=30) OR (v1>=41 AND v2 BETWEEN 16 AND 25 AND v3>=99));`,
		ExpectedPlan: "Filter(((t2.v1 <= 75) AND (t2.v4 <= 30)) OR (((t2.v1 >= 41) AND (t2.v2 BETWEEN 16 AND 25)) AND (t2.v3 >= 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 2 AND 64) OR (v1>=23));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=26 AND v2<1 AND v3=82 AND v4<=42) OR (v1 BETWEEN 42 AND 73));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 26) AND (t2.v2 < 1)) AND (t2.v3 = 82)) AND (t2.v4 <= 42)) OR (t2.v1 BETWEEN 42 AND 73))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>=23 AND v2<=10) AND (v1>=75 AND v4 BETWEEN 24 AND 68) AND (v1>44 AND v2>8 AND v3<=16);`,
		ExpectedPlan: "Filter((((((t2.v1 >= 23) AND (t2.v2 <= 10)) AND (t2.v1 >= 75)) AND (t2.v1 > 44)) AND (t2.v2 > 8)) AND (t2.v3 <= 16))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((((v1>6 AND v2>61 AND v3=0 AND v4>=76) OR (v1<23)) OR (v1<>46 AND v2=29 AND v3>4)) OR (v1>=59)) OR (v1=87 AND v2<=98 AND v3>=47));`,
		ExpectedPlan: "Filter((((((((t2.v1 > 6) AND (t2.v2 > 61)) AND (t2.v3 = 0)) AND (t2.v4 >= 76)) OR (t2.v1 < 23)) OR (((NOT((t2.v1 = 46))) AND (t2.v2 = 29)) AND (t2.v3 > 4))) OR (t2.v1 >= 59)) OR (((t2.v1 = 87) AND (t2.v2 <= 98)) AND (t2.v3 >= 47)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=59 AND v2 BETWEEN 15 AND 53 AND v3<>17 AND v4>=10) OR (v1 BETWEEN 37 AND 95 AND v2<=32 AND v3>=81));`,
		ExpectedPlan: "Filter(((((t2.v1 = 59) AND (t2.v2 BETWEEN 15 AND 53)) AND (NOT((t2.v3 = 17)))) AND (t2.v4 >= 10)) OR (((t2.v1 BETWEEN 37 AND 95) AND (t2.v2 <= 32)) AND (t2.v3 >= 81)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 6 AND 92 AND v2=75 AND v3>79) OR (v1>=10)) OR (v1<=35 AND v2<=42)) AND (v1<>65);`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 6 AND 92) AND (t2.v2 = 75)) AND (t2.v3 > 79)) OR (t2.v1 >= 10)) OR ((t2.v1 <= 35) AND (t2.v2 <= 42)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>84 AND v4<=53 AND v2=77 AND v3>=40) OR (v1>78 AND v2<>1 AND v3=98 AND v4>=76));`,
		ExpectedPlan: "Filter(((((t2.v1 > 84) AND (t2.v4 <= 53)) AND (t2.v2 = 77)) AND (t2.v3 >= 40)) OR ((((t2.v1 > 78) AND (NOT((t2.v2 = 1)))) AND (t2.v3 = 98)) AND (t2.v4 >= 76)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>79 AND v2<=85) OR (v1<>13)) OR (v1 BETWEEN 4 AND 67));`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 79))) AND (t2.v2 <= 85)) OR (NOT((t2.v1 = 13)))) OR (t2.v1 BETWEEN 4 AND 67))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>34) OR (v1<35 AND v2>=93)) OR (v1>8));`,
		ExpectedPlan: "Filter(((t2.v1 > 34) OR ((t2.v1 < 35) AND (t2.v2 >= 93))) OR (t2.v1 > 8))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((((v1<65) OR (v1<>44)) OR (v1<=39 AND v3>=14)) OR (v1<=33 AND v2<>11)) OR (v1=75 AND v2=0 AND v3<28));`,
		ExpectedPlan: "Filter(((((t2.v1 < 65) OR (NOT((t2.v1 = 44)))) OR ((t2.v1 <= 39) AND (t2.v3 >= 14))) OR ((t2.v1 <= 33) AND (NOT((t2.v2 = 11))))) OR (((t2.v1 = 75) AND (t2.v2 = 0)) AND (t2.v3 < 28)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>50 AND v2>=46) AND (v1<>17 AND v2=45 AND v3<=79) OR (v1=10 AND v2>=35)) AND (v1=44 AND v2=38);`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 50))) AND (t2.v2 >= 46)) AND (((NOT((t2.v1 = 17))) AND (t2.v2 = 45)) AND (t2.v3 <= 79))) OR ((t2.v1 = 10) AND (t2.v2 >= 35))) AND (t2.v1 = 44))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<34) OR (v1<=62 AND v4<>18 AND v2 BETWEEN 1 AND 41)) OR (v1>=65 AND v2>=93 AND v3 BETWEEN 34 AND 41));`,
		ExpectedPlan: "Filter(((t2.v1 < 34) OR (((t2.v1 <= 62) AND (NOT((t2.v4 = 18)))) AND (t2.v2 BETWEEN 1 AND 41))) OR (((t2.v1 >= 65) AND (t2.v2 >= 93)) AND (t2.v3 BETWEEN 34 AND 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>8) OR (v1>20 AND v4>=99));`,
		ExpectedPlan: "Filter((t2.v1 > 8) OR ((t2.v1 > 20) AND (t2.v4 >= 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>57) OR (v1<87 AND v2<>91 AND v3 BETWEEN 47 AND 98));`,
		ExpectedPlan: "Filter((t2.v1 > 57) OR (((t2.v1 < 87) AND (NOT((t2.v2 = 91)))) AND (t2.v3 BETWEEN 47 AND 98)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=57) OR (v1=88 AND v2 BETWEEN 72 AND 93));`,
		ExpectedPlan: "Filter((t2.v1 = 57) OR ((t2.v1 = 88) AND (t2.v2 BETWEEN 72 AND 93)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>10 AND v2=20 AND v3<=21 AND v4<>88) OR (v1<28 AND v2 BETWEEN 38 AND 59 AND v3<>98 AND v4>=26));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 10))) AND (t2.v2 = 20)) AND (t2.v3 <= 21)) AND (NOT((t2.v4 = 88)))) OR ((((t2.v1 < 28) AND (t2.v2 BETWEEN 38 AND 59)) AND (NOT((t2.v3 = 98)))) AND (t2.v4 >= 26)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>5 AND v3<>53 AND v4>=49) OR (v1<18 AND v2<94));`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 5))) AND (NOT((t2.v3 = 53)))) AND (t2.v4 >= 49)) OR ((t2.v1 < 18) AND (t2.v2 < 94)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<52 AND v2 BETWEEN 33 AND 75 AND v3=32) OR (v1<=98 AND v2<=41 AND v3<>87 AND v4<>83));`,
		ExpectedPlan: "Filter((((t2.v1 < 52) AND (t2.v2 BETWEEN 33 AND 75)) AND (t2.v3 = 32)) OR ((((t2.v1 <= 98) AND (t2.v2 <= 41)) AND (NOT((t2.v3 = 87)))) AND (NOT((t2.v4 = 83)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>28 AND v4>57 AND v2<62 AND v3 BETWEEN 14 AND 41) AND (v1<>72 AND v2>=13 AND v3>29 AND v4>38) OR (v1<=22 AND v2>58));`,
		ExpectedPlan: "Filter((((((t2.v1 > 28) AND (t2.v4 > 57)) AND (t2.v2 < 62)) AND (t2.v3 BETWEEN 14 AND 41)) AND ((((NOT((t2.v1 = 72))) AND (t2.v2 >= 13)) AND (t2.v3 > 29)) AND (t2.v4 > 38))) OR ((t2.v1 <= 22) AND (t2.v2 > 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=13 AND v2<=52 AND v3=28 AND v4>88) OR (v1<>5 AND v2<=42));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 13) AND (t2.v2 <= 52)) AND (t2.v3 = 28)) AND (t2.v4 > 88)) OR ((NOT((t2.v1 = 5))) AND (t2.v2 <= 42)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>35 AND v4<>20 AND v2<81 AND v3=27) OR (v1>13 AND v3=27));`,
		ExpectedPlan: "Filter(((((t2.v1 > 35) AND (NOT((t2.v4 = 20)))) AND (t2.v2 < 81)) AND (t2.v3 = 27)) OR ((t2.v1 > 13) AND (t2.v3 = 27)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=26) OR (v1<59 AND v2 BETWEEN 2 AND 30 AND v3>=69));`,
		ExpectedPlan: "Filter((t2.v1 >= 26) OR (((t2.v1 < 59) AND (t2.v2 BETWEEN 2 AND 30)) AND (t2.v3 >= 69)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<11) OR (v1<>9 AND v2 BETWEEN 51 AND 62 AND v3=98));`,
		ExpectedPlan: "Filter((t2.v1 < 11) OR (((NOT((t2.v1 = 9))) AND (t2.v2 BETWEEN 51 AND 62)) AND (t2.v3 = 98)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=92 AND v2>25) OR (v1=91 AND v2=21 AND v3<=18 AND v4<>15)) OR (v1=79 AND v2>67 AND v3<>48 AND v4<42));`,
		ExpectedPlan: "Filter((((t2.v1 = 92) AND (t2.v2 > 25)) OR ((((t2.v1 = 91) AND (t2.v2 = 21)) AND (t2.v3 <= 18)) AND (NOT((t2.v4 = 15))))) OR ((((t2.v1 = 79) AND (t2.v2 > 67)) AND (NOT((t2.v3 = 48)))) AND (t2.v4 < 42)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=86 AND v2<5 AND v3<36 AND v4<81) OR (v1>=52 AND v2>24 AND v3<5)) OR (v1 BETWEEN 5 AND 80 AND v3<>80));`,
		ExpectedPlan: "Filter((((((t2.v1 = 86) AND (t2.v2 < 5)) AND (t2.v3 < 36)) AND (t2.v4 < 81)) OR (((t2.v1 >= 52) AND (t2.v2 > 24)) AND (t2.v3 < 5))) OR ((t2.v1 BETWEEN 5 AND 80) AND (NOT((t2.v3 = 80)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>67) OR (v1>69 AND v2>11 AND v3=13 AND v4=20));`,
		ExpectedPlan: "Filter((t2.v1 > 67) OR ((((t2.v1 > 69) AND (t2.v2 > 11)) AND (t2.v3 = 13)) AND (t2.v4 = 20)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>31) OR (v1 BETWEEN 27 AND 87 AND v2=71 AND v3=38 AND v4=1));`,
		ExpectedPlan: "Filter((NOT((t2.v1 = 31))) OR ((((t2.v1 BETWEEN 27 AND 87) AND (t2.v2 = 71)) AND (t2.v3 = 38)) AND (t2.v4 = 1)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>2 AND v4=0 AND v2 BETWEEN 6 AND 23 AND v3 BETWEEN 46 AND 52) OR (v1<=63 AND v2>=71 AND v3=28)) AND (v1<=52);`,
		ExpectedPlan: "Filter(((((t2.v1 > 2) AND (t2.v4 = 0)) AND (t2.v2 BETWEEN 6 AND 23)) AND (t2.v3 BETWEEN 46 AND 52)) OR (((t2.v1 <= 63) AND (t2.v2 >= 71)) AND (t2.v3 = 28)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 10 AND 90) AND (v1=86 AND v4>=4) AND (v1 BETWEEN 6 AND 58 AND v2=85);`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 10 AND 90) AND (t2.v1 = 86)) AND (t2.v4 >= 4)) AND (t2.v1 BETWEEN 6 AND 58))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=46 AND v4>41 AND v2<>12) OR (v1>17 AND v2>=34 AND v3<>68 AND v4<=13)) OR (v1>=98 AND v4 BETWEEN 3 AND 62 AND v2=39));`,
		ExpectedPlan: "Filter(((((t2.v1 = 46) AND (t2.v4 > 41)) AND (NOT((t2.v2 = 12)))) OR ((((t2.v1 > 17) AND (t2.v2 >= 34)) AND (NOT((t2.v3 = 68)))) AND (t2.v4 <= 13))) OR (((t2.v1 >= 98) AND (t2.v4 BETWEEN 3 AND 62)) AND (t2.v2 = 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=12 AND v2<>4 AND v3 BETWEEN 18 AND 42) OR (v1>=73)) OR (v1<60 AND v2=93 AND v3>=79));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 12) AND (NOT((t2.v2 = 4)))) AND (t2.v3 BETWEEN 18 AND 42)) OR (t2.v1 >= 73)) OR (((t2.v1 < 60) AND (t2.v2 = 93)) AND (t2.v3 >= 79)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=55 AND v2>50) OR (v1<>51 AND v2>=37));`,
		ExpectedPlan: "Filter(((t2.v1 = 55) AND (t2.v2 > 50)) OR ((NOT((t2.v1 = 51))) AND (t2.v2 >= 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 66 AND 76 AND v2>=84 AND v3>1 AND v4 BETWEEN 71 AND 95) AND (v1>36 AND v2<>41) OR (v1<44 AND v2<=50 AND v3=36 AND v4<=42));`,
		ExpectedPlan: "Filter((((((t2.v1 BETWEEN 66 AND 76) AND (t2.v2 >= 84)) AND (t2.v3 > 1)) AND (t2.v4 BETWEEN 71 AND 95)) AND ((t2.v1 > 36) AND (NOT((t2.v2 = 41))))) OR ((((t2.v1 < 44) AND (t2.v2 <= 50)) AND (t2.v3 = 36)) AND (t2.v4 <= 42)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=21 AND v2=44 AND v3>=68) OR (v1>=38 AND v2>=15));`,
		ExpectedPlan: "Filter((((t2.v1 <= 21) AND (t2.v2 = 44)) AND (t2.v3 >= 68)) OR ((t2.v1 >= 38) AND (t2.v2 >= 15)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1<>37 AND v2>67 AND v3>52) AND (v1<48 AND v2<>73 AND v3=25 AND v4=22);`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 37))) AND (t2.v2 > 67)) AND (t2.v3 > 52)) AND (t2.v1 < 48)) AND (NOT((t2.v2 = 73)))) AND (t2.v3 = 25))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 57 AND 62 AND v2>=99) OR (v1>31));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 57 AND 62) AND (t2.v2 >= 99)) OR (t2.v1 > 31))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>22 AND v3<>49) OR (v1>=41 AND v2<=74 AND v3<=46));`,
		ExpectedPlan: "Filter(((NOT((t2.v1 = 22))) AND (NOT((t2.v3 = 49)))) OR (((t2.v1 >= 41) AND (t2.v2 <= 74)) AND (t2.v3 <= 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=82 AND v4<=67 AND v2=40) OR (v1>63)) OR (v1<=16));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 82) AND (t2.v4 <= 67)) AND (t2.v2 = 40)) OR (t2.v1 > 63)) OR (t2.v1 <= 16))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<=39 AND v2<>82 AND v3>=33 AND v4>=84) OR (v1=57 AND v2<25 AND v3<>55 AND v4<=82)) OR (v1>10 AND v2>28 AND v3>=65)) OR (v1<=13 AND v2=66));`,
		ExpectedPlan: "Filter(((((((t2.v1 <= 39) AND (NOT((t2.v2 = 82)))) AND (t2.v3 >= 33)) AND (t2.v4 >= 84)) OR ((((t2.v1 = 57) AND (t2.v2 < 25)) AND (NOT((t2.v3 = 55)))) AND (t2.v4 <= 82))) OR (((t2.v1 > 10) AND (t2.v2 > 28)) AND (t2.v3 >= 65))) OR ((t2.v1 <= 13) AND (t2.v2 = 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=60 AND v2<=25 AND v3<>9) OR (v1 BETWEEN 19 AND 92 AND v2>=33 AND v3<=40 AND v4=53));`,
		ExpectedPlan: "Filter((((t2.v1 <= 60) AND (t2.v2 <= 25)) AND (NOT((t2.v3 = 9)))) OR ((((t2.v1 BETWEEN 19 AND 92) AND (t2.v2 >= 33)) AND (t2.v3 <= 40)) AND (t2.v4 = 53)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=21 AND v2<=27 AND v3>=86 AND v4>99) OR (v1<76 AND v2<>97));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 21) AND (t2.v2 <= 27)) AND (t2.v3 >= 86)) AND (t2.v4 > 99)) OR ((t2.v1 < 76) AND (NOT((t2.v2 = 97)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 4 AND 8 AND v3>=12) OR (v1>=12 AND v2>=0 AND v3=18));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 4 AND 8) AND (t2.v3 >= 12)) OR (((t2.v1 >= 12) AND (t2.v2 >= 0)) AND (t2.v3 = 18)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>65 AND v2<=52 AND v3>37) OR (v1>11)) OR (v1<=54 AND v2 BETWEEN 30 AND 85 AND v3 BETWEEN 14 AND 27 AND v4>=35)) OR (v1>44 AND v2<>76 AND v3>=52));`,
		ExpectedPlan: "Filter((((((t2.v1 > 65) AND (t2.v2 <= 52)) AND (t2.v3 > 37)) OR (t2.v1 > 11)) OR ((((t2.v1 <= 54) AND (t2.v2 BETWEEN 30 AND 85)) AND (t2.v3 BETWEEN 14 AND 27)) AND (t2.v4 >= 35))) OR (((t2.v1 > 44) AND (NOT((t2.v2 = 76)))) AND (t2.v3 >= 52)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=54) OR (v1<17 AND v2=34 AND v3>=59));`,
		ExpectedPlan: "Filter((t2.v1 >= 54) OR (((t2.v1 < 17) AND (t2.v2 = 34)) AND (t2.v3 >= 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>9 AND v4<>61 AND v2=98 AND v3<1) OR (v1<2 AND v2 BETWEEN 3 AND 70));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 9))) AND (NOT((t2.v4 = 61)))) AND (t2.v2 = 98)) AND (t2.v3 < 1)) OR ((t2.v1 < 2) AND (t2.v2 BETWEEN 3 AND 70)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=6 AND v2>93) OR (v1 BETWEEN 38 AND 46));`,
		ExpectedPlan: "Filter(((t2.v1 <= 6) AND (t2.v2 > 93)) OR (t2.v1 BETWEEN 38 AND 46))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1 BETWEEN 16 AND 72) OR (v1=20)) OR (v1>61 AND v2<>48 AND v3<>83 AND v4=46)) OR (v1=5 AND v2=59));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 16 AND 72) OR (t2.v1 = 20)) OR ((((t2.v1 > 61) AND (NOT((t2.v2 = 48)))) AND (NOT((t2.v3 = 83)))) AND (t2.v4 = 46))) OR ((t2.v1 = 5) AND (t2.v2 = 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>41 AND v2>74 AND v3>37 AND v4<38) OR (v1=58 AND v2>=1)) OR (v1<=4 AND v2>0 AND v3 BETWEEN 39 AND 72 AND v4>=29));`,
		ExpectedPlan: "Filter((((((t2.v1 > 41) AND (t2.v2 > 74)) AND (t2.v3 > 37)) AND (t2.v4 < 38)) OR ((t2.v1 = 58) AND (t2.v2 >= 1))) OR ((((t2.v1 <= 4) AND (t2.v2 > 0)) AND (t2.v3 BETWEEN 39 AND 72)) AND (t2.v4 >= 29)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>53 AND v4<99 AND v2<>31) OR (v1<>5 AND v2>70 AND v3>=71));`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 53))) AND (t2.v4 < 99)) AND (NOT((t2.v2 = 31)))) OR (((NOT((t2.v1 = 5))) AND (t2.v2 > 70)) AND (t2.v3 >= 71)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>1 AND v4=93) OR (v1<10 AND v2 BETWEEN 40 AND 74 AND v3>=27));`,
		ExpectedPlan: "Filter(((t2.v1 > 1) AND (t2.v4 = 93)) OR (((t2.v1 < 10) AND (t2.v2 BETWEEN 40 AND 74)) AND (t2.v3 >= 27)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=92 AND v2>=64 AND v3=39 AND v4 BETWEEN 16 AND 53) OR (v1<54 AND v2 BETWEEN 8 AND 17 AND v3=21 AND v4=86));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 92) AND (t2.v2 >= 64)) AND (t2.v3 = 39)) AND (t2.v4 BETWEEN 16 AND 53)) OR ((((t2.v1 < 54) AND (t2.v2 BETWEEN 8 AND 17)) AND (t2.v3 = 21)) AND (t2.v4 = 86)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 16 AND 31 AND v4 BETWEEN 18 AND 96) OR (v1=40 AND v2<=35 AND v3>=51 AND v4>=83));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 16 AND 31) AND (t2.v4 BETWEEN 18 AND 96)) OR ((((t2.v1 = 40) AND (t2.v2 <= 35)) AND (t2.v3 >= 51)) AND (t2.v4 >= 83)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 68 AND 78 AND v2>96 AND v3<58 AND v4<14) OR (v1=71)) AND (v1>15 AND v2>=19) OR (v1>36));`,
		ExpectedPlan: "Filter(((((((t2.v1 BETWEEN 68 AND 78) AND (t2.v2 > 96)) AND (t2.v3 < 58)) AND (t2.v4 < 14)) OR (t2.v1 = 71)) AND ((t2.v1 > 15) AND (t2.v2 >= 19))) OR (t2.v1 > 36))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 33 AND 71 AND v2<=61 AND v3<=32 AND v4 BETWEEN 18 AND 73) AND (v1<3) AND (v1<=59 AND v2=47 AND v3<49 AND v4>36);`,
		ExpectedPlan: "Filter(((((((t2.v1 BETWEEN 33 AND 71) AND (t2.v2 <= 61)) AND (t2.v3 <= 32)) AND (t2.v1 < 3)) AND (t2.v1 <= 59)) AND (t2.v2 = 47)) AND (t2.v3 < 49))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<77 AND v2=43 AND v3<92 AND v4=13) OR (v1=38 AND v2<=46)) OR (v1 BETWEEN 10 AND 79 AND v2>=11 AND v3 BETWEEN 14 AND 14));`,
		ExpectedPlan: "Filter((((((t2.v1 < 77) AND (t2.v2 = 43)) AND (t2.v3 < 92)) AND (t2.v4 = 13)) OR ((t2.v1 = 38) AND (t2.v2 <= 46))) OR (((t2.v1 BETWEEN 10 AND 79) AND (t2.v2 >= 11)) AND (t2.v3 BETWEEN 14 AND 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=47 AND v4=13) AND (v1<=27 AND v3<54 AND v4 BETWEEN 27 AND 40) OR (v1>=40 AND v4=98 AND v2=25 AND v3>66));`,
		ExpectedPlan: "Filter((((t2.v1 >= 47) AND (t2.v4 = 13)) AND (((t2.v1 <= 27) AND (t2.v3 < 54)) AND (t2.v4 BETWEEN 27 AND 40))) OR ((((t2.v1 >= 40) AND (t2.v4 = 98)) AND (t2.v2 = 25)) AND (t2.v3 > 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<98 AND v3 BETWEEN 80 AND 82) OR (v1 BETWEEN 31 AND 38 AND v2=39));`,
		ExpectedPlan: "Filter(((t2.v1 < 98) AND (t2.v3 BETWEEN 80 AND 82)) OR ((t2.v1 BETWEEN 31 AND 38) AND (t2.v2 = 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=75 AND v2 BETWEEN 45 AND 51 AND v3<15) OR (v1>=74 AND v2>=37 AND v3<76));`,
		ExpectedPlan: "Filter((((t2.v1 >= 75) AND (t2.v2 BETWEEN 45 AND 51)) AND (t2.v3 < 15)) OR (((t2.v1 >= 74) AND (t2.v2 >= 37)) AND (t2.v3 < 76)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=40) OR (v1<>32 AND v4<=37));`,
		ExpectedPlan: "Filter((t2.v1 >= 40) OR ((NOT((t2.v1 = 32))) AND (t2.v4 <= 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>63 AND v3 BETWEEN 43 AND 50 AND v4<29 AND v2>=89) OR (v1>80));`,
		ExpectedPlan: "Filter(((((t2.v1 > 63) AND (t2.v3 BETWEEN 43 AND 50)) AND (t2.v4 < 29)) AND (t2.v2 >= 89)) OR (t2.v1 > 80))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=81) OR (v1>=27 AND v2>=21 AND v3 BETWEEN 1 AND 63 AND v4>=92));`,
		ExpectedPlan: "Filter((t2.v1 >= 81) OR ((((t2.v1 >= 27) AND (t2.v2 >= 21)) AND (t2.v3 BETWEEN 1 AND 63)) AND (t2.v4 >= 92)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>13) OR (v1>72 AND v2=2 AND v3<=40)) OR (v1>77 AND v2<21));`,
		ExpectedPlan: "Filter(((t2.v1 > 13) OR (((t2.v1 > 72) AND (t2.v2 = 2)) AND (t2.v3 <= 40))) OR ((t2.v1 > 77) AND (t2.v2 < 21)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>54 AND v2>23 AND v3 BETWEEN 28 AND 48 AND v4>=37) OR (v1>93 AND v2>=51 AND v3<9 AND v4<>49)) OR (v1>=71 AND v2<>33));`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 54))) AND (t2.v2 > 23)) AND (t2.v3 BETWEEN 28 AND 48)) AND (t2.v4 >= 37)) OR ((((t2.v1 > 93) AND (t2.v2 >= 51)) AND (t2.v3 < 9)) AND (NOT((t2.v4 = 49))))) OR ((t2.v1 >= 71) AND (NOT((t2.v2 = 33)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 15 AND 69 AND v4=83 AND v2<=43) OR (v1<51 AND v2<24 AND v3<>27 AND v4<>50)) OR (v1<>37));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 15 AND 69) AND (t2.v4 = 83)) AND (t2.v2 <= 43)) OR ((((t2.v1 < 51) AND (t2.v2 < 24)) AND (NOT((t2.v3 = 27)))) AND (NOT((t2.v4 = 50))))) OR (NOT((t2.v1 = 37))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 55 AND 66 AND v2<>81 AND v3=6 AND v4<=19) OR (v1<>91));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 55 AND 66) AND (NOT((t2.v2 = 81)))) AND (t2.v3 = 6)) AND (t2.v4 <= 19)) OR (NOT((t2.v1 = 91))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=21 AND v2<50 AND v3>=39) OR (v1<=79 AND v4>62 AND v2=31));`,
		ExpectedPlan: "Filter((((t2.v1 = 21) AND (t2.v2 < 50)) AND (t2.v3 >= 39)) OR (((t2.v1 <= 79) AND (t2.v4 > 62)) AND (t2.v2 = 31)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>78) OR (v1>=9 AND v2<>84));`,
		ExpectedPlan: "Filter((t2.v1 > 78) OR ((t2.v1 >= 9) AND (NOT((t2.v2 = 84)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>16 AND v3>=29) OR (v1>=47 AND v2<>63));`,
		ExpectedPlan: "Filter(((t2.v1 > 16) AND (t2.v3 >= 29)) OR ((t2.v1 >= 47) AND (NOT((t2.v2 = 63)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=16 AND v2>=9 AND v3<>48) OR (v1>=76 AND v2<>86)) OR (v1<28 AND v2=1 AND v3<=23 AND v4 BETWEEN 13 AND 55));`,
		ExpectedPlan: "Filter(((((t2.v1 = 16) AND (t2.v2 >= 9)) AND (NOT((t2.v3 = 48)))) OR ((t2.v1 >= 76) AND (NOT((t2.v2 = 86))))) OR ((((t2.v1 < 28) AND (t2.v2 = 1)) AND (t2.v3 <= 23)) AND (t2.v4 BETWEEN 13 AND 55)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=35 AND v2>67) OR (v1<>55));`,
		ExpectedPlan: "Filter(((t2.v1 = 35) AND (t2.v2 > 67)) OR (NOT((t2.v1 = 55))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<89 AND v2<5 AND v3 BETWEEN 53 AND 61) OR (v1<>72 AND v3<20));`,
		ExpectedPlan: "Filter((((t2.v1 < 89) AND (t2.v2 < 5)) AND (t2.v3 BETWEEN 53 AND 61)) OR ((NOT((t2.v1 = 72))) AND (t2.v3 < 20)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=58 AND v2<=89 AND v3=78 AND v4<=58) OR (v1>39)) AND (v1<>25 AND v2>1 AND v3<18);`,
		ExpectedPlan: "Filter(((((((t2.v1 = 58) AND (t2.v2 <= 89)) AND (t2.v3 = 78)) AND (t2.v4 <= 58)) OR (t2.v1 > 39)) AND (NOT((t2.v1 = 25)))) AND (t2.v2 > 1))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>94) OR (v1=33 AND v2 BETWEEN 53 AND 60 AND v3 BETWEEN 37 AND 73));`,
		ExpectedPlan: "Filter((t2.v1 > 94) OR (((t2.v1 = 33) AND (t2.v2 BETWEEN 53 AND 60)) AND (t2.v3 BETWEEN 37 AND 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=40 AND v2<>8 AND v3<=69) OR (v1<=72)) OR (v1 BETWEEN 87 AND 89 AND v2 BETWEEN 52 AND 58));`,
		ExpectedPlan: "Filter(((((t2.v1 = 40) AND (NOT((t2.v2 = 8)))) AND (t2.v3 <= 69)) OR (t2.v1 <= 72)) OR ((t2.v1 BETWEEN 87 AND 89) AND (t2.v2 BETWEEN 52 AND 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<9 AND v2=97 AND v3<>54 AND v4>71) OR (v1>48 AND v2 BETWEEN 7 AND 23 AND v3<>95 AND v4>86)) OR (v1 BETWEEN 36 AND 90));`,
		ExpectedPlan: "Filter((((((t2.v1 < 9) AND (t2.v2 = 97)) AND (NOT((t2.v3 = 54)))) AND (t2.v4 > 71)) OR ((((t2.v1 > 48) AND (t2.v2 BETWEEN 7 AND 23)) AND (NOT((t2.v3 = 95)))) AND (t2.v4 > 86))) OR (t2.v1 BETWEEN 36 AND 90))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=38 AND v2<70) OR (v1>79));`,
		ExpectedPlan: "Filter(((t2.v1 >= 38) AND (t2.v2 < 70)) OR (t2.v1 > 79))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<47 AND v2 BETWEEN 22 AND 85) AND (v1=73) OR (v1<42));`,
		ExpectedPlan: "Filter((((t2.v1 < 47) AND (t2.v2 BETWEEN 22 AND 85)) AND (t2.v1 = 73)) OR (t2.v1 < 42))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<29) AND (v1<41 AND v2>52 AND v3<>55) OR (v1 BETWEEN 16 AND 28 AND v2>=9 AND v3=43 AND v4<6));`,
		ExpectedPlan: "Filter(((t2.v1 < 29) AND (((t2.v1 < 41) AND (t2.v2 > 52)) AND (NOT((t2.v3 = 55))))) OR ((((t2.v1 BETWEEN 16 AND 28) AND (t2.v2 >= 9)) AND (t2.v3 = 43)) AND (t2.v4 < 6)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<56 AND v2<=52) OR (v1>=30 AND v2<73 AND v3>40 AND v4>=13)) AND (v1<30 AND v4<>25 AND v2<>82 AND v3 BETWEEN 80 AND 88);`,
		ExpectedPlan: "Filter((((((t2.v1 < 56) AND (t2.v2 <= 52)) OR ((((t2.v1 >= 30) AND (t2.v2 < 73)) AND (t2.v3 > 40)) AND (t2.v4 >= 13))) AND (t2.v1 < 30)) AND (NOT((t2.v2 = 82)))) AND (t2.v3 BETWEEN 80 AND 88))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 9 AND 53 AND v2 BETWEEN 26 AND 56) OR (v1 BETWEEN 29 AND 72 AND v2<18 AND v3=73 AND v4<=12));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 9 AND 53) AND (t2.v2 BETWEEN 26 AND 56)) OR ((((t2.v1 BETWEEN 29 AND 72) AND (t2.v2 < 18)) AND (t2.v3 = 73)) AND (t2.v4 <= 12)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>96 AND v2<27) OR (v1<82)) AND (v1>=80 AND v2 BETWEEN 14 AND 53);`,
		ExpectedPlan: "Filter((((t2.v1 > 96) AND (t2.v2 < 27)) OR (t2.v1 < 82)) AND (t2.v1 >= 80))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>86) OR (v1>=48 AND v4>9));`,
		ExpectedPlan: "Filter((t2.v1 > 86) OR ((t2.v1 >= 48) AND (t2.v4 > 9)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=41 AND v2=79 AND v3<16 AND v4>=2) OR (v1<16 AND v4>59));`,
		ExpectedPlan: "Filter(((((t2.v1 = 41) AND (t2.v2 = 79)) AND (t2.v3 < 16)) AND (t2.v4 >= 2)) OR ((t2.v1 < 16) AND (t2.v4 > 59)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>=69 AND v2 BETWEEN 38 AND 45) AND (v1<>35 AND v2<28 AND v3>14);`,
		ExpectedPlan: "Filter((((t2.v1 >= 69) AND (t2.v2 BETWEEN 38 AND 45)) AND (NOT((t2.v1 = 35)))) AND (t2.v2 < 28))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=93 AND v2<=10 AND v3 BETWEEN 21 AND 83) AND (v1<>5 AND v2>59 AND v3<>17) OR (v1<69 AND v3<>65 AND v4>=51 AND v2<=48)) OR (v1 BETWEEN 37 AND 57 AND v2 BETWEEN 44 AND 57 AND v3<40 AND v4=98));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 93) AND (t2.v2 <= 10)) AND (t2.v3 BETWEEN 21 AND 83)) AND (((NOT((t2.v1 = 5))) AND (t2.v2 > 59)) AND (NOT((t2.v3 = 17))))) OR ((((t2.v1 < 69) AND (NOT((t2.v3 = 65)))) AND (t2.v4 >= 51)) AND (t2.v2 <= 48))) OR ((((t2.v1 BETWEEN 37 AND 57) AND (t2.v2 BETWEEN 44 AND 57)) AND (t2.v3 < 40)) AND (t2.v4 = 98)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<46) OR (v1<>60));`,
		ExpectedPlan: "Filter((t2.v1 < 46) OR (NOT((t2.v1 = 60))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<97 AND v2<=47 AND v3=91) OR (v1=74 AND v4>72 AND v2<>44 AND v3 BETWEEN 4 AND 51));`,
		ExpectedPlan: "Filter((((t2.v1 < 97) AND (t2.v2 <= 47)) AND (t2.v3 = 91)) OR ((((t2.v1 = 74) AND (t2.v4 > 72)) AND (NOT((t2.v2 = 44)))) AND (t2.v3 BETWEEN 4 AND 51)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 26 AND 60 AND v2>53 AND v3<=9 AND v4<8) OR (v1>0 AND v2<=69));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 26 AND 60) AND (t2.v2 > 53)) AND (t2.v3 <= 9)) AND (t2.v4 < 8)) OR ((t2.v1 > 0) AND (t2.v2 <= 69)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=33 AND v2<2 AND v3<>63) OR (v1 BETWEEN 20 AND 95 AND v2<>7 AND v3 BETWEEN 95 AND 96 AND v4 BETWEEN 34 AND 41)) OR (v1 BETWEEN 27 AND 44 AND v4<>28 AND v2<=43 AND v3<=64));`,
		ExpectedPlan: "Filter(((((t2.v1 = 33) AND (t2.v2 < 2)) AND (NOT((t2.v3 = 63)))) OR ((((t2.v1 BETWEEN 20 AND 95) AND (NOT((t2.v2 = 7)))) AND (t2.v3 BETWEEN 95 AND 96)) AND (t2.v4 BETWEEN 34 AND 41))) OR ((((t2.v1 BETWEEN 27 AND 44) AND (NOT((t2.v4 = 28)))) AND (t2.v2 <= 43)) AND (t2.v3 <= 64)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1 BETWEEN 13 AND 36 AND v2>40) OR (v1<>28 AND v2<29)) OR (v1 BETWEEN 36 AND 89 AND v2>=92 AND v3>39 AND v4<16)) OR (v1<=1));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 13 AND 36) AND (t2.v2 > 40)) OR ((NOT((t2.v1 = 28))) AND (t2.v2 < 29))) OR ((((t2.v1 BETWEEN 36 AND 89) AND (t2.v2 >= 92)) AND (t2.v3 > 39)) AND (t2.v4 < 16))) OR (t2.v1 <= 1))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=68 AND v2=49) OR (v1<=35 AND v2>=59 AND v3>=88 AND v4 BETWEEN 1 AND 62));`,
		ExpectedPlan: "Filter(((t2.v1 = 68) AND (t2.v2 = 49)) OR ((((t2.v1 <= 35) AND (t2.v2 >= 59)) AND (t2.v3 >= 88)) AND (t2.v4 BETWEEN 1 AND 62)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>33) OR (v1<23 AND v4<=23 AND v2>=41));`,
		ExpectedPlan: "Filter((t2.v1 > 33) OR (((t2.v1 < 23) AND (t2.v4 <= 23)) AND (t2.v2 >= 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=78 AND v2=26 AND v3 BETWEEN 70 AND 89) OR (v1 BETWEEN 12 AND 78 AND v2>41 AND v3 BETWEEN 2 AND 11 AND v4 BETWEEN 12 AND 97)) OR (v1>16 AND v2=85 AND v3<56 AND v4<19));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 78) AND (t2.v2 = 26)) AND (t2.v3 BETWEEN 70 AND 89)) OR ((((t2.v1 BETWEEN 12 AND 78) AND (t2.v2 > 41)) AND (t2.v3 BETWEEN 2 AND 11)) AND (t2.v4 BETWEEN 12 AND 97))) OR ((((t2.v1 > 16) AND (t2.v2 = 85)) AND (t2.v3 < 56)) AND (t2.v4 < 19)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=51 AND v2=3 AND v3>48 AND v4>=49) OR (v1>25 AND v3=37));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 51) AND (t2.v2 = 3)) AND (t2.v3 > 48)) AND (t2.v4 >= 49)) OR ((t2.v1 > 25) AND (t2.v3 = 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<4 AND v2<>1 AND v3<=34) OR (v1>=63)) OR (v1<58 AND v2=33)) AND (v1<=55) OR (v1 BETWEEN 1 AND 80 AND v2<=51));`,
		ExpectedPlan: "Filter(((((((t2.v1 < 4) AND (NOT((t2.v2 = 1)))) AND (t2.v3 <= 34)) OR (t2.v1 >= 63)) OR ((t2.v1 < 58) AND (t2.v2 = 33))) AND (t2.v1 <= 55)) OR ((t2.v1 BETWEEN 1 AND 80) AND (t2.v2 <= 51)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 33 AND 82 AND v2<26) OR (v1>=98 AND v4>30 AND v2 BETWEEN 47 AND 67 AND v3 BETWEEN 9 AND 54)) OR (v1>=5)) AND (v1<>85 AND v4<>31);`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 33 AND 82) AND (t2.v2 < 26)) OR ((((t2.v1 >= 98) AND (t2.v4 > 30)) AND (t2.v2 BETWEEN 47 AND 67)) AND (t2.v3 BETWEEN 9 AND 54))) OR (t2.v1 >= 5)) AND (NOT((t2.v4 = 31))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=82 AND v3<>55 AND v4>26) OR (v1=35)) OR (v1 BETWEEN 18 AND 70 AND v2>=17));`,
		ExpectedPlan: "Filter(((((t2.v1 = 82) AND (NOT((t2.v3 = 55)))) AND (t2.v4 > 26)) OR (t2.v1 = 35)) OR ((t2.v1 BETWEEN 18 AND 70) AND (t2.v2 >= 17)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>45 AND v2<=55 AND v3>=2 AND v4<46) OR (v1>=0 AND v2<>6));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 45))) AND (t2.v2 <= 55)) AND (t2.v3 >= 2)) AND (t2.v4 < 46)) OR ((t2.v1 >= 0) AND (NOT((t2.v2 = 6)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=85 AND v2>=46 AND v3=87 AND v4>3) OR (v1=52));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 85) AND (t2.v2 >= 46)) AND (t2.v3 = 87)) AND (t2.v4 > 3)) OR (t2.v1 = 52))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<90 AND v4=77) OR (v1<>32 AND v2<=17 AND v3=68)) OR (v1<41));`,
		ExpectedPlan: "Filter((((t2.v1 < 90) AND (t2.v4 = 77)) OR (((NOT((t2.v1 = 32))) AND (t2.v2 <= 17)) AND (t2.v3 = 68))) OR (t2.v1 < 41))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=2) AND (v1>=13 AND v2<=23 AND v3<=23) OR (v1 BETWEEN 18 AND 57));`,
		ExpectedPlan: "Filter(((t2.v1 = 2) AND (((t2.v1 >= 13) AND (t2.v2 <= 23)) AND (t2.v3 <= 23))) OR (t2.v1 BETWEEN 18 AND 57))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 32 AND 72 AND v2<>89 AND v3>=39) OR (v1>50 AND v4>80));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 32 AND 72) AND (NOT((t2.v2 = 89)))) AND (t2.v3 >= 39)) OR ((t2.v1 > 50) AND (t2.v4 > 80)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<44) OR (v1<>37 AND v2<=12 AND v3>65 AND v4<47)) OR (v1<>76));`,
		ExpectedPlan: "Filter(((t2.v1 < 44) OR ((((NOT((t2.v1 = 37))) AND (t2.v2 <= 12)) AND (t2.v3 > 65)) AND (t2.v4 < 47))) OR (NOT((t2.v1 = 76))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 29 AND 37) OR (v1<>54 AND v2<=65 AND v3<=1 AND v4<>10)) OR (v1<>55 AND v2 BETWEEN 49 AND 56 AND v3>=25 AND v4<=8));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 29 AND 37) OR ((((NOT((t2.v1 = 54))) AND (t2.v2 <= 65)) AND (t2.v3 <= 1)) AND (NOT((t2.v4 = 10))))) OR ((((NOT((t2.v1 = 55))) AND (t2.v2 BETWEEN 49 AND 56)) AND (t2.v3 >= 25)) AND (t2.v4 <= 8)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=80 AND v2<95 AND v3>6) OR (v1 BETWEEN 7 AND 14 AND v2 BETWEEN 27 AND 49 AND v3>57 AND v4 BETWEEN 28 AND 60));`,
		ExpectedPlan: "Filter((((t2.v1 = 80) AND (t2.v2 < 95)) AND (t2.v3 > 6)) OR ((((t2.v1 BETWEEN 7 AND 14) AND (t2.v2 BETWEEN 27 AND 49)) AND (t2.v3 > 57)) AND (t2.v4 BETWEEN 28 AND 60)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>10 AND v2<43 AND v3<>15) OR (v1<=71 AND v4<>22));`,
		ExpectedPlan: "Filter((((t2.v1 > 10) AND (t2.v2 < 43)) AND (NOT((t2.v3 = 15)))) OR ((t2.v1 <= 71) AND (NOT((t2.v4 = 22)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 18 AND 36 AND v4<>87 AND v2>=13) OR (v1>=63 AND v3<=89)) AND (v1<76 AND v4<49 AND v2<=96);`,
		ExpectedPlan: "Filter((((((t2.v1 BETWEEN 18 AND 36) AND (NOT((t2.v4 = 87)))) AND (t2.v2 >= 13)) OR ((t2.v1 >= 63) AND (t2.v3 <= 89))) AND (t2.v1 < 76)) AND (t2.v4 < 49))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<93 AND v2<>16) OR (v1>=23 AND v4>=19)) OR (v1<48 AND v2<=45 AND v3<>46 AND v4>76)) AND (v1=22 AND v3=41) OR (v1<=17 AND v2>=41));`,
		ExpectedPlan: "Filter((((((t2.v1 < 93) AND (NOT((t2.v2 = 16)))) OR ((t2.v1 >= 23) AND (t2.v4 >= 19))) OR ((((t2.v1 < 48) AND (t2.v2 <= 45)) AND (NOT((t2.v3 = 46)))) AND (t2.v4 > 76))) AND ((t2.v1 = 22) AND (t2.v3 = 41))) OR ((t2.v1 <= 17) AND (t2.v2 >= 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>17 AND v4>50 AND v2 BETWEEN 11 AND 23 AND v3=23) OR (v1<73));`,
		ExpectedPlan: "Filter(((((t2.v1 > 17) AND (t2.v4 > 50)) AND (t2.v2 BETWEEN 11 AND 23)) AND (t2.v3 = 23)) OR (t2.v1 < 73))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 5 AND 41 AND v3<78 AND v4<41) OR (v1>84 AND v2<>43));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 5 AND 41) AND (t2.v3 < 78)) AND (t2.v4 < 41)) OR ((t2.v1 > 84) AND (NOT((t2.v2 = 43)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=24 AND v2 BETWEEN 43 AND 84) OR (v1>=90 AND v2>1 AND v3<>70)) OR (v1>=66 AND v2<95));`,
		ExpectedPlan: "Filter((((t2.v1 = 24) AND (t2.v2 BETWEEN 43 AND 84)) OR (((t2.v1 >= 90) AND (t2.v2 > 1)) AND (NOT((t2.v3 = 70))))) OR ((t2.v1 >= 66) AND (t2.v2 < 95)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<=18 AND v2<=70) OR (v1>55 AND v2>52 AND v3<>70)) OR (v1=58)) AND (v1<>22 AND v4>76) OR (v1>14 AND v2<32 AND v3>97));`,
		ExpectedPlan: "Filter((((((t2.v1 <= 18) AND (t2.v2 <= 70)) OR (((t2.v1 > 55) AND (t2.v2 > 52)) AND (NOT((t2.v3 = 70))))) OR (t2.v1 = 58)) AND ((NOT((t2.v1 = 22))) AND (t2.v4 > 76))) OR (((t2.v1 > 14) AND (t2.v2 < 32)) AND (t2.v3 > 97)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>=9 AND v2>69) AND (v1 BETWEEN 39 AND 73);`,
		ExpectedPlan: "Filter((t2.v1 >= 9) AND (t2.v1 BETWEEN 39 AND 73))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<87 AND v2 BETWEEN 2 AND 34 AND v3=87 AND v4>=76) OR (v1<>77 AND v2<=44 AND v3>34));`,
		ExpectedPlan: "Filter(((((t2.v1 < 87) AND (t2.v2 BETWEEN 2 AND 34)) AND (t2.v3 = 87)) AND (t2.v4 >= 76)) OR (((NOT((t2.v1 = 77))) AND (t2.v2 <= 44)) AND (t2.v3 > 34)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=9 AND v4>=68 AND v2>21) OR (v1=5 AND v2<69 AND v3<=15 AND v4>=61));`,
		ExpectedPlan: "Filter((((t2.v1 = 9) AND (t2.v4 >= 68)) AND (t2.v2 > 21)) OR ((((t2.v1 = 5) AND (t2.v2 < 69)) AND (t2.v3 <= 15)) AND (t2.v4 >= 61)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=22) OR (v1>55));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 47 AND 57 AND v2>=83) OR (v1=91 AND v2>34));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 47 AND 57) AND (t2.v2 >= 83)) OR ((t2.v1 = 91) AND (t2.v2 > 34)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 23 AND 25) AND (v1<98 AND v2>=20 AND v3>37);`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 23 AND 25) AND (t2.v1 < 98)) AND (t2.v2 >= 20))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=6) OR (v1>61 AND v2<=34)) OR (v1>10 AND v2<>50 AND v3<>62 AND v4<=84));`,
		ExpectedPlan: "Filter(((t2.v1 = 6) OR ((t2.v1 > 61) AND (t2.v2 <= 34))) OR ((((t2.v1 > 10) AND (NOT((t2.v2 = 50)))) AND (NOT((t2.v3 = 62)))) AND (t2.v4 <= 84)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>74) OR (v1<>86 AND v2<=91)) AND (v1>=8);`,
		ExpectedPlan: "Filter((NOT((t2.v1 = 74))) OR ((NOT((t2.v1 = 86))) AND (t2.v2 <= 91)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>25 AND v2 BETWEEN 23 AND 54) OR (v1<>40 AND v3>90)) OR (v1<>7 AND v4<=78));`,
		ExpectedPlan: "Filter((((t2.v1 > 25) AND (t2.v2 BETWEEN 23 AND 54)) OR ((NOT((t2.v1 = 40))) AND (t2.v3 > 90))) OR ((NOT((t2.v1 = 7))) AND (t2.v4 <= 78)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=25) OR (v1>40 AND v2 BETWEEN 26 AND 40 AND v3<76));`,
		ExpectedPlan: "Filter((t2.v1 = 25) OR (((t2.v1 > 40) AND (t2.v2 BETWEEN 26 AND 40)) AND (t2.v3 < 76)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=13 AND v2<85) OR (v1=23 AND v2<>68 AND v3=33));`,
		ExpectedPlan: "Filter(((t2.v1 = 13) AND (t2.v2 < 85)) OR (((t2.v1 = 23) AND (NOT((t2.v2 = 68)))) AND (t2.v3 = 33)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<42 AND v2>95 AND v3>17 AND v4<>97) OR (v1>=13 AND v2<>10 AND v3 BETWEEN 73 AND 85 AND v4=48)) OR (v1>55 AND v2=85 AND v3>30));`,
		ExpectedPlan: "Filter((((((t2.v1 < 42) AND (t2.v2 > 95)) AND (t2.v3 > 17)) AND (NOT((t2.v4 = 97)))) OR ((((t2.v1 >= 13) AND (NOT((t2.v2 = 10)))) AND (t2.v3 BETWEEN 73 AND 85)) AND (t2.v4 = 48))) OR (((t2.v1 > 55) AND (t2.v2 = 85)) AND (t2.v3 > 30)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 5 AND 32) OR (v1>7)) OR (v1=34));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>=34 AND v2<>61 AND v3<>3) AND (v1 BETWEEN 69 AND 93) AND (v1=36 AND v2>14);`,
		ExpectedPlan: "Filter(((((t2.v1 >= 34) AND (NOT((t2.v2 = 61)))) AND (t2.v1 BETWEEN 69 AND 93)) AND (t2.v1 = 36)) AND (t2.v2 > 14))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>75) OR (v1<>74 AND v3 BETWEEN 29 AND 73));`,
		ExpectedPlan: "Filter((NOT((t2.v1 = 75))) OR ((NOT((t2.v1 = 74))) AND (t2.v3 BETWEEN 29 AND 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1<>91 AND v3=27 AND v4=22 AND v2<>68) AND (v1<=88);`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 91))) AND (t2.v3 = 27)) AND (NOT((t2.v2 = 68)))) AND (t2.v1 <= 88))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<>18 AND v2<>90 AND v3>95) OR (v1>=44)) OR (v1<4 AND v3<=26 AND v4<>67 AND v2>=37)) OR (v1<36 AND v2<=15 AND v3 BETWEEN 25 AND 36 AND v4<=14));`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 18))) AND (NOT((t2.v2 = 90)))) AND (t2.v3 > 95)) OR (t2.v1 >= 44)) OR ((((t2.v1 < 4) AND (t2.v3 <= 26)) AND (NOT((t2.v4 = 67)))) AND (t2.v2 >= 37))) OR ((((t2.v1 < 36) AND (t2.v2 <= 15)) AND (t2.v3 BETWEEN 25 AND 36)) AND (t2.v4 <= 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 44 AND 87 AND v2<52 AND v3<52 AND v4<1) OR (v1<30 AND v4 BETWEEN 8 AND 97 AND v2<=24));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 44 AND 87) AND (t2.v2 < 52)) AND (t2.v3 < 52)) AND (t2.v4 < 1)) OR (((t2.v1 < 30) AND (t2.v4 BETWEEN 8 AND 97)) AND (t2.v2 <= 24)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>48 AND v2<=83) OR (v1>28 AND v2 BETWEEN 9 AND 87 AND v3<>73)) OR (v1>=53 AND v2>=91 AND v3 BETWEEN 33 AND 97));`,
		ExpectedPlan: "Filter((((t2.v1 > 48) AND (t2.v2 <= 83)) OR (((t2.v1 > 28) AND (t2.v2 BETWEEN 9 AND 87)) AND (NOT((t2.v3 = 73))))) OR (((t2.v1 >= 53) AND (t2.v2 >= 91)) AND (t2.v3 BETWEEN 33 AND 97)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>8 AND v2 BETWEEN 34 AND 48) OR (v1<>54));`,
		ExpectedPlan: "Filter(((NOT((t2.v1 = 8))) AND (t2.v2 BETWEEN 34 AND 48)) OR (NOT((t2.v1 = 54))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=78 AND v2<74 AND v3<42 AND v4>=34) OR (v1<=29 AND v2<=27 AND v3>31 AND v4 BETWEEN 35 AND 41));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 78) AND (t2.v2 < 74)) AND (t2.v3 < 42)) AND (t2.v4 >= 34)) OR ((((t2.v1 <= 29) AND (t2.v2 <= 27)) AND (t2.v3 > 31)) AND (t2.v4 BETWEEN 35 AND 41)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 9 AND 35 AND v4<=69 AND v2 BETWEEN 34 AND 53 AND v3<>28) AND (v1 BETWEEN 12 AND 48);`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 9 AND 35) AND (t2.v2 BETWEEN 34 AND 53)) AND (NOT((t2.v3 = 28)))) AND (t2.v1 BETWEEN 12 AND 48))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 13 AND 77 AND v2>75 AND v3<73 AND v4>=6) AND (v1<=58 AND v2=48 AND v3 BETWEEN 33 AND 73);`,
		ExpectedPlan: "Filter((((((t2.v1 BETWEEN 13 AND 77) AND (t2.v2 > 75)) AND (t2.v3 < 73)) AND (t2.v1 <= 58)) AND (t2.v2 = 48)) AND (t2.v3 BETWEEN 33 AND 73))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>47 AND v3>47 AND v4 BETWEEN 51 AND 86 AND v2=26) OR (v1<82 AND v2<=17 AND v3<17 AND v4>=46));`,
		ExpectedPlan: "Filter(((((t2.v1 > 47) AND (t2.v3 > 47)) AND (t2.v4 BETWEEN 51 AND 86)) AND (t2.v2 = 26)) OR ((((t2.v1 < 82) AND (t2.v2 <= 17)) AND (t2.v3 < 17)) AND (t2.v4 >= 46)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>87) OR (v1>82 AND v4>=22)) OR (v1>=52 AND v2<>47 AND v3=37)) OR (v1<=14 AND v2<57 AND v3<10));`,
		ExpectedPlan: "Filter((((t2.v1 > 87) OR ((t2.v1 > 82) AND (t2.v4 >= 22))) OR (((t2.v1 >= 52) AND (NOT((t2.v2 = 47)))) AND (t2.v3 = 37))) OR (((t2.v1 <= 14) AND (t2.v2 < 57)) AND (t2.v3 < 10)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>=99 AND v3<=41) AND (v1<>38 AND v2<94 AND v3 BETWEEN 83 AND 95 AND v4>=86);`,
		ExpectedPlan: "Filter(((((t2.v1 >= 99) AND (t2.v3 <= 41)) AND (NOT((t2.v1 = 38)))) AND (t2.v2 < 94)) AND (t2.v3 BETWEEN 83 AND 95))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>78) AND (v1>32 AND v2>11 AND v3>=78);`,
		ExpectedPlan: "Filter(((t2.v1 > 78) AND (t2.v1 > 32)) AND (t2.v2 > 11))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1<>3 AND v2=26 AND v3=22 AND v4<=76) AND (v1 BETWEEN 59 AND 92 AND v2 BETWEEN 36 AND 80);`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 3))) AND (t2.v2 = 26)) AND (t2.v3 = 22)) AND (t2.v1 BETWEEN 59 AND 92)) AND (t2.v2 BETWEEN 36 AND 80))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>10) OR (v1=12));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>=12 AND v3>=45 AND v4<98) OR (v1<>51 AND v3=79 AND v4<=24)) OR (v1 BETWEEN 4 AND 59 AND v4<82)) OR (v1>=29 AND v2<>21));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 12) AND (t2.v3 >= 45)) AND (t2.v4 < 98)) OR (((NOT((t2.v1 = 51))) AND (t2.v3 = 79)) AND (t2.v4 <= 24))) OR ((t2.v1 BETWEEN 4 AND 59) AND (t2.v4 < 82))) OR ((t2.v1 >= 29) AND (NOT((t2.v2 = 21)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>10 AND v2<=75 AND v3>=70) OR (v1<89 AND v2<=32));`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 10))) AND (t2.v2 <= 75)) AND (t2.v3 >= 70)) OR ((t2.v1 < 89) AND (t2.v2 <= 32)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=15) OR (v1=15)) OR (v1 BETWEEN 14 AND 25 AND v4>55 AND v2<53 AND v3=95));`,
		ExpectedPlan: "Filter(((t2.v1 >= 15) OR (t2.v1 = 15)) OR ((((t2.v1 BETWEEN 14 AND 25) AND (t2.v4 > 55)) AND (t2.v2 < 53)) AND (t2.v3 = 95)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>48 AND v2 BETWEEN 4 AND 84 AND v3<=3 AND v4<>31) AND (v1 BETWEEN 2 AND 15 AND v3>75);`,
		ExpectedPlan: "Filter(((((t2.v1 > 48) AND (t2.v2 BETWEEN 4 AND 84)) AND (t2.v3 <= 3)) AND (t2.v1 BETWEEN 2 AND 15)) AND (t2.v3 > 75))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<41 AND v4=9 AND v2>77 AND v3=41) OR (v1>62 AND v2>=48 AND v3=13 AND v4>61)) OR (v1 BETWEEN 33 AND 75)) OR (v1 BETWEEN 45 AND 65 AND v4 BETWEEN 4 AND 68));`,
		ExpectedPlan: "Filter(((((((t2.v1 < 41) AND (t2.v4 = 9)) AND (t2.v2 > 77)) AND (t2.v3 = 41)) OR ((((t2.v1 > 62) AND (t2.v2 >= 48)) AND (t2.v3 = 13)) AND (t2.v4 > 61))) OR (t2.v1 BETWEEN 33 AND 75)) OR ((t2.v1 BETWEEN 45 AND 65) AND (t2.v4 BETWEEN 4 AND 68)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>20) OR (v1>=71 AND v4 BETWEEN 12 AND 20 AND v2<=30 AND v3 BETWEEN 14 AND 44)) AND (v1>97 AND v2=91 AND v3>=5) OR (v1>7 AND v2<34 AND v3<55 AND v4 BETWEEN 88 AND 97)) AND (v1 BETWEEN 2 AND 16 AND v2<>23 AND v3=75 AND v4>99);`,
		ExpectedPlan: "Filter(((((((t2.v1 > 20) OR ((((t2.v1 >= 71) AND (t2.v4 BETWEEN 12 AND 20)) AND (t2.v2 <= 30)) AND (t2.v3 BETWEEN 14 AND 44))) AND (((t2.v1 > 97) AND (t2.v2 = 91)) AND (t2.v3 >= 5))) OR ((((t2.v1 > 7) AND (t2.v2 < 34)) AND (t2.v3 < 55)) AND (t2.v4 BETWEEN 88 AND 97))) AND (t2.v1 BETWEEN 2 AND 16)) AND (NOT((t2.v2 = 23)))) AND (t2.v3 = 75))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=55 AND v2<13 AND v3<=96 AND v4>=49) OR (v1 BETWEEN 39 AND 98 AND v2=77 AND v3>85));`,
		ExpectedPlan: "Filter(((((t2.v1 = 55) AND (t2.v2 < 13)) AND (t2.v3 <= 96)) AND (t2.v4 >= 49)) OR (((t2.v1 BETWEEN 39 AND 98) AND (t2.v2 = 77)) AND (t2.v3 > 85)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=74 AND v2<>13 AND v3<67 AND v4 BETWEEN 1 AND 70) OR (v1 BETWEEN 30 AND 50 AND v2<27 AND v3>=35));`,
		ExpectedPlan: "Filter(((((t2.v1 = 74) AND (NOT((t2.v2 = 13)))) AND (t2.v3 < 67)) AND (t2.v4 BETWEEN 1 AND 70)) OR (((t2.v1 BETWEEN 30 AND 50) AND (t2.v2 < 27)) AND (t2.v3 >= 35)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1=76) OR (v1>22 AND v3<49 AND v4=2)) OR (v1=85 AND v4>79)) OR (v1=10 AND v2=47 AND v3 BETWEEN 6 AND 21 AND v4>97));`,
		ExpectedPlan: "Filter((((t2.v1 = 76) OR (((t2.v1 > 22) AND (t2.v3 < 49)) AND (t2.v4 = 2))) OR ((t2.v1 = 85) AND (t2.v4 > 79))) OR ((((t2.v1 = 10) AND (t2.v2 = 47)) AND (t2.v3 BETWEEN 6 AND 21)) AND (t2.v4 > 97)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>38 AND v2>98) OR (v1<>29 AND v2=75)) OR (v1>58 AND v2<>49 AND v3 BETWEEN 25 AND 58));`,
		ExpectedPlan: "Filter((((t2.v1 > 38) AND (t2.v2 > 98)) OR ((NOT((t2.v1 = 29))) AND (t2.v2 = 75))) OR (((t2.v1 > 58) AND (NOT((t2.v2 = 49)))) AND (t2.v3 BETWEEN 25 AND 58)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>82 AND v4=74 AND v2=8 AND v3>=43) OR (v1=1 AND v2>=54 AND v3 BETWEEN 41 AND 91 AND v4>=0));`,
		ExpectedPlan: "Filter(((((NOT((t2.v1 = 82))) AND (t2.v4 = 74)) AND (t2.v2 = 8)) AND (t2.v3 >= 43)) OR ((((t2.v1 = 1) AND (t2.v2 >= 54)) AND (t2.v3 BETWEEN 41 AND 91)) AND (t2.v4 >= 0)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=26 AND v2<=94 AND v3<=76) OR (v1<34 AND v2 BETWEEN 5 AND 20));`,
		ExpectedPlan: "Filter((((t2.v1 = 26) AND (t2.v2 <= 94)) AND (t2.v3 <= 76)) OR ((t2.v1 < 34) AND (t2.v2 BETWEEN 5 AND 20)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>74 AND v2<=3 AND v3>51 AND v4<1) OR (v1>=92 AND v2<=2));`,
		ExpectedPlan: "Filter(((((t2.v1 > 74) AND (t2.v2 <= 3)) AND (t2.v3 > 51)) AND (t2.v4 < 1)) OR ((t2.v1 >= 92) AND (t2.v2 <= 2)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<=10 AND v2 BETWEEN 29 AND 83 AND v3<30 AND v4=54) OR (v1=68 AND v2=9 AND v3<=31)) AND (v1=87 AND v2>=91) OR (v1<=3 AND v2<>65 AND v3<8 AND v4<54)) OR (v1<7 AND v2>=4 AND v3<=47));`,
		ExpectedPlan: "Filter((((((((t2.v1 <= 10) AND (t2.v2 BETWEEN 29 AND 83)) AND (t2.v3 < 30)) AND (t2.v4 = 54)) OR (((t2.v1 = 68) AND (t2.v2 = 9)) AND (t2.v3 <= 31))) AND ((t2.v1 = 87) AND (t2.v2 >= 91))) OR ((((t2.v1 <= 3) AND (NOT((t2.v2 = 65)))) AND (t2.v3 < 8)) AND (t2.v4 < 54))) OR (((t2.v1 < 7) AND (t2.v2 >= 4)) AND (t2.v3 <= 47)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<35) OR (v1>=5 AND v2>=10 AND v3=65));`,
		ExpectedPlan: "Filter((t2.v1 < 35) OR (((t2.v1 >= 5) AND (t2.v2 >= 10)) AND (t2.v3 = 65)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>31 AND v2<=37 AND v3>56 AND v4 BETWEEN 10 AND 31) OR (v1>8)) AND (v1>=27 AND v2<>44);`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 31))) AND (t2.v2 <= 37)) AND (t2.v3 > 56)) AND (t2.v4 BETWEEN 10 AND 31)) OR (t2.v1 > 8)) AND (t2.v1 >= 27))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>52) OR (v1<21 AND v2<61 AND v3=13)) OR (v1=89 AND v3>33));`,
		ExpectedPlan: "Filter(((t2.v1 > 52) OR (((t2.v1 < 21) AND (t2.v2 < 61)) AND (t2.v3 = 13))) OR ((t2.v1 = 89) AND (t2.v3 > 33)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<30 AND v4>11 AND v2<=11) OR (v1<>19 AND v2<>47 AND v3 BETWEEN 38 AND 77 AND v4>31)) OR (v1 BETWEEN 0 AND 27 AND v2 BETWEEN 33 AND 34)) OR (v1<32)) AND (v1<9 AND v3=54 AND v4<>31 AND v2<>95);`,
		ExpectedPlan: "Filter(((((((((t2.v1 < 30) AND (t2.v4 > 11)) AND (t2.v2 <= 11)) OR ((((NOT((t2.v1 = 19))) AND (NOT((t2.v2 = 47)))) AND (t2.v3 BETWEEN 38 AND 77)) AND (t2.v4 > 31))) OR ((t2.v1 BETWEEN 0 AND 27) AND (t2.v2 BETWEEN 33 AND 34))) OR (t2.v1 < 32)) AND (t2.v1 < 9)) AND (t2.v3 = 54)) AND (NOT((t2.v2 = 95))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=48) OR (v1 BETWEEN 2 AND 81));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<>36) OR (v1<>70 AND v2 BETWEEN 23 AND 39)) OR (v1>51 AND v2>=57)) OR (v1<50 AND v2<=3 AND v3 BETWEEN 1 AND 74));`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 36))) OR ((NOT((t2.v1 = 70))) AND (t2.v2 BETWEEN 23 AND 39))) OR ((t2.v1 > 51) AND (t2.v2 >= 57))) OR (((t2.v1 < 50) AND (t2.v2 <= 3)) AND (t2.v3 BETWEEN 1 AND 74)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1>30) OR (v1>98 AND v4>43 AND v2<>80)) OR (v1 BETWEEN 2 AND 23 AND v2>=34)) OR (v1>=42));`,
		ExpectedPlan: "Filter((((t2.v1 > 30) OR (((t2.v1 > 98) AND (t2.v4 > 43)) AND (NOT((t2.v2 = 80))))) OR ((t2.v1 BETWEEN 2 AND 23) AND (t2.v2 >= 34))) OR (t2.v1 >= 42))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<68 AND v2<81 AND v3<34 AND v4<>33) OR (v1<=78 AND v4 BETWEEN 34 AND 99 AND v2>=79 AND v3>=9)) OR (v1=27 AND v4 BETWEEN 20 AND 41 AND v2<98 AND v3>=15));`,
		ExpectedPlan: "Filter((((((t2.v1 < 68) AND (t2.v2 < 81)) AND (t2.v3 < 34)) AND (NOT((t2.v4 = 33)))) OR ((((t2.v1 <= 78) AND (t2.v4 BETWEEN 34 AND 99)) AND (t2.v2 >= 79)) AND (t2.v3 >= 9))) OR ((((t2.v1 = 27) AND (t2.v4 BETWEEN 20 AND 41)) AND (t2.v2 < 98)) AND (t2.v3 >= 15)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<23 AND v2<=45 AND v3<0) OR (v1>=31)) OR (v1>=50));`,
		ExpectedPlan: "Filter(((((t2.v1 < 23) AND (t2.v2 <= 45)) AND (t2.v3 < 0)) OR (t2.v1 >= 31)) OR (t2.v1 >= 50))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<16) OR (v1>=19 AND v2<25 AND v3>77));`,
		ExpectedPlan: "Filter((t2.v1 < 16) OR (((t2.v1 >= 19) AND (t2.v2 < 25)) AND (t2.v3 > 77)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<29 AND v2 BETWEEN 81 AND 92) OR (v1>20 AND v2>=53 AND v3 BETWEEN 20 AND 68));`,
		ExpectedPlan: "Filter(((t2.v1 < 29) AND (t2.v2 BETWEEN 81 AND 92)) OR (((t2.v1 > 20) AND (t2.v2 >= 53)) AND (t2.v3 BETWEEN 20 AND 68)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1 BETWEEN 25 AND 59 AND v2=1 AND v3<93 AND v4<=16) OR (v1<40 AND v2 BETWEEN 14 AND 37 AND v3>62 AND v4<58)) OR (v1<>17 AND v2<>36)) OR (v1 BETWEEN 7 AND 99 AND v2<>6 AND v3=43 AND v4<89));`,
		ExpectedPlan: "Filter(((((((t2.v1 BETWEEN 25 AND 59) AND (t2.v2 = 1)) AND (t2.v3 < 93)) AND (t2.v4 <= 16)) OR ((((t2.v1 < 40) AND (t2.v2 BETWEEN 14 AND 37)) AND (t2.v3 > 62)) AND (t2.v4 < 58))) OR ((NOT((t2.v1 = 17))) AND (NOT((t2.v2 = 36))))) OR ((((t2.v1 BETWEEN 7 AND 99) AND (NOT((t2.v2 = 6)))) AND (t2.v3 = 43)) AND (t2.v4 < 89)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1=46) AND (v1>=93 AND v3<>51 AND v4=93 AND v2=8);`,
		ExpectedPlan: "Filter((((t2.v1 = 46) AND (t2.v1 >= 93)) AND (NOT((t2.v3 = 51)))) AND (t2.v2 = 8))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<=5 AND v2>=14 AND v3<=2) OR (v1<53 AND v4=99 AND v2=72)) OR (v1<>49 AND v2<>39 AND v3>=70 AND v4<>24)) OR (v1<79));`,
		ExpectedPlan: "Filter((((((t2.v1 <= 5) AND (t2.v2 >= 14)) AND (t2.v3 <= 2)) OR (((t2.v1 < 53) AND (t2.v4 = 99)) AND (t2.v2 = 72))) OR ((((NOT((t2.v1 = 49))) AND (NOT((t2.v2 = 39)))) AND (t2.v3 >= 70)) AND (NOT((t2.v4 = 24))))) OR (t2.v1 < 79))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1<99 AND v2<=42) OR (v1=47 AND v4 BETWEEN 33 AND 63 AND v2>=10 AND v3<=57)) OR (v1>44)) OR (v1<>87 AND v2>42 AND v3<69));`,
		ExpectedPlan: "Filter(((((t2.v1 < 99) AND (t2.v2 <= 42)) OR ((((t2.v1 = 47) AND (t2.v4 BETWEEN 33 AND 63)) AND (t2.v2 >= 10)) AND (t2.v3 <= 57))) OR (t2.v1 > 44)) OR (((NOT((t2.v1 = 87))) AND (t2.v2 > 42)) AND (t2.v3 < 69)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=29 AND v2 BETWEEN 50 AND 86 AND v3<=6 AND v4 BETWEEN 8 AND 48) OR (v1>86 AND v2 BETWEEN 62 AND 70 AND v3=33));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 29) AND (t2.v2 BETWEEN 50 AND 86)) AND (t2.v3 <= 6)) AND (t2.v4 BETWEEN 8 AND 48)) OR (((t2.v1 > 86) AND (t2.v2 BETWEEN 62 AND 70)) AND (t2.v3 = 33)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=15) OR (v1>=59 AND v2<18)) OR (v1 BETWEEN 23 AND 31 AND v3>50 AND v4 BETWEEN 15 AND 54));`,
		ExpectedPlan: "Filter(((t2.v1 >= 15) OR ((t2.v1 >= 59) AND (t2.v2 < 18))) OR (((t2.v1 BETWEEN 23 AND 31) AND (t2.v3 > 50)) AND (t2.v4 BETWEEN 15 AND 54)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=50 AND v2<=84 AND v3 BETWEEN 22 AND 26) OR (v1<=18 AND v2<49 AND v3>19 AND v4 BETWEEN 61 AND 75)) AND (v1>48 AND v2>=56 AND v3=6) OR (v1<=88 AND v2>=76 AND v3<40 AND v4<=18));`,
		ExpectedPlan: "Filter((((((t2.v1 >= 50) AND (t2.v2 <= 84)) AND (t2.v3 BETWEEN 22 AND 26)) OR ((((t2.v1 <= 18) AND (t2.v2 < 49)) AND (t2.v3 > 19)) AND (t2.v4 BETWEEN 61 AND 75))) AND (((t2.v1 > 48) AND (t2.v2 >= 56)) AND (t2.v3 = 6))) OR ((((t2.v1 <= 88) AND (t2.v2 >= 76)) AND (t2.v3 < 40)) AND (t2.v4 <= 18)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=27) OR (v1>=11 AND v2<97 AND v3<97 AND v4<44));`,
		ExpectedPlan: "Filter((t2.v1 = 27) OR ((((t2.v1 >= 11) AND (t2.v2 < 97)) AND (t2.v3 < 97)) AND (t2.v4 < 44)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=89 AND v2<=93) OR (v1<=54));`,
		ExpectedPlan: "Filter(((t2.v1 <= 89) AND (t2.v2 <= 93)) OR (t2.v1 <= 54))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=74 AND v2<=31) OR (v1<11)) OR (v1 BETWEEN 26 AND 38));`,
		ExpectedPlan: "Filter((((t2.v1 = 74) AND (t2.v2 <= 31)) OR (t2.v1 < 11)) OR (t2.v1 BETWEEN 26 AND 38))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=10 AND v2<12 AND v3=54 AND v4>89) OR (v1=99 AND v4=37));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 10) AND (t2.v2 < 12)) AND (t2.v3 = 54)) AND (t2.v4 > 89)) OR ((t2.v1 = 99) AND (t2.v4 = 37)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=50 AND v2<50) OR (v1<19)) OR (v1=51));`,
		ExpectedPlan: "Filter((((t2.v1 <= 50) AND (t2.v2 < 50)) OR (t2.v1 < 19)) OR (t2.v1 = 51))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=62 AND v2<89) AND (v1<90 AND v2>=19) OR (v1<=1 AND v2>49));`,
		ExpectedPlan: "Filter((((t2.v1 = 62) AND (t2.v2 < 89)) AND ((t2.v1 < 90) AND (t2.v2 >= 19))) OR ((t2.v1 <= 1) AND (t2.v2 > 49)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1<=61 AND v2<=64) AND (v1>=0);`,
		ExpectedPlan: "Filter((t2.v1 <= 61) AND (t2.v1 >= 0))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 5 AND 69) OR (v1<52 AND v4<14 AND v2>=25 AND v3=63));`,
		ExpectedPlan: "Filter((t2.v1 BETWEEN 5 AND 69) OR ((((t2.v1 < 52) AND (t2.v4 < 14)) AND (t2.v2 >= 25)) AND (t2.v3 = 63)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=21 AND v2<>0 AND v3<49) OR (v1<=70 AND v2>16 AND v3<=89 AND v4>=27)) OR (v1>=14));`,
		ExpectedPlan: "Filter(((((t2.v1 = 21) AND (NOT((t2.v2 = 0)))) AND (t2.v3 < 49)) OR ((((t2.v1 <= 70) AND (t2.v2 > 16)) AND (t2.v3 <= 89)) AND (t2.v4 >= 27))) OR (t2.v1 >= 14))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>14) OR (v1>=82));`,
		ExpectedPlan: "Projected table access on [pk v1 v2 v3 v4]\n" +
			" └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=19 AND v3<72 AND v4=23) OR (v1<=36 AND v2>99));`,
		ExpectedPlan: "Filter((((t2.v1 = 19) AND (t2.v3 < 72)) AND (t2.v4 = 23)) OR ((t2.v1 <= 36) AND (t2.v2 > 99)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>43) OR (v1>=41 AND v4=32 AND v2<=66)) AND (v1>43 AND v2 BETWEEN 83 AND 97);`,
		ExpectedPlan: "Filter(((NOT((t2.v1 = 43))) OR (((t2.v1 >= 41) AND (t2.v4 = 32)) AND (t2.v2 <= 66))) AND (t2.v1 > 43))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=8 AND v4>=44) AND (v1=84 AND v2=41 AND v3 BETWEEN 5 AND 81) OR (v1<>31 AND v2<=96 AND v3<=20 AND v4<=14));`,
		ExpectedPlan: "Filter((((t2.v1 <= 8) AND (t2.v4 >= 44)) AND (((t2.v1 = 84) AND (t2.v2 = 41)) AND (t2.v3 BETWEEN 5 AND 81))) OR ((((NOT((t2.v1 = 31))) AND (t2.v2 <= 96)) AND (t2.v3 <= 20)) AND (t2.v4 <= 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 52 AND 55) OR (v1>1 AND v2>36 AND v3<=47)) OR (v1 BETWEEN 0 AND 38 AND v2<=49 AND v3>=8));`,
		ExpectedPlan: "Filter(((t2.v1 BETWEEN 52 AND 55) OR (((t2.v1 > 1) AND (t2.v2 > 36)) AND (t2.v3 <= 47))) OR (((t2.v1 BETWEEN 0 AND 38) AND (t2.v2 <= 49)) AND (t2.v3 >= 8)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=11 AND v2>=41 AND v3=9) AND (v1<>41 AND v3<>69 AND v4<24) OR (v1>48 AND v4<79));`,
		ExpectedPlan: "Filter(((((t2.v1 <= 11) AND (t2.v2 >= 41)) AND (t2.v3 = 9)) AND (((NOT((t2.v1 = 41))) AND (NOT((t2.v3 = 69)))) AND (t2.v4 < 24))) OR ((t2.v1 > 48) AND (t2.v4 < 79)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1=23 AND v4>=52 AND v2>=61) AND (v1<>85 AND v3>2 AND v4<15);`,
		ExpectedPlan: "Filter((((t2.v1 = 23) AND (t2.v2 >= 61)) AND (NOT((t2.v1 = 85)))) AND (t2.v3 > 2))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1 BETWEEN 32 AND 51 AND v4 BETWEEN 5 AND 14 AND v2=46 AND v3>=31) OR (v1>=32 AND v2<=26 AND v3>52 AND v4>55));`,
		ExpectedPlan: "Filter(((((t2.v1 BETWEEN 32 AND 51) AND (t2.v4 BETWEEN 5 AND 14)) AND (t2.v2 = 46)) AND (t2.v3 >= 31)) OR ((((t2.v1 >= 32) AND (t2.v2 <= 26)) AND (t2.v3 > 52)) AND (t2.v4 > 55)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=16 AND v2<59 AND v3<=43) OR (v1=17 AND v2<=4 AND v3>71));`,
		ExpectedPlan: "Filter((((t2.v1 >= 16) AND (t2.v2 < 59)) AND (t2.v3 <= 43)) OR (((t2.v1 = 17) AND (t2.v2 <= 4)) AND (t2.v3 > 71)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1=42 AND v4=47) OR (v1>=28)) AND (v1<>10) OR (v1 BETWEEN 20 AND 60 AND v2>96 AND v3<>28)) OR (v1=99 AND v2<=62 AND v3=30 AND v4 BETWEEN 92 AND 93));`,
		ExpectedPlan: "Filter((((((t2.v1 = 42) AND (t2.v4 = 47)) OR (t2.v1 >= 28)) AND (NOT((t2.v1 = 10)))) OR (((t2.v1 BETWEEN 20 AND 60) AND (t2.v2 > 96)) AND (NOT((t2.v3 = 28))))) OR ((((t2.v1 = 99) AND (t2.v2 <= 62)) AND (t2.v3 = 30)) AND (t2.v4 BETWEEN 92 AND 93)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=50 AND v3=4 AND v4=53 AND v2>=80) OR (v1<54 AND v4<=76 AND v2>48)) OR (v1>=38 AND v4<76 AND v2=56));`,
		ExpectedPlan: "Filter((((((t2.v1 = 50) AND (t2.v3 = 4)) AND (t2.v4 = 53)) AND (t2.v2 >= 80)) OR (((t2.v1 < 54) AND (t2.v4 <= 76)) AND (t2.v2 > 48))) OR (((t2.v1 >= 38) AND (t2.v4 < 76)) AND (t2.v2 = 56)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=79 AND v2>24) OR (v1<76 AND v3<=59 AND v4<=36 AND v2=39));`,
		ExpectedPlan: "Filter(((t2.v1 = 79) AND (t2.v2 > 24)) OR ((((t2.v1 < 76) AND (t2.v3 <= 59)) AND (t2.v4 <= 36)) AND (t2.v2 = 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<=15 AND v2 BETWEEN 21 AND 76 AND v3=23) OR (v1 BETWEEN 2 AND 55));`,
		ExpectedPlan: "Filter((((t2.v1 <= 15) AND (t2.v2 BETWEEN 21 AND 76)) AND (t2.v3 = 23)) OR (t2.v1 BETWEEN 2 AND 55))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=85 AND v2>37 AND v3<=57 AND v4 BETWEEN 12 AND 49) AND (v1>10) OR (v1>56)) OR (v1>=57));`,
		ExpectedPlan: "Filter(((((((t2.v1 = 85) AND (t2.v2 > 37)) AND (t2.v3 <= 57)) AND (t2.v4 BETWEEN 12 AND 49)) AND (t2.v1 > 10)) OR (t2.v1 > 56)) OR (t2.v1 >= 57))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((((v1<>89 AND v2>=75) OR (v1<=5)) OR (v1=5 AND v2<19 AND v3>=1)) OR (v1>=18 AND v2>=17 AND v3 BETWEEN 78 AND 83)) OR (v1>=11 AND v3<=9 AND v4>39));`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 89))) AND (t2.v2 >= 75)) OR (t2.v1 <= 5)) OR (((t2.v1 = 5) AND (t2.v2 < 19)) AND (t2.v3 >= 1))) OR (((t2.v1 >= 18) AND (t2.v2 >= 17)) AND (t2.v3 BETWEEN 78 AND 83))) OR (((t2.v1 >= 11) AND (t2.v3 <= 9)) AND (t2.v4 > 39)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1 BETWEEN 36 AND 48 AND v4<97 AND v2>=99 AND v3=3) OR (v1<>84 AND v2=46 AND v3=4)) OR (v1>73 AND v2 BETWEEN 34 AND 39 AND v3 BETWEEN 34 AND 71 AND v4>=15)) OR (v1<>82));`,
		ExpectedPlan: "Filter(((((((t2.v1 BETWEEN 36 AND 48) AND (t2.v4 < 97)) AND (t2.v2 >= 99)) AND (t2.v3 = 3)) OR (((NOT((t2.v1 = 84))) AND (t2.v2 = 46)) AND (t2.v3 = 4))) OR ((((t2.v1 > 73) AND (t2.v2 BETWEEN 34 AND 39)) AND (t2.v3 BETWEEN 34 AND 71)) AND (t2.v4 >= 15))) OR (NOT((t2.v1 = 82))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1<=50 AND v3>=51 AND v4<>69) AND (v1>1 AND v3<24);`,
		ExpectedPlan: "Filter(((t2.v3 >= 51) AND (NOT((t2.v4 = 69)))) AND (t2.v3 < 24))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>10 AND v2=72 AND v3<31) OR (v1<67 AND v3 BETWEEN 13 AND 70 AND v4>66 AND v2>39)) OR (v1<82)) AND (v1>=66);`,
		ExpectedPlan: "Filter(((((t2.v1 > 10) AND (t2.v2 = 72)) AND (t2.v3 < 31)) OR ((((t2.v1 < 67) AND (t2.v3 BETWEEN 13 AND 70)) AND (t2.v4 > 66)) AND (t2.v2 > 39))) OR (t2.v1 < 82))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=84 AND v2<85 AND v3 BETWEEN 75 AND 86 AND v4<=34) OR (v1>=37 AND v2<59 AND v3 BETWEEN 2 AND 26 AND v4>6));`,
		ExpectedPlan: "Filter(((((t2.v1 = 84) AND (t2.v2 < 85)) AND (t2.v3 BETWEEN 75 AND 86)) AND (t2.v4 <= 34)) OR ((((t2.v1 >= 37) AND (t2.v2 < 59)) AND (t2.v3 BETWEEN 2 AND 26)) AND (t2.v4 > 6)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>10 AND v2=42) OR (v1>=85 AND v2<>6 AND v3=34 AND v4<=45));`,
		ExpectedPlan: "Filter(((t2.v1 > 10) AND (t2.v2 = 42)) OR ((((t2.v1 >= 85) AND (NOT((t2.v2 = 6)))) AND (t2.v3 = 34)) AND (t2.v4 <= 45)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=24 AND v2<>33 AND v3=77 AND v4<>63) OR (v1<>22 AND v2<=58 AND v3>71 AND v4>=87)) OR (v1<=85 AND v2>18 AND v3<=40));`,
		ExpectedPlan: "Filter((((((t2.v1 = 24) AND (NOT((t2.v2 = 33)))) AND (t2.v3 = 77)) AND (NOT((t2.v4 = 63)))) OR ((((NOT((t2.v1 = 22))) AND (t2.v2 <= 58)) AND (t2.v3 > 71)) AND (t2.v4 >= 87))) OR (((t2.v1 <= 85) AND (t2.v2 > 18)) AND (t2.v3 <= 40)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<72 AND v2>=67) OR (v1<>88 AND v2<>23 AND v3=23));`,
		ExpectedPlan: "Filter(((t2.v1 < 72) AND (t2.v2 >= 67)) OR (((NOT((t2.v1 = 88))) AND (NOT((t2.v2 = 23)))) AND (t2.v3 = 23)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=11 AND v2>=99) OR (v1<18 AND v2>=34 AND v3<53)) OR (v1>68));`,
		ExpectedPlan: "Filter((((t2.v1 = 11) AND (t2.v2 >= 99)) OR (((t2.v1 < 18) AND (t2.v2 >= 34)) AND (t2.v3 < 53))) OR (t2.v1 > 68))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<=40 AND v2<0) OR (v1>=35 AND v2<=95 AND v3<>61)) OR (v1>49));`,
		ExpectedPlan: "Filter((((t2.v1 <= 40) AND (t2.v2 < 0)) OR (((t2.v1 >= 35) AND (t2.v2 <= 95)) AND (NOT((t2.v3 = 61))))) OR (t2.v1 > 49))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1=85 AND v2<81 AND v3 BETWEEN 14 AND 61 AND v4<>99) OR (v1 BETWEEN 31 AND 86 AND v4<>43)) OR (v1 BETWEEN 15 AND 67)) AND (v1 BETWEEN 37 AND 55);`,
		ExpectedPlan: "Filter((((((t2.v1 = 85) AND (t2.v2 < 81)) AND (t2.v3 BETWEEN 14 AND 61)) AND (NOT((t2.v4 = 99)))) OR ((t2.v1 BETWEEN 31 AND 86) AND (NOT((t2.v4 = 43))))) OR (t2.v1 BETWEEN 15 AND 67))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>=52 AND v4>=86) OR (v1>=86 AND v3=79 AND v4=9 AND v2 BETWEEN 2 AND 6)) OR (v1>98 AND v2<=44 AND v3<>53));`,
		ExpectedPlan: "Filter((((t2.v1 >= 52) AND (t2.v4 >= 86)) OR ((((t2.v1 >= 86) AND (t2.v3 = 79)) AND (t2.v4 = 9)) AND (t2.v2 BETWEEN 2 AND 6))) OR (((t2.v1 > 98) AND (t2.v2 <= 44)) AND (NOT((t2.v3 = 53)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>5 AND v4 BETWEEN 14 AND 43 AND v2>=62) OR (v1>=91 AND v2>=28 AND v3>=83 AND v4<>91));`,
		ExpectedPlan: "Filter((((t2.v1 > 5) AND (t2.v4 BETWEEN 14 AND 43)) AND (t2.v2 >= 62)) OR ((((t2.v1 >= 91) AND (t2.v2 >= 28)) AND (t2.v3 >= 83)) AND (NOT((t2.v4 = 91)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1<>87) OR (v1>91 AND v2>23 AND v3<74));`,
		ExpectedPlan: "Filter((NOT((t2.v1 = 87))) OR (((t2.v1 > 91) AND (t2.v2 > 23)) AND (t2.v3 < 74)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1 BETWEEN 1 AND 19 AND v2 BETWEEN 22 AND 48) AND (v1 BETWEEN 6 AND 47 AND v2>=25 AND v3<27);`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 1 AND 19) AND (t2.v2 BETWEEN 22 AND 48)) AND (t2.v1 BETWEEN 6 AND 47)) AND (t2.v2 >= 25))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((((v1=76 AND v2>35 AND v3<=59 AND v4>25) OR (v1 BETWEEN 35 AND 82 AND v2 BETWEEN 8 AND 37 AND v3>18 AND v4<=70)) OR (v1<=95 AND v3=70 AND v4=11)) OR (v1 BETWEEN 15 AND 23 AND v2<>24 AND v3<=50 AND v4<>84));`,
		ExpectedPlan: "Filter(((((((t2.v1 = 76) AND (t2.v2 > 35)) AND (t2.v3 <= 59)) AND (t2.v4 > 25)) OR ((((t2.v1 BETWEEN 35 AND 82) AND (t2.v2 BETWEEN 8 AND 37)) AND (t2.v3 > 18)) AND (t2.v4 <= 70))) OR (((t2.v1 <= 95) AND (t2.v3 = 70)) AND (t2.v4 = 11))) OR ((((t2.v1 BETWEEN 15 AND 23) AND (NOT((t2.v2 = 24)))) AND (t2.v3 <= 50)) AND (NOT((t2.v4 = 84)))))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>42 AND v2=44 AND v3<>73) OR (v1>24 AND v2>49 AND v3>=7));`,
		ExpectedPlan: "Filter((((t2.v1 > 42) AND (t2.v2 = 44)) AND (NOT((t2.v3 = 73)))) OR (((t2.v1 > 24) AND (t2.v2 > 49)) AND (t2.v3 >= 7)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=79 AND v3<89 AND v4>=3) OR (v1<63 AND v2<66));`,
		ExpectedPlan: "Filter((((t2.v1 = 79) AND (t2.v3 < 89)) AND (t2.v4 >= 3)) OR ((t2.v1 < 63) AND (t2.v2 < 66)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>66) OR (v1=33)) OR (v1<>39 AND v2>53 AND v3<73 AND v4<75));`,
		ExpectedPlan: "Filter(((NOT((t2.v1 = 66))) OR (t2.v1 = 33)) OR ((((NOT((t2.v1 = 39))) AND (t2.v2 > 53)) AND (t2.v3 < 73)) AND (t2.v4 < 75)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1=15) OR (v1>36 AND v3=13 AND v4<=98 AND v2 BETWEEN 70 AND 85));`,
		ExpectedPlan: "Filter((t2.v1 = 15) OR ((((t2.v1 > 36) AND (t2.v3 = 13)) AND (t2.v4 <= 98)) AND (t2.v2 BETWEEN 70 AND 85)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 23 AND 45 AND v4<30) OR (v1>=36 AND v2<>6 AND v3 BETWEEN 30 AND 53)) OR (v1 BETWEEN 41 AND 95));`,
		ExpectedPlan: "Filter((((t2.v1 BETWEEN 23 AND 45) AND (t2.v4 < 30)) OR (((t2.v1 >= 36) AND (NOT((t2.v2 = 6)))) AND (t2.v3 BETWEEN 30 AND 53))) OR (t2.v1 BETWEEN 41 AND 95))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>6 AND v4<>9 AND v2<>77 AND v3>=81) OR (v1<>21 AND v2>=17 AND v3<=3));`,
		ExpectedPlan: "Filter(((((t2.v1 > 6) AND (NOT((t2.v4 = 9)))) AND (NOT((t2.v2 = 77)))) AND (t2.v3 >= 81)) OR (((NOT((t2.v1 = 21))) AND (t2.v2 >= 17)) AND (t2.v3 <= 3)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1 BETWEEN 94 AND 99 AND v2>4 AND v3<94 AND v4<=59) OR (v1=19 AND v2 BETWEEN 47 AND 54)) AND (v1>=83) OR (v1 BETWEEN 50 AND 97 AND v2<12 AND v3>23));`,
		ExpectedPlan: "Filter(((((((t2.v1 BETWEEN 94 AND 99) AND (t2.v2 > 4)) AND (t2.v3 < 94)) AND (t2.v4 <= 59)) OR ((t2.v1 = 19) AND (t2.v2 BETWEEN 47 AND 54))) AND (t2.v1 >= 83)) OR (((t2.v1 BETWEEN 50 AND 97) AND (t2.v2 < 12)) AND (t2.v3 > 23)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>19 AND v2>46 AND v3=26 AND v4>=47) OR (v1>18 AND v2<=79 AND v3=45 AND v4<=7)) OR (v1 BETWEEN 2 AND 21 AND v2>32));`,
		ExpectedPlan: "Filter((((((NOT((t2.v1 = 19))) AND (t2.v2 > 46)) AND (t2.v3 = 26)) AND (t2.v4 >= 47)) OR ((((t2.v1 > 18) AND (t2.v2 <= 79)) AND (t2.v3 = 45)) AND (t2.v4 <= 7))) OR ((t2.v1 BETWEEN 2 AND 21) AND (t2.v2 > 32)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (v1>=5) AND (v1=50 AND v2<=50);`,
		ExpectedPlan: "Filter((t2.v1 >= 5) AND (t2.v1 = 50))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>=82 AND v2 BETWEEN 34 AND 50 AND v3<26 AND v4 BETWEEN 48 AND 76) OR (v1<=6));`,
		ExpectedPlan: "Filter(((((t2.v1 >= 82) AND (t2.v2 BETWEEN 34 AND 50)) AND (t2.v3 < 26)) AND (t2.v4 BETWEEN 48 AND 76)) OR (t2.v1 <= 6))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE ((v1>29) OR (v1<>94 AND v2>=56 AND v3=14));`,
		ExpectedPlan: "Filter((t2.v1 > 29) OR (((NOT((t2.v1 = 94))) AND (t2.v2 >= 56)) AND (t2.v3 = 14)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1>8 AND v2<97 AND v3=51 AND v4<=26) OR (v1>87)) OR (v1<10 AND v2<=45 AND v3>=73));`,
		ExpectedPlan: "Filter((((((t2.v1 > 8) AND (t2.v2 < 97)) AND (t2.v3 = 51)) AND (t2.v4 <= 26)) OR (t2.v1 > 87)) OR (((t2.v1 < 10) AND (t2.v2 <= 45)) AND (t2.v3 >= 73)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
	{
		Query: `SELECT * FROM t2 WHERE (((v1<>15 AND v2>1) OR (v1<46)) OR (v1>47 AND v2>=9 AND v3 BETWEEN 39 AND 87 AND v4>=10));`,
		ExpectedPlan: "Filter((((NOT((t2.v1 = 15))) AND (t2.v2 > 1)) OR (t2.v1 < 46)) OR ((((t2.v1 > 47) AND (t2.v2 >= 9)) AND (t2.v3 BETWEEN 39 AND 87)) AND (t2.v4 >= 10)))\n" +
			" └─ Projected table access on [pk v1 v2 v3 v4]\n" +
			"     └─ IndexedTableAccess(t2 on [t2.v1,t2.v2,t2.v3,t2.v4])\n" +
			"",
	},
}
