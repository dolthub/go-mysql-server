// Copyright 2023 Dolthub, Inc.
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

var ConvertTests = []struct {
	Field   string
	Op      string
	Operand string
	ExpCnt  int
}{
	{Field: "i8", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i8", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i8", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i8", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i8", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i8", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i8", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i8", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i8", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i8", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "i16", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i16", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i16", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i16", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i16", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i16", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i16", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i16", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i16", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i16", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "i32", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i32", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i32", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i32", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i32", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i32", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i32", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i32", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i32", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i32", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "i64", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i64", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i64", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i64", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i64", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i64", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i64", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "i64", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i64", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "i64", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "u8", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u8", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u8", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u8", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u8", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u8", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u8", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u8", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u8", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u8", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "u16", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u16", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u16", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u16", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u16", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u16", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u16", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u16", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u16", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u16", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "u32", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u32", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u32", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u32", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u32", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u32", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u32", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u32", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u32", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u32", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "u64", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u64", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u64", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u64", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u64", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u64", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u64", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "u64", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u64", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "u64", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "f32", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "f32", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f32", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "f32", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f32", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f32", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "f32", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "f32", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f32", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f32", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "f64", Op: "=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "f64", Op: "<=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f64", Op: ">=", Operand: "3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "f64", Op: "<>", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f64", Op: "!=", Operand: "3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f64", Op: "=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "f64", Op: "<=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 0},
	{Field: "f64", Op: ">=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f64", Op: "<>", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},
	{Field: "f64", Op: "!=", Operand: "-3720481604718463778705849469618542795", ExpCnt: 1},

	{Field: "i8", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "i8", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "i8", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "i8", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "i8", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "i16", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "i16", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "i16", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "i16", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "i16", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "i32", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "i32", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "i32", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "i32", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "i32", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "i64", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "i64", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "i64", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "i64", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "i64", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "u8", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "u8", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "u8", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "u8", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "u8", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "u16", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "u16", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "u16", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "u16", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "u16", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "u32", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "u32", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "u32", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "u32", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "u32", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "u64", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "u64", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "u64", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "u64", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "u64", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "f32", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "f32", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "f32", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "f32", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "f32", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "f64", Op: "=", Operand: "'string'", ExpCnt: 0},
	{Field: "f64", Op: "<=", Operand: "'string'", ExpCnt: 0},
	{Field: "f64", Op: ">=", Operand: "'string'", ExpCnt: 1},
	{Field: "f64", Op: "<>", Operand: "'string'", ExpCnt: 1},
	{Field: "f64", Op: "!=", Operand: "'string'", ExpCnt: 1},

	{Field: "i8", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "i8", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "i8", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "i8", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "i8", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "i16", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "i16", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "i16", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "i16", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "i16", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "i32", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "i32", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "i32", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "i32", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "i32", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "i64", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "i64", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "i64", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "i64", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "i64", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "u8", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "u8", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "u8", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "u8", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "u8", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "u16", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "u16", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "u16", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "u16", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "u16", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "u32", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "u32", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "u32", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "u32", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "u32", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "u64", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "u64", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "u64", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "u64", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "u64", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "f32", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "f32", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "f32", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "f32", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "f32", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},

	{Field: "f64", Op: "=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "f64", Op: "<=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 0},
	{Field: "f64", Op: ">=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "f64", Op: "<>", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
	{Field: "f64", Op: "!=", Operand: "STR_TO_DATE('21,5,2013','%d,%m,%Y');", ExpCnt: 1},
}
