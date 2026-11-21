// Copyright 2026 BlueboxC
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

package types

import (
	"reflect"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestBooleanYearEnumSet(t *testing.T) {
	ctx := sql.NewEmptyContext()
	for _, typ := range []sql.Type{Year, MustCreateEnumType([]string{"x", "y"}, sql.Collation_Default), MustCreateSetType([]string{"x", "y"}, sql.Collation_Default)} {
		for _, v := range []bool{false, true} {
			n := 0
			if v {
				n = 1
			}
			got, gr, ge := typ.Convert(ctx, v)
			want, wr, we := typ.Convert(ctx, n)
			if !reflect.DeepEqual(got, want) || gr != wr || (ge == nil) != (we == nil) {
				t.Fatalf("%s %t: %v/%v vs integer %v/%v", typ, v, got, ge, want, we)
			}
		}
		if _, _, err := typ.Convert(ctx, "true"); err == nil {
			t.Fatalf("%s accepted text true", typ)
		}
	}
}
