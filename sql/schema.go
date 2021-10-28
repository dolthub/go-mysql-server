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

package sql

import (
	"reflect"
	"strings"

	"gopkg.in/src-d/go-errors.v1"
)

var (
	// ErrUnexpectedType is thrown when a received type is not the expected
	ErrUnexpectedType = errors.NewKind("value at %d has unexpected type: %s")
)

// Schema is the definition of a table.
type Schema []*Column

// CheckRow checks the row conforms to the schema.
func (s Schema) CheckRow(row Row) error {
	expected := len(s)
	got := len(row)
	if expected != got {
		return ErrUnexpectedRowLength.New(expected, got)
	}

	for idx, f := range s {
		v := row[idx]
		if f.Check(v) {
			continue
		}

		typ := reflect.TypeOf(v).String()
		return ErrUnexpectedType.New(idx, typ)
	}

	return nil
}

// Contains returns whether the schema contains a column with the given name.
func (s Schema) Contains(column string, source string) bool {
	return s.IndexOf(column, source) >= 0
}

// IndexOf returns the index of the given column in the schema or -1 if it's
// not present.
func (s Schema) IndexOf(column, source string) int {
	column = strings.ToLower(column)
	source = strings.ToLower(source)
	for i, col := range s {
		if strings.ToLower(col.Name) == column && strings.ToLower(col.Source) == source {
			return i
		}
	}
	return -1
}

// Equals checks whether the given schema is equal to this one.
func (s Schema) Equals(s2 Schema) bool {
	if len(s) != len(s2) {
		return false
	}

	for i := range s {
		if !s[i].Equals(s2[i]) {
			return false
		}
	}

	return true
}

// HasAutoIncrement returns true if the schema has an auto increment column.
func (s Schema) HasAutoIncrement() bool {
	for _, c := range s {
		if c.AutoIncrement {
			return true
		}
	}

	return false
}

func IsKeyless(s Schema) bool {
	for _, c := range s {
		if c.PrimaryKey {
			return false
		}
	}

	return true
}
