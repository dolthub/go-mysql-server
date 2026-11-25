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

package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestMemberPositions_IsIdentity(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12013
	from := []string{"a", "b"}
	tests := []struct {
		name     string
		to       []string
		expected bool
	}{
		{name: "same members", to: []string{"a", "b"}, expected: true},
		{name: "member added at the end", to: []string{"a", "b", "c"}, expected: true},
		{name: "members reordered", to: []string{"b", "a"}, expected: false},
		{name: "member added at the start", to: []string{"c", "a", "b"}, expected: false},
		{name: "member removed", to: []string{"a"}, expected: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, newMemberPositions(from, test.to).IsIdentity())
		})
	}
}

func TestMemberType_MapMembers(t *testing.T) {
	// https://github.com/dolthub/dolt/issues/12013
	fromEnum := MustCreateEnumType([]string{"a", "b", "1"}, sql.Collation_Default).(sql.MemberType)
	fromSet := MustCreateSetType([]string{"a", "b"}, sql.Collation_Default).(sql.MemberType)
	tests := []struct {
		name     string
		from     sql.MemberType
		to       sql.Type
		stored   interface{}
		expected interface{}
		ok       bool
	}{
		{name: "reordered enum members keep their names", from: fromEnum, to: MustCreateEnumType([]string{"b", "a", "1"}, sql.Collation_Default), stored: uint16(1), expected: uint16(2), ok: true},
		{name: "empty enum value keeps index zero", from: fromEnum, to: MustCreateEnumType([]string{"b", "a"}, sql.Collation_Default), stored: uint16(0), expected: uint16(0), ok: true},
		{name: "removed enum member has no index", from: fromEnum, to: MustCreateEnumType([]string{"b"}, sql.Collation_Default), stored: uint16(1), expected: nil, ok: false},
		{name: "numeric enum member name is matched by name", from: fromEnum, to: MustCreateEnumType([]string{"a", "b"}, sql.Collation_Default), stored: uint16(3), expected: nil, ok: false},
		{name: "reordered set members keep their names", from: fromSet, to: MustCreateSetType([]string{"b", "a"}, sql.Collation_Default), stored: uint64(1), expected: uint64(2), ok: true},
		{name: "several set members move together", from: fromSet, to: MustCreateSetType([]string{"c", "b", "a"}, sql.Collation_Default), stored: uint64(3), expected: uint64(6), ok: true},
		{name: "empty set stays empty", from: fromSet, to: MustCreateSetType([]string{"b", "a"}, sql.Collation_Default), stored: uint64(0), expected: uint64(0), ok: true},
		{name: "removed set member has no bit", from: fromSet, to: MustCreateSetType([]string{"b"}, sql.Collation_Default), stored: uint64(1), expected: nil, ok: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			m, ok := test.from.MapMembers(test.to)
			require.True(t, ok)
			remapped, ok := m.Remap(test.stored)
			assert.Equal(t, test.ok, ok)
			assert.Equal(t, test.expected, remapped)
		})
	}
	t.Run("enum has no map to a set", func(t *testing.T) {
		_, ok := fromEnum.MapMembers(fromSet)
		assert.False(t, ok)
	})
	t.Run("set has no map to an enum", func(t *testing.T) {
		_, ok := fromSet.MapMembers(fromEnum)
		assert.False(t, ok)
	})
}
