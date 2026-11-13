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

package aggregation

import (
	"context"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type countDistinctExtendedValue struct {
	serialized byte
}

func (v countDistinctExtendedValue) String() string {
	return "same display value"
}

type countDistinctExtendedType struct {
	sql.FakeExtendedType
}

func (t countDistinctExtendedType) SerializeValue(_ context.Context, value any) ([]byte, error) {
	return []byte{value.(countDistinctExtendedValue).serialized}, nil
}

// hashableExtendedValue is a number of minutes whose serialized form differs for equal values, e.g. {1, 0} and {0, 60}.
type hashableExtendedValue struct {
	hours int
	mins  int
}

func (v hashableExtendedValue) WriteHashKey(_ *sql.Context, w io.Writer) error {
	_, err := w.Write([]byte(strconv.Itoa(v.hours*60 + v.mins)))
	return err
}

type hashableExtendedType struct {
	sql.FakeExtendedType
}

func (t hashableExtendedType) SerializeValue(_ context.Context, value any) ([]byte, error) {
	var serialized []byte
	switch v := value.(type) {
	case hashableExtendedValue:
		serialized = []byte{byte(v.hours), byte(v.mins)}
	case []hashableExtendedValue:
		for _, elem := range v {
			serialized = append(serialized, byte(elem.hours), byte(elem.mins))
		}
	}
	return serialized, nil
}

func TestCountEval1(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewLiteral(1, types.Int32))
	b, _ := c.NewBuffer(ctx)
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, nil))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow(1)))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), evalBuffer(t, b))
}

func TestCountEvalStar(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewStar())
	b, _ := c.NewBuffer(ctx)
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, nil))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow(1)))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow(1, 2, 3)))
	require.Equal(int64(5), evalBuffer(t, b))
}

func TestCountEvalString(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCount(expression.NewGetField(0, types.Text, "", true))
	b, _ := c.NewBuffer(ctx)
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.Equal(int64(1), evalBuffer(t, b))

	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.Equal(int64(1), evalBuffer(t, b))
}

func TestCountDistinctEval1(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewLiteral(1, types.Int32))
	b, _ := c.NewBuffer(ctx)
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, nil))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow(1)))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow(1, 2, 3)))
	require.Equal(int64(1), evalBuffer(t, b))
}

func TestCountDistinctEvalStar(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewStar())
	b, _ := c.NewBuffer(ctx)
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, nil))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow(1)))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow(1, 2, 3)))
	require.Equal(int64(4), evalBuffer(t, b))
}

func TestCountDistinctEvalString(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	c := NewCountDistinct(expression.NewGetField(0, types.Text, "", true))
	b, _ := c.NewBuffer(ctx)
	require.Equal(int64(0), evalBuffer(t, b))

	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.Equal(int64(1), evalBuffer(t, b))

	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.NoError(b.Update(ctx, sql.NewRow("foo")))
	require.NoError(b.Update(ctx, sql.NewRow("bar")))
	require.Equal(int64(2), evalBuffer(t, b))
}

func TestCountDistinctEvalExtendedType(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	extendedType := countDistinctExtendedType{sql.FakeExtendedType{
		Name:    "count_distinct_extended_type",
		ZeroVal: countDistinctExtendedValue{},
	}}
	c := NewCountDistinct(expression.NewGetField(0, extendedType, "", true))
	b, _ := c.NewBuffer(ctx)
	require.NoError(b.Update(ctx, sql.NewRow(countDistinctExtendedValue{serialized: 1})))
	require.NoError(b.Update(ctx, sql.NewRow(countDistinctExtendedValue{serialized: 1})))
	require.NoError(b.Update(ctx, sql.NewRow(countDistinctExtendedValue{serialized: 2})))
	require.NoError(b.Update(ctx, sql.NewRow(nil)))
	require.Equal(int64(2), evalBuffer(t, b))
}

func TestCountDistinctEvalHashableExtendedType(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	extendedType := hashableExtendedType{sql.FakeExtendedType{
		Name:    "hashable_extended_type",
		ZeroVal: hashableExtendedValue{},
	}}
	c := NewCountDistinct(expression.NewGetField(0, extendedType, "", true))
	b, _ := c.NewBuffer(ctx)
	require.NoError(b.Update(ctx, sql.NewRow(hashableExtendedValue{hours: 1})))
	require.NoError(b.Update(ctx, sql.NewRow(hashableExtendedValue{mins: 60})))
	require.NoError(b.Update(ctx, sql.NewRow(hashableExtendedValue{mins: 61})))
	require.Equal(int64(2), evalBuffer(t, b))
}

func TestCountDistinctEvalHashableExtendedTypeSlice(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	extendedType := hashableExtendedType{sql.FakeExtendedType{
		Name:    "hashable_extended_type_slice",
		ZeroVal: []hashableExtendedValue{},
	}}
	c := NewCountDistinct(expression.NewGetField(0, extendedType, "", true))
	b, _ := c.NewBuffer(ctx)
	require.NoError(b.Update(ctx, sql.NewRow([]hashableExtendedValue{{hours: 1}, {mins: 5}})))
	require.NoError(b.Update(ctx, sql.NewRow([]hashableExtendedValue{{mins: 60}, {mins: 5}})))
	require.NoError(b.Update(ctx, sql.NewRow([]hashableExtendedValue{{mins: 60}, {mins: 6}})))
	require.Equal(int64(2), evalBuffer(t, b))
}

func TestCountDistinctEvalTimesInDifferentLocations(t *testing.T) {
	require := require.New(t)
	ctx := sql.NewEmptyContext()

	utc := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	plusOne := time.Date(2024, 1, 1, 1, 0, 0, 0, time.FixedZone("", 3600))
	later := time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC)
	c := NewCountDistinct(expression.NewGetField(0, types.DatetimeMaxPrecision, "", true))
	b, _ := c.NewBuffer(ctx)
	require.NoError(b.Update(ctx, sql.NewRow(utc)))
	require.NoError(b.Update(ctx, sql.NewRow(plusOne)))
	require.NoError(b.Update(ctx, sql.NewRow(later)))
	require.Equal(int64(2), evalBuffer(t, b))
}
