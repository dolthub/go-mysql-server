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
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRowsToRowIterEmpty(t *testing.T) {
	require := require.New(t)

	ctx := NewEmptyContext()
	iter := RowsToRowIter()
	r, err := iter.Next(ctx)
	require.Equal(io.EOF, err)
	require.Nil(r)

	r, err = iter.Next(ctx)
	require.Equal(io.EOF, err)
	require.Nil(r)

	err = iter.Close(ctx)
	require.NoError(err)
}

func TestRowsToRowIter(t *testing.T) {
	require := require.New(t)

	ctx := NewEmptyContext()
	iter := RowsToRowIter(NewRow(1), NewRow(2), NewRow(3))
	r, err := iter.Next(ctx)
	require.NoError(err)
	require.Equal(NewRow(1), r)

	r, err = iter.Next(ctx)
	require.NoError(err)
	require.Equal(NewRow(2), r)

	r, err = iter.Next(ctx)
	require.NoError(err)
	require.Equal(NewRow(3), r)

	r, err = iter.Next(ctx)
	require.Equal(io.EOF, err)
	require.Nil(r)

	r, err = iter.Next(ctx)
	require.Equal(io.EOF, err)
	require.Nil(r)

	err = iter.Close(ctx)
	require.NoError(err)
}
