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
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrayGenerator(t *testing.T) {
	require := require.New(t)

	expected := []interface{}{"a", "b", "c"}
	gen := NewArrayGenerator(expected)

	var values []interface{}
	for {
		v, err := gen.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(err)
		}
		values = append(values, v)
	}

	require.Equal(expected, values)
}

func TestToGenerator(t *testing.T) {
	require := require.New(t)

	gen, err := ToGenerator([]interface{}{1, 2, 3})
	require.NoError(err)
	require.Equal(NewArrayGenerator([]interface{}{1, 2, 3}), gen)

	gen, err = ToGenerator(new(fakeGen))
	require.NoError(err)
	require.Equal(new(fakeGen), gen)

	gen, err = ToGenerator(nil)
	require.NoError(err)
	require.Equal(NewArrayGenerator(nil), gen)

	_, err = ToGenerator("foo")
	require.Error(err)
}

type fakeGen struct{}

func (fakeGen) Next() (interface{}, error) { return nil, fmt.Errorf("not implemented") }
func (fakeGen) Close() error               { return nil }
