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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

const versionPostfix = "test"

func TestNewVersion(t *testing.T) {
	require := require.New(t)

	ctx := sql.NewEmptyContext()
	f, _ := NewVersion(versionPostfix)(ctx)

	val, err := f.Eval(ctx, nil)
	require.NoError(err)
	require.Equal("8.0.11-"+versionPostfix, val)

	f, err = NewVersion("")(ctx)
	require.NoError(err)

	val, err = f.Eval(ctx, nil)
	require.NoError(err)
	require.Equal("8.0.11", val)
}
