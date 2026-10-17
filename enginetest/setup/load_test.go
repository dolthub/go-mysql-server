// Copyright 2022 Dolthub, Inc.
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

package setup

import (
	"io/fs"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileSourceScanner(t *testing.T) {
	out, err := load("mydb")
	require.NoError(t, err)

	exp := SetupScript{
		"create database if not exists mydb",
		"use mydb",
	}
	require.Equal(t, exp, out)
}

// TestEveryScriptIsLoaded checks that every embedded script file is loaded by
// an exported variable in load.go. Go cannot enumerate package-level
// variables via reflection, so mustLoad records each script it loads instead.
func TestEveryScriptIsLoaded(t *testing.T) {
	entries, err := fs.ReadDir(dataFS, dataDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	for _, e := range entries {
		require.False(t, e.IsDir(), "unexpected directory %s/%s", dataDir, e.Name())
		_, ok := loadedScripts[e.Name()]
		require.True(t, ok, "%s/%s has no variable in load.go; add `%sData = mustLoad(%q)`",
			dataDir, e.Name(), exportedName(e.Name()), e.Name())
	}
}

func exportedName(name string) string {
	if name == "" {
		return name
	}
	return string(name[0]-'a'+'A') + name[1:]
}
