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

package regex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func dummy(s string) (Matcher, Disposer, error) { return nil, nil, nil }

func getDefault() string {
	for _, n := range Engines() {
		if n == "oniguruma" {
			return n
		}
	}

	return "go"
}

func TestRegistration(t *testing.T) {
	require := require.New(t)

	engines := Engines()
	require.NotNil(engines)
	number := len(engines)

	defaultEngine := getDefault()
	require.Equal(defaultEngine, Default())

	err := Register("", dummy)
	require.Equal(true, ErrRegexNameEmpty.Is(err))
	engines = Engines()
	require.Len(engines, number)

	err = Register("nil", dummy)
	require.NoError(err)
	require.Len(Engines(), number+1)

	matcher, disposer, err := New("nil", "")
	require.NoError(err)
	require.Nil(matcher)
	require.Nil(disposer)
}

func TestDefault(t *testing.T) {
	require := require.New(t)

	def := getDefault()
	require.Equal(def, Default())

	SetDefault("default")
	require.Equal("default", Default())

	SetDefault("")
	require.Equal(def, Default())
}

func TestMatcher(t *testing.T) {
	for _, name := range Engines() {
		if name == "nil" {
			continue
		}

		t.Run(name, func(t *testing.T) {
			m, d, err := New(name, "a{3}")
			require.NoError(t, err)

			require.Equal(t, true, m.Match("ooaaaoo"))
			require.Equal(t, false, m.Match("ooaaoo"))

			d.Dispose()
		})
	}
}

func TestMatcherMultiPatterns(t *testing.T) {
	const (
		email = `[\w\.+-]+@[\w\.-]+\.[\w\.-]+`
		url   = `[\w]+://[^/\s?#]+[^\s?#]+(?:\?[^\s#]*)?(?:#[^\s]*)?`
		ip    = `(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9])`

		data = `mysql://root@255.255.255.255:3306`
	)

	for _, name := range Engines() {
		if name == "nil" {
			continue
		}

		t.Run(name, func(t *testing.T) {
			m, d, err := New(name, email)
			require.NoError(t, err)
			require.Equal(t, true, m.Match(data))
			d.Dispose()

			m, d, err = New(name, url)
			require.NoError(t, err)
			require.Equal(t, true, m.Match(data))
			d.Dispose()

			m, d, err = New(name, ip)
			require.NoError(t, err)
			require.Equal(t, true, m.Match(data))
			d.Dispose()
		})
	}
}
