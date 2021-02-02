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

package parse

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests that readLetterOrPoint reads only letters and points, not consuming
// the reader when the rune is not of those kinds
func TestReadLetterOrPoint(t *testing.T) {
	require := require.New(t)

	testFixtures := []struct {
		string            string
		expectedBuffer    string
		expectedRemaining string
	}{
		{
			"asd.ASD.ñu",
			"asd.ASD.ñu",
			"",
		},
		{
			"5anytext",
			"",
			"5anytext",
		},
		{
			"",
			"",
			"",
		},
		{
			"as df",
			"as",
			" df",
		},
		{
			"a.s df",
			"a.s",
			" df",
		},
		{
			"a.s-",
			"a.s",
			"-",
		},
		{
			"a.s_",
			"a.s",
			"_",
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.string))
		var buffer bytes.Buffer

		for i := 0; i < len(fixture.string); i++ {
			err := readLetterOrPoint(reader, &buffer)
			require.NoError(err)
		}

		remaining, _ := reader.ReadString('\n')
		require.Equal(remaining, fixture.expectedRemaining)

		require.Equal(buffer.String(), fixture.expectedBuffer)
	}
}

// Tests that readValidScopedIdentRune reads a single rune that is either part
// of an identifier or the specified separator. It checks that the function
// does not consume the reader when it encounters any other rune
func TestReadValidScopedIdentRune(t *testing.T) {
	require := require.New(t)

	testFixtures := []struct {
		string            string
		separator         rune
		expectedBuffer    string
		expectedRemaining string
		expectedError     bool
	}{
		{
			"ident_1.ident_2",
			'.',
			"ident_1.ident_2",
			"",
			false,
		},
		{
			"$ident_1.ident_2",
			'.',
			"",
			"$ident_1.ident_2",
			true,
		},
		{
			"",
			'.',
			"",
			"",
			false,
		},
		{
			"ident_1 ident_2",
			'.',
			"ident_1",
			" ident_2",
			true,
		},
		{
			"ident_1 ident_2",
			' ',
			"ident_1 ident_2",
			"",
			false,
		},
		{
			"ident_1.ident_2 ident_3",
			'.',
			"ident_1.ident_2",
			" ident_3",
			true,
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.string))
		var buffer bytes.Buffer

		var rune rune
		var err error
		for i := 0; i < len(fixture.string); i++ {
			if rune, err = readValidScopedIdentRune(reader, fixture.separator); err != nil {
				break
			}

			buffer.WriteRune(rune)
		}
		if fixture.expectedError {
			require.Error(err)
		} else {
			require.NoError(err)
		}

		remaining, _ := reader.ReadString('\n')
		require.Equal(remaining, fixture.expectedRemaining)

		require.Equal(fixture.expectedBuffer, buffer.String())
	}
}

// Tests that readIdentList reads a list of identifiers separated by a user-
// specified rune, populating the passed slice with the identifiers found.
func TestReadIdentList(t *testing.T) {
	require := require.New(t)

	testFixtures := []struct {
		string            string
		separator         rune
		expectedIdents    []string
		expectedRemaining string
	}{
		{
			"ident_1.ident_2",
			'.',
			[]string{"ident_1", "ident_2"},
			"",
		},
		{
			"$ident_1.ident_2",
			'.',
			nil,
			"$ident_1.ident_2",
		},
		{
			"",
			'.',
			nil,
			"",
		},
		{
			"ident_1 ident_2",
			'.',
			[]string{"ident_1"},
			" ident_2",
		},
		{
			"ident_1 ident_2",
			' ',
			[]string{"ident_1", "ident_2"},
			"",
		},
		{
			"ident_1.ident_2 ident_3",
			'.',
			[]string{"ident_1", "ident_2"},
			" ident_3",
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.string))
		var actualIdents []string

		err := readIdentList(fixture.separator, &actualIdents)(reader)
		require.NoError(err)

		remaining, _ := reader.ReadString('\n')
		require.Equal(fixture.expectedRemaining, remaining)

		require.Equal(fixture.expectedIdents, actualIdents)
	}
}

// Tests that maybe reads, and consumes, the specified string, if and only if
// it is there, reporting the result in the boolean passed.
func TestMaybe(t *testing.T) {
	require := require.New(t)

	testFixtures := []struct {
		input             string
		maybeString       string
		expectedMatched   bool
		expectedRemaining string
	}{
		{
			"ident_1.ident_2",
			"ident_1",
			true,
			".ident_2",
		},
		{
			"ident_1.ident_2",
			"random",
			false,
			"ident_1.ident_2",
		},
		{
			"ident_1.ident_2",
			"ident_1.ident_2",
			true,
			"",
		},
		{
			"ident_1",
			"ident_1butlonger",
			false,
			"ident_1",
		},
		{
			"ident_1",
			"",
			true,
			"ident_1",
		},
		{
			"",
			"",
			true,
			"",
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.input))
		var actualMatched bool

		err := maybe(&actualMatched, fixture.maybeString)(reader)
		require.NoError(err)

		remaining, _ := reader.ReadString('\n')
		require.Equal(fixture.expectedRemaining, remaining)

		require.Equal(fixture.expectedMatched, actualMatched)
	}
}

// Tests that multiMaybe reads, and consumes, the list of strings passed if all
// of them are in the reader, reporting the result in the boolean passed.
func TestMultiMaybe(t *testing.T) {
	require := require.New(t)

	testFixtures := []struct {
		input             string
		maybeStrings      []string
		expectedMatched   bool
		expectedRemaining string
	}{
		{
			"unodostres",
			[]string{"uno", "dos", "tres"},
			true,
			"",
		},
		{
			"uno dos tres",
			[]string{"uno", "dos", "tres"},
			true,
			"",
		},
		{
			"uno      dos tres",
			[]string{"uno", "dos", "tres"},
			true,
			"",
		},
		{
			"uno dos tres",
			[]string{"random"},
			false,
			"uno dos tres",
		},
		{
			"uno dos tres",
			[]string{"uno", "random"},
			false,
			"uno dos tres",
		},
		{
			"uno dos tres",
			[]string{"uno", "dos", "tres", "cuatro"},
			false,
			"uno dos tres",
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.input))
		var actualMatched bool

		err := multiMaybe(&actualMatched, fixture.maybeStrings...)(reader)
		require.NoError(err)

		remaining, _ := reader.ReadString('\n')
		require.Equal(fixture.expectedRemaining, remaining)

		require.Equal(fixture.expectedMatched, actualMatched)
	}
}

// Tests that maybeList reads the specified list of strings separated by the
// user-specified separator, not consuming the reader if the opening rune is
// not found. It checks that the function populates the list with the found
// strings even if there is an error in the middle of the parsing.
func TestMaybeList(t *testing.T) {
	require := require.New(t)

	testFixtures := []struct {
		stringWithList string
		openingRune    rune
		separatorRune  rune
		closingRune    rune
		expectedList   []string
		expectedError  bool
	}{
		{
			"(uno, dos, tres)",
			'(', ',', ')',
			[]string{"uno", "dos", "tres"},
			false,
		},
		{
			"-uno&dos & tres-",
			'-', '&', '-',
			[]string{"uno", "dos", "tres"},
			false,
		},
		{
			"-(uno, dos, tres)",
			'(', ',', ')',
			nil,
			false,
		},
		{
			"(uno, dos,( tres)",
			'(', ',', ')',
			[]string{"uno", "dos"},
			true,
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.stringWithList))
		var actualList []string

		err := maybeList(fixture.openingRune, fixture.separatorRune, fixture.closingRune, &actualList)(reader)

		if fixture.expectedError {
			require.Error(err)
		} else {
			require.NoError(err)
		}

		require.Equal(fixture.expectedList, actualList)
	}
}

// Tests that readSpaces consumes all the spaces it ecounters in the reader,
// reporting the number of spaces read to the user through the integer passed.
func TestReadSpaces(t *testing.T) {
	require := require.New(t)

	testFixtures := []struct {
		stringWithSpaces  string
		runesBeforeSpaces int
		expectedNumSpaces int
		expectedRemaining string
	}{
		{
			"one",
			3, 0,
			"",
		},
		{
			"two",
			0, 0,
			"two",
		},
		{
			"   three",
			0, 3,
			"three",
		},
		{
			"four    four ",
			4, 4,
			"four ",
		},
		{
			"five     ",
			4, 5,
			"",
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.stringWithSpaces))
		var actualNumSpaces int

		// Check that readSpaces does not read spaces when there are none
		if fixture.runesBeforeSpaces > 0 {
			err := readSpaces(reader, &actualNumSpaces)
			require.NoError(err)
			require.Equal(0, actualNumSpaces)
		}

		// Read all the runes before the spaces
		for i := 0; i < fixture.runesBeforeSpaces; i++ {
			_, _, err := reader.ReadRune()
			require.NoError(err)
		}

		// Read all the spaces
		err := readSpaces(reader, &actualNumSpaces)
		require.NoError(err)
		require.Equal(fixture.expectedNumSpaces, actualNumSpaces)

		actualRemaining, _ := reader.ReadString('\n')
		require.Equal(fixture.expectedRemaining, actualRemaining)
	}
}

// Tests that readQualifiedIdentifierList correctly parses well-formed lists,
// populating the list of identifiers, and that it errors with partial lists
// and when it does not found any identifiers
func TestReadQualifiedIdentifierList(t *testing.T) {
	require := require.New(t)

	testFixtures := []struct {
		string            string
		expectedList      []qualifiedName
		expectedError     bool
		expectedRemaining string
	}{
		{
			"my_db.myview, db_2.mytable ,   aTable",
			[]qualifiedName{{"my_db", "myview"}, {"db_2", "mytable"}, {"", "aTable"}},
			false,
			"",
		},
		{
			"single_identifier -remaining",
			[]qualifiedName{{"", "single_identifier"}},
			false,
			"-remaining",
		},
		{
			"",
			nil,
			true,
			"",
		},
		{
			"partial_list,",
			[]qualifiedName{{"", "partial_list"}},
			true,
			"",
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.string))
		var actualList []qualifiedName

		err := readQualifiedIdentifierList(&actualList)(reader)

		if fixture.expectedError {
			require.Error(err)
		} else {
			require.NoError(err)
		}

		require.Equal(fixture.expectedList, actualList)

		actualRemaining, _ := reader.ReadString('\n')
		require.Equal(fixture.expectedRemaining, actualRemaining)
	}
}
