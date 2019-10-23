package parse

import (
	"bufio"
	"strings"
	"testing"
	"bytes"

	"github.com/stretchr/testify/require"
)

func TestReadLetterOrPoint(t *testing.T) {
	testFixtures := []struct {
		string  string
		expectedBuffer string
		expectedRemaining string
	}{
		{ "asd.ASD.ñu",
			"asd.ASD.ñu",
			"",
		},
		{ "5anytext",
			"",
			"5anytext",
		},
		{ "",
			"",
			"",
		},
		{ "as df",
			"as",
			" df",
		},
		{ "a.s df",
			"a.s",
			" df",
		},
		{ "a.s-",
			"a.s",
			"-",
		},
	}

	for _, fixture := range testFixtures {
		reader := bufio.NewReader(strings.NewReader(fixture.string))
		var buffer bytes.Buffer

		for i := 0; i < len(fixture.string); i++ {
			readLetterOrPoint(reader, &buffer)
		}

		remaining, _ := reader.ReadString('\n')
		require.Equal(t, remaining, fixture.expectedRemaining)

		require.Equal(t, buffer.String(), fixture.expectedBuffer)
	}
}

func TestReadValidScopedIdentRune(t *testing.T) {
}

func TestReadScopedIdent(t *testing.T) {
}

func TestMaybe(t *testing.T) {
}

func TestMultiMaybe(t *testing.T) {
}

func TestMaybeList(t *testing.T) {
}
