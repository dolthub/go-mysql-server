package regex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func dummy(s string) (Matcher, error) { return nil, nil }

func getDefault() string {
	for _, n := range Engines() {
		if n == "oniguruma" {
			return "oniguruma"
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

	err = Register("go", dummy)
	require.Equal(true, ErrRegexAlreadyRegistered.Is(err))

	err = Register("nil", dummy)
	require.NoError(err)
	require.Len(Engines(), number+1)

	matcher, err := New("nil", "")
	require.NoError(err)
	require.Nil(matcher)
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
			m, err := New(name, "a{3}")
			require.NoError(t, err)

			require.Equal(t, true, m.Match("ooaaaoo"))
			require.Equal(t, false, m.Match("ooaaoo"))
		})
	}
}
