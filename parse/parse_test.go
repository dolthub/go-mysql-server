package parse

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSelectFieldList(t *testing.T) {
	cases := []struct {
		input  string
		err    bool
		typ    projectionType
		fields []*projectionField
	}{
		{
			"foo., bar",
			true,
			projectionFields,
			nil,
		},
		{
			",",
			true,
			projectionFields,
			nil,
		},
		{
			".",
			true,
			projectionFields,
			nil,
		},
		{
			".foo",
			true,
			projectionFields,
			nil,
		},
		{
			"foo, bar",
			false,
			projectionFields,
			[]*projectionField{
				&projectionField{"", "foo"},
				&projectionField{"", "bar"},
			},
		},
		{
			"foo.bar, bar",
			false,
			projectionFields,
			[]*projectionField{
				&projectionField{"foo", "bar"},
				&projectionField{"", "bar"},
			},
		},
	}

	require := require.New(t)
	for _, c := range cases {
		l := NewLexer(strings.NewReader(c.input + " "))
		require.Nil(l.Run())

		proj, err := parseSelectFieldList(l)
		if c.err {
			require.NotNil(err)
		} else {
			require.Nil(err)
			require.Equal(c.typ, proj.typ)
			require.Equal(len(c.fields), len(proj.fields))
			for i := range c.fields {
				require.Equal(c.fields[i].parent, proj.fields[i].parent)
				require.Equal(c.fields[i].name, proj.fields[i].name)
			}
		}
	}
}

func TestParseSelectFields(t *testing.T) {
	require := require.New(t)

	cases := []struct {
		input  string
		err    bool
		typ    projectionType
		fields []*projectionField
	}{
		{
			"*",
			false,
			projectionAll,
			nil,
		},
		{
			">=",
			true,
			projectionFields,
			nil,
		},
		{
			"foo, bar",
			false,
			projectionFields,
			[]*projectionField{
				&projectionField{"", "foo"},
				&projectionField{"", "bar"},
			},
		},
	}

	for _, c := range cases {
		l := NewLexer(strings.NewReader(c.input + " "))
		require.Nil(l.Run())

		proj, err := parseSelectFields(l)
		if c.err {
			require.NotNil(err)
		} else {
			require.Nil(err)
			require.Equal(c.typ, proj.typ)
			require.Equal(len(c.fields), len(proj.fields))
			for i := range c.fields {
				require.Equal(c.fields[i].parent, proj.fields[i].parent)
				require.Equal(c.fields[i].name, proj.fields[i].name)
			}
		}
	}
}
