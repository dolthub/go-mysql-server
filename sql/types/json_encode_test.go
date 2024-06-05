package types

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestMarshalToMySqlString(t *testing.T) {
	tests := []struct {
		name     string
		val      interface{}
		expected string
	}{
		{
			name: "simple",
			val: map[string]interface{}{
				"foo": "bar",
			},
			expected: `{"foo": "bar"}`,
		},
		{
			name: "nested",
			val: map[string]interface{}{
				"foo": map[string]interface{}{
					"bar": "baz",
				},
			},
			expected: `{"foo": {"bar": "baz"}}`,
		},
		{
			name:     "array",
			val:      []interface{}{"foo", "bar", "baz"},
			expected: `["foo", "bar", "baz"]`,
		},
		{
			name: "array of maps",
			val: []interface{}{
				map[string]interface{}{
					"foo": "bar",
				},
				map[string]interface{}{
					"baz": "qux",
				},
				map[string]interface{}{
					"str":     "str",
					"float64": 1.0,
					"float32": float32(1.0),
					"int64":   int64(-1),
					"int32":   int32(-1),
					"int16":   int16(-1),
					"int8":    int8(-1),
					"int":     -1,
					"uint64":  uint64(1),
					"uint32":  uint32(1),
					"uint16":  uint16(1),
					"uint8":   uint8(1),
					"bool":    true,
				},
			},
			expected: `[{"foo": "bar"}, {"baz": "qux"}, {"int": -1, "str": "str", "bool": true, "int8": -1, "int16": -1, "int32": -1, "int64": -1, "uint8": 1, "uint16": 1, "uint32": 1, "uint64": 1, "float32": 1, "float64": 1}]`,
		},
		{
			name: "map of strings",
			val: map[string]string{
				"foo": "bar",
				"baz": "qux",
			},
			expected: `{"baz": "qux", "foo": "bar"}`,
		},
		{
			name: "map of timestamps",
			val: map[string]interface{}{
				"a": time.Date(2023, 1, 2, 3, 4, 5, 6, time.UTC),
				"b": time.Date(2023, 6, 5, 4, 3, 2, 1, time.UTC),
			},
			expected: `{"a": "2023-01-02T03:04:05Z", "b": "2023-06-05T04:03:02Z"}`,
		},
		{
			name: "string formatting",
			val: []string{
				`simple`,
				`With "quotes"`,
				`With "quotes" not at the end`,
				`with 'single quotes'`,
				`with
newlines
`,
				`with \n escaped newline`,
				`with	`,
			},
			expected: `["simple", "With \"quotes\"", "With \"quotes\" not at the end", "with 'single quotes'", "with\nnewlines\n", "with \\n escaped newline", "with\t"]`,
		},
		{
			name: "complicated string",
			val: []string{
				`{
	"nested": "json",
	"nested_escapedQuotes": "here \"you\" go"
}`},
			expected: `["{\n\t\"nested\": \"json\",\n\t\"nested_escapedQuotes\": \"here \\\"you\\\" go\"\n}"]`,
		},
		{
			name:     "decimal",
			val:      decimal.New(123, -2),
			expected: "1.23",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := marshalToMySqlString(test.val)
			if err != nil {
				t.Fatal(err)
			}

			if actual != test.expected {
				t.Errorf("expected %s, got %s", test.expected, actual)
			}
		})
	}
}
