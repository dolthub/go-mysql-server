package types

import (
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
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
				"a": time.Date(2023, 1, 2, 3, 4, 5, 6000, time.UTC),
				"b": time.Date(2023, 6, 5, 4, 3, 2, 10000, time.UTC),
			},
			expected: `{"a": "2023-01-02 03:04:05.000006", "b": "2023-06-05 04:03:02.000010"}`,
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
			val:      apd.New(123, -2),
			expected: "1.23",
		},
		{
			name: "formatted key strings",
			val: map[string]interface{}{
				"baz\n\\n": "qux",
				"foo\"":    "bar\t",
			},
			expected: `{"foo\"": "bar\t", "baz\n\\n": "qux"}`,
		},
		{
			// See https://dev.mysql.com/doc/refman/8.4/en/json.html
			name:     "control characters",
			val:      []string{"\x00\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x1f\x7f"},
			expected: "[\"\\u0000\\u0007\\b\\t\\n\\u000b\\f\\r\\u000e\\u001f\x7f\"]",
		},
		{
			name:     "map of strings with control characters",
			val:      map[string]string{"a\x0bb": "c\x0bd"},
			expected: "{\"a\\u000bb\": \"c\\u000bd\"}",
		},
		{
			name:     "multibyte utf8 passes through",
			val:      []string{"日本語"},
			expected: `["日本語"]`,
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
