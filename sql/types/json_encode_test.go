package types

import "testing"

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
