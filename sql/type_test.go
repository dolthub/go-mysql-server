// Copyright 2021 Dolthub, Inc.
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

package sql

import (
	"fmt"
	"testing"

	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/assert"
)

func TestFloatCovert(t *testing.T) {
	tests := []struct {
		length   string
		scale    string
		expected Type
		err      bool
	}{
		{"53", "0", nil, true},
		{"-1", "", nil, true},
		{"54", "", nil, true},
		{"", "", Float32, false},
		{"0", "", Float32, false},
		{"24", "", Float32, false},
		{"25", "", Float64, false},
		{"53", "", Float64, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.length, test.scale, test.err), func(t *testing.T) {
			var precision *sqlparser.SQLVal = nil
			var scale *sqlparser.SQLVal = nil

			if test.length != "" {
				precision = &sqlparser.SQLVal{
					Type: sqlparser.IntVal,
					Val:  []byte(test.length),
				}
			}

			if test.scale != "" {
				scale = &sqlparser.SQLVal{
					Type: sqlparser.IntVal,
					Val:  []byte(test.scale),
				}
			}

			ct := &sqlparser.ColumnType{
				Type:   "FLOAT",
				Scale:  scale,
				Length: precision,
			}
			res, err := ColumnTypeToType(ct)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.Equal(t, test.expected, res)
			}
		})
	}
}
