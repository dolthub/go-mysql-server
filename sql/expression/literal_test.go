// Copyright 2026 Dolthub, Inc.
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

package expression

import (
	"testing"
	"time"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestLiteralStringRoundTrips(t *testing.T) {
	tests := []struct {
		name     string
		literal  *Literal
		expected string
	}{
		{"binary string", NewLiteral("a\x00", types.MustCreateBinary(query.Type_VARBINARY, 2)), "0x6100"},
		{"date", NewLiteral(time.Date(2026, time.September, 4, 0, 0, 0, 0, time.UTC), types.Date), "'2026-09-04'"},
		{"datetime", NewLiteral(time.Date(2026, time.September, 4, 1, 2, 3, 456000000, time.UTC), types.DatetimeMaxPrecision), "'2026-09-04 01:02:03.456'"},
		{"time", NewLiteral(types.Timespan(45_296_123_456), types.Time), "'12:34:56.123456'"},
		{"json", NewLiteral(types.MustJSON(`{"a": 1}`), types.JSON), `CAST('{"a": 1}' AS JSON)`},
		{"geometry", NewLiteral(types.Point{SRID: 4326, X: 1, Y: 2}, types.PointType{}), "ST_GeomFromWKB(0x0101000000000000000000F03F0000000000000040, 4326)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, tt.literal.String())
			_, err := sql.NewMysqlParser().ParseSimple("SELECT " + tt.literal.String())
			require.NoError(t, err)
		})
	}
}
