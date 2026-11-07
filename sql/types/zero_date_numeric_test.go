// Copyright 2026 BlueboxC
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

package types

import (
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestZeroDateNumericConversions(t *testing.T) {
	for _, typ := range []sql.DatetimeType{Date, Datetime, Timestamp} {
		f, err := typ.ToFloat64(ZeroTime)
		if err != nil || f != 0 {
			t.Fatalf("%s zero float: %v %v", typ, f, err)
		}
		d, err := typ.ToDecimal(ZeroTime)
		if err != nil || !d.IsZero() {
			t.Fatalf("%s zero decimal: %v %v", typ, d, err)
		}
	}
	ordinary := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	f, err := Datetime.ToFloat64(ordinary)
	if err != nil || f != 20200102030405 {
		t.Fatalf("ordinary datetime: %v %v", f, err)
	}
	if DateToNumber(ZeroTime) != 0 || DateTimeToNumber(ZeroTime) != 0 || DateToNumber(ordinary) != 20200102 {
		t.Fatal("date numeric control")
	}
}
