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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalcDaynr(t *testing.T) {
	require.EqualValues(t, calcDaynr(0, 0, 0), 0)
	require.EqualValues(t, calcDaynr(9999, 12, 31), 3652424)
	require.EqualValues(t, calcDaynr(1970, 1, 1), 719528)
	require.EqualValues(t, calcDaynr(2006, 12, 16), 733026)
	require.EqualValues(t, calcDaynr(10, 1, 2), 3654)
	require.EqualValues(t, calcDaynr(2008, 2, 20), 733457)
}

func TestCalcWeek(t *testing.T) {
	_, w := calcWeek(2008, 2, 20, weekMode(0))

	_, w = calcWeek(2008, 2, 20, weekMode(1))
	require.EqualValues(t, w, 8)

	_, w = calcWeek(2008, 12, 31, weekMode(1))
	require.EqualValues(t, w, 53)
}
