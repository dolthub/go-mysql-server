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

package sql

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecimalAccuracy(t *testing.T) {
	t.Skip("This runs 821471 tests, which take quite a while. Re-run this if the max precision is ever updated.")
	precision := 65

	tests := []struct {
		scale     int
		intervals []string
	}{
		{1, []string{"1"}},
		{2, []string{"1"}},
		{3, []string{"1"}},
		{4, []string{"1", "2"}},
		{5, []string{"1", "7", "19"}},
		{6, []string{"1", "17", "173"}},
		{7, []string{"1", "13", "1381"}},
		{8, []string{"1", "127", "15139"}},
		{9, []string{"1", "167", "11311", "157733"}},
		{10, []string{"1", "193", "12119", "1299827"}},
		{11, []string{"1", "1409", "13597", "11566817"}},
		{12, []string{"1", "1462", "162736", "19973059", "153698913"}},
		{13, []string{"1", "17173", "164916", "12810490", "1106465924"}},
		{14, []string{"1", "14145", "1929683", "11237352", "12259001771"}},
		{15, []string{"1", "19702", "1826075", "197350780", "117062654737"}},
		{16, []string{"1", "154259", "1722308", "192079755", "1568355872155"}},
		{17, []string{"1", "199621", "17380400", "189789317", "18535814105416"}},
		{18, []string{"1", "164284", "19555364", "1343158899", "191285386028951"}},
		{19, []string{"1", "1370167", "14327353", "1613296706", "1786126145971438"}},
		{20, []string{"1", "1682382", "156896829", "1502199604", "15400467202762943"}},
		{21, []string{"1", "1908105", "132910114", "17668300548", "145914194398307528"}},
		{22, []string{"1", "11192652", "181987462", "13471431866", "1112655573846229769"}},
		{23, []string{"1", "19628451", "1498686974", "13119001111", "17583200755082903973"}},
		{24, []string{"1", "14855266", "1844358042", "140667369937", "138362583526008386641"}},
		{25, []string{"1", "132605238", "1459826257", "138157739511", "1456272936346618537992"}},
		{26, []string{"1", "178623779", "19310677332", "124692319379", "15924740334465525606269"}},
		{27, []string{"1", "136953077", "13506952725", "1383331590521", "137480986566749829385216"}},
		{28, []string{"1", "1838754847", "16879518108", "1840612305937", "1389868035366355336138689"}},
		{29, []string{"1", "1760427312", "169649694515", "1810557411178", "12907494895459213754558234"}},
		{30, []string{"1", "1823936104", "131352779146", "17050328377892", "146384189585475736836539491"}},
	}

	for _, test := range tests {
		decimalType := MustCreateDecimalType(uint8(precision), uint8(test.scale))
		decimalInt := big.NewInt(0)
		bigIntervals := make([]*big.Int, len(test.intervals))
		for i, interval := range test.intervals {
			bigInterval := new(big.Int)
			_ = bigInterval.UnmarshalText([]byte(interval))
			bigIntervals[i] = bigInterval
		}
		intervalIndex := 0
		baseStr := strings.Repeat("9", precision-test.scale) + "."
		upperBound := new(big.Int)
		_ = upperBound.UnmarshalText([]byte("1" + strings.Repeat("0", test.scale)))

		for decimalInt.Cmp(upperBound) == -1 {
			decimalStr := decimalInt.Text(10)
			fullDecimalStr := strings.Repeat("0", test.scale-len(decimalStr)) + decimalStr
			fullStr := baseStr + fullDecimalStr

			t.Run(fmt.Sprintf("Scale:%v DecVal:%v", test.scale, fullDecimalStr), func(t *testing.T) {
				res, err := decimalType.Convert(fullStr)
				require.NoError(t, err)
				require.Equal(t, fullStr, res.(decimal.Decimal).StringFixed(int32(decimalType.Scale())))
			})

			decimalInt.Add(decimalInt, bigIntervals[intervalIndex])
			intervalIndex = (intervalIndex + 1) % len(bigIntervals)
		}
	}
}

func TestDecimalCompare(t *testing.T) {
	tests := []struct {
		precision   uint8
		scale       uint8
		val1        interface{}
		val2        interface{}
		expectedCmp int
	}{
		{1, 0, nil, 0, 1},
		{1, 0, 0, nil, -1},
		{1, 0, nil, nil, 0},
		{1, 0, "-3.2", 2, -1},
		{1, 1, ".738193", .6948274, 1},
		{5, 0, 0, 1, -1},
		{5, 0, 0, "1", -1},
		{5, 0, "0.23e1", 3, -1},
		{5, 0, "46572e-2", big.NewInt(466), -1},
		{20, 10, "48204.23457e4", 93828432, 1},
		{20, 10, "-.0000000001", 0, -1},
		{20, 10, "-.00000000001", 0, -1},
		{65, 0, "99999999999999999999999999999999999999999999999999999999999999999",
			"99999999999999999999999999999999999999999999999999999999999999998", 1},
		{65, 30, "99999999999999999999999999999999999.999999999999999999999999999998",
			"99999999999999999999999999999999999.999999999999999999999999999999", -1},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.val1, test.val2), func(t *testing.T) {
			cmp, err := MustCreateDecimalType(test.precision, test.scale).Compare(test.val1, test.val2)
			require.NoError(t, err)
			assert.Equal(t, test.expectedCmp, cmp)
		})
	}
}

func TestDecimalCreate(t *testing.T) {
	tests := []struct {
		precision    uint8
		scale        uint8
		expectedType decimalType
		expectedErr  bool
	}{
		{0, 0, decimalType{decimal.New(1, 10), 10, 0}, false},
		{0, 1, decimalType{}, true},
		{0, 5, decimalType{}, true},
		{0, 10, decimalType{}, true},
		{0, 30, decimalType{}, true},
		{0, 65, decimalType{}, true},
		{0, 66, decimalType{}, true},
		{1, 0, decimalType{decimal.New(1, 1), 1, 0}, false},
		{1, 1, decimalType{decimal.New(1, 0), 1, 1}, false},
		{1, 5, decimalType{}, true},
		{1, 10, decimalType{}, true},
		{1, 30, decimalType{}, true},
		{1, 65, decimalType{}, true},
		{1, 66, decimalType{}, true},
		{5, 0, decimalType{decimal.New(1, 5), 5, 0}, false},
		{5, 1, decimalType{decimal.New(1, 4), 5, 1}, false},
		{5, 5, decimalType{decimal.New(1, 0), 5, 5}, false},
		{5, 10, decimalType{}, true},
		{5, 30, decimalType{}, true},
		{5, 65, decimalType{}, true},
		{5, 66, decimalType{}, true},
		{10, 0, decimalType{decimal.New(1, 10), 10, 0}, false},
		{10, 1, decimalType{decimal.New(1, 9), 10, 1}, false},
		{10, 5, decimalType{decimal.New(1, 5), 10, 5}, false},
		{10, 10, decimalType{decimal.New(1, 0), 10, 10}, false},
		{10, 30, decimalType{}, true},
		{10, 65, decimalType{}, true},
		{10, 66, decimalType{}, true},
		{30, 0, decimalType{decimal.New(1, 30), 30, 0}, false},
		{30, 1, decimalType{decimal.New(1, 29), 30, 1}, false},
		{30, 5, decimalType{decimal.New(1, 25), 30, 5}, false},
		{30, 10, decimalType{decimal.New(1, 20), 30, 10}, false},
		{30, 30, decimalType{decimal.New(1, 0), 30, 30}, false},
		{30, 65, decimalType{}, true},
		{30, 66, decimalType{}, true},
		{65, 0, decimalType{decimal.New(1, 65), 65, 0}, false},
		{65, 1, decimalType{decimal.New(1, 64), 65, 1}, false},
		{65, 5, decimalType{decimal.New(1, 60), 65, 5}, false},
		{65, 10, decimalType{decimal.New(1, 55), 65, 10}, false},
		{65, 30, decimalType{decimal.New(1, 35), 65, 30}, false},
		{65, 65, decimalType{}, true},
		{65, 66, decimalType{}, true},
		{66, 00, decimalType{}, true},
		{66, 01, decimalType{}, true},
		{66, 05, decimalType{}, true},
		{66, 10, decimalType{}, true},
		{66, 30, decimalType{}, true},
		{66, 65, decimalType{}, true},
		{66, 66, decimalType{}, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v", test.precision, test.scale), func(t *testing.T) {
			typ, err := CreateDecimalType(test.precision, test.scale)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedType, typ)
			}
		})
	}
}

func TestDecimalConvert(t *testing.T) {
	tests := []struct {
		precision   uint8
		scale       uint8
		val         interface{}
		expectedVal interface{}
		expectedErr bool
	}{
		{1, 0, nil, nil, false},
		{1, 0, byte(0), "0", false},
		{1, 0, int8(3), "3", false},
		{1, 0, "-3.7e0", "-4", false},
		{1, 0, uint(4), "4", false},
		{1, 0, int16(9), "9", false},
		{1, 0, "0.00000000000000000003e20", "3", false},
		{1, 0, float64(-9.4), "-9", false},
		{1, 0, float32(9.5), "", true},
		{1, 0, int32(-10), "", true},

		{1, 1, 0, "0.0", false},
		{1, 1, .01, "0.0", false},
		{1, 1, .1, "0.1", false},
		{1, 1, ".22", "0.2", false},
		{1, 1, .55, "0.6", false},
		{1, 1, "-.7863294659345624", "-0.8", false},
		{1, 1, "2634193746329327479.32030573792e-19", "0.3", false},
		{1, 1, 1, "", true},
		{1, 1, new(big.Rat).SetInt64(2), "", true},

		{5, 0, 0, "0", false},
		{5, 0, 5000.2, "5000", false},
		{5, 0, "7742", "7742", false},
		{5, 0, new(big.Float).SetFloat64(-4723.875), "-4724", false},
		{5, 0, 99999, "99999", false},
		{5, 0, "0xf8e1", "63713", false},
		{5, 0, "0b1001110101100110", "40294", false},
		{5, 0, new(big.Rat).SetFrac64(999999, 10), "", true},
		{5, 0, 673927, "", true},

		{10, 5, 0, "0.00000", false},
		{10, 5, "99999.999994", "99999.99999", false},
		{10, 5, "5.5729136e3", "5572.91360", false},
		{10, 5, "600e-2", "6.00000", false},
		{10, 5, new(big.Rat).SetFrac64(-22, 7), "-3.14286", false},
		{10, 5, 100000, "", true},
		{10, 5, "-99999.999995", "", true},

		{65, 0, "99999999999999999999999999999999999999999999999999999999999999999",
			"99999999999999999999999999999999999999999999999999999999999999999", false},
		{65, 0, "99999999999999999999999999999999999999999999999999999999999999999.1",
			"99999999999999999999999999999999999999999999999999999999999999999", false},
		{65, 0, "99999999999999999999999999999999999999999999999999999999999999999.99", "", true},

		{65, 12, "16976349273982359874209023948672021737840592720387475.2719128737543572927374503832837350563300243035038234972093785",
			"16976349273982359874209023948672021737840592720387475.271912873754", false},
		{65, 12, "99999999999999999999999999999999999999999999999999999.9999999999999", "", true},

		{20, 10, []byte{32}, nil, true},
		{20, 10, time.Date(2019, 12, 12, 12, 12, 12, 0, time.UTC), nil, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.precision, test.scale, test.val), func(t *testing.T) {
			typ := MustCreateDecimalType(test.precision, test.scale)
			val, err := typ.Convert(test.val)
			if test.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				if test.expectedVal == nil {
					assert.Nil(t, val)
				} else {
					expectedVal, err := decimal.NewFromString(test.expectedVal.(string))
					require.NoError(t, err)
					assert.True(t, expectedVal.Equal(val.(decimal.Decimal)))
					assert.Equal(t, typ.ValueType(), reflect.TypeOf(val))
				}
			}
		})
	}
}

func TestDecimalString(t *testing.T) {
	tests := []struct {
		precision   uint8
		scale       uint8
		expectedStr string
	}{
		{0, 0, "decimal(10,0)"},
		{1, 0, "decimal(1,0)"},
		{5, 0, "decimal(5,0)"},
		{10, 0, "decimal(10,0)"},
		{65, 0, "decimal(65,0)"},
		{1, 1, "decimal(1,1)"},
		{5, 1, "decimal(5,1)"},
		{10, 1, "decimal(10,1)"},
		{65, 1, "decimal(65,1)"},
		{5, 5, "decimal(5,5)"},
		{10, 5, "decimal(10,5)"},
		{65, 5, "decimal(65,5)"},
		{10, 10, "decimal(10,10)"},
		{65, 10, "decimal(65,10)"},
		{65, 30, "decimal(65,30)"},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v %v", test.precision, test.scale, test.expectedStr), func(t *testing.T) {
			str := MustCreateDecimalType(test.precision, test.scale).String()
			assert.Equal(t, test.expectedStr, str)
		})
	}
}

func TestDecimalZero(t *testing.T) {
	tests := []struct {
		precision uint8
		scale     uint8
	}{
		{0, 0},
		{1, 0},
		{5, 0},
		{10, 0},
		{65, 0},
		{1, 1},
		{5, 1},
		{10, 1},
		{65, 1},
		{5, 5},
		{10, 5},
		{65, 5},
		{10, 10},
		{65, 10},
		{65, 30},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v zero", test.precision, test.scale), func(t *testing.T) {
			dt := MustCreateDecimalType(test.precision, test.scale)
			_, ok := dt.Zero().(decimal.Decimal)
			assert.True(t, ok)
		})
	}
}
