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

package types

import (
	"cmp"
	"math"
)

// compareNumbers is a utility function for comparing numbers of any built-in integer or float type.
// It avoids any precision loss whenever the inputs can be represented as valid int64, uint64, or float64.
func compareNumbers(t, u interface{}) int {
	return newNumber(t).compare(newNumber(u))
}

// number is an interface that represents any possible built-in number integer or float type, and can be compared
// to any built-in integer or float type.
type number interface {
	compare(other number) int
}

func newNumber(t interface{}) number {
	switch v := t.(type) {
	case uint:
		return uint64number(v)
	case uint8:
		return uint64number(v)
	case uint16:
		return uint64number(v)
	case uint32:
		return uint64number(v)
	case uint64:
		return uint64number(v)
	case int:
		return int64number(v)
	case int8:
		return int64number(v)
	case int16:
		return int64number(v)
	case int32:
		return int64number(v)
	case int64:
		return int64number(v)
	case float32:
		return float64number(v)
	case float64:
		return float64number(v)
	}
	panic("unreachable")
}

type int64number int64

func (j int64number) compare(other number) int {
	switch o := other.(type) {
	case int64number:
		return cmp.Compare(j, o)
	case uint64number:
		return compareIntToUint(j, o)
	case float64number:
		return compareIntToFloat(j, o)
	}
	panic("unreachable")
}

type uint64number uint64

func (j uint64number) compare(other number) int {
	switch o := other.(type) {
	case int64number:
		return compareUintToInt(j, o)
	case float64number:
		return compareUintToFloat(j, o)
	case uint64number:
		return cmp.Compare(j, o)
	}
	panic("unreachable")
}

type float64number float64

func (j float64number) compare(other number) int {
	switch o := other.(type) {
	case int64number:
		return compareFloatToInt(j, o)
	case uint64number:
		return compareFloatToUint(j, o)
	case float64number:
		return cmp.Compare(j, o)
	}
	panic("unreachable")
}

// compareIntToFloat compares an int64 to a float64 without unnecessary precision loss.
func compareIntToFloat(i int64number, f float64number) int {
	if float64(i) > float64(f) || int64(i) > int64(f) {
		return 1
	}
	if float64(i) < float64(f) || int64(i) < int64(f) {
		return -1
	}
	return 0
}

func compareFloatToInt(f float64number, i int64number) int {
	return -compareIntToFloat(i, f)
}

// compareUintToFloat compares a uint64 to a float64 without unnecessary precision loss.
func compareUintToFloat(u uint64number, f float64number) int {
	if f < 0 {
		return 1
	}
	if f > math.MaxUint64 {
		return -1
	}
	if float64(u) > float64(f) || uint64(u) > uint64(f) {
		return 1
	}
	if float64(u) < float64(f) || uint64(u) < uint64(f) {
		return -1
	}
	return 0
}

func compareFloatToUint(f float64number, u uint64number) int {
	return -compareUintToFloat(u, f)
}

func compareIntToUint(i int64number, u uint64number) int {
	if i < 0 {
		return -1
	}
	if u > math.MaxInt64 {
		return -1
	}
	if int64(i) > int64(u) {
		return 1
	}
	if int64(i) < int64(u) {
		return -1
	}
	return 0
}

func compareUintToInt(u uint64number, i int64number) int {
	return -compareIntToUint(i, u)
}
