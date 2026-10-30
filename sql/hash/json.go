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

package hash

import (
	"context"
	"encoding/binary"
	"math"
	"slices"
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/cockroachdb/apd/v3"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// writeJSONHash hashes JSON structure using numeric equality rather than its serialized representation.
// Values of different numeric kinds compare equal when they represent the same number.
func writeJSONHash(ctx context.Context, digest *xxhash.Digest, value any) error {
	var buf [64]byte
	switch value := value.(type) {
	case sql.JSONWrapper:
		unwrapped, err := value.ToInterface(ctx)
		if err != nil {
			return err
		}
		return writeJSONHash(ctx, digest, unwrapped)
	case nil:
		digest.WriteString("null")
	case bool:
		if value {
			digest.WriteString("true")
		} else {
			digest.WriteString("false")
		}
	case string:
		digest.WriteString("s")
		writeJSONHashLength(digest, len(value))
		digest.WriteString(value)
	case types.JsonArray:
		digest.WriteString("a")
		writeJSONHashLength(digest, len(value))
		for _, element := range value {
			if err := writeJSONHash(ctx, digest, element); err != nil {
				return err
			}
		}
	case types.JsonObject:
		digest.WriteString("o")
		writeJSONHashLength(digest, len(value))
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		for _, key := range keys {
			writeJSONHashLength(digest, len(key))
			digest.WriteString(key)
			if err := writeJSONHash(ctx, digest, value[key]); err != nil {
				return err
			}
		}
	case int:
		writeJSONHashNumber(digest, strconv.AppendInt(buf[:0], int64(value), 10))
	case int8:
		writeJSONHashNumber(digest, strconv.AppendInt(buf[:0], int64(value), 10))
	case int16:
		writeJSONHashNumber(digest, strconv.AppendInt(buf[:0], int64(value), 10))
	case int32:
		writeJSONHashNumber(digest, strconv.AppendInt(buf[:0], int64(value), 10))
	case int64:
		writeJSONHashNumber(digest, strconv.AppendInt(buf[:0], value, 10))
	case uint:
		writeJSONHashNumber(digest, strconv.AppendUint(buf[:0], uint64(value), 10))
	case uint8:
		writeJSONHashNumber(digest, strconv.AppendUint(buf[:0], uint64(value), 10))
	case uint16:
		writeJSONHashNumber(digest, strconv.AppendUint(buf[:0], uint64(value), 10))
	case uint32:
		writeJSONHashNumber(digest, strconv.AppendUint(buf[:0], uint64(value), 10))
	case uint64:
		writeJSONHashNumber(digest, strconv.AppendUint(buf[:0], value, 10))
	case float32:
		writeJSONHashFloat(digest, float64(value))
	case float64:
		writeJSONHashFloat(digest, value)
	case *apd.Decimal:
		// CompareJSON compares decimals after converting them to float64.
		number, _ := value.Float64()
		writeJSONHashFloat(digest, number)
	default:
		return sql.ErrInvalidType.New(value)
	}
	return nil
}

// writeJSONHashLength separates variable length values without depending on delimiters in their contents.
func writeJSONHashLength(digest *xxhash.Digest, length int) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(length))
	digest.Write(buf[:])
}

// writeJSONHashNumber terminates a canonical number so neighboring array elements remain distinct.
func writeJSONHashNumber(digest *xxhash.Digest, number []byte) {
	digest.WriteString("n")
	digest.Write(number)
	digest.WriteString("\x00")
}

// writeJSONHashFloat gives integral floats the exact spelling of an equal integer, including signed zero.
func writeJSONHashFloat(digest *xxhash.Digest, number float64) {
	var buf [64]byte
	var value []byte
	switch {
	case number == 0:
		value = append(buf[:0], '0')
	case number >= 0 && number < 1<<64 && math.Trunc(number) == number:
		value = strconv.AppendUint(buf[:0], uint64(number), 10)
	case number >= -1<<63 && number < 0 && math.Trunc(number) == number:
		value = strconv.AppendInt(buf[:0], int64(number), 10)
	default:
		value = strconv.AppendFloat(buf[:0], number, 'g', -1, 64)
	}
	writeJSONHashNumber(digest, value)
}
