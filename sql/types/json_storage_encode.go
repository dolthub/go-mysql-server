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
	"bytes"
	"encoding/json"
	"math"
	"reflect"
	"slices"
	"strconv"
	"sync"
	"unicode/utf8"
)

// jsonStorageEncoder writes decoded JSON directly, reusing output and key buffers.
// Storage uses lexicographic key ordering and no insignificant whitespace.
type jsonStorageEncoder struct {
	buf   []byte
	keys  []string
	depth int
}

var jsonStorageEncoderPool = sync.Pool{New: func() any {
	return &jsonStorageEncoder{buf: make([]byte, 0, 1024), keys: make([]string, 0, 16)}
}}

// marshalJSONForStorage preserves numeric kinds without copying the document's containers.
func marshalJSONForStorage(value any) ([]byte, error) {
	e := jsonStorageEncoderPool.Get().(*jsonStorageEncoder)
	defer func() {
		e.buf = e.buf[:0]
		clear(e.keys[:cap(e.keys)])
		e.keys = e.keys[:0]
		// Do not retain unusually large buffers after a large document is encoded.
		if cap(e.buf) > 1<<20 {
			e.buf = nil
		}
		if cap(e.keys) > 4096 {
			e.keys = nil
		}
		jsonStorageEncoderPool.Put(e)
	}()
	if err := e.appendValue(value); err != nil {
		return nil, err
	}
	return bytes.Clone(e.buf), nil
}

// appendValue encodes JSON's decoded value types, preserving standard Go encoding for other values.
func (e *jsonStorageEncoder) appendValue(value any) error {
	switch v := value.(type) {
	case nil:
		e.buf = append(e.buf, "null"...)
	case bool:
		e.buf = strconv.AppendBool(e.buf, v)
	case string:
		e.buf = appendJSONString(e.buf, v)
	case int64:
		e.buf = strconv.AppendInt(e.buf, v, 10)
	case uint64:
		e.buf = strconv.AppendUint(e.buf, v, 10)
	case float64:
		var err error
		e.buf, err = appendJSONFloat(e.buf, v, 64)
		return err
	case float32:
		var err error
		e.buf, err = appendJSONFloat(e.buf, float64(v), 32)
		return err
	case []any:
		if v == nil {
			e.buf = append(e.buf, "null"...)
			return nil
		}
		if e.depth >= 10000 {
			return &json.UnsupportedValueError{Value: reflect.ValueOf(v), Str: "cycle or excessive JSON nesting"}
		}
		e.depth++
		defer func() { e.depth-- }()
		e.buf = append(e.buf, '[')
		for i, element := range v {
			if i != 0 {
				e.buf = append(e.buf, ',')
			}
			if err := e.appendValue(element); err != nil {
				return err
			}
		}
		e.buf = append(e.buf, ']')
	case map[string]any:
		if v == nil {
			e.buf = append(e.buf, "null"...)
			return nil
		}
		if e.depth >= 10000 {
			return &json.UnsupportedValueError{Value: reflect.ValueOf(v), Str: "cycle or excessive JSON nesting"}
		}
		e.depth++
		defer func() { e.depth-- }()
		keyStart := len(e.keys)
		defer func() { e.keys = e.keys[:keyStart] }()
		for key := range v {
			e.keys = append(e.keys, key)
		}
		keys := e.keys[keyStart:]
		slices.Sort(keys)
		e.buf = append(e.buf, '{')
		for i, key := range keys {
			if i != 0 {
				e.buf = append(e.buf, ',')
			}
			e.buf = appendJSONString(e.buf, key)
			e.buf = append(e.buf, ':')
			if err := e.appendValue(v[key]); err != nil {
				return err
			}
		}
		e.buf = append(e.buf, '}')
	default:
		// Preserve encoding/json's contracts for custom marshalers, structs, and typed containers.
		var buffer bytes.Buffer
		encoder := json.NewEncoder(&buffer)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(value); err != nil {
			return err
		}
		e.buf = append(e.buf, bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'})...)
	}
	return nil
}

// appendJSONString matches encoding/json's string escaping with HTML escaping disabled.
func appendJSONString(dst []byte, value string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(value); {
		c := value[i]
		if c < utf8.RuneSelf {
			if escaped := escapeSeq[c]; escaped != nil {
				dst = append(dst, value[start:i]...)
				dst = append(dst, escaped...)
				start = i + 1
			}
			i++
			continue
		}
		r, width := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && width == 1 {
			dst = append(dst, value[start:i]...)
			dst = append(dst, "\ufffd"...)
			start = i + 1
		} else if r == '\u2028' || r == '\u2029' {
			dst = append(dst, value[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hexDigits[r&0xf])
			start = i + width
		}
		i += width
	}
	dst = append(dst, value[start:]...)
	return append(dst, '"')
}

// appendJSONFloat uses JSON's float formatting and retains a double marker for whole values.
func appendJSONFloat(dst []byte, value float64, bits int) ([]byte, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, &json.UnsupportedValueError{Value: reflect.ValueOf(value), Str: strconv.FormatFloat(value, 'g', -1, bits)}
	}
	format := byte('f')
	abs := math.Abs(value)
	if abs != 0 {
		if bits == 32 {
			if float32(abs) < 1e-6 || float32(abs) >= 1e21 {
				format = 'e'
			}
		} else if abs < 1e-6 || abs >= 1e21 {
			format = 'e'
		}
	}
	start := len(dst)
	dst = strconv.AppendFloat(dst, value, format, -1, bits)
	if format == 'e' {
		// encoding/json removes the leading zero from single-digit negative exponents.
		n := len(dst)
		if dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	} else if !bytes.ContainsAny(dst[start:], ".") {
		dst = append(dst, '.', '0')
	}
	return dst, nil
}
