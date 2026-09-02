package types

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/apd/v3"

	"github.com/dolthub/go-mysql-server/sql"
)

const hexDigits = "0123456789abcdef"

const firstNonControlByte = 0x20

// escapeSeq maps a byte to its JSON escape, or nil when the byte is safe to
// write unchanged.
var escapeSeq = func() (seq [256][]byte) {
	for c := 0; c < firstNonControlByte; c++ {
		seq[c] = []byte{'\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf]}
	}
	seq['\b'] = []byte(`\b`)
	seq['\t'] = []byte(`\t`)
	seq['\n'] = []byte(`\n`)
	seq['\f'] = []byte(`\f`)
	seq['\r'] = []byte(`\r`)
	seq['"'] = []byte(`\"`)
	seq['\\'] = []byte(`\\`)
	return seq
}()

type NoCopyBuilder struct {
	buffers   [][]byte
	curr      int
	lastAlloc int64
	totalSize int64
}

func NewNoCopyBuilder(initialAlloc int64) *NoCopyBuilder {
	return &NoCopyBuilder{
		buffers:   [][]byte{make([]byte, 0, initialAlloc)},
		lastAlloc: initialAlloc,
	}
}

func (b *NoCopyBuilder) Write(p []byte) (int, error) {
	currBuff := b.buffers[b.curr]

	toWrite := len(p)
	sourcePos := 0
	destPos := len(currBuff)
	space := cap(currBuff) - destPos

	if space > 0 {
		firstWrite := toWrite
		if firstWrite > space {
			firstWrite = space
		}

		currBuff = currBuff[:destPos+firstWrite]
		n := copy(currBuff[destPos:], p[sourcePos:firstWrite])
		b.buffers[b.curr] = currBuff

		if n != firstWrite {
			return -1, fmt.Errorf("failed to copy %d bytes to buffer", firstWrite)
		}

		toWrite -= firstWrite
		sourcePos += firstWrite
	}

	if toWrite > 0 {
		toAlloc := b.lastAlloc * 2
		if toAlloc < int64(toWrite) {
			toAlloc = int64(toWrite * 2)
		}

		newBuff := make([]byte, toWrite, toAlloc)
		b.buffers = append(b.buffers, newBuff)
		b.curr++
		b.lastAlloc = toAlloc

		n := copy(newBuff, p[sourcePos:])
		if n != toWrite {
			return -1, fmt.Errorf("failed to copy %d bytes to buffer", toWrite)
		}
	}

	b.totalSize += int64(len(p))
	return len(p), nil
}

func (b *NoCopyBuilder) Bytes() []byte {
	if len(b.buffers) == 1 {
		return b.buffers[0]
	}

	res := make([]byte, b.totalSize)
	pos := 0
	for _, buff := range b.buffers {
		n := copy(res[pos:], buff)
		if n != len(buff) {
			panic(fmt.Errorf("failed to copy %d bytes to buffer", len(buff)))
		}
		pos += len(buff)
	}

	return res
}

func (b *NoCopyBuilder) String() string {
	return string(b.Bytes())
}

func WriteStrings(wr io.Writer, strs ...string) (int, error) {
	var totalN int
	for _, str := range strs {
		n, err := wr.Write([]byte(str))
		if err != nil {
			return totalN, err
		}

		totalN += n
	}

	return totalN, nil
}

// marshalToMySqlString is a helper function to marshal a JSONDocument to a string that is
// compatible with MySQL's JSON output, including spaces.
func marshalToMySqlString(val interface{}) (string, error) {
	b := NewNoCopyBuilder(1024)
	err := writeMarshalledValue(b, val)
	if err != nil {
		return "", err
	}

	return b.String(), nil
}

// marshalToMySqlBytes is a helper function to marshal a JSONDocument to a byte slice that is
// compatible with MySQL's JSON output, including spaces.
func marshalToMySqlBytes(val interface{}) ([]byte, error) {
	b := NewNoCopyBuilder(1024)
	err := writeMarshalledValue(b, val)
	if err != nil {
		return nil, err
	}

	return b.Bytes(), nil
}

func sortKeys[T any](m map[string]T) []string {
	var keys []string
	for k := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) != len(keys[j]) {
			return len(keys[i]) < len(keys[j])
		}
		return keys[i] < keys[j]
	})
	return keys
}

func writeMarshalledValue(writer io.Writer, val interface{}) error {
	switch val := val.(type) {
	case []interface{}:
		writer.Write([]byte{'['})
		for i, v := range val {
			err := writeMarshalledValue(writer, v)
			if err != nil {
				return err
			}

			if i != len(val)-1 {
				writer.Write([]byte{',', ' '})
			}
		}
		writer.Write([]byte{']'})
		return nil

	case map[string]string:
		keys := sortKeys(val)

		writer.Write([]byte{'{'})
		for i, k := range keys {
			if err := writeMarshalledValue(writer, k); err != nil {
				return err
			}
			writer.Write([]byte(`: `))
			if err := writeMarshalledValue(writer, val[k]); err != nil {
				return err
			}

			if i != len(keys)-1 {
				writer.Write([]byte{',', ' '})
			}
		}

		writer.Write([]byte{'}'})
		return nil

	case map[string]interface{}:
		keys := sortKeys(val)

		writer.Write([]byte{'{'})
		for i, k := range keys {
			err := writeMarshalledValue(writer, k)
			if err != nil {
				return err
			}
			writer.Write([]byte(`: `))
			err = writeMarshalledValue(writer, val[k])
			if err != nil {
				return err
			}

			if i != len(keys)-1 {
				writer.Write([]byte{',', ' '})
			}
		}

		writer.Write([]byte{'}'})
		return nil

	case string:
		writer.Write([]byte{'"'})
		// Iterate by byte. Every byte that needs escaping is ASCII, so the bytes
		// of a multi byte UTF-8 sequence are always copied through unchanged.
		start := 0
		for i := 0; i < len(val); i++ {
			esc := escapeSeq[val[i]]
			if esc == nil {
				continue
			}
			if start != i {
				writer.Write([]byte(val[start:i]))
			}
			writer.Write(esc)
			start = i + 1
		}

		if start != len(val) {
			writer.Write([]byte(val[start:]))
		}

		writer.Write([]byte{'"'})
		return nil

	case float64:
		// JSON doesn't distinguish between integers and floats, so we need to check if the float is an integer
		if val == float64(int64(val)) {
			_, err := writer.Write([]byte(strconv.FormatInt(int64(val), 10)))
			return err
		}

		_, err := writer.Write([]byte(strconv.FormatFloat(val, 'f', -1, 64)))
		return err

	case float32:
		// JSON doesn't distinguish between integers and floats, so we need to check if the float is an integer
		if val == float32(int32(val)) {
			_, err := writer.Write([]byte(strconv.FormatInt(int64(val), 10)))
			return err
		}

		_, err := writer.Write([]byte(strconv.FormatFloat(float64(val), 'f', -1, 32)))
		return err

	case int64:
		_, err := writer.Write([]byte(strconv.FormatInt(val, 10)))
		return err

	case int32:
		_, err := writer.Write([]byte(strconv.FormatInt(int64(val), 10)))
		return err

	case int16:
		_, err := writer.Write([]byte(strconv.FormatInt(int64(val), 10)))
		return err

	case int8:
		_, err := writer.Write([]byte(strconv.FormatInt(int64(val), 10)))
		return err

	case int:
		_, err := writer.Write([]byte(strconv.FormatInt(int64(val), 10)))
		return err

	case uint64:
		_, err := writer.Write([]byte(strconv.FormatUint(val, 10)))
		return err

	case uint32:
		_, err := writer.Write([]byte(strconv.FormatUint(uint64(val), 10)))
		return err

	case uint16:
		_, err := writer.Write([]byte(strconv.FormatUint(uint64(val), 10)))
		return err

	case uint8:
		_, err := writer.Write([]byte(strconv.FormatUint(uint64(val), 10)))
		return err

	case bool:
		if val {
			writer.Write([]byte("true"))
		} else {
			writer.Write([]byte("false"))
		}

		return nil

	case nil:
		writer.Write([]byte("null"))
		return nil

	case time.Time:
		writer.Write([]byte{'"'})
		writer.Write([]byte(val.Format(sql.DatetimeLayoutNoTrim)))
		writer.Write([]byte{'"'})
		return nil
	case *apd.Decimal:
		writer.Write([]byte(val.Text('f')))
		return nil
	case json.Marshaler:
		bytes, err := val.MarshalJSON()
		if err != nil {
			return err
		}
		writer.Write(bytes)
		return nil
	default:
		r := reflect.ValueOf(val)
		switch r.Kind() {
		case reflect.Slice, reflect.Array:
			writer.Write([]byte{'['})
			for i := 0; i < r.Len(); i++ {
				err := writeMarshalledValue(writer, r.Index(i).Interface())
				if err != nil {
					return err
				}

				if i != r.Len()-1 {
					writer.Write([]byte{',', ' '})
				}
			}
			writer.Write([]byte{']'})
			return nil

		case reflect.Map:
			interfMap := make(map[string]interface{})
			for _, k := range r.MapKeys() {
				interfMap[k.String()] = r.MapIndex(k).Interface()
			}

			return writeMarshalledValue(writer, interfMap)

		default:
			return fmt.Errorf("unsupported type: %T", val)
		}
	}
}
