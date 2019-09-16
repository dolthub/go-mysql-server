package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

func TestIsNull(t *testing.T) {
	require.True(t, IsNull(nil))

	n := numberT{sqltypes.Uint64}
	require.Equal(t, sqltypes.NULL, mustSQL(n.SQL(nil)))
	require.Equal(t, sqltypes.NewUint64(0), mustSQL(n.SQL(uint64(0))))
}

func TestText(t *testing.T) {
	convert(t, Text, "", "")
	convert(t, Text, 1, "1")

	lt(t, Text, "a", "b")
	eq(t, Text, "a", "a")
	gt(t, Text, "b", "a")

	var3, err := VarChar(3).Convert("abc")
	require.NoError(t, err)
	convert(t, Text, var3, "abc")
}

func TestBoolean(t *testing.T) {
	convert(t, Boolean, "", false)
	convert(t, Boolean, "true", false)
	convert(t, Boolean, 0, false)
	convert(t, Boolean, 1, true)
	convert(t, Boolean, -1, true)
	convert(t, Boolean, 0.0, false)
	convert(t, Boolean, 0.4, false)
	convert(t, Boolean, 0.5, true)
	convert(t, Boolean, 1.0, true)
	convert(t, Boolean, -1.0, true)

	eq(t, Boolean, true, true)
	eq(t, Boolean, false, false)
}

func TestInt32(t *testing.T) {
	convert(t, Int32, int32(1), int32(1))
	convert(t, Int32, 1, int32(1))
	convert(t, Int32, int64(1), int32(1))
	convert(t, Int32, "5", int32(5))
	convertErr(t, Int32, "")

	lt(t, Int32, int32(1), int32(2))
	eq(t, Int32, int32(1), int32(1))
	gt(t, Int32, int32(3), int32(2))
}

func TestNumberComparison(t *testing.T) {
	eq(t, Int64, int32(1), int32(1))
	eq(t, Int64, int32(1), int64(1))
	gt(t, Int64, int64(5), int32(1))
	gt(t, Int64, int32(5), int64(1))
	lt(t, Int32, int64(1), int32(5))

	eq(t, Uint32, int32(1), uint32(1))
	eq(t, Uint32, int32(1), int64(1))
	gt(t, Uint32, int64(5), uint32(1))
	gt(t, Uint32, uint32(5), int64(1))
	lt(t, Uint32, uint64(1), int32(5))

	eq(t, Uint8, uint8(255), uint8(255))
	eq(t, Uint8, uint8(255), int32(255))
	eq(t, Uint8, uint8(255), int64(255))
	eq(t, Uint8, uint8(255), int64(255))
	gt(t, Uint8, uint8(255), int32(1))
	gt(t, Uint8, uint8(255), int64(1))
	lt(t, Uint8, uint8(255), int16(256))

	// Exhaustive numeric type equality test
	type typeAndValue struct {
		t numberT
		v interface{}
	}

	allTypes := []typeAndValue{
		{Int8, int8(42)},
		{Uint8, uint8(42)},
		{Int16, int16(42)},
		{Uint16, uint16(42)},
		{Int32, int32(42)},
		{Uint32, uint32(42)},
		{Int64, int64(42)},
		{Uint64, uint64(42)},
		{Float32, float32(42)},
		{Float64, float64(42)},
	}
	for _, a := range allTypes {
		for _, b := range allTypes {
			eq(t, a.t, a.v, b.v)
		}
	}

	// Float comparisons against other floats
	greaterFloat := 7.5
	lesserFloat := 7.4
	gt(t, Float64, float64(greaterFloat), float64(lesserFloat))
	lt(t, Float64, float64(lesserFloat), float64(greaterFloat))
	eq(t, Float64, float64(greaterFloat), float64(greaterFloat))
	gt(t, Float64, float64(greaterFloat), float32(lesserFloat))
	lt(t, Float64, float64(lesserFloat), float32(greaterFloat))
	eq(t, Float64, float64(greaterFloat), float32(greaterFloat))
	gt(t, Float32, float32(greaterFloat), float32(lesserFloat))
	lt(t, Float32, float32(lesserFloat), float32(greaterFloat))
	eq(t, Float32, float32(greaterFloat), float32(greaterFloat))
	gt(t, Float32, float32(greaterFloat), float64(lesserFloat))
	lt(t, Float32, float32(lesserFloat), float64(greaterFloat))
	eq(t, Float32, float32(greaterFloat), float64(greaterFloat))

	// Float comparisons against other types, testing comparison and truncation (when an int type is the left side of a
	// comparison with a float type)
	lessInt := 7
	floatComps := []typeAndValue{
		{Int8, int8(lessInt)},
		{Uint8, uint8(lessInt)},
		{Int16, int16(lessInt)},
		{Uint16, uint16(lessInt)},
		{Int32, int32(lessInt)},
		{Uint32, uint32(lessInt)},
		{Int64, int64(lessInt)},
		{Uint64, uint64(lessInt)},
	}
	for _, a := range floatComps {
		gt(t, Float64, float64(greaterFloat), a.v)
		eq(t, a.t, float64(greaterFloat), a.v)
		gt(t, Float32, float32(greaterFloat), a.v)
		eq(t, a.t, float32(greaterFloat), a.v)
	}
}

func TestInt64(t *testing.T) {
	convert(t, Int64, int32(1), int64(1))
	convert(t, Int64, 1, int64(1))
	convert(t, Int64, int64(1), int64(1))
	convertErr(t, Int64, "")
	convert(t, Int64, "5", int64(5))

	lt(t, Int64, int64(1), int64(2))
	eq(t, Int64, int64(1), int64(1))
	gt(t, Int64, int64(3), int64(2))
}

func TestFloat64(t *testing.T) {
	require := require.New(t)

	var f = numberT{
		t: query.Type_FLOAT64,
	}
	val, err := f.SQL(23.222)
	require.NoError(err)
	require.True(val.IsFloat())
	require.Equal(sqltypes.NewFloat64(23.222), val)
}

func TestTimestamp(t *testing.T) {
	require := require.New(t)

	now := time.Now().UTC()
	v, err := Timestamp.Convert(now)
	require.NoError(err)
	require.Equal(now, v)

	v, err = Timestamp.Convert(now.Format(TimestampLayout))
	require.NoError(err)
	require.Equal(
		now.Format(TimestampLayout),
		v.(time.Time).Format(TimestampLayout),
	)

	v, err = Timestamp.Convert(now.Unix())
	require.NoError(err)
	require.Equal(
		now.Format(TimestampLayout),
		v.(time.Time).Format(TimestampLayout),
	)

	sql, err := Timestamp.SQL(now)
	require.NoError(err)
	require.Equal([]byte(now.Format(TimestampLayout)), sql.Raw())

	after := now.Add(time.Second)
	lt(t, Timestamp, now, after)
	eq(t, Timestamp, now, now)
	gt(t, Timestamp, after, now)
}

func TestExtraTimestamps(t *testing.T) {
	tests := []struct {
		date     string
		expected string
	}{
		{
			date:     "2018-10-18T05:22:25Z",
			expected: "2018-10-18 05:22:25",
		},
		{
			date:     "2018-10-18T05:22:25+07:00",
			expected: "2018-10-17 22:22:25",
		},
		{
			date:     "20181018052225",
			expected: "2018-10-18 05:22:25",
		},
		{
			date:     "20181018",
			expected: "2018-10-18 00:00:00",
		},
		{
			date:     "2018-10-18",
			expected: "2018-10-18 00:00:00",
		},
	}

	for _, c := range tests {
		t.Run(c.date, func(t *testing.T) {
			require := require.New(t)

			p, err := Timestamp.Convert(c.date)
			require.NoError(err)

			str := string([]byte(p.(time.Time).Format(TimestampLayout)))
			require.Equal(c.expected, str)
		})
	}
}

// Generic tests for Date and Datetime.
// typ should be Date or Datetime
func commonTestsDatesTypes(typ Type, layout string, t *testing.T) {
	require := require.New(t)
	now := time.Now().UTC()
	v, err := typ.Convert(now)
	require.NoError(err)
	require.Equal(now.Format(layout), v.(time.Time).Format(layout))

	v, err = typ.Convert(now.Format(layout))
	require.NoError(err)
	require.Equal(
		now.Format(layout),
		v.(time.Time).Format(layout),
	)

	v, err = typ.Convert(now.Unix())
	require.NoError(err)
	require.Equal(
		now.Format(layout),
		v.(time.Time).Format(layout),
	)

	sql, err := typ.SQL(now)
	require.NoError(err)
	require.Equal([]byte(now.Format(layout)), sql.Raw())

	after := now.Add(26 * time.Hour)
	lt(t, typ, now, after)
	eq(t, typ, now, now)
	gt(t, typ, after, now)
}

func TestDate(t *testing.T) {
	commonTestsDatesTypes(Date, DateLayout, t)

	now := time.Now().UTC()
	after := now.Add(time.Second)
	eq(t, Date, now, after)
	eq(t, Date, now, now)
	eq(t, Date, after, now)
}

func TestDatetime(t *testing.T) {
	commonTestsDatesTypes(Datetime, DatetimeLayout, t)

	now := time.Now().UTC()
	after := now.Add(time.Millisecond)
	lt(t, Datetime, now, after)
	eq(t, Datetime, now, now)
	gt(t, Datetime, after, now)
}

func TestBlob(t *testing.T) {
	require := require.New(t)

	convert(t, Blob, "", []byte{})
	convert(t, Blob, nil, []byte(nil))

	_, err := Blob.Convert(1)
	require.NotNil(err)
	require.True(ErrInvalidType.Is(err))

	lt(t, Blob, []byte("A"), []byte("B"))
	eq(t, Blob, []byte("A"), []byte("A"))
	gt(t, Blob, []byte("C"), []byte("B"))
}

func TestJSON(t *testing.T) {
	convert(t, JSON, "", []byte(`""`))
	convert(t, JSON, []int{1, 2}, []byte("[1,2]"))
	convert(t, JSON, `{"a": true, "b": 3}`, []byte(`{"a":true,"b":3}`))

	lt(t, JSON, []byte("A"), []byte("B"))
	eq(t, JSON, []byte("A"), []byte("A"))
	gt(t, JSON, []byte("C"), []byte("B"))
}

func TestTuple(t *testing.T) {
	require := require.New(t)

	typ := Tuple(Int32, Text, Int64)
	_, err := typ.Convert("foo")
	require.Error(err)
	require.True(ErrNotTuple.Is(err))

	_, err = typ.Convert([]interface{}{1, 2})
	require.Error(err)
	require.True(ErrInvalidColumnNumber.Is(err))

	convert(t, typ, []interface{}{1, 2, 3}, []interface{}{int32(1), "2", int64(3)})

	_, err = typ.SQL(nil)
	require.Error(err)

	require.Equal(sqltypes.Expression, typ.Type())

	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{2, 2, 3})
	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 3, 3})
	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 2, 4})
	eq(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{2, 2, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{1, 3, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{1, 2, 4}, []interface{}{1, 2, 3})
}

func TestVarChar(t *testing.T) {
	typ := VarChar(3)
	require.True(t, IsVarChar(typ))
	require.True(t, IsText(typ))
	convert(t, typ, "foo", "foo")
	fooByte := []byte{'f', 'o', 'o'}
	convert(t, typ, fooByte, "foo")

	typ = VarChar(1)
	convertErr(t, typ, "foo")
	convertErr(t, typ, fooByte)
	convertErr(t, typ, 123)

	typ = VarChar(10)
	convert(t, typ, 123, "123")
	convertErr(t, typ, 1234567890123)

	convert(t, typ, "", "")
	convert(t, typ, 1, "1")

	lt(t, typ, "a", "b")
	eq(t, typ, "a", "a")
	gt(t, typ, "b", "a")

	text, err := Text.Convert("abc")
	require.NoError(t, err)

	convert(t, typ, text, "abc")
	typ1 := VarChar(1)
	convertErr(t, typ1, text)
}

func TestArray(t *testing.T) {
	require := require.New(t)

	typ := Array(Int64)
	_, err := typ.Convert("foo")
	require.Error(err)
	require.True(ErrNotArray.Is(err))

	convert(t, typ, []interface{}{1, 2, 3}, []interface{}{int64(1), int64(2), int64(3)})
	convert(
		t,
		typ,
		NewArrayGenerator([]interface{}{1, 2, 3}),
		[]interface{}{int64(1), int64(2), int64(3)},
	)

	require.Equal(sqltypes.TypeJSON, typ.Type())

	lt(t, typ, []interface{}{5, 6}, []interface{}{2, 2, 3})
	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{2, 2, 3})
	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 3, 3})
	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 2, 4})
	eq(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{2, 2, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{1, 3, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{1, 2, 4}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{1, 2, 4}, []interface{}{5, 6})

	expected := []byte("[1,2,3]")

	v, err := Array(Int64).SQL([]interface{}{1, 2, 3})
	require.NoError(err)
	require.Equal(expected, v.Raw())

	v, err = Array(Int64).SQL(NewArrayGenerator([]interface{}{1, 2, 3}))
	require.NoError(err)
	require.Equal(expected, v.Raw())
}

func TestUnderlyingType(t *testing.T) {
	require.Equal(t, Text, UnderlyingType(Array(Text)))
	require.Equal(t, Text, UnderlyingType(Text))
}

type testJSONStruct struct {
	A int
	B string
}

func TestJSONArraySQL(t *testing.T) {
	require := require.New(t)
	val, err := Array(JSON).SQL([]interface{}{
		testJSONStruct{1, "foo"},
		testJSONStruct{2, "bar"},
	})
	require.NoError(err)
	expected := `[{"A":1,"B":"foo"},{"A":2,"B":"bar"}]`
	require.Equal(expected, string(val.Raw()))
}

func eq(t *testing.T, typ Type, a, b interface{}) {
	t.Helper()
	cmp, err := typ.Compare(a, b)
	require.NoError(t, err)
	require.Equal(t, 0, cmp)
}

func lt(t *testing.T, typ Type, a, b interface{}) {
	t.Helper()
	cmp, err := typ.Compare(a, b)
	require.NoError(t, err)
	require.Equal(t, -1, cmp)
}

func gt(t *testing.T, typ Type, a, b interface{}) {
	t.Helper()
	cmp, err := typ.Compare(a, b)
	require.NoError(t, err)
	require.Equal(t, 1, cmp)
}

func convert(t *testing.T, typ Type, val interface{}, to interface{}) {
	t.Helper()
	v, err := typ.Convert(val)
	require.NoError(t, err)
	require.Equal(t, to, v)
}

func convertErr(t *testing.T, typ Type, val interface{}) {
	t.Helper()
	_, err := typ.Convert(val)
	require.Error(t, err)
}

func mustSQL(v sqltypes.Value, err error) sqltypes.Value {
	if err != nil {
		panic(err)
	}
	return v
}
