package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-vitess.v1/sqltypes"
	"gopkg.in/src-d/go-vitess.v1/vt/proto/query"
)

func TestText(t *testing.T) {
	convert(t, Text, "", "")
	convert(t, Text, 1, "1")

	lt(t, Text, "a", "b")
	eq(t, Text, "a", "a")
	gt(t, Text, "b", "a")
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
	val := f.SQL(23.222)
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

	sql := Timestamp.SQL(now)
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

func TestDate(t *testing.T) {
	require := require.New(t)
	now := time.Now().UTC()
	v, err := Date.Convert(now)
	require.NoError(err)
	require.Equal(now.Format(DateLayout), v.(time.Time).Format(DateLayout))

	v, err = Date.Convert(now.Format(DateLayout))
	require.NoError(err)
	require.Equal(
		now.Format(DateLayout),
		v.(time.Time).Format(DateLayout),
	)

	v, err = Date.Convert(now.Unix())
	require.NoError(err)
	require.Equal(
		now.Format(DateLayout),
		v.(time.Time).Format(DateLayout),
	)

	sql := Date.SQL(now)
	require.Equal([]byte(now.Format(DateLayout)), sql.Raw())

	after := now.Add(time.Second)
	eq(t, Date, now, after)
	eq(t, Date, now, now)
	eq(t, Date, after, now)

	after = now.Add(26 * time.Hour)
	lt(t, Date, now, after)
	eq(t, Date, now, now)
	gt(t, Date, after, now)
}

func TestBlob(t *testing.T) {
	require := require.New(t)

	convert(t, Blob, "", []byte{})
	convert(t, Blob, nil, []byte(nil))
	MustConvert(Blob, nil)

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

	require.Panics(func() {
		typ.SQL(nil)
	})

	require.Equal(sqltypes.Expression, typ.Type())

	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{2, 2, 3})
	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 3, 3})
	lt(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 2, 4})
	eq(t, typ, []interface{}{1, 2, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{2, 2, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{1, 3, 3}, []interface{}{1, 2, 3})
	gt(t, typ, []interface{}{1, 2, 4}, []interface{}{1, 2, 3})
}

func TestArray(t *testing.T) {
	require := require.New(t)

	typ := Array(Int64)
	_, err := typ.Convert("foo")
	require.Error(err)
	require.True(ErrNotArray.Is(err))

	convert(t, typ, []interface{}{1, 2, 3}, []interface{}{int64(1), int64(2), int64(3)})

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
