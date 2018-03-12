package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestType_Text(t *testing.T) {
	require := require.New(t)

	v, err := Text.Convert("")
	require.Nil(err)
	require.Equal("", v)
	v, err = Text.Convert(1)
	require.Nil(err)
	require.Equal("1", v)

	require.Equal(-1, Text.Compare("a", "b"))
	require.Equal(0, Text.Compare("a", "a"))
	require.Equal(1, Text.Compare("b", "a"))
}

func TestType_Int32(t *testing.T) {
	require := require.New(t)

	v, err := Int32.Convert(int32(1))
	require.Nil(err)
	require.Equal(int32(1), v)
	v, err = Int32.Convert(1)
	require.Nil(err)
	require.Equal(int32(1), v)
	v, err = Int32.Convert(int64(1))
	require.Nil(err)
	require.Equal(int32(1), v)
	v, err = Int32.Convert("")
	require.NotNil(err)
	require.Equal(int32(0), v)

	require.Equal(-1, Int32.Compare(int32(1), int32(2)))
	require.Equal(0, Int32.Compare(int32(1), int32(1)))
	require.Equal(1, Int32.Compare(int32(2), int32(1)))
}

func TestType_Int64(t *testing.T) {
	require := require.New(t)

	v, err := Int64.Convert(int64(1))
	require.Nil(err)
	require.Equal(int64(1), v)
	v, err = Int64.Convert(1)
	require.Nil(err)
	require.Equal(int64(1), v)
	v, err = Int64.Convert(int32(1))
	require.Nil(err)
	require.Equal(int64(1), v)
	v, err = Int64.Convert(int64(9223372036854775807))
	require.Nil(err)
	require.Equal(int64(9223372036854775807), v)
	v, err = Int64.Convert(uint32(4294967295))
	require.Nil(err)
	require.Equal(int64(4294967295), v)
	v, err = Int64.Convert("")
	require.NotNil(err)
	require.Equal(int64(0), v)

	require.Equal(-1, Int64.Compare(int64(1), int64(2)))
	require.Equal(0, Int64.Compare(int64(1), int64(1)))
	require.Equal(1, Int64.Compare(int64(2), int64(1)))
}

func TestType_Timestamp(t *testing.T) {
	require := require.New(t)

	now := time.Now().UTC()
	v, err := Timestamp.Convert(now)
	require.Nil(err)
	require.Equal(now, v)

	v, err = Timestamp.Convert(now.Format(TimestampLayout))
	require.Nil(err)
	require.Equal(
		now.Format(TimestampLayout),
		v.(time.Time).Format(TimestampLayout),
	)

	v, err = Timestamp.Convert(now.Unix())
	require.Nil(err)
	require.Equal(
		now.Format(TimestampLayout),
		v.(time.Time).Format(TimestampLayout),
	)

	sql := Timestamp.SQL(now)
	require.Equal([]byte(now.Format(TimestampLayout)), sql.Raw())

	after := now.Add(time.Second)
	require.Equal(-1, Timestamp.Compare(now, after))
	require.Equal(0, Timestamp.Compare(now, now))
	require.Equal(1, Timestamp.Compare(after, now))
}

func TestType_Date(t *testing.T) {
	require := require.New(t)

	now := time.Now()
	v, err := Date.Convert(now)
	require.Nil(err)
	require.Equal(now.Format(DateLayout), v.(time.Time).Format(DateLayout))

	v, err = Date.Convert(now.Format(DateLayout))
	require.Nil(err)
	require.Equal(
		now.Format(DateLayout),
		v.(time.Time).Format(DateLayout),
	)

	v, err = Date.Convert(now.Unix())
	require.Nil(err)
	require.Equal(
		now.Format(DateLayout),
		v.(time.Time).Format(DateLayout),
	)

	sql := Date.SQL(now)
	require.Equal([]byte(now.Format(DateLayout)), sql.Raw())

	after := now.Add(time.Second)
	require.Equal(0, Date.Compare(now, after))
	require.Equal(0, Date.Compare(now, now))
	require.Equal(0, Date.Compare(after, now))

	after = now.Add(26 * time.Hour)
	require.Equal(-1, Date.Compare(now, after))
	require.Equal(0, Date.Compare(now, now))
	require.Equal(1, Date.Compare(after, now))
}

func TestType_Blob(t *testing.T) {
	require := require.New(t)

	v, err := Blob.Convert("")
	require.Nil(err)
	require.Equal([]byte{}, v)
	v, err = Blob.Convert(1)
	require.NotNil(err)
	require.True(ErrInvalidType.Is(err))
	require.Nil(v)

	require.Equal(-1, Blob.Compare([]byte{'A'}, []byte{'B'}))
	require.Equal(0, Blob.Compare([]byte{'A'}, []byte{'A'}))
	require.Equal(1, Blob.Compare([]byte{'B'}, []byte{'A'}))
}

func TestType_JSON(t *testing.T) {
	require := require.New(t)

	v, err := JSON.Convert("")
	require.Nil(err)
	require.Equal([]byte(`""`), v)
	v, err = JSON.Convert([]int{1, 2})
	require.Nil(err)
	require.Equal([]byte("[1,2]"), v.([]byte))

	require.Equal(-1, JSON.Compare([]byte{'A'}, []byte{'B'}))
	require.Equal(0, JSON.Compare([]byte{'A'}, []byte{'A'}))
	require.Equal(1, JSON.Compare([]byte{'B'}, []byte{'A'}))
}
