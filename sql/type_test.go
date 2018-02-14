package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestType_Text(t *testing.T) {
	assert := assert.New(t)

	v, err := Text.Convert("")
	assert.Nil(err)
	assert.Equal("", v)
	v, err = Text.Convert(1)
	assert.Nil(err)
	assert.Equal("1", v)

	assert.Equal(-1, Text.Compare("a", "b"))
	assert.Equal(0, Text.Compare("a", "a"))
	assert.Equal(1, Text.Compare("b", "a"))
}

func TestType_Int32(t *testing.T) {
	assert := assert.New(t)

	v, err := Int32.Convert(int32(1))
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Int32.Convert(1)
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Int32.Convert(int64(1))
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Int32.Convert("")
	assert.NotNil(err)
	assert.Equal(int32(0), v)

	assert.Equal(-1, Int32.Compare(int32(1), int32(2)))
	assert.Equal(0, Int32.Compare(int32(1), int32(1)))
	assert.Equal(1, Int32.Compare(int32(2), int32(1)))
}

func TestType_Int64(t *testing.T) {
	assert := assert.New(t)

	v, err := Int64.Convert(int64(1))
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = Int64.Convert(1)
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = Int64.Convert(int32(1))
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = Int64.Convert(int64(9223372036854775807))
	assert.Nil(err)
	assert.Equal(int64(9223372036854775807), v)
	v, err = Int64.Convert(uint32(4294967295))
	assert.Nil(err)
	assert.Equal(int64(4294967295), v)
	v, err = Int64.Convert("")
	assert.NotNil(err)
	assert.Equal(int64(0), v)

	assert.Equal(-1, Int64.Compare(int64(1), int64(2)))
	assert.Equal(0, Int64.Compare(int64(1), int64(1)))
	assert.Equal(1, Int64.Compare(int64(2), int64(1)))
}

func TestType_Timestamp(t *testing.T) {
	assert := assert.New(t)

	now := time.Now()
	v, err := Timestamp.Convert(now)
	assert.Nil(err)
	assert.Equal(now, v)

	v, err = Timestamp.Convert(now.Format(TimestampLayout))
	assert.Nil(err)
	assert.Equal(
		now.Format(TimestampLayout),
		v.(time.Time).Format(TimestampLayout),
	)

	v, err = Timestamp.Convert(now.Unix())
	assert.Nil(err)
	assert.Equal(
		now.Format(TimestampLayout),
		v.(time.Time).Format(TimestampLayout),
	)

	sql := Timestamp.SQL(now)
	assert.Equal([]byte(now.Format(TimestampLayout)), sql.Raw())

	after := now.Add(time.Second)
	assert.Equal(-1, Timestamp.Compare(now, after))
	assert.Equal(0, Timestamp.Compare(now, now))
	assert.Equal(1, Timestamp.Compare(after, now))
}

func TestType_Blob(t *testing.T) {
	assert := assert.New(t)

	v, err := Blob.Convert("")
	assert.Nil(err)
	assert.Equal([]byte{}, v)
	v, err = Blob.Convert(1)
	assert.Equal(ErrInvalidType, err)
	assert.Nil(v)

	assert.Equal(-1, Blob.Compare([]byte{'A'}, []byte{'B'}))
	assert.Equal(0, Blob.Compare([]byte{'A'}, []byte{'A'}))
	assert.Equal(1, Blob.Compare([]byte{'B'}, []byte{'A'}))
}

func TestType_JSON(t *testing.T) {
	assert := assert.New(t)

	v, err := JSON.Convert("")
	assert.Nil(err)
	assert.Equal([]byte(`""`), v)
	v, err = JSON.Convert([]int{1, 2})
	assert.Nil(err)
	assert.Equal([]byte("[1,2]"), v.([]byte))

	assert.Equal(-1, JSON.Compare([]byte{'A'}, []byte{'B'}))
	assert.Equal(0, JSON.Compare([]byte{'A'}, []byte{'A'}))
	assert.Equal(1, JSON.Compare([]byte{'B'}, []byte{'A'}))
}
