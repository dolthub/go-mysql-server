package sql


import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType_String(t *testing.T) {
	var v interface{}
	var err error
	assert := assert.New(t)
	assert.True(String.Check(""))
	assert.False(String.Check(1))
	assert.False(String.Check(int32(1)))
	v, err = String.Convert("")
	assert.Nil(err)
	assert.Equal("", v)
	v, err = String.Convert(1)
	assert.Equal(ErrInvalidType, err)
	assert.Nil(v)
}

func TestType_Integer(t *testing.T) {
	var v interface{}
	var err error
	assert := assert.New(t)
	assert.True(Integer.Check(int32(1)))
	assert.False(Integer.Check(1))
	assert.False(Integer.Check(int64(1)))
	assert.False(Integer.Check(""))
	v, err = Integer.Convert(int32(1))
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Integer.Convert(1)
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Integer.Convert(int64(1))
	assert.Nil(err)
	assert.Equal(int32(1), v)
	v, err = Integer.Convert("")
	assert.NotNil(err)
	assert.Nil(v)
	v, err = Integer.Convert(int64(9223372036854775807))
	assert.NotNil(err)
	assert.Nil(v)
	v, err = Integer.Convert(uint32(4294967295))
	assert.NotNil(err)
	assert.Nil(v)
	v, err = Integer.Convert(uint64(18446744073709551615))
	assert.NotNil(err)
	assert.Nil(v)
}

func TestType_BigInteger(t *testing.T) {
	var v interface{}
	var err error
	assert := assert.New(t)
	assert.True(BigInteger.Check(int64(1)))
	assert.False(BigInteger.Check(1))
	assert.False(BigInteger.Check(int32(1)))
	assert.False(BigInteger.Check(""))
	v, err = BigInteger.Convert(int64(1))
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = BigInteger.Convert(1)
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = BigInteger.Convert(int32(1))
	assert.Nil(err)
	assert.Equal(int64(1), v)
	v, err = BigInteger.Convert(int64(9223372036854775807))
	assert.Nil(err)
	assert.Equal(int64(9223372036854775807), v)
	v, err = BigInteger.Convert(uint32(4294967295))
	assert.Nil(err)
	assert.Equal(int64(4294967295), v)
	v, err = BigInteger.Convert(uint64(18446744073709551615))
	assert.NotNil(err)
	assert.Nil(v)
	v, err = BigInteger.Convert("")
	assert.NotNil(err)
	assert.Nil(v)
}
