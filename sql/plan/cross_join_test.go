package plan

import (
	"io"
	"testing"

	"gopkg.in/sqle/sqle.v0/mem"
	"gopkg.in/sqle/sqle.v0/sql"

	"github.com/stretchr/testify/assert"
)

var lSchema = sql.Schema{
	{Name: "lcol1", Type: sql.String},
	{Name: "lcol2", Type: sql.String},
	{Name: "lcol3", Type: sql.Integer},
	{Name: "lcol4", Type: sql.BigInteger},
}

var rSchema = sql.Schema{
	{Name: "rcol1", Type: sql.String},
	{Name: "rcol2", Type: sql.String},
	{Name: "rcol3", Type: sql.Integer},
	{Name: "rcol4", Type: sql.BigInteger},
}

func TestCrossJoin(t *testing.T) {
	assert := assert.New(t)

	resultSchema := sql.Schema{
		{Name: "lcol1", Type: sql.String},
		{Name: "lcol2", Type: sql.String},
		{Name: "lcol3", Type: sql.Integer},
		{Name: "lcol4", Type: sql.BigInteger},
		{Name: "rcol1", Type: sql.String},
		{Name: "rcol2", Type: sql.String},
		{Name: "rcol3", Type: sql.Integer},
		{Name: "rcol4", Type: sql.BigInteger},
	}

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)
	insertData(assert, ltable)
	insertData(assert, rtable)

	j := NewCrossJoin(ltable, rtable)

	assert.Equal(resultSchema, j.Schema())

	iter, err := j.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err := iter.Next()
	assert.Nil(err)
	assert.NotNil(row)

	assert.Equal(8, len(row))

	assert.Equal("col1_1", row[0])
	assert.Equal("col2_1", row[1])
	assert.Equal(int32(1111), row[2])
	assert.Equal(int64(2222), row[3])
	assert.Equal("col1_1", row[4])
	assert.Equal("col2_1", row[5])
	assert.Equal(int32(1111), row[6])
	assert.Equal(int64(2222), row[7])

	row, err = iter.Next()
	assert.Nil(err)
	assert.NotNil(row)

	assert.Equal("col1_1", row[0])
	assert.Equal("col2_1", row[1])
	assert.Equal(int32(1111), row[2])
	assert.Equal(int64(2222), row[3])
	assert.Equal("col1_2", row[4])
	assert.Equal("col2_2", row[5])
	assert.Equal(int32(3333), row[6])
	assert.Equal(int64(4444), row[7])

	for i := 0; i < 2; i++ {
		row, err = iter.Next()
		assert.Nil(err)
		assert.NotNil(row)
	}

	// total: 4 rows
	row, err = iter.Next()
	assert.NotNil(err)
	assert.Equal(err, io.EOF)
	assert.Nil(row)
}

func TestCrossJoin_Empty(t *testing.T) {
	assert := assert.New(t)

	ltable := mem.NewTable("left", lSchema)
	rtable := mem.NewTable("right", rSchema)
	insertData(assert, ltable)

	j := NewCrossJoin(ltable, rtable)

	iter, err := j.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err := iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(row)

	ltable = mem.NewTable("left", lSchema)
	rtable = mem.NewTable("right", rSchema)
	insertData(assert, rtable)

	j = NewCrossJoin(ltable, rtable)

	iter, err = j.RowIter()
	assert.Nil(err)
	assert.NotNil(iter)

	row, err = iter.Next()
	assert.Equal(io.EOF, err)
	assert.Nil(row)
}

func insertData(assert *assert.Assertions, table *mem.Table) {
	err := table.Insert(sql.NewRow("col1_1", "col2_1", int32(1111), int64(2222)))
	assert.Nil(err)
	err = table.Insert(sql.NewRow("col1_2", "col2_2", int32(3333), int64(4444)))
	assert.Nil(err)
}
