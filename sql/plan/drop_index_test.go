package plan

import (
	"crypto/sha1"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/src-d/go-mysql-server.v0/mem"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/expression"
)

func TestDeleteIndex(t *testing.T) {
	require := require.New(t)

	table := &indexableTable{mem.NewTable("foo", sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	})}

	driver := new(mockDriver)
	catalog := sql.NewCatalog()
	catalog.RegisterIndexDriver(driver)
	db := mem.NewDatabase("foo")
	db.AddTable("foo", table)
	catalog.Databases = append(catalog.Databases, db)

	var expressions = []sql.Expression{
		expression.NewGetFieldWithTable(0, sql.Int64, "foo", "c", true),
		expression.NewGetFieldWithTable(1, sql.Int64, "foo", "a", true),
	}
	var hashes []sql.ExpressionHash

	for _, e := range expressions {
		h := sha1.Sum([]byte(e.String()))
		exh := sql.ExpressionHash(h[:])
		hashes = append(hashes, exh)
	}

	done, err := catalog.AddIndex(&mockIndex{id: "idx", db: "foo", table: "foo", exprs: hashes})
	require.NoError(err)
	close(done)

	time.Sleep(50 * time.Millisecond)
	idx := catalog.Index("foo", "idx")
	require.NotNil(idx)
	catalog.ReleaseIndex(idx)

	di := NewDropIndex("idx", table)
	di.Catalog = catalog
	di.CurrentDatabase = "foo"

	_, err = di.RowIter(sql.NewEmptyContext())
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Equal([]string{"idx"}, driver.deleted)
	require.Nil(catalog.Index("foo", "idx"))
}
