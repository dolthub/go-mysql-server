package server

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-vitess/mysql"
	"github.com/src-d/go-vitess/sqltypes"
	"github.com/src-d/go-vitess/vt/proto/query"
)

type Handler struct {
	mu sync.Mutex

	driver      driver.Driver
	connections map[uint32]driver.Conn
}

func NewHandler(d driver.Driver) *Handler {
	return &Handler{
		driver:      d,
		connections: make(map[uint32]driver.Conn, 0),
	}
}
func (h *Handler) NewConnection(c *mysql.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	logrus.Infof("NewConnection(%v): client %v", "foo", c.ConnectionID)

	var err error
	h.connections[c.ConnectionID], err = h.driver.Open("foo")
	if err != nil {
		panic(err)
	}
}

func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	logrus.Infof("ConnectionClosed(%v): client %v", "foo", c.ConnectionID)
	delete(h.connections, c.ConnectionID)
}

func (h *Handler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	h.mu.Lock()
	d := h.connections[c.ConnectionID]
	h.mu.Unlock()

	stmt, err := d.Prepare(query)
	if err != nil {
		return err
	}

	selectStmt, ok := stmt.(driver.StmtQueryContext)
	if !ok {
		return fmt.Errorf("interface driver.StmtQueryContext not implemented")
	}

	if err := h.doQuery(selectStmt, callback); err != nil {
		return err
	}

	return nil
}

func (h *Handler) doQuery(q driver.StmtQueryContext, callback func(*sqltypes.Result) error) error {
	rows, err := q.QueryContext(context.TODO(), nil)
	if err != nil {
		return err
	}

	sqlRows, ok := rows.(sql.Rows)
	if !ok {
		return errors.New("sql.Rows not implemented for the given driver")
	}

	s := sqlRows.Schema()
	r := &sqltypes.Result{Fields: schemaToFields(s)}

	values := make([]driver.Value, len(s))
	for {
		if err := rows.Next(values); err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		r.Rows = append(r.Rows, valuesToSQL(s, values))
		r.RowsAffected++
	}

	return callback(r)
}

func valuesToSQL(s sql.Schema, values []driver.Value) []sqltypes.Value {
	o := make([]sqltypes.Value, len(values))
	for i, v := range values {
		o[i] = s[i].Type.SQL(v)
	}

	return o
}

func schemaToFields(s sql.Schema) []*query.Field {
	fields := make([]*query.Field, len(s))
	for i, c := range s {
		fields[i] = &query.Field{
			Name: c.Name,
			Type: c.Type.Type(),
		}
	}

	return fields
}
