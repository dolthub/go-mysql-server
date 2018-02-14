package server

import (
	"io"
	"sync"

	"github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/sql"

	"github.com/sirupsen/logrus"
	"github.com/src-d/go-vitess/mysql"
	"github.com/src-d/go-vitess/sqltypes"
	"github.com/src-d/go-vitess/vt/proto/query"
)

type Handler struct {
	mu sync.Mutex
	e  *sqle.Engine
}

func NewHandler(e *sqle.Engine) *Handler {
	return &Handler{e: e}
}

func (h *Handler) NewConnection(c *mysql.Conn) {
	logrus.Infof("NewConnection: client %v", c.ConnectionID)
}

func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	logrus.Infof("ConnectionClosed: client %v", c.ConnectionID)
}

func (h *Handler) ComQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	schema, rows, err := h.e.Query(query)
	if err != nil {
		return err
	}

	r := &sqltypes.Result{Fields: schemaToFields(schema)}
	for {
		row, err := rows.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		r.Rows = append(r.Rows, rowToSQL(schema, row))
		r.RowsAffected++
	}

	return callback(r)
}

func rowToSQL(s sql.Schema, row sql.Row) []sqltypes.Value {
	o := make([]sqltypes.Value, len(row))
	for i, v := range row {
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
