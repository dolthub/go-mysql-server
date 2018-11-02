package server

import (
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/sql"

	"github.com/sirupsen/logrus"
	"gopkg.in/src-d/go-vitess.v1/mysql"
	"gopkg.in/src-d/go-vitess.v1/sqltypes"
	"gopkg.in/src-d/go-vitess.v1/vt/proto/query"
)

var regKillCmd = regexp.MustCompile(`^kill (?:(query|connection) )?(\d+)$`)

var errConnectionNotFound = errors.NewKind("Connection not found: %c")

// TODO parametrize
const rowsBatch = 100

// Handler is a connection handler for a SQLe engine.
type Handler struct {
	mu sync.Mutex
	e  *sqle.Engine
	sm *SessionManager
	c  map[uint32]*mysql.Conn
}

// NewHandler creates a new Handler given a SQLe engine.
func NewHandler(e *sqle.Engine, sm *SessionManager) *Handler {
	return &Handler{
		e:  e,
		sm: sm,
		c:  make(map[uint32]*mysql.Conn),
	}
}

// NewConnection reports that a new connection has been established.
func (h *Handler) NewConnection(c *mysql.Conn) {
	h.mu.Lock()
	if _, ok := h.c[c.ConnectionID]; !ok {
		h.c[c.ConnectionID] = c
	}
	h.mu.Unlock()

	logrus.Infof("NewConnection: client %v", c.ConnectionID)
}

// ConnectionClosed reports that a connection has been closed.
func (h *Handler) ConnectionClosed(c *mysql.Conn) {
	h.sm.CloseConn(c)

	h.mu.Lock()
	delete(h.c, c.ConnectionID)
	h.mu.Unlock()

	if err := h.e.Catalog.UnlockTables(nil, c.ConnectionID); err != nil {
		logrus.Errorf("unable to unlock tables on session close: %s", err)
	}

	logrus.Infof("ConnectionClosed: client %v", c.ConnectionID)
}

// ComQuery executes a SQL query on the SQLe engine.
func (h *Handler) ComQuery(
	c *mysql.Conn,
	query string,
	callback func(*sqltypes.Result) error,
) (err error) {
	ctx := h.sm.NewContextWithQuery(c, query)

	handled, err := h.handleKill(c, query)
	if err != nil {
		return err
	}

	if handled {
		return nil
	}

	start := time.Now()
	schema, rows, err := h.e.Query(ctx, query)
	defer func() {
		if q, ok := h.e.Auth.(*auth.Audit); ok {
			q.Query(ctx, time.Since(start), err)
		}
	}()

	if err != nil {
		return err
	}

	var r *sqltypes.Result
	var proccesedAtLeastOneBatch bool
	for {
		if r == nil {
			r = &sqltypes.Result{Fields: schemaToFields(schema)}
		}

		if r.RowsAffected == rowsBatch {
			if err := callback(r); err != nil {
				return err
			}

			r = nil
			proccesedAtLeastOneBatch = true

			continue
		}

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

	if err := rows.Close(); err != nil {
		return err
	}

	// Even if r.RowsAffected = 0, the callback must be
	// called to update the state in the go-vitess' listener
	// and avoid returning errors when the query doesn't
	// produce any results.
	if r != nil && (r.RowsAffected == 0 && proccesedAtLeastOneBatch) {
		return nil
	}

	return callback(r)
}

// WarningCount is called at the end of each query to obtain
// the value to be returned to the client in the EOF packet.
// Note that this will be called either in the context of the
// ComQuery callback if the result does not contain any fields,
// or after the last ComQuery call completes.
func (h *Handler) WarningCount(c *mysql.Conn) uint16 {
	sess, ok := h.sm.sessions[c.ConnectionID]
	if !ok {
		return 0
	}

	return sess.WarningCount()
}

func (h *Handler) handleKill(conn *mysql.Conn, query string) (bool, error) {
	q := strings.ToLower(query)
	s := regKillCmd.FindStringSubmatch(q)
	if s == nil {
		return false, nil
	}

	id, err := strconv.ParseUint(s[2], 10, 64)
	if err != nil {
		return false, err
	}

	// KILL CONNECTION and KILL should close the connection. KILL QUERY only
	// cancels the query.
	//
	// https://dev.mysql.com/doc/refman/5.7/en/kill.html

	if s[1] == "query" {
		logrus.Infof("kill query: id %v", id)
		h.e.Catalog.Kill(id)
	} else {
		logrus.Infof("kill connection: id %v, pid: %v", conn.ConnectionID, id)
		h.mu.Lock()
		c, ok := h.c[conn.ConnectionID]
		delete(h.c, conn.ConnectionID)
		h.mu.Unlock()

		if !ok {
			return false, errConnectionNotFound.New(conn.ConnectionID)
		}

		h.e.Catalog.KillConnection(id)
		h.sm.CloseConn(c)
		c.Close()
	}

	return true, nil
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
