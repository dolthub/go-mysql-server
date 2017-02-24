package gitql

import (
	gosql "database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"gopkg.in/sqle/sqle.v0/sql"
	"gopkg.in/sqle/sqle.v0/sql/analyzer"
	"gopkg.in/sqle/sqle.v0/sql/expression"
	"gopkg.in/sqle/sqle.v0/sql/parse"
)

var (
	ErrNotSupported = errors.New("feature not supported yet")
)

const (
	DriverName = "gitql"
)

func init() {
	gosql.Register(DriverName, defaultDriver)
}

type drv struct{}

var defaultDriver = &drv{}

func (d *drv) Open(name string) (driver.Conn, error) {
	if name != "" {
		return nil, fmt.Errorf("data source not found: %s", name)
	}

	e := DefaultEngine
	return &session{Engine: e}, nil
}

// DefaultEngine is the default Engine instance, used when opening a connection
// to gitql:// when using database/sql.
var DefaultEngine = New()

// Engine is a SQL engine.
// It implements the standard database/sql/driver/Driver interface, so it can
// be registered as a database/sql driver.
type Engine struct {
	Catalog  *sql.Catalog
	Analyzer *analyzer.Analyzer
}

// New creates a new Engine.
func New() *Engine {
	c := sql.NewCatalog()
	err := expression.RegisterDefaults(c)
	if err != nil {
		panic(err)
	}

	a := analyzer.New(c)
	return &Engine{c, a}
}

// Open creates a new session for the engine and returns
// it as a driver.Conn.
//
// Name parameter is ignored.
func (e *Engine) Open(name string) (driver.Conn, error) {
	return &session{Engine: e}, nil
}

// Query executes a query without attaching to any session.
func (e *Engine) Query(query string) (sql.Schema, sql.RowIter, error) {
	parsed, err := parse.Parse(query)
	if err != nil {
		return nil, nil, err
	}

	analyzed, err := e.Analyzer.Analyze(parsed)
	if err != nil {
		return nil, nil, err
	}

	iter, err := analyzed.RowIter()
	if err != nil {
		return nil, nil, err
	}

	return analyzed.Schema(), iter, nil
}

func (e *Engine) AddDatabase(db sql.Database) {
	e.Catalog.Databases = append(e.Catalog.Databases, db)
	e.Analyzer.CurrentDatabase = db.Name()
}

// Session represents a SQL session.
// It implements the standard database/sql/driver/Conn interface.
type session struct {
	*Engine
	closed bool
	//TODO: Current database
}

// Prepare returns a prepared statement, bound to this connection.
// Placeholders are not supported yet.
func (s *session) Prepare(query string) (driver.Stmt, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}

	return &stmt{session: s, query: query}, nil
}

// Close closes the session.
func (s *session) Close() error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	s.closed = true
	return nil
}

// Begin starts and returns a new transaction.
func (s *session) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("transactions not supported")
}

func (s *session) checkOpen() error {
	if s.closed {
		return driver.ErrBadConn
	}

	return nil
}

type stmt struct {
	*session
	query  string
	closed bool
}

// Close closes the statement.
func (s *stmt) Close() error {
	if err := s.checkOpen(); err != nil {
		return err
	}

	s.closed = true
	return nil
}

// NumInput returns the number of placeholder parameters.
// Always returns -1 since placeholders are not supported yet.
func (s *stmt) NumInput() int {
	return -1
}

// Exec executes a query that doesn't return rows, such as an INSERT or UPDATE.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, ErrNotSupported
}

// Query executes a query that may return rows, such as a SELECT.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, ErrNotSupported
	}

	schema, iter, err := s.session.Engine.Query(s.query)
	if err != nil {
		return nil, err
	}

	return &rows{schema: schema, iter: iter}, nil
}

func (s *stmt) checkOpen() error {
	if s.closed {
		return driver.ErrBadConn
	}

	return nil
}

type rows struct {
	schema sql.Schema
	iter   sql.RowIter
}

// Columns returns the names of the columns.
func (rs *rows) Columns() []string {
	c := make([]string, len(rs.schema))
	for i := 0; i < len(rs.schema); i++ {
		c[i] = rs.schema[i].Name
	}

	return c
}

// Close closes the rows iterator.
func (rs *rows) Close() error {
	return rs.iter.Close()
}

// Next populates the given array with the next row values.
// Returns io.EOF when there are no more values.
func (rs *rows) Next(dest []driver.Value) error {
	r, err := rs.iter.Next()
	if err != nil {
		return err
	}

	for i := range dest {
		f := rs.schema[i]
		dest[i] = f.Type.Native(r[i])
	}

	return nil
}
