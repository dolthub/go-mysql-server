// Copyright 2020-2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"sync"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
)

// ScanKind indicates how values should be scanned.
type ScanKind int

const (
	// ScanAsString indicates values should be scanned as strings.
	//
	// Applies to JSON columns.
	ScanAsString ScanKind = iota

	// ScanAsBytes indicates values should be scanned as byte arrays.
	//
	// Applies to JSON columns.
	ScanAsBytes

	// ScanAsObject indicates values should be scanned as objects.
	//
	// Applies to JSON columns.
	ScanAsObject

	// ScanAsStored indicates values should not be modified during scanning.
	//
	// Applies to JSON columns.
	ScanAsStored
)

// Options for the driver.
type Options struct {
	// JSON indicates how JSON row values should be scanned
	JSON ScanKind
}

// Provider resolves SQL catalogs from DSNs.
//
// Provider can optionally implement ProviderWithSessionBuilder and ProviderWithSessionBuilder.
type Provider interface {
	// Resolve determines the server name and DatabaseProvider for the given dsn.
	// The will create a separate sql.Engine for each "server name" value.
	Resolve(dsn string, options *Options) (string, sql.DatabaseProvider, error)
}

// ProviderWithContextBuilder is a Provider that also provides a base sql.Context.
// Provider should parse the DSN and use it to adjust the context in NewContext.
type ProviderWithContextBuilder interface {
	Provider
	ContextBuilder
}

// ProviderWithSessionBuilder is a Provider that also constructs sessions.
type ProviderWithSessionBuilder interface {
	Provider
	SessionBuilder
}

// A Driver exposes an engine as a stdlib SQL driver.
type Driver struct {
	provider Provider
	options  *Options
	sessions SessionBuilder
	contexts ContextBuilder

	mu  sync.Mutex
	dbs map[string]*dbConn
}

// New returns a driver using the specified provider.
func New(provider Provider, options *Options) *Driver {
	var sessionBuilder SessionBuilder
	if provWithSessionBuilder, ok := provider.(ProviderWithSessionBuilder); ok {
		sessionBuilder = provWithSessionBuilder
	} else {
		sessionBuilder = NewDefaultSessionBuilder()
	}

	var contextBuilder ContextBuilder
	if provWithCtxBuilder, ok := provider.(ProviderWithContextBuilder); ok {
		contextBuilder = provWithCtxBuilder
	} else {
		contextBuilder = DefaultContextBuilder{}
	}

	return &Driver{
		provider: provider,
		options:  options,
		sessions: sessionBuilder,
		contexts: contextBuilder,
		dbs:      map[string]*dbConn{},
	}
}

// Open returns a new connection to the database.
func (d *Driver) Open(name string) (driver.Conn, error) {
	conn, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return conn.Connect(context.Background())
}

// OpenConnector calls the driver factory and returns a new connector.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	options := d.options // copy
	if options == nil {
		options = &Options{}
	}

	dsnURI, err := url.Parse(dsn)
	if err == nil {
		query := dsnURI.Query()
		qJSON := query.Get("jsonAs")
		switch qJSON {
		case "":
			// default
		case "object":
			options.JSON = ScanAsObject
		case "string":
			options.JSON = ScanAsString
		case "bytes":
			options.JSON = ScanAsBytes
		default:
			return nil, fmt.Errorf("%q is not a valid option for 'jsonAs'", qJSON)
		}

		query.Del("jsonAs")
		dsnURI.RawQuery = query.Encode()
		dsn = dsnURI.String()
	}

	serverName, pro, err := d.provider.Resolve(dsn, options)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	db, ok := d.dbs[serverName]
	if !ok {
		anlz := analyzer.NewDefaultWithVersion(pro)
		engine := sqle.New(anlz, nil)
		db = &dbConn{engine: engine}
		d.dbs[serverName] = db
	}
	d.mu.Unlock()

	return &Connector{
		driver:     d,
		options:    options,
		serverName: serverName,
		dsn:        dsn,
		dbConn:     db,
	}, nil
}

func (d *Driver) Close() error {
	var firstErr error
	for _, conn := range d.dbs {
		if firstErr == nil {
			firstErr = conn.close()
		} else {
			conn.close()
		}
	}
	return firstErr
}

type dbConn struct {
	engine *sqle.Engine

	mu     sync.Mutex
	connID uint32
	procID uint64
}

func (c *dbConn) nextConnectionID() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connID++
	return c.connID
}

func (c *dbConn) nextProcessID() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.procID++
	return c.procID
}

func (c *dbConn) close() error {
	return c.engine.Close()
}

// A Connector represents a driver in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
type Connector struct {
	driver     *Driver
	options    *Options
	serverName string
	dsn        string
	dbConn     *dbConn
}

// Driver returns the driver.
func (c *Connector) Driver() driver.Driver { return c.driver }

// Server returns the server name.
func (c *Connector) Server() string { return c.serverName }

// DSN returns the original DSN passed by the client.
func (c *Connector) DSN() string { return c.dsn }

// Connect returns a connection to the database.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	id := c.dbConn.nextConnectionID()

	session, err := c.driver.sessions.NewSession(ctx, id, c)
	if err != nil {
		return nil, err
	}

	indexes := sql.NewIndexRegistry()
	views := sql.NewViewRegistry()
	return &Conn{
		dsn:      c.dsn,
		options:  c.options,
		dbConn:   c.dbConn,
		session:  session,
		contexts: c.driver.contexts,
		indexes:  indexes,
		views:    views,
	}, nil
}
