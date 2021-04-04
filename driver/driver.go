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

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
)

// EngineFactory resolves SQL engines
type EngineFactory interface {
	Resolve(name string) (string, ProcessManager, *sqle.Engine, error)
}

// New returns a driver using the specified engine factory.
func New(factory EngineFactory) *Driver {
	return &Driver{
		factory: factory,
	}
}

// Driver exposes an engine as a stdlib SQL driver.
type Driver struct {
	factory EngineFactory
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
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	server, pm, engine, err := d.factory.Resolve(name)
	if err != nil {
		return nil, err
	}

	return &Connector{
		server:  server,
		procMgr: pm,
		engine:  engine,
	}, nil
}

// A Connector represents a driver in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
type Connector struct {
	driver  *Driver
	server  string
	procMgr ProcessManager
	engine  *sqle.Engine
}

// Driver returns the driver.
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

// Connect returns a connection to the database.
func (c *Connector) Connect(context.Context) (driver.Conn, error) {
	id := c.procMgr.NextConnectionID()

	session := sql.NewSession(c.server, fmt.Sprintf("#%d", id), "", id)
	indexes := sql.NewIndexRegistry()
	views := sql.NewViewRegistry()
	return &Conn{
		procMgr: c.procMgr,
		engine:  c.engine,
		session: session,
		indexes: indexes,
		views:   views,
	}, nil
}
