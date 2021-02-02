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

package auth

import (
	"net"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/go-mysql-server/sql"
)

// AuditMethod is called to log the audit trail of actions.
type AuditMethod interface {
	// Authentication logs an authentication event.
	Authentication(user, address string, err error)
	// Authorization logs an authorization event.
	Authorization(ctx *sql.Context, p Permission, err error)
	// Query logs a query execution.
	Query(ctx *sql.Context, d time.Duration, err error)
}

// MysqlAudit wraps mysql.AuthServer to emit audit trails.
type MysqlAudit struct {
	mysql.AuthServer
	audit AuditMethod
}

// ValidateHash sends authentication calls to an AuditMethod.
func (m *MysqlAudit) ValidateHash(
	salt []byte,
	user string,
	resp []byte,
	addr net.Addr,
) (mysql.Getter, error) {
	getter, err := m.AuthServer.ValidateHash(salt, user, resp, addr)
	m.audit.Authentication(user, addr.String(), err)

	return getter, err
}

// NewAudit creates a wrapped Auth that sends audit trails to the specified
// method.
func NewAudit(auth Auth, method AuditMethod) Auth {
	return &Audit{
		auth:   auth,
		method: method,
	}
}

// Audit is an Auth method proxy that sends audit trails to the specified
// AuditMethod.
type Audit struct {
	auth   Auth
	method AuditMethod
}

// Mysql implements Auth interface.
func (a *Audit) Mysql() mysql.AuthServer {
	return &MysqlAudit{
		AuthServer: a.auth.Mysql(),
		audit:      a.method,
	}
}

// Allowed implements Auth interface.
func (a *Audit) Allowed(ctx *sql.Context, permission Permission) error {
	err := a.auth.Allowed(ctx, permission)
	a.method.Authorization(ctx, permission, err)

	return err
}

// Query implements AuditQuery interface.
func (a *Audit) Query(ctx *sql.Context, d time.Duration, err error) {
	if q, ok := a.auth.(*Audit); ok {
		q.Query(ctx, d, err)
	}

	a.method.Query(ctx, d, err)
}

// NewAuditLog creates a new AuditMethod that logs to a logrus.Logger.
func NewAuditLog(l *logrus.Logger) AuditMethod {
	la := l.WithField("system", "audit")

	return &AuditLog{
		log: la,
	}
}

const auditLogMessage = "audit trail"

// AuditLog logs audit trails to a logrus.Logger.
type AuditLog struct {
	log *logrus.Entry
}

// Authentication implements AuditMethod interface.
func (a *AuditLog) Authentication(user string, address string, err error) {
	fields := logrus.Fields{
		"action":  "authentication",
		"user":    user,
		"address": address,
		"success": true,
	}

	if err != nil {
		fields["success"] = false
		fields["err"] = err
	}

	a.log.WithFields(fields).Info(auditLogMessage)
}

func auditInfo(ctx *sql.Context, err error) logrus.Fields {
	fields := logrus.Fields{
		"user":          ctx.Client().User,
		"query":         ctx.Query(),
		"address":       ctx.Client().Address,
		"connection_id": ctx.Session.ID(),
		"pid":           ctx.Pid(),
		"success":       true,
	}

	if err != nil {
		fields["success"] = false
		fields["err"] = err
	}

	return fields
}

// Authorization implements AuditMethod interface.
func (a *AuditLog) Authorization(ctx *sql.Context, p Permission, err error) {
	fields := auditInfo(ctx, err)
	fields["action"] = "authorization"
	fields["permission"] = p.String()

	a.log.WithFields(fields).Info(auditLogMessage)
}

func (a *AuditLog) Query(ctx *sql.Context, d time.Duration, err error) {
	fields := auditInfo(ctx, err)
	fields["action"] = "query"
	fields["duration"] = d

	a.log.WithFields(fields).Info(auditLogMessage)
}
