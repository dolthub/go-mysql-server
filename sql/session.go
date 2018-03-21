package sql

import "context"

// Session holds the session data.
type Session interface {
	// TODO: add config
}

// BaseSession is the basic session type.
type BaseSession struct {
	// TODO: add config
}

// NewBaseSession creates a new basic session.
func NewBaseSession() Session {
	return &BaseSession{}
}

// Context of the query execution.
type Context struct {
	context.Context
	Session
}

// NewContext creates a new query context.
func NewContext(ctx context.Context, session Session) *Context {
	return &Context{ctx, session}
}
