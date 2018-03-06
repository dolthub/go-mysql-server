package sql

import (
	"context"
)

// Session holds context and session data
type Session interface {
	context.Context
}

// BaseSession is the basic session type
type BaseSession struct {
	context.Context
}

// NewBaseSession creates a new basic session
func NewBaseSession(ctx context.Context) Session {
	return &BaseSession{
		Context: ctx,
	}
}
