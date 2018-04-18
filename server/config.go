package server

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"
	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

const (
	jaegerDefaultServiceName = "go-mysql-server"
)

// Config for the mysql server.
type Config struct {
	// Protocol for the connection.
	Protocol string
	// Address of the server.
	Address string
	// Auth of the server.
	Auth mysql.AuthServer
}

// Tracer creates a new tracer for the current configuration. It also returns
// an io.Closer to close the tracer and an error, if any.
func (c Config) Tracer() (opentracing.Tracer, io.Closer, error) {
	cfg, err := jaegercfg.FromEnv()
	if err != nil {
		return nil, nil, err
	}

	if cfg.ServiceName == "" {
		cfg.ServiceName = jaegerDefaultServiceName
	}

	return cfg.NewTracer(
		jaegercfg.Logger(jaeger.StdLogger),
		jaegercfg.Metrics(metrics.NullFactory),
	)
}
