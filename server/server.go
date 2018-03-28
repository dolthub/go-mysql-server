package server

import (
	"io"

	opentracing "github.com/opentracing/opentracing-go"
	jaeger "github.com/uber/jaeger-client-go"
	"gopkg.in/src-d/go-mysql-server.v0"

	"gopkg.in/src-d/go-vitess.v0/mysql"
)

// Config for the mysql server.
type Config struct {
	// Protocol for the connection.
	Protocol string
	// Address of the server.
	Address string
	// Auth of the server.
	Auth mysql.AuthServer
	// EnableTracing will enable the tracing, if it's true.
	EnableTracing bool
	// TracingAddr is the address where tracing will be sent.
	// If this is empty, and tracing is enabled, it will be reported
	// to the logs via stdout.
	TracingAddr string
	// TracingMaxPacketSize is the max packet size for sending traces
	// to the remote endpoint.
	TracingMaxPacketSize uint64
	// TracingSamplingRate is the rate of traces we want to sample.
	// Only takes effect is TracingAddr is not empty.
	TracingSamplingRate float64
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

// Tracer creates a new tracer for the current configuration. It also returns
// an io.Closer to close the tracer and an error, if any.
func (c Config) Tracer() (opentracing.Tracer, io.Closer, error) {
	if !c.EnableTracing {
		return opentracing.NoopTracer{}, nopCloser{}, nil
	}

	var reporter jaeger.Reporter
	var sampler jaeger.Sampler
	if c.TracingAddr == "" {
		reporter = jaeger.NewLoggingReporter(jaeger.StdLogger)
		sampler = jaeger.NewConstSampler(true)
	} else {
		transport, err := jaeger.NewUDPTransport(
			c.TracingAddr,
			int(c.TracingMaxPacketSize),
		)
		if err != nil {
			return nil, nil, err
		}
		reporter = jaeger.NewRemoteReporter(transport)
		sampler, err = jaeger.NewProbabilisticSampler(c.TracingSamplingRate)
		if err != nil {
			return nil, nil, err
		}
	}

	tracer, closer := jaeger.NewTracer("go-mysql-server", sampler, reporter)
	return tracer, closer, nil
}

// Server is a MySQL server for SQLe engines.
type Server struct {
	Listener *mysql.Listener
	closer   io.Closer
}

// NewDefaultServer creates a Server with the default session builder.
func NewDefaultServer(cfg Config, e *sqle.Engine) (*Server, error) {
	return NewServer(cfg, e, DefaultSessionBuilder)
}

// NewServer creates a server with the given protocol, address, authentication
// details given a SQLe engine and a session builder.
func NewServer(cfg Config, e *sqle.Engine, sb SessionBuilder) (*Server, error) {
	tracer, close, err := cfg.Tracer()
	if err != nil {
		return nil, err
	}

	handler := NewHandler(e, NewSessionManager(sb, tracer))
	l, err := mysql.NewListener(cfg.Protocol, cfg.Address, cfg.Auth, handler)
	if err != nil {
		return nil, err
	}

	return &Server{Listener: l, closer: close}, nil
}

// Start starts accepting connections on the server.
func (s *Server) Start() error {
	s.Listener.Accept()
	return nil
}

// Close closes the server connection.
func (s *Server) Close() error {
	s.Listener.Close()
	return s.closer.Close()
}
