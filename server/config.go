package server

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-lib/metrics"
	errors "gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-vitess.v0/mysql"
)

const (
	// environment variable names
	envServiceName            = "JAEGER_SERVICE_NAME"
	envDisabled               = "JAEGER_DISABLED"
	envRPCMetrics             = "JAEGER_RPC_METRICS"
	envTags                   = "JAEGER_TAGS"
	envSamplerType            = "JAEGER_SAMPLER_TYPE"
	envSamplerParam           = "JAEGER_SAMPLER_PARAM"
	envSamplerManagerHostPort = "JAEGER_SAMPLER_MANAGER_HOST_PORT"
	envSamplerMaxOperations   = "JAEGER_SAMPLER_MAX_OPERATIONS"
	envSamplerRefreshInterval = "JAEGER_SAMPLER_REFRESH_INTERVAL"
	envReporterMaxQueueSize   = "JAEGER_REPORTER_MAX_QUEUE_SIZE"
	envReporterFlushInterval  = "JAEGER_REPORTER_FLUSH_INTERVAL"
	envReporterLogSpans       = "JAEGER_REPORTER_LOG_SPANS"
	envAgentHost              = "JAEGER_AGENT_HOST"
	envAgentPort              = "JAEGER_AGENT_PORT"

	jaegerDefaultUDPSpanServerHost = "localhost"
	jaegerDefaultUDPSpanServerPort = 6831
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
	cfg := &jaegercfg.Configuration{}

	serviceName := "go-mysql-server"
	if e := os.Getenv(envServiceName); e != "" {
		serviceName = e
	}

	if e := os.Getenv(envRPCMetrics); e != "" {
		if value, err := strconv.ParseBool(e); err == nil {
			cfg.RPCMetrics = value
		} else {
			return nil, nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envRPCMetrics, e)
		}
	}

	if e := os.Getenv(envDisabled); e != "" {
		if value, err := strconv.ParseBool(e); err == nil {
			cfg.Disabled = value
		} else {
			return nil, nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envDisabled, e)
		}
	}

	if sampler, err := samplerConfigFromEnv(); err == nil {
		cfg.Sampler = sampler
	} else {
		return nil, nil, errors.NewKind("cannot obtain sampler config from env").Wrap(err)
	}

	if reporter, err := reporterConfigFromEnv(); err == nil {
		cfg.Reporter = reporter
	} else {
		return nil, nil, errors.NewKind("cannot obtain reporter config from env").Wrap(err)
	}

	var opts = []jaegercfg.Option{
		jaegercfg.Metrics(metrics.NullFactory),
		jaegercfg.Logger(jaeger.StdLogger),
	}
	if e := os.Getenv(envTags); e != "" {
		tags := parseTags(e)
		for _, tag := range tags {
			opts = append(opts, jaegercfg.Tag(tag.Key, tag.Value))
		}
	}

	tracer, closer, err := cfg.New(serviceName, opts...)
	if err != nil {
		return nil, nil, errors.NewKind("Could not initialize jaeger tracer").Wrap(err)
	}
	opentracing.SetGlobalTracer(tracer)

	return tracer, closer, err
}

// samplerConfigFromEnv creates a new SamplerConfig based on the environment variables
func samplerConfigFromEnv() (*jaegercfg.SamplerConfig, error) {
	sc := &jaegercfg.SamplerConfig{}

	if e := os.Getenv(envSamplerType); e != "" {
		sc.Type = e
	}

	if e := os.Getenv(envSamplerParam); e != "" {
		if value, err := strconv.ParseFloat(e, 64); err == nil {
			sc.Param = value
		} else {
			return nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envSamplerParam, e)
		}
	}

	if e := os.Getenv(envSamplerManagerHostPort); e != "" {
		sc.SamplingServerURL = e
	}

	if e := os.Getenv(envSamplerMaxOperations); e != "" {
		if value, err := strconv.ParseInt(e, 10, 0); err == nil {
			sc.MaxOperations = int(value)
		} else {
			return nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envSamplerMaxOperations, e)
		}
	}

	if e := os.Getenv(envSamplerRefreshInterval); e != "" {
		if value, err := time.ParseDuration(e); err == nil {
			sc.SamplingRefreshInterval = value
		} else {
			return nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envSamplerRefreshInterval, e)
		}
	}

	return sc, nil
}

// reporterConfigFromEnv creates a new ReporterConfig based on the environment variables
func reporterConfigFromEnv() (*jaegercfg.ReporterConfig, error) {
	rc := &jaegercfg.ReporterConfig{}

	if e := os.Getenv(envReporterMaxQueueSize); e != "" {
		if value, err := strconv.ParseInt(e, 10, 0); err == nil {
			rc.QueueSize = int(value)
		} else {
			return nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envReporterMaxQueueSize, e)
		}
	}

	if e := os.Getenv(envReporterFlushInterval); e != "" {
		if value, err := time.ParseDuration(e); err == nil {
			rc.BufferFlushInterval = value
		} else {
			return nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envReporterFlushInterval, e)
		}
	}

	if e := os.Getenv(envReporterLogSpans); e != "" {
		if value, err := strconv.ParseBool(e); err == nil {
			rc.LogSpans = value
		} else {
			return nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envReporterLogSpans, e)
		}
	}

	host := jaegerDefaultUDPSpanServerHost
	if e := os.Getenv(envAgentHost); e != "" {
		host = e
	}

	port := jaegerDefaultUDPSpanServerPort
	if e := os.Getenv(envAgentPort); e != "" {
		if value, err := strconv.ParseInt(e, 10, 0); err == nil {
			port = int(value)
		} else {
			return nil, errors.NewKind("cannot parse env var %s=%s").Wrap(err, envAgentPort, e)
		}
	}

	// the side effect of this is that we are building the default value, even if none of the env vars
	// were not explicitly passed
	rc.LocalAgentHostPort = fmt.Sprintf("%s:%d", host, port)

	return rc, nil
}

// parseTags parses the given string into a collection of Tags.
// Spec for this value:
// - comma separated list of key=value
// - value can be specified using the notation ${envVar:defaultValue}, where `envVar`
// is an environment variable and `defaultValue` is the value to use in case the env var is not set
func parseTags(sTags string) []opentracing.Tag {
	pairs := strings.Split(sTags, ",")
	tags := make([]opentracing.Tag, 0)
	for _, p := range pairs {
		kv := strings.SplitN(p, "=", 2)
		k, v := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])

		if strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") {
			ed := strings.SplitN(v[2:len(v)-1], ":", 2)
			e, d := ed[0], ed[1]
			v = os.Getenv(e)
			if v == "" && d != "" {
				v = d
			}
		}

		tag := opentracing.Tag{Key: k, Value: v}
		tags = append(tags, tag)
	}

	return tags
}
