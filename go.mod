module github.com/dolthub/go-mysql-server

require (
	github.com/cespare/xxhash v1.1.0
	github.com/dolthub/sqllogictest/go v0.0.0-20201107003712-816f3ae12d81
	github.com/dolthub/vitess v0.0.0-20250512224608-8fb9c6ea092c
	github.com/go-kit/kit v0.12.0
	github.com/go-sql-driver/mysql v1.8.1
	github.com/gocraft/dbr/v2 v2.7.2
	github.com/google/flatbuffers v23.5.26+incompatible
	github.com/google/uuid v1.6.0
	github.com/hashicorp/golang-lru v1.0.2
	github.com/lestrrat-go/strftime v1.0.4
	github.com/mitchellh/hashstructure v1.1.0
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2
	github.com/shopspring/decimal v1.4.0
	github.com/sirupsen/logrus v1.9.3
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/trace v1.43.0
	golang.org/x/sync v0.20.0
	golang.org/x/text v0.37.0
	golang.org/x/tools v0.44.0
	gopkg.in/src-d/go-errors.v1 v1.0.0
)

require (
	golang.org/x/crypto v0.52.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20251202230838-ff82c1b0f217 // indirect
)

require (
	filippo.io/edwards25519 v1.1.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	golang.org/x/mod v0.35.0 // indirect
	golang.org/x/net v0.54.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/telemetry v0.0.0-20260409153401-be6f6cb8b1fa // indirect
	google.golang.org/grpc v1.79.3 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/oliveagle/jsonpath => github.com/dolthub/jsonpath v0.0.0-20210609232853-d49537a30474

go 1.26.2

replace (
	go.opentelemetry.io/otel => go.opentelemetry.io/otel v1.43.0
	go.opentelemetry.io/otel/metric => go.opentelemetry.io/otel/metric v1.43.0
	go.opentelemetry.io/otel/sdk => go.opentelemetry.io/otel/sdk v1.43.0
	go.opentelemetry.io/otel/sdk/metric => go.opentelemetry.io/otel/sdk/metric v1.43.0
	go.opentelemetry.io/otel/trace => go.opentelemetry.io/otel/trace v1.43.0
)
