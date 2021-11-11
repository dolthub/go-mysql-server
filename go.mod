module github.com/dolthub/go-mysql-server

require (
	github.com/cespare/xxhash v1.1.0
	github.com/dolthub/sqllogictest/go v0.0.0-20201107003712-816f3ae12d81
	github.com/dolthub/vitess v0.0.0-20211108230711-9b4006809d3b
	github.com/go-kit/kit v0.10.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529
	github.com/google/uuid v1.2.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/kr/text v0.2.0 // indirect
	github.com/lestrrat-go/strftime v1.0.4
	github.com/mitchellh/hashstructure v1.1.0
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0
	github.com/sanity-io/litter v1.2.0
	github.com/shopspring/decimal v1.2.0
	github.com/sirupsen/logrus v1.8.1
	github.com/src-d/go-oniguruma v1.1.0
	github.com/stretchr/testify v1.7.0
	golang.org/x/net v0.0.0-20210505214959-0714010a04ed // indirect
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	golang.org/x/sys v0.0.0-20210503173754-0981d6026fa6 // indirect
	golang.org/x/tools v0.1.0 // indirect
	google.golang.org/genproto v0.0.0-20210506142907-4a47615972c2 // indirect
	google.golang.org/grpc v1.37.0 // indirect
	gopkg.in/check.v1 v1.0.0-20200902074654-038fdea0a05b // indirect
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776 // indirect
)

replace github.com/oliveagle/jsonpath => github.com/dolthub/jsonpath v0.0.0-20210609232853-d49537a30474

go 1.15
