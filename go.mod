module github.com/dolthub/go-mysql-server

require (
	github.com/cespare/xxhash v1.1.0
	//github.com/dolthub/dolt/go v0.0.0-20211015192641-fded10e24879
	//github.com/dolthub/dolt/go v0.0.0-20211015192641-fded10e24879
	github.com/dolthub/sqllogictest/go v0.0.0-20201107003712-816f3ae12d81
	//github.com/dolthub/vitess v2.1.1+incompatible // indirect
	//github.com/dolthub/vitess v0.0.0-20211013185428-a8845fb919c1
	github.com/go-kit/kit v0.10.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529
	github.com/google/uuid v1.2.0
	github.com/hashicorp/golang-lru v0.5.4
	github.com/lestrrat-go/strftime v1.0.4
	github.com/mitchellh/hashstructure v1.1.0
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pmezard/go-difflib v1.0.0
	github.com/sanity-io/litter v1.2.0
	github.com/shopspring/decimal v1.2.0
	github.com/sirupsen/logrus v1.8.1
	github.com/src-d/go-oniguruma v1.1.0
	github.com/stretchr/testify v1.7.0
	golang.org/x/sync v0.0.0-20201020160332-67f06af15bc9
	gopkg.in/src-d/go-errors.v1 v1.0.0
)

replace github.com/oliveagle/jsonpath => github.com/dolthub/jsonpath v0.0.0-20210609232853-d49537a30474

replace github.com/dolthub/dolt => /Users/max-hoffman/go/src/github.com/dolthub/dolt

go 1.15

replace github.com/dolthub/vitess => /Users/max-hoffman/go/src/github.com/dolthub/vitess
