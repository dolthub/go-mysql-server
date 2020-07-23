module github.com/liquidata-inc/go-mysql-server

require (
	github.com/VividCortex/gohistogram v1.0.0 // indirect
	github.com/fastly/go-utils v0.0.0-20180712184237-d95a45783239 // indirect
	github.com/go-kit/kit v0.9.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/hashicorp/golang-lru v0.5.3
	github.com/jehiah/go-strftime v0.0.0-20171201141054-1d33003b3869 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/lestrrat-go/strftime v1.0.1
	github.com/mitchellh/hashstructure v1.0.0
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pilosa/pilosa v1.4.0
	github.com/pmezard/go-difflib v1.0.0
	github.com/sanity-io/litter v1.2.0
	github.com/shopspring/decimal v0.0.0-20191130220710-360f2bc03045
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cast v1.3.0
	github.com/src-d/go-oniguruma v1.1.0
	github.com/stretchr/testify v1.4.0
	github.com/tebeka/strftime v0.1.4 // indirect
	go.etcd.io/bbolt v1.3.3
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/grpc v1.27.0 // indirect
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v2 v2.2.4
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

replace vitess.io/vitess => github.com/liquidata-inc/vitess v0.0.0-20200723020829-dc668aea2a8c

go 1.13
