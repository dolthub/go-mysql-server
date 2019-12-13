module github.com/src-d/go-mysql-server

require (
	github.com/VividCortex/gohistogram v1.0.0 // indirect
	github.com/go-kit/kit v0.8.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gocraft/dbr v0.0.0-20190708200302-a54124dfc613
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/google/btree v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.3
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mitchellh/hashstructure v1.0.0
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pilosa/pilosa v1.4.0
	github.com/sanity-io/litter v1.2.0
	github.com/sirupsen/logrus v1.4.2
	github.com/spf13/cast v1.3.0
	github.com/src-d/go-oniguruma v1.1.0
	github.com/stretchr/testify v1.4.0
	go.etcd.io/bbolt v1.3.3
	golang.org/x/net v0.0.0-20191119073136-fc4aabc6c914 // indirect
	golang.org/x/tools v0.0.0-20190524140312-2c0ae7006135 // indirect
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/grpc v1.25.1 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v2 v2.2.2
	honnef.co/go/tools v0.0.0-20190523083050-ea95bdfd59fc // indirect
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

replace vitess.io/vitess => github.com/liquidata-inc/vitess v0.0.0-20191211232339-2b6a4d297915

go 1.13
