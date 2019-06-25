module github.com/src-d/go-mysql-server

require (
	github.com/StackExchange/wmi v0.0.0-20181212234831-e0a55b97c705 // indirect
	github.com/VividCortex/gohistogram v1.0.0 // indirect
	github.com/go-kit/kit v0.8.0
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/go-sql-driver/mysql v1.4.1
	github.com/gogo/protobuf v1.2.1 // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b
	github.com/golang/protobuf v1.3.0 // indirect
	github.com/gorilla/handlers v1.4.0 // indirect
	github.com/mitchellh/hashstructure v1.0.0
	github.com/oliveagle/jsonpath v0.0.0-20180606110733-2e52cf6e6852
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pbnjay/memory v0.0.0-20190104145345-974d429e7ae4
	github.com/pilosa/pilosa v1.3.0
	github.com/sanity-io/litter v1.1.0
	github.com/shirou/w32 v0.0.0-20160930032740-bb4de0191aa4 // indirect
	github.com/sirupsen/logrus v1.3.0
	github.com/spf13/cast v1.3.0
	github.com/src-d/go-oniguruma v1.0.0
	github.com/stretchr/testify v1.2.2
	go.etcd.io/bbolt v1.3.2
	golang.org/x/net v0.0.0-20190227022144-312bce6e941f // indirect
	google.golang.org/grpc v1.19.0 // indirect
	gopkg.in/src-d/go-errors.v1 v1.0.0
	gopkg.in/yaml.v2 v2.2.2
	vitess.io/vitess v3.0.0-rc.3.0.20190602171040-12bfde34629c+incompatible
)

replace vitess.io/vitess => github.com/liquidata-inc/vitess

// For local development, clone vitess into $GOPATH/src like so: git clone git@github.com:liquidata-inc/vitess.git vitess.io/vitess
// Then use this local override:
//replace vitess.io/vitess => ../../../vitess.io/vitess
