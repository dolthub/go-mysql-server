# Tooling to create the package `gopkg.in/src-d/go-vitess.v1`.

# config
PACKAGE := gopkg.in/src-d/go-vitess.v1
REMOTE := git@github.com:src-d/go-vitess.git
VITESS_GIT := git@github.com:vitessio/vitess.git
VITESS_PKG := vitess.io/vitess/go
VITESS_SRC := ${GOPATH}/src/${PACKAGE}

CWD := $(PWD)

all: prepare-package
prepare-package: | filter-branch rename-packages prepare-git commit

clone:
	git clone --single-branch --depth 1 --no-tags ${VITESS_GIT} ${VITESS_SRC}

filter-branch:	clone
	cd ${VITESS_SRC} && \
	git filter-branch --subdirectory-filter go && \
	rm -fr README.md cacheservice cmd exit ioutil2 memcache race ratelimiter stats/influxdbbackend stats/opentsdb stats/prometheusbackend testfiles vtbench zk vt/mysqlctl/cephbackupstorage && \
	cp -f "${CWD}/doc.go" "${CWD}/README.md" ${VITESS_SRC}

rename-packages:
	cd ${VITESS_SRC} && \
	gorep -from=${VITESS_PKG} -to=${PACKAGE}

prepare-git:
	cd ${VITESS_SRC} && \
	git remote rm origin && \
	git remote add origin $(REMOTE)

commit:
	cd ${VITESS_SRC} && \
	git add . && \
	git commit -m "update from upstream ${VITESS_GIT}" -a -s

clean:
	rm -rf ${VITESS_SRC}

.PHONY: clean
