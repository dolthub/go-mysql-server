# Tooling to create the package `gopkg.in/src-d/go-vitess.v0`.

# config
PACKAGE := gopkg.in/src-d/go-vitess.v0
REMOTE := git@github.com:src-d/go-vitess.git
VITESS_GIT := https://github.com/youtube/vitess
VITESS_PKG := github.com/youtube/vitess/go/mysql
DEPENDENCIES := \
	github.com/novalagung/gorep \
	github.com/youtube/vitess

VITESS_SRC := ${GOPATH}/src/${PACKAGE}
PACKAGES := $(VITESS_PKG) $(shell go list -f '{{ join .Deps "\n" }}' ${VITESS_PKG} | grep -i vitess)
FOLDERS := $(shell echo ${PACKAGES} | sed -e 's/github.com\/youtube\/vitess\///g')
GIT_COMMIT := $(shell cd ${VITESS_SRC} && git log --format='%H' -n 1)
ETC_PATH := $(PWD)/etc

all: prepare-package
prepare-package: | filter-branch rename-packages prepare-git

$(VITESS_SRC):
	git clone --single-branch --no-tags ${VITESS_GIT} $@

filter-branch: $(VITESS_SRC)
	cd ${VITESS_SRC} && \
		git filter-branch --index-filter ' \
			git rm --cached -qr --ignore-unmatch -- . && \
			git reset -q $$GIT_COMMIT -- ${FOLDERS} \
		' \
	--prune-empty \
	-- --all

rename-packages:
	cd ${VITESS_SRC} && \
		git mv go/* . && \
		gorep -from=github.com/youtube/vitess/go -to=${PACKAGE} && \
		cp -rf ${ETC_PATH}/* . && git add * && \
		git commit -m "update from upstream ${VITESS_GIT}/commit/${GIT_COMMIT}" -a -s

prepare-git:
	cd ${VITESS_SRC} && \
		git remote rm origin && \
		git remote add origin $(REMOTE)

clean:
	rm -rf ${VITESS_SRC}

.PHONY: $(PACKAGES) test clean
