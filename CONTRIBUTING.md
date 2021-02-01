# Dolthub Inc. Contributing Guidelines

Dolthub Inc. projects accept contributions via GitHub pull requests.
This document outlines some of the conventions on development
workflow, commit message formatting, contact points, and other
resources to make it easier to get your contribution accepted.

## Contributor License Agreement (CLA)

By contributing to this project you agree to the Contributor License
Agreement (CLA). This grants copyright of your work on the project to
the project owners, Dolthub Inc, as well as waives certain other
rights related to your contribution. The first time you submit a PR,
you will be prompted to read and sign the CLA. We cannot accept
contributions that do not sign the CLA.

## Support Channel

The official support channel, for both users and contributors, is
GitHub issues.

## How to Contribute

Pull Requests (PRs) are the exclusive way to contribute code to
go-mysql-server.  In order for a PR to be accepted it needs to pass a
list of requirements:

- The contribution must be correctly explained with natural language
  and providing a minimum working example that reproduces it.
- All PRs must be written idiomatically:
    - for Go: formatted according to
      [gofmt](https://golang.org/cmd/gofmt/), and without any warnings
      from [go lint](https://github.com/golang/lint) nor [go
      vet](https://golang.org/cmd/vet/)
    - for other languages, similar constraints apply.
- They should in general include tests, and those shall pass.
    - If the PR is a bug fix, it has to include a new unit test that
      fails before the patch is merged.
    - If the PR is a new feature, it has to come with a suite of unit
      tests, that tests the new functionality.
    - In any case, all the PRs have to pass the personal evaluation of
      at least one of the [maintainers](MAINTAINERS) of the project.

### Getting started

If you are a new contributor to the project, reading
[ARCHITECTURE.md](/ARCHITECTURE.md) is highly recommended, as it
contains all the details about the architecture of go-mysql-server and
its components.
