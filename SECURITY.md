# Security Policy

## Supported Versions

This repository receives periodic releases, but is mostly consumed at specific
Git commits. By default, the tip of `main` is the release artifact which is
supported for all security updates. For impactful security fixes, new released
versions will also be created. If you need ongoing security support for an
older version of go-mysql-server, please [contact
DoltHub](https://www.dolthub.com/contact), the company behind this project.

## Scope

This code is not fully robust against generating `panic`s in its handling of
untrusted input like SQL queries. Code calling into this project should always
`recover()` if it needs to avoid crashing in certain error cases. Note that
`recover()` only works in the goroutine where the `panic` occurs, so this is
only sufficient for panics which unwind back into your calling code.

For the time being, these panics are treated as bugs, not security issues.
Please report them in GitHub Issues and we will be happy to address them.

A `panic` which a caller cannot `recover()` from is still a security issue. In
particular, a `panic` that reaches the top of a goroutine which this project
owns or spawned will crash the process regardless of what the caller does.
Please report those to the address below instead.

## Reporting a Vulnerability

Any security issues with go-mysql-server can be reported to [security@dolthub.com](mailto:security@dolthub.com).

Reports will be responded to within one business day. The majority of
our team operates on Pacific Time and on a US holiday schedule.

DoltHub does not currently run a security bounty program for go-mysql-server.
