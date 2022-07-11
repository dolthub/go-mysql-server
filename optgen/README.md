# Optgen

Optgen is a small setup for generating analyzer code from templates derived
from [cockroachdb's optgen](https://github.com/cockroachdb/cockroach/tree/master/pkg/sql/opt/optgen/cmd/optgen).

Usage:
```bash
$ go install ./optgen/cmd/optgen
$ go generate ./...
```

The bulk of analyzer logic is normalization rules, join ordering transforms,
and execution specific code.
Analyzer expressions are mostly boilerplate, and specific normalization
rules and join transforms only need types, fields, and literal values to
manipulate logical query plans.

Leaning into templates and this harness can reduce code footprint and standardize optimizer nodes
when the opportunities arise.

If we end up using more of cockroach DB's optimizer, they codegen their
expressions, normalization rules, exploration rules, and execution code
stem from this general setup.

## Generators

AggGen: `sql` package unary aggregate expression definitions.

FrameGen: `plan` package nodes that implement `sql.WindowFrame`.

FrameFactoryGen: `parse` package function `NewFrame`
that coverts `ast.Frame`into a `sql.WindowFrame` definitions
from `plan`.

FramerGen: `sql` package `sql.WindowFramer` implementations
and `NewFramer` function that converts a `plan.WindowFrame`
into `sql.WindowFramer` implementations.
