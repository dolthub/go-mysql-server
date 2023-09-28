# Plangen

This tool makes it easy to regenerate plan tests for our current plan suites:
- `PlanTests`
- `IndexPlanTests`
- `IntegrationPlanTests`
- `ImdbPlanTests`
- `TpchPlanTests`
- `TpcdsPlanTests`

Devs still need to human-verify the correctness of plans created by this tool.

The `main.go` file has a go generator line. The `testdata/spec.yaml` includes
specifications of file paths and objects that seed in-place rewrites.

# Background

We have suites of "plan tests" that verify transform logic without executing
plans. These have the obvious downside of not testing correctness, which
is why most tests in the default `PlanTests` suite have companion correctness
tests in `QueryTests`. Plan differences are easier to surface and root cause
than data-dependent correctness failures, which makes them more flexible and
user-friendly. When performing refactors or fixing correctness regressions, for
example, plans are usually the easiest way to identify the breaking change at
the first abstraction that matters: the execution plan.


