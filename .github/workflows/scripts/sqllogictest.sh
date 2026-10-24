#!/bin/bash
#
# Helpers for running the sqllogictest suite (github.com/dolthub/sqllogictest) against the memory engine in CI.
#
# Usage:
#   sqllogictest.sh run <logictest binary> <test dir> <shard index> <shard count> <out file>
#     Runs every <shard count>th test file (balanced by file size) under <test dir> and writes the results log.
#   sqllogictest.sh compare <base results> <pr results>
#     Prints a summary of both result logs and exits non-zero if any test that passes in <base results> does not
#     pass in <pr results>.

set -eo pipefail
export LC_ALL=C

# results prints "<file>:<line> <result>" for every result entry in the log |$1|. Panic stack traces and other output
# are ignored.
results() {
  awk '
    $3 ~ /^[^ ]+:[0-9]+:$/ {
      key = substr($3, 1, length($3) - 1)
      if (index($0, " not ok: ") > 0) { res = "not_ok" }
      else if ($0 ~ / did not run$/) { res = "did_not_run" }
      else if ($0 ~ / timeout$/) { res = "timeout" }
      else if ($0 ~ / skipped$/) { res = "skipped" }
      else if ($0 ~ / ok$/) { res = "ok" }
      else { next }
      print key, res
    }' "$1" | sort -u -k1,1
}

summarize() {
  local name="$1" file="$2"
  local ok total
  ok=$(awk '$2 == "ok"' "$file" | wc -l | tr -d ' ')
  total=$(awk '$2 != "skipped"' "$file" | wc -l | tr -d ' ')
  echo "$name: $ok / $total passing ($(awk -v ok="$ok" -v t="$total" 'BEGIN { printf "%.6f", t ? 100 * ok / t : 0 }')%)"
}

case "$1" in
  run)
    bin="$2"; dir="$3"; shard="$4"; count="$5"; out="$6"
    files=$(find "$dir" -name '*.test' -print0 | xargs -0 ls -S | awk -v s="$shard" -v c="$count" '(NR - 1) % c == s')
    echo "Running $(echo "$files" | wc -l | tr -d ' ') test files"
    tmp=$(mktemp -d)
    # Each file runs in its own process since the logictest runner keeps global state.
    echo "$files" | xargs -P "$(getconf _NPROCESSORS_ONLN)" -I {} sh -c '"$1" run "$2" > "$3/$(echo "$2" | tr / _).log" 2>&1 || true' _ "$bin" {} "$tmp"
    cat "$tmp"/*.log > "$out"
    ;;
  compare)
    base="$2"; pr="$3"
    tmp=$(mktemp -d)
    results "$base" > "$tmp/base"
    results "$pr" > "$tmp/pr"
    summarize "main" "$tmp/base"
    summarize "PR  " "$tmp/pr"

    # Tests that pass on main but don't pass (or are missing) on the PR
    join -a 1 -e missing -o 0,1.2,2.2 "$tmp/base" "$tmp/pr" | awk '$2 == "ok" && $3 != "ok"' > "$tmp/regressions"
    # Tests that pass on the PR but not on main
    join -a 2 -e missing -o 0,1.2,2.2 "$tmp/base" "$tmp/pr" | awk '$3 == "ok" && $2 != "ok"' > "$tmp/fixes"

    echo
    echo "Newly passing tests: $(wc -l < "$tmp/fixes" | tr -d ' ')"
    head -n 50 "$tmp/fixes" | awk '{ print "  " $1 }'

    echo
    echo "Regressions: $(wc -l < "$tmp/regressions" | tr -d ' ')"
    if [ -s "$tmp/regressions" ]; then
      while read -r key _ res; do
        echo "  $key ($res)"
        # Show the PR's result line, which includes the query and failure message
        { grep -F " $key: " "$pr" || true; } | head -n 1 | cut -c 1-1000 | sed 's/^/    /'
      done < <(head -n 200 "$tmp/regressions")
      exit 1
    fi
    ;;
  *)
    echo "Unrecognized command $1"
    exit 1
    ;;
esac
