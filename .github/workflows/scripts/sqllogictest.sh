#!/bin/bash
#
# Helpers for running the sqllogictest suite (github.com/dolthub/sqllogictest) against the memory engine in CI.
#
# Usage:
#   sqllogictest.sh run <logictest binary> <test dir> <shard index> <shard count> <out file> [parallelism]
#     Runs every <shard count>th test file (balanced by file size) under <test dir> and writes the results log. Runs
#     [parallelism] test files at a time, defaulting to the number of CPUs. If a test file's process exits with an
#     error, e.g. from a crash, the log records it and the remaining test files still run.
#   sqllogictest.sh compare <base results> <pr results>
#     Prints a summary of both result logs and exits non-zero if any test that passes in <base results> does not
#     pass in <pr results>, if any test process crashed in <pr results>, if less than MIN_PASS_PCT percent of tests
#     pass in <pr results>, or if any test file is more than SLOWDOWN_FACTOR times slower in <pr results> (and slower
#     by at least SLOWDOWN_MIN_MS, to ignore noise in fast files). Only tests that pass in both logs count towards a
#     file's time, since a failing test's time says nothing about performance.

set -eo pipefail
export LC_ALL=C

SLOWDOWN_FACTOR=${SLOWDOWN_FACTOR:-2}
SLOWDOWN_MIN_MS=${SLOWDOWN_MIN_MS:-10000}
MIN_PASS_PCT=${MIN_PASS_PCT:-99.99}
SKIP_FILE="$(dirname "$0")/sqllogictest-skip.txt"

# results prints "<file>:<line> <result>" for every result entry in the log |$1|. Panic stack traces and other output
# are ignored. Records listed in SKIP_FILE are reported as skipped.
results() {
  awk '
    FILENAME == ARGV[1] {
      if ($1 != "" && $1 !~ /^#/) { skip[$1] = 1 }
      next
    }
    $3 ~ /^[^ ]+:[0-9]+:$/ {
      key = substr($3, 1, length($3) - 1)
      if (key in skip) { res = "skipped" }
      else if (index($0, " not ok: ") > 0) { res = "not_ok" }
      else if ($0 ~ / did not run$/) { res = "did_not_run" }
      else if ($0 ~ / timeout$/) { res = "timeout" }
      else if ($0 ~ / skipped$/) { res = "skipped" }
      else if ($0 ~ / ok$/) { res = "ok" }
      else { next }
      print key, res
    }' "$SKIP_FILE" "$1" | sort -u -k1,1
}

# query_times prints "<file>:<line> <milliseconds>" for every result entry in the log |$1|.
query_times() {
  awk '$3 ~ /^[^ ]+:[0-9]+:$/ && $2 ~ /^[0-9]+$/ { print substr($3, 1, length($3) - 1), $2 }' "$1" | sort -u -k1,1
}

pass_pct() {
  awk '$2 != "skipped" { t++ } $2 == "ok" { ok++ } END { printf "%.6f", t ? 100 * ok / t : 0 }' "$1"
}

summarize() {
  local name="$1" file="$2"
  local ok total
  ok=$(awk '$2 == "ok"' "$file" | wc -l | tr -d ' ')
  total=$(awk '$2 != "skipped"' "$file" | wc -l | tr -d ' ')
  echo "$name: $ok / $total passing ($(pass_pct "$file")%)"
}

case "$1" in
  run)
    bin="$2"; dir="$3"; shard="$4"; count="$5"; out="$6"; parallelism="${7:-$(getconf _NPROCESSORS_ONLN)}"
    files=$(find "$dir" -name '*.test' -print0 | xargs -0 ls -S | awk -v s="$shard" -v c="$count" '(NR - 1) % c == s')
    echo "Running $(echo "$files" | wc -l | tr -d ' ') test files"
    tmp=$(mktemp -d)
    # Each file runs in its own process since the logictest runner keeps global state. The runner only exits with an
    # error if the whole process fails (failing and panicking queries are logged as results), so record that in the log
    # instead of exiting, which would lose the results of every other file.
    echo "$files" | xargs -P "$parallelism" -I {} sh -c '
      log="$3/$(echo "$2" | tr / _).log"
      "$1" run "$2" > "$log" 2>&1 || echo "CRASHED: ${2#"$4"/} exited with status $?" >> "$log"
    ' _ "$bin" {} "$tmp" "$dir"
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

    # "<file>:<line> <main ms> <PR ms>" for every test that passes on both main and the PR
    join "$tmp/base" "$tmp/pr" | awk '$2 == "ok" && $3 == "ok" { print $1 }' > "$tmp/both_ok"
    query_times "$base" > "$tmp/base_times"
    query_times "$pr" > "$tmp/pr_times"
    join "$tmp/base_times" "$tmp/pr_times" > "$tmp/all_times"
    join "$tmp/both_ok" "$tmp/all_times" > "$tmp/times"
    # "<file> <main ms> <PR ms>" for files that are too much slower on the PR
    awk '{ split($1, parts, ":"); b[parts[1]] += $2; p[parts[1]] += $3 } END { for (f in b) print f, b[f], p[f] }' "$tmp/times" \
      | sort -k1,1 | awk -v f="$SLOWDOWN_FACTOR" -v min="$SLOWDOWN_MIN_MS" '$3 > f * $2 && $3 - $2 >= min' > "$tmp/slow"
    echo
    awk '{ b += $2; p += $3 } END { printf "Total time of tests passing on both: main %.1fs, PR %.1fs\n", b / 1000, p / 1000 }' "$tmp/times"

    echo
    echo "Slowest queries:"
    for v in base pr; do
      [ "$v" = base ] && { name="main"; log="$base"; } || { name="PR"; log="$pr"; }
      echo "  $name"
      awk '$3 ~ /^[^ ]+:[0-9]+:$/ && $2 ~ /^[0-9]+$/ { print $2, substr($3, 1, length($3) - 1) }' "$log" \
        | sort -rn | awk 'NR <= 10' | awk '{ printf "    %8.1fs  %s\n", $1 / 1000, $2 }'
    done

    failed=0
    echo
    { grep '^CRASHED: ' "$base" || true; } | sed 's/^CRASHED: //' > "$tmp/base_crashes"
    { grep '^CRASHED: ' "$pr" || true; } | sed 's/^CRASHED: //' > "$tmp/pr_crashes"
    echo "Test processes that crashed: main $(wc -l < "$tmp/base_crashes" | tr -d ' '), PR $(wc -l < "$tmp/pr_crashes" | tr -d ' ')"
    sed 's/^/  main: /' "$tmp/base_crashes"
    sed 's/^/  PR: /' "$tmp/pr_crashes"
    # A crash means some tests never ran, which would go unreported if the same file also crashes on main
    if [ -s "$tmp/pr_crashes" ]; then
      failed=1
    fi

    pct=$(pass_pct "$tmp/pr")
    if awk -v p="$pct" -v min="$MIN_PASS_PCT" 'BEGIN { exit !(p < min) }'; then
      echo
      echo "PR pass rate ${pct}% is below the minimum of ${MIN_PASS_PCT}%"
      failed=1
    fi

    echo
    echo "Regressions: $(wc -l < "$tmp/regressions" | tr -d ' ')"
    if [ -s "$tmp/regressions" ]; then
      while read -r key _ res; do
        echo "  $key ($res)"
        # Show the PR's result line, which includes the query and failure message
        { grep -F " $key: " "$pr" || true; } | awk 'NR <= 1' | cut -c 1-1000 | sed 's/^/    /'
      done < <(head -n 200 "$tmp/regressions")
      failed=1
    fi

    echo
    echo "Test files more than ${SLOWDOWN_FACTOR}x slower: $(wc -l < "$tmp/slow" | tr -d ' ')"
    if [ -s "$tmp/slow" ]; then
      while read -r file base_ms pr_ms; do
        awk -v f="$file" -v b="$base_ms" -v p="$pr_ms" 'BEGIN { printf "  %s: main %.1fs, PR %.1fs (%s)\n", f, b / 1000, p / 1000, b ? sprintf("%.1fx", p / b) : "main took 0ms" }'
        # Show the queries with the largest slowdown in this file
        awk -v f="$file:" 'index($1, f) == 1 { print $3 - $2, $0 }' "$tmp/times" | sort -rn | awk 'NR <= 3' \
          | awk '{ printf "    %s: main %dms, PR %dms\n", $2, $3, $4 }'
      done < "$tmp/slow"
      failed=1
    fi
    exit $failed
    ;;
  *)
    echo "Unrecognized command $1"
    exit 1
    ;;
esac
