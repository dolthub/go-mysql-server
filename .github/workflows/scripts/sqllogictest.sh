#!/bin/bash
#
# Helpers for running the sqllogictest suite (github.com/dolthub/sqllogictest) against the memory engine in CI.
#
# Usage:
#   sqllogictest.sh run <logictest binary> <test dir> <shard index> <shard count> <out file>
#     Runs every <shard count>th test file (balanced by file size) under <test dir> and writes the results log.
#   sqllogictest.sh compare <base results> <pr results>
#     Prints a summary of both result logs and exits non-zero if any test that passes in <base results> does not
#     pass in <pr results>, or if any test file is more than SLOWDOWN_FACTOR times slower in <pr results> (and slower
#     by at least SLOWDOWN_MIN_MS, to ignore noise in fast files).

set -eo pipefail
export LC_ALL=C

SLOWDOWN_FACTOR=${SLOWDOWN_FACTOR:-2}
SLOWDOWN_MIN_MS=${SLOWDOWN_MIN_MS:-10000}
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

# file_times prints "<file> <milliseconds>" for every test file in the log |$1|, summing the durations of its records.
file_times() {
  awk '
    $3 ~ /^[^ ]+:[0-9]+:$/ && $2 ~ /^[0-9]+$/ {
      split($3, parts, ":")
      ms[parts[1]] += $2
    }
    END { for (f in ms) print f, ms[f] }' "$1" | sort -k1,1
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

    file_times "$base" > "$tmp/base_times"
    file_times "$pr" > "$tmp/pr_times"
    join "$tmp/base_times" "$tmp/pr_times" \
      | awk -v f="$SLOWDOWN_FACTOR" -v min="$SLOWDOWN_MIN_MS" '$3 > f * $2 && $3 - $2 >= min' > "$tmp/slow"
    echo
    awk '{ s += $2 } END { printf "Total time: main %.1fs, ", s / 1000 }' "$tmp/base_times"
    awk '{ s += $2 } END { printf "PR %.1fs\n", s / 1000 }' "$tmp/pr_times"

    failed=0
    echo
    echo "Regressions: $(wc -l < "$tmp/regressions" | tr -d ' ')"
    if [ -s "$tmp/regressions" ]; then
      while read -r key _ res; do
        echo "  $key ($res)"
        # Show the PR's result line, which includes the query and failure message
        { grep -F " $key: " "$pr" || true; } | head -n 1 | cut -c 1-1000 | sed 's/^/    /'
      done < <(head -n 200 "$tmp/regressions")
      failed=1
    fi

    echo
    echo "Test files more than ${SLOWDOWN_FACTOR}x slower: $(wc -l < "$tmp/slow" | tr -d ' ')"
    if [ -s "$tmp/slow" ]; then
      while read -r file base_ms pr_ms; do
        awk -v f="$file" -v b="$base_ms" -v p="$pr_ms" 'BEGIN { printf "  %s: main %.1fs, PR %.1fs (%.1fx)\n", f, b / 1000, p / 1000, b ? p / b : 0 }'
        # Show the queries with the largest slowdown in this file
        join <(awk -v f="$file:" 'index($3, f) == 1 && $2 ~ /^[0-9]+$/ { print substr($3, 1, length($3) - 1), $2 }' "$base" | sort -k1,1) \
             <(awk -v f="$file:" 'index($3, f) == 1 && $2 ~ /^[0-9]+$/ { print substr($3, 1, length($3) - 1), $2 }' "$pr" | sort -k1,1) \
          | awk '{ print $3 - $2, $0 }' | sort -rn | head -n 3 \
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
