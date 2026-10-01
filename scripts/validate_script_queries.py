#!/usr/bin/env python3
"""Verify the script-query split against its original Git revision.

Run from any directory: python3 scripts/validate_script_queries.py [original-revision]
The default is the main commit on which the split was based. Requires only Python
and Git. Checks exact lines (including whitespace and multiplicity), exact test
literals, aggregate ordering, shared declarations, runners, and file sizes.
"""
import collections
from pathlib import Path
import re
import subprocess
import sys

BASE = "54c14acb7"
ROOT = Path(__file__).resolve().parents[1]
SOURCE = "enginetest/queries/script_queries.go"


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def bodies(text):
    return dict(re.findall(r"^var (\w+) = \[\]ScriptTest\{\n(.*?)^}\n", text, re.M | re.S))


def literals(body):
    return re.findall(r"^\t\{\n.*?^\t\},\n", body, re.M | re.S)


def lines(text):
    # Only punctuation-only / blank lines may differ. Preserve every other byte.
    return collections.Counter(line for line in text.splitlines() if re.search(r"\w", line))


def main():
    revision = sys.argv[1] if len(sys.argv) > 1 else BASE
    original = subprocess.check_output(
        ["git", "show", f"{revision}:{SOURCE}"], cwd=ROOT, text=True
    )
    old = bodies(original)
    current = (ROOT / SOURCE).read_text()
    remaining = bodies(current)
    themed = {}
    files = sorted((ROOT / SOURCE).parent.glob("script_*_queries.go"))
    check(bool(files), "No theme files found")
    for path in [ROOT / SOURCE, *files]:
        check(len(path.read_text().splitlines()) <= 3000, f"{path.name} exceeds 3000 lines")
    for path in files:
        found = bodies(path.read_text())
        check(len(found) == 1, f"Expected one script collection in {path.name}")
        check(not themed.keys() & found.keys(), f"Duplicate collection in {path.name}")
        themed.update(found)
    check(remaining.keys() == old.keys(), "Original collections were lost or added")
    before = "".join(old.values())
    after = "".join(v for k, v in remaining.items() if k != "ScriptTests") + "".join(themed.values())
    missing, added = lines(before) - lines(after), lines(after) - lines(before)
    check(not missing and not added, f"Changed lines:\nMissing: {missing}\nAdded: {added}")
    old_tests = literals(old["ScriptTests"])
    new_tests = {name: literals(body) for name, body in themed.items()}
    check(collections.Counter(old_tests) == collections.Counter(
        test for tests in new_tests.values() for test in tests
    ), "Test literals changed, disappeared, or were duplicated")
    refs = re.findall(r"^\t(\w+)\[(\d+)\],$", remaining["ScriptTests"], re.M)
    rebuilt = [new_tests[name][int(index)] for name, index in refs]
    check(rebuilt == old_tests, "Combined ScriptTests ordering or membership changed")
    # Everything outside the moved collection and imports must remain byte-for-byte identical.
    def shared(text):
        text = re.sub(r"^import \(\n.*?^\)\n", "", text, flags=re.M | re.S)
        return re.sub(r"^var ScriptTests = \[\]ScriptTest\{\n.*?^}\n", "", text, flags=re.M | re.S)
    check(shared(original) == shared(current), "Shared types or other collections changed")
    engine = (ROOT / "enginetest/enginetests.go").read_text()
    memory = (ROOT / "enginetest/memory_engine_test.go").read_text()
    check("func TestScripts(" not in engine + memory, "Monolithic ordinary runner remains")
    for name in themed:
        theme = name.removesuffix("ScriptTests")
        method = f"Test{theme}Scripts"
        runner = re.search(rf"func {method}\(.*?\n}}", engine, re.S)
        check(runner is not None, f"Missing runner {method}")
        check(f"range queries.{name}" in runner[0], f"Wrong collection in {method}")
        check("harness.Setup(setup.MydbData)" in runner[0] and "TestScript(t, harness, script)" in runner[0]
              and "sh.SkipQueryTest(script.Name)" in runner[0], f"Runner behavior changed in {method}")
        check(f"enginetest.{method}(" in memory, f"Missing memory-engine test {method}")
    print(f"Validated {len(old_tests)} exact test literals across {len(files)} theme files; "
          "all original lines, multiplicities, ordering, shared declarations, and runners preserved.")


if __name__ == "__main__":
    main()
