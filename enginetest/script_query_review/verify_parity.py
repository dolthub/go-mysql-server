#!/usr/bin/env python3
"""Review-only parity checks. Requires Python 3, Go, and the original Git commit.

Run from any directory:
    python3 enginetest/script_query_review/verify_parity.py

The restored script_queries.go is checked against the original commit, then
excluded from destination counts so its retained copies cannot mask omissions.
The report identifies every original script's exact destination. Temporary Go
AST tooling and baseline sources are created and deleted automatically.
"""
import collections
import html.parser
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

BASELINE = "54c14acb7"
ROOT = Path(__file__).resolve().parents[2]
REPORT = Path(__file__).with_name("report.html")

# Go's parser preserves byte offsets, including UTF-8 and raw-string whitespace.
GO_INVENTORY = r'''
package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
)

type Entry struct {
	Name string `json:"name"`
	Text string `json:"text"`
}
type Collection struct {
	File    string  `json:"file"`
	Name    string  `json:"name"`
	Kind    string  `json:"kind"`
	Start   int     `json:"start"`
	End     int     `json:"end"`
	Open    int     `json:"open"`
	Close   int     `json:"close"`
	Entries []Entry `json:"entries"`
}

func main() {
	root := os.Args[1]
	var results []Collection
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		fs := token.NewFileSet()
		file, err := parser.ParseFile(fs, path, data, parser.ParseComments)
		if err != nil {
			return err
		}
		offset := func(p token.Pos) int { return fs.Position(p).Offset }
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			gd, ok := declaration.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				if len(vs.Names) != 1 || len(vs.Values) != 1 {
					continue
				}
				value, ok := vs.Values[0].(*ast.CompositeLit)
				if !ok {
					continue
				}
				array, ok := value.Type.(*ast.ArrayType)
				if !ok {
					continue
				}
				element, ok := array.Elt.(*ast.Ident)
				if !ok {
					continue
				}
				name := vs.Names[0].Name
				if element.Name != "ScriptTest" && name != "ComplexIndexQueries" {
					continue
				}
				start := offset(gd.Pos())
				if gd.Doc != nil {
					start = offset(gd.Doc.Pos())
				}
				c := Collection{
					File:  filepath.ToSlash(rel),
					Name:  name,
					Kind:  element.Name,
					Start: start,
					End:   offset(gd.End()),
					Open:  offset(value.Lbrace),
					Close: offset(value.Rbrace),
				}
				if element.Name == "ScriptTest" {
					for _, item := range value.Elts {
						literal := item.(*ast.CompositeLit)
						e := Entry{Text: string(data[offset(literal.Pos()):offset(literal.End())])}
						for _, field := range literal.Elts {
							kv, ok := field.(*ast.KeyValueExpr)
							if !ok {
								continue
							}
							key, ok := kv.Key.(*ast.Ident)
							if !ok || key.Name != "Name" {
								continue
							}
							basic, ok := kv.Value.(*ast.BasicLit)
							if !ok {
								panic("nonliteral ScriptTest name")
							}
							e.Name, err = strconv.Unquote(basic.Value)
							if err != nil {
								return err
							}
						}
						c.Entries = append(c.Entries, e)
					}
				}
				results = append(results, c)
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	if err = json.NewEncoder(os.Stdout).Encode(results); err != nil {
		panic(err)
	}
}
'''


class Destinations(html.parser.HTMLParser):
    """Read the report's exact name, original array/index, and final collection."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.destinations = {}
        self.file = None
        self.codes = None
        self.in_code = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "details":
            self.file = attrs.get("id")
        if tag == "tr" and "data-script" in attrs:
            self.codes = []
        if tag == "code" and self.codes is not None:
            self.codes.append("")
            self.in_code = True

    def handle_data(self, text):
        if self.in_code:
            self.codes[-1] += text

    def handle_endtag(self, tag):
        if tag == "code":
            self.in_code = False
        if tag == "tr" and self.codes is not None:
            assert len(self.codes) == 3, self.codes
            name, original_id, variable = self.codes
            assert original_id not in self.destinations, original_id
            self.destinations[original_id] = (name, self.file, variable)
            self.codes = None
        if tag == "details":
            self.file = None


def run(*args, **kwargs):
    return subprocess.check_output(args, cwd=ROOT, **kwargs)


def literal_counts(collections_):
    return collections.Counter(
        e["text"] for c in collections_ for e in (c["entries"] or [])
    )


def body_lines(collections_, directory):
    result = collections.Counter()
    for c in collections_:
        if c["kind"] != "ScriptTest":
            continue
        data = (directory / c["file"]).read_bytes()
        for line in data[c["open"] + 1:c["close"]].splitlines():
            # Only blank lines and punctuation-only scaffolding may differ.
            if line.strip() and re.search(rb"[\w\"'`/]", line):
                result[line] += 1
    return result


def verify():
    parsed = Destinations()
    parsed.feed(REPORT.read_text())
    with tempfile.TemporaryDirectory(prefix="gms-script-parity-") as temporary:
        temporary = Path(temporary)
        baseline = temporary / "baseline"
        baseline.mkdir()
        archive = run("git", "archive", BASELINE, "enginetest/queries")
        with tarfile.open(fileobj=io.BytesIO(archive)) as files:
            for member in files:
                if not member.isfile() or not member.name.endswith(".go"):
                    continue
                path = Path(member.name)
                assert not path.is_absolute() and ".." not in path.parts
                target = baseline / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(files.extractfile(member).read())
        baseline = baseline / "enginetest/queries"
        current = ROOT / "enginetest/queries"
        assert (current / "script_queries.go").read_bytes() == (
            baseline / "script_queries.go"
        ).read_bytes(), "The restored original file changed"
        snapshot = (current / "script_queries_pruned.go").read_bytes()
        assert snapshot.startswith(b"//go:build ignore\n\n"), "Snapshot must not compile"
        old_snapshot = run("git", "show", "bfa373f08:enginetest/queries/script_queries.go")
        expected_snapshot = b"//go:build ignore\n\n" + old_snapshot.replace(
            b"// Copyright 2020-2021 Dolthub, Inc.",
            b"// Copyright 2026 Dolthub, Inc.", 1,
        )
        assert snapshot == expected_snapshot, "The pruned snapshot changed"
        helper = temporary / "inventory.go"
        helper.write_text(GO_INVENTORY)
        executable = temporary / "inventory"
        env = os.environ.copy()
        env.update(GOWORK="off", GO111MODULE="off")
        subprocess.run(["go", "build", "-o", str(executable), str(helper)],
                       cwd=temporary, env=env, check=True)

        def inventory(directory):
            return json.loads(run(str(executable), str(directory)))

        before = inventory(baseline)
        after = [c for c in inventory(current)
                 if c["file"] not in ("script_queries.go", "script_queries_pruned.go")]
        assert literal_counts(before) == literal_counts(after), (
            "Script literal contents, formatting, or duplicate counts changed"
        )
        assert body_lines(before, baseline) == body_lines(after, current), (
            "Script body lines or comments changed"
        )
        originals = [c for c in before if c["file"] == "script_queries.go"]
        expected = collections.defaultdict(collections.Counter)
        original_ids = set()
        for c in originals:
            for index, entry in enumerate(c["entries"]):
                original_id = f'{c["name"]}[{index}]'
                original_ids.add(original_id)
                name, filename, variable = parsed.destinations[original_id]
                assert name == entry["name"], (original_id, "report name changed")
                expected[(filename, variable)][entry["text"]] += 1
        assert original_ids == parsed.destinations.keys(), "Report entries differ"
        destinations = []
        for (filename, variable), literals in expected.items():
            matches = [c for c in after if c["file"] == filename and c["name"] == variable]
            assert len(matches) == 1, (filename, variable)
            c = matches[0]
            assert collections.Counter(e["text"] for e in c["entries"]) == literals, (
                filename, variable, "original script missing, duplicated, or changed"
            )
            destinations.append(c)
        assert body_lines(originals, baseline) == body_lines(destinations, current), (
            "Original script or prefix-comment lines changed"
        )
        # Self-contained original suites must still have ordinary and prepared runners.
        engine = (ROOT / "enginetest/enginetests.go").read_text()
        memory = (ROOT / "enginetest/memory_engine_test.go").read_text()
        wrappers = re.findall(
            r"func (Test\w+Scripts(?:Prepared)?)\(t \*testing.T, harness Harness\) "
            r"\{\n\ttestScriptTests\(t, harness, queries\.(\w+), (true|false)\)\n\}",
            engine,
        )
        active_variables = {variable for original_id, (_, _, variable)
                            in parsed.destinations.items()
                            if original_id.startswith("ScriptTests[")}
        for variable in active_variables:
            for prepared in ("false", "true"):
                matches = [name for name, var, mode in wrappers
                           if var == variable and mode == prepared]
                assert len(matches) == 1, (variable, prepared, "runner missing")
                if prepared == "false":
                    assert memory.count("func " + matches[0] + "(") == 1, matches[0]
        assert "queries.ScriptTests" not in engine, "Restored aggregate must stay unused"
        for original_id, (_, _, variable) in parsed.destinations.items():
            if original_id.startswith("BrokenScriptTests["):
                assert "queries." + variable not in engine, "Disabled script enabled"
            elif original_id.startswith(("CreateDatabaseScripts[", "DropDatabaseScripts[")):
                assert "range queries." + variable in engine, "Database runner missing"
        old_complex = next(c for c in before if c["name"] == "ComplexIndexQueries")
        new_complex = next(c for c in after if c["name"] == "ComplexIndexQueries")
        old_bytes = (baseline / old_complex["file"]).read_bytes()
        new_bytes = (current / new_complex["file"]).read_bytes()
        assert old_bytes[old_complex["start"]:old_complex["end"]] == (
            new_bytes[new_complex["start"]:new_complex["end"]]
        ), "ComplexIndexQueries declaration changed"
        regex = (baseline / "regex_queries.go").read_bytes()
        regex = regex[regex.index(b"type RegexTest"):].strip()
        assert regex in (current / "string_matching_script_queries.go").read_bytes(), (
            "Moved regexp cases, type, or helper changed"
        )
        files = {c["file"] for c in after}
        lengths = {f: len((current / f).read_bytes().splitlines()) for f in files
                   if f in {c["file"] for c in destinations}
                   or f in ("complex_index_script_queries.go", "index_prefix_script_queries.go",
                            "procedure_ddl_script_queries.go")}
        assert all(n <= 3000 for n in lengths.values()), lengths
        print(f"PASS: {sum(literal_counts(originals).values())} original scripts in "
              f"{len({c['file'] for c in destinations})} feature files; "
              f"{sum(literal_counts(before).values())} total ScriptTest literals.")
        print("Exact names, literals, body/comment lines, and duplicate multiplicities match.")
        print("Restored original, pruned snapshot, report destinations, complex-index and "
              "regexp coverage, and ordinary/prepared runners verified.")
        print(f"Longest reorganized query file: {max(lengths.values())} lines; "
              "restored original intentionally excluded from that limit.")


if __name__ == "__main__":
    verify()
