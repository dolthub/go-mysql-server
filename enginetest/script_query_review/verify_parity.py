#!/usr/bin/env python3
"""Review-only parity checks. Requires Python 3, Go, and the original and merged main Git commits.

Run from any directory:
    python3 enginetest/script_query_review/verify_parity.py

The restored script_queries.go is checked against the merged main commit, then
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
import shutil
import tarfile
import tempfile

BASELINE = "ae7f5dc67647cbe8f6aa81bd9560154e34ddd87d"
ORIGINAL_BASELINE = "54c14acb7"
UPSTREAM_SCRIPTS = {
    "IN with mixed integer and fractional list compares without truncation":
        ("tuple_queries.go", "TupleComparisonsScriptTests"),
    "correlated subquery references outer aggregate":
        ("aggregation_script_queries.go", "AggregationScriptTests"),
}
ROOT = Path(__file__).resolve().parents[2]
REPORT = Path(__file__).with_name("report.html")
REFERENCE_HASHES = {'enginetest/enginetests.go': '790deb99b24e7f4c846a85c48d813b00b93b350fc4587948377cc4776622a31f', 'enginetest/memory_engine_test.go': 'b186c79b272cb2943882d089cd19ce18be94e291327be54c5691e058b7c11b8a', 'enginetest/queries/aggregation_script_queries.go': '19e3ad4823b3a344f4b8213cd2c6491aa78c07c47a5f2b397c172ae3a09db137', 'enginetest/queries/alter_table_queries.go': 'ac0c10dceb2a07518e09f200d98d7518aea9ef64970ea36a90ab8c01d40070d6', 'enginetest/queries/auto_increment_script_queries.go': 'bb4a14f7c6e49cb223b9593c49c06042e8b22a30fffa630892fa106b8ecfc4d7', 'enginetest/queries/charset_collation_engine.go': '9b20b8d48d3335b37a5878272157c60c3ff14c024c0a34a4f9469f9bcbeca286', 'enginetest/queries/column_alias_queries.go': None, 'enginetest/queries/column_default_queries.go': '9d44176f01cfef28e679739884bf8447ca6d341a1d59dbb24d93af56dc5ccc06', 'enginetest/queries/complex_index_script_queries.go': 'afdeb1582fd0de2db6e5eb2f215ad5b18e10d5360e73e0cc1ccae81949f2ffbf', 'enginetest/queries/conversions_script_queries.go': '37a9588abdf67fe055b5f5be1b9b340b521d729180caca27673cb52238b78dfb', 'enginetest/queries/create_database_script_queries.go': '10812187f356142e3eaf34d76b23f92947002e6ae66e5a717606c1fe42d919e5', 'enginetest/queries/create_table_queries.go': '68912f442f8ac7133977c732754c09aaf79dd200c550e83ccc9ebcf55f776368', 'enginetest/queries/delete_queries.go': '529960fb6bbe69bcbf89a713b2d8ec2f39602966e3d591fca80a3744355d145e', 'enginetest/queries/descending_indexes_script_queries.go': '0154abcd7f713d45d07e2d4266a01eb6f5c5e865929db039c9c9b8fef95473c0', 'enginetest/queries/drop_database_script_queries.go': None, 'enginetest/queries/drop_table_script_queries.go': None, 'enginetest/queries/enums_and_sets_script_queries.go': '4276b19800691f9c7921f79047604b0c3eb287914ae6eac5e889e526cae2c85b', 'enginetest/queries/expressions_script_queries.go': 'c147c7ef9d7db932b335e98bee77066090594943f5fc018043c0a7bf89b9e574', 'enginetest/queries/foreign_key_queries.go': '811d8a9f2f7bf353e82451da22b897037cb4d975908f3a13cda13a641a1cedba', 'enginetest/queries/foreign_key_resolution_script_queries.go': '4b05a6c3fb9b51a148899492339acb11818c7efaa65cb7cd0e63a41b1d914c92', 'enginetest/queries/foreign_key_types_script_queries.go': '46d99b5e28db84a26aded4b65787708dfca5bb2afb464648900161e47f0696ac', 'enginetest/queries/index_key_types_script_queries.go': '9b34c3dbb32a3b3e5b45b60895422fb8ec245cdd6028e1ef52603cf54e0184f2', 'enginetest/queries/index_prefix_script_queries.go': '83ba20a6554d316fdb1e4487287fab0981b0a3f3c5fc6e7f63ca028eb8cc497b', 'enginetest/queries/index_queries.go': '602e96a9f4a3d153c5c4cd592b02d746a876b60214ed6e9750a424fb06b5f955', 'enginetest/queries/insert_ignore_script_queries.go': '852658acc8a9dc3a87d1a60defa5bc1000ba119f79dd34edd6b73fb851dbbceb', 'enginetest/queries/insert_queries.go': '23f51c435dff678b03d76d836003461bec736335276ee451af555ef7602fa3d4', 'enginetest/queries/join_queries.go': 'd542531faad25fdde1c3af6427ee15352931cd29af162bf5c994f61d55982ad8', 'enginetest/queries/json_scripts.go': 'ddd9fda8b7ef3db3228c111a2d78b8721a005ada392bd593913d62048f043f81', 'enginetest/queries/logic_test_scripts.go': None, 'enginetest/queries/name_resolution_script_queries.go': '2617a20b0936591c7935d50b6abefec0dff30be1318bcd84615d60f488b360de', 'enginetest/queries/numeric_script_queries.go': '4659b59cdf389e4cb12f1cf3deddf1401aa997bc02fe960873813d6eabb1370c', 'enginetest/queries/order_by_group_by_queries.go': None, 'enginetest/queries/ordering_script_queries.go': '63bcee3b817f4ae01e0d66019356303ba3ceef7faa6586e1e2f3ba747b8d9d33', 'enginetest/queries/primary_keys_script_queries.go': 'fa87c4180e67d0d30b36ff1701a15b9fc27f85d88423c861a680b4be382488a7', 'enginetest/queries/procedure_ddl_script_queries.go': 'ba348e1be7d15fbbd5c7cea583cf6d427a69ccefb543266dafe0ffe6dd5f158c', 'enginetest/queries/procedure_queries.go': 'd059e384325b65091c8a4a2fddbe64a13d7a5e39a60283c86ed210cb7a81040e', 'enginetest/queries/script_queries_pruned.go': '4a13851f718e0eb375e2043fdbad910961aeb2ad926e84d273f6239c4798f66f', 'enginetest/queries/session_results_script_queries.go': '2ca585a2e526ccce45a6c3b48354b0d5bc661200675d84f2a1833dd3b3e347b2', 'enginetest/queries/set_operations_script_queries.go': 'c4ee75a4eaf058054cad85bd3e5b29613e21100025d4fc65683edbe38562cb11', 'enginetest/queries/stats_queries.go': 'b5d1f4065720357434514fafc462171321b2ca10c3d6263307315334696392a6', 'enginetest/queries/string_functions_script_queries.go': '2d1be4a104bebbea948e6f238ee18f364c88bd4079c6bff4259cd2a1845d5b57', 'enginetest/queries/string_matching_script_queries.go': None, 'enginetest/queries/subqueries_script_queries.go': 'e34dfb3708c5c00e744e4fd2f941ecb6c301ec6b6a7211be3484955e372f8151', 'enginetest/queries/time_queries.go': '79ef888a609cce82bb5fa6dd6986d12bf61c2f36e2240915d828ca672ce06756', 'enginetest/queries/transactions_script_queries.go': '5d08fce2042291134f818a8c183d3c7ef8753cd755508815fdd963667af49595', 'enginetest/queries/tuple_queries.go': 'a573562a2260ded85d37dea816807e1bfb58c7e8e63bf4522749b35aa8b0ea8b', 'enginetest/queries/update_joins_script_queries.go': 'eb0636743a14dffc3436efdd75884abe7bc10541e191df73adc6885af875d7c6', 'enginetest/queries/update_queries.go': 'f9b5fc7ea7f6e9d06a8e7f33552ee41065e564025bcbac1502af785fb9580d1a', 'enginetest/queries/uuid_script_queries.go': None, 'enginetest/queries/variable_queries.go': '53e0db5315642b0f39bfbccb2fe44f6eec6538d725a2ff306c882239f2445dee', 'enginetest/queries/view_queries.go': 'b59083d89fd9356ccfbd4350c3ccc3d7d3560c1fa1440f6750a15e23b553210c', 'enginetest/queries/numeric_error_queries.go': None, 'enginetest/queries/call_asof_queries.go': None, 'enginetest/queries/drop_script_queries.go': '6ceb12c9106b45ef240f5e0c06537808dc8c465a8a3a22328b48bd727ed65be7'}

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

		// One original script now lives directly beside TestDropTable's cases.
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok || (fn.Name.Name != "testDropTable" && fn.Name.Name != "TestDropTableWarnings") {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				literal, ok := node.(*ast.CompositeLit)
				if !ok { return true }
				kind, ok := literal.Type.(*ast.SelectorExpr)
				if !ok || kind.Sel.Name != "ScriptTest" { return true }
				entry := Entry{Text: string(data[offset(literal.Lbrace):offset(literal.End())])}
				for _, element := range literal.Elts {
					field, ok := element.(*ast.KeyValueExpr)
					if !ok { continue }
					key, ok := field.Key.(*ast.Ident)
					if !ok || key.Name != "Name" { continue }
					entry.Name, err = strconv.Unquote(field.Value.(*ast.BasicLit).Value)
					if err != nil { panic(err) }
				}
				results = append(results, Collection{
					File: rel, Name: fn.Name.Name, Kind: "ScriptTest",
					Start: offset(literal.Lbrace), End: offset(literal.End()),
					Open: offset(literal.Lbrace), Close: offset(literal.Rbrace),
					Entries: []Entry{entry},
				})
				return false
			})
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


class History(html.parser.HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "tr" and "data-history-commit" in attrs:
            self.rows.append((attrs["data-history-commit"],
                              attrs["data-history-destination"],
                              json.loads(attrs["data-history-sources"])))


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
        current = temporary / "current"
        shutil.copytree(ROOT / "enginetest/queries", current)
        shutil.copyfile(ROOT / "enginetest/enginetests.go", current / "enginetests.go")
        assert (current / "script_queries.go").read_bytes() == (
            baseline / "script_queries.go"
        ).read_bytes(), "The restored original file changed"
        snapshot = (current / "script_queries_pruned.go").read_bytes()
        assert snapshot.startswith(b"//go:build ignore\n\n"), "Snapshot must not compile"
        # The new history must not depend on commits exclusive to the previous PR.
        original_types = dict(re.findall(
            rb"(?ms)^type (ScriptTest|ScriptTestAssertion) struct \{(.*?)^\}",
            (baseline / "script_queries.go").read_bytes(),
        ))
        snapshot_types = dict(re.findall(
            rb"(?ms)^type (ScriptTest|ScriptTestAssertion) struct \{(.*?)^\}", snapshot,
        ))
        assert len(snapshot_types) == 2 and snapshot_types == original_types, (
            "The pruned snapshot's shared definitions changed"
        )
        helper = temporary / "inventory.go"
        helper.write_text(GO_INVENTORY)
        executable = temporary / "inventory"
        env = os.environ.copy()
        env.update(GOWORK="off", GO111MODULE="off")
        subprocess.run(["go", "build", "-o", str(executable), str(helper)],
                       cwd=temporary, env=env, check=True)

        def inventory(directory):
            return json.loads(run(str(executable), str(directory)))

        for path, expected_hash in REFERENCE_HASHES.items():
            file = ROOT / path
            if expected_hash is None:
                assert not file.exists(), (path, "deleted source file reappeared")
            else:
                import hashlib
                assert hashlib.sha256(file.read_bytes()).hexdigest() == expected_hash, (
                    path, "differs from the reviewed refactor including orphan consolidation"
                )
        # Check each move's source files introduce no cases for another destination.
        history = History()
        history.feed(REPORT.read_text())
        assert history.rows, "Commit-by-commit review index missing"
        for row in history.rows:
            sha, destination, sources = row
            changed = set(run("git", "diff-tree", "--no-commit-id", "--name-only",
                              "--no-renames", "-r", sha).decode().splitlines())
            assert changed == {destination, *sources}, (sha, "file scope changed")
            if not destination.startswith("enginetest/queries/"):
                continue
            for source in sources:
                if not source.startswith("enginetest/queries/"):
                    continue
                revisions = []
                for revision in (sha + "^", sha):
                    directory = temporary / "history"
                    directory.mkdir(exist_ok=True)
                    file = directory / "source.go"
                    blob = subprocess.run(["git", "show", revision + ":" + source],
                                          cwd=ROOT, stdout=subprocess.PIPE,
                                          stderr=subprocess.DEVNULL)
                    if blob.returncode:
                        revisions.append(collections.Counter())
                        continue
                    file.write_bytes(blob.stdout)
                    declarations = inventory(directory)
                    literals = literal_counts(declarations)
                    for declaration in declarations:
                        if declaration["kind"] != "ScriptTest":
                            literals[blob.stdout[declaration["start"]:
                                                 declaration["end"]].decode()] += 1
                    revisions.append(literals)
                assert not (revisions[1] - revisions[0]), (
                    sha, source, "source file adds cases for a second destination"
                )
        before = inventory(baseline)
        after = [c for c in inventory(current)
                 if c["file"] not in ("script_queries.go", "script_queries_pruned.go")]
        assert literal_counts(before) == literal_counts(after), (
            "Script literal contents, formatting, or duplicate counts changed"
        )
        assert body_lines(before, baseline) == body_lines(after, current), (
            "Script body lines or comments changed"
        )
        historical = temporary / "historical"
        historical.mkdir()
        (historical / "script_queries.go").write_bytes(run(
            "git", "show", ORIGINAL_BASELINE + ":enginetest/queries/script_queries.go"
        ))
        originals = inventory(historical)
        upstream_originals = [c for c in before if c["file"] == "script_queries.go"]
        upstream_additions = literal_counts(upstream_originals) - literal_counts(originals)
        assert not (literal_counts(originals) - literal_counts(upstream_originals)), (
            "Historical scripts changed upstream; review mappings need updating"
        )
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
        upstream_names = set()
        for c in upstream_originals:
            for entry in c["entries"]:
                if upstream_additions[entry["text"]]:
                    upstream_names.add(entry["name"])
                    destination = UPSTREAM_SCRIPTS[entry["name"]]
                    expected[destination][entry["text"]] += 1
        assert upstream_names == UPSTREAM_SCRIPTS.keys(), "Upstream mappings differ"
        # Original cases now share batches with exact pre-existing literals.
        batch_baseline = temporary / "batch-baseline"
        batch_baseline.mkdir()
        for filename in ("insert_queries.go", "json_scripts.go", "join_queries.go", "ordering_script_queries.go", "procedure_queries.go", "update_queries.go", "view_queries.go"):
            (batch_baseline / filename).write_bytes(run(
                "git", "show", (BASELINE if filename == "view_queries.go" else "71afce862") + ":enginetest/queries/" + filename
            ))
        batch_collections = inventory(batch_baseline)
        existing_batches = []
        for filename, variable in (("insert_queries.go", "InsertScripts"),
                                   ("json_scripts.go", "JsonScripts"),
                                   ("procedure_queries.go", "ProcedureLogicTests"),
                                   ("procedure_queries.go", "ProcedureCallTests"),
                                   ("update_queries.go", "UpdateScriptTests"),
                                   ("view_queries.go", "ViewScripts")):
            c = next(c for c in batch_collections if c["name"] == variable)
            expected[(filename, variable)].update(e["text"] for e in c["entries"])
            existing_batches.append(c)
        row_insert = next(e for c in batch_collections if c["name"] == "SQLLogicJoinTests"
                          for e in c["entries"] if e["name"] == "values and rows")
        expected[("insert_queries.go", "InsertScripts")][row_insert["text"]] += 1
        (batch_baseline / "values_row.go").write_text(
            "package queries\nvar ValuesRow = []ScriptTest{\n\t" + row_insert["text"] + ",\n}\n"
        )
        existing_batches.append(next(c for c in inventory(batch_baseline)
                                     if c["name"] == "ValuesRow"))
        existing_ordering = next(c for c in batch_collections if c["name"] == "OrderByScriptTests")
        existing_batches.append(existing_ordering)
        expected[("ordering_script_queries.go", "OrderingScriptTests")].update(
            e["text"] for e in existing_ordering["entries"]
        )
        destinations = []
        for (filename, variable), literals in expected.items():
            matches = [c for c in after if c["file"] == filename and c["name"] == variable]
            assert len(matches) == 1, (filename, variable)
            c = matches[0]
            assert collections.Counter(e["text"] for e in c["entries"]) == literals, (
                filename, variable, "original script missing, duplicated, or changed"
            )
            destinations.append(c)
        assert (body_lines(upstream_originals, baseline) +
                body_lines(existing_batches, batch_baseline)) == body_lines(destinations, current), (
            "Original script or prefix-comment lines changed"
        )
        # Self-contained original suites must still have ordinary and prepared runners.
        engine = (ROOT / "enginetest/enginetests.go").read_text()
        memory = (ROOT / "enginetest/memory_engine_test.go").read_text()
        # The focused prepared regression must read the relocated collection,
        # so removing the retained aggregate cannot silently remove its coverage.
        correlated = memory.split("func TestCorrelatedAggregateScopePrepared(", 1)[1].split("\nfunc ", 1)[0]
        assert "range queries.AggregationScriptTests" in correlated
        assert "TestScriptPrepared(" in correlated and "t.Fatal(" in correlated
        # A skipped JSON script must not skip the enclosing runner and its later cases.
        prepared_json = engine.split("func TestJsonScriptsPrepared(", 1)[1].split("\nfunc ", 1)[0]
        assert 't.Run(script.Name, func(t *testing.T)' in prepared_json
        wrappers = re.findall(
            r"func (Test\w+Scripts(?:Prepared)?)\(t \*testing.T, harness Harness\) "
            r"\{\n\ttestScriptTests\(t, harness, queries\.(\w+), (true|false)\)\n\}",
            engine,
        )
        active_variables = {variable for original_id, (_, _, variable)
                            in parsed.destinations.items()
                            if original_id.startswith("ScriptTests[")}
        for variable in active_variables:
            if variable == "InsertScripts":
                ordinary = engine.split("func TestInsertInto(", 1)[1].split("\nfunc ", 1)[0]
                prepared = engine.split("func TestInsertScriptsPrepared(", 1)[1].split("\nfunc ", 1)[0]
                assert "range queries.InsertScripts" in ordinary and "TestScript(t, harness, script)" in ordinary
                assert "range queries.InsertScripts" in prepared and "TestScriptPrepared(t, harness, script)" in prepared
                assert "func TestInsertInto(t *testing.T)" in memory
                assert "InsertRegressionScriptTests" not in engine
                continue
            if variable == "JsonScripts":
                for name, call in (("TestJsonScripts", "TestScript"),
                                   ("TestJsonScriptsPrepared", "TestScriptPrepared")):
                    method = engine.split("func " + name + "(", 1)[1].split("\nfunc ", 1)[0]
                    assert "range queries.JsonScripts" in method and call + "(t, harness, script)" in method
                assert "func TestJsonScripts(t *testing.T)" in memory
                assert "JSONFunctionsScriptTests" not in engine
                continue
            if variable in ("ProcedureLogicTests", "ProcedureCallTests"):
                ordinary = engine.split("func TestStoredProcedures(", 1)[1].split("\nfunc ", 1)[0]
                prepared = engine.split("func TestStoredProceduresPrepared(", 1)[1].split("\nfunc ", 1)[0]
                assert "range queries." + variable in ordinary
                assert "testScriptTests(t, harness, queries." + variable + ", true)" in prepared
                assert "func TestStoredProcedures(t *testing.T)" in memory
                assert "ProceduresScriptTests" not in engine
                continue
            if variable == "UpdateScriptTests":
                for name, call in (("TestUpdate", "TestScript"),
                                   ("TestUpdateQueriesPrepared", "TestScriptPrepared")):
                    method = engine.split("func " + name + "(", 1)[1].split("\nfunc ", 1)[0]
                    assert "range queries.UpdateScriptTests" in method and call + "(t, harness, tt)" in method
                assert "func TestUpdate(t *testing.T)" in memory
                assert "UpdateRegressionScriptTests" not in engine
                continue
            if variable == "TestDropTableWarnings":
                for name, mode in (("TestDropTable", "false"), ("TestDropTablePrepared", "true")):
                    method = engine.split("func " + name + "(", 1)[1].split("\nfunc ", 1)[0]
                    assert "testDropTable(t, harness, " + mode + ")" in method
                    assert "func " + name + "(t *testing.T)" in memory
                drop_table = engine.split("func testDropTable(", 1)[1].split("\nfunc ", 1)[0]
                assert "TestDropTableWarnings(t, harness, prepared)" in drop_table
                method = engine.split("func TestDropTableWarnings(", 1)[1].split("\nfunc ", 1)[0]
                assert "TestScript(t, harness, script)" in method
                assert "TestScriptPrepared(t, harness, script)" in method
                assert "DropTableScriptTests" not in engine
                continue
            if variable == "ViewScripts":
                for name, call in (("TestViews", "TestScript"),
                                   ("TestViewsPrepared", "TestScriptPrepared")):
                    method = engine.split("func " + name + "(", 1)[1].split("\nfunc ", 1)[0]
                    assert "range queries.ViewScripts" in method and call + "(t, harness, script)" in method
                    assert "func " + name + "(t *testing.T)" in memory
                assert "ViewsScriptTests" not in engine
                continue
            for prepared in ("false", "true"):
                matches = [name for name, var, mode in wrappers
                           if var == variable and mode == prepared]
                assert len(matches) == 1, (variable, prepared, "runner missing")
                if prepared == "false":
                    assert memory.count("func " + matches[0] + "(") == 1, matches[0]
        obsolete_methods = ['TestInsertRegressionScripts', 'TestInsertRegressionScriptsPrepared', 'TestJSONFunctionsScripts', 'TestJSONFunctionsScriptsPrepared', 'TestProceduresScripts', 'TestProceduresScriptsPrepared', 'TestStringMatchingScripts', 'TestStringMatchingScriptsPrepared', 'TestUpdateRegressionScripts', 'TestUpdateRegressionScriptsPrepared', 'TestUUIDScripts', 'TestUUIDScriptsPrepared', 'TestDropTableScripts', 'TestDropTableScriptsPrepared', 'TestViewsScripts', 'TestViewsScriptsPrepared']
        for name in obsolete_methods:
            assert "func " + name + "(" not in engine, (name, "redundant runner retained")
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
        assert regex == (current / "regex_queries.go").read_bytes(), (
            "Original regexp declarations or !race constraint changed"
        )
        assert b"type RegexTest" not in (
            current / "string_functions_script_queries.go"
        ).read_bytes(), "Duplicate regexp type declaration"
        files = {c["file"] for c in after}
        lengths = {f: len((current / f).read_bytes().splitlines()) for f in files
                   if (f != "enginetests.go" and f in {c["file"] for c in destinations})
                   or f in ("complex_index_script_queries.go", "index_prefix_script_queries.go",
                            "procedure_ddl_script_queries.go")}
        assert all(n <= 3000 for n in lengths.values()), lengths
        print(f"PASS: {sum(literal_counts(originals).values())} original scripts plus "
              f"{sum(upstream_additions.values())} upstream additions in "
              f"{len({c['file'] for c in destinations})} feature files; "
              f"{sum(literal_counts(before).values())} total ScriptTest literals.")
        print("Exact names, literals, body/comment lines, and duplicate multiplicities match.")
        print("Restored original, pruned snapshot, report destinations, complex-index and "
              "regexp coverage, ordinary/prepared runners, reference hashes, and commit scopes verified.")
        print(f"Longest reorganized query file: {max(lengths.values())} lines; "
              "restored original intentionally excluded from that limit.")


if __name__ == "__main__":
    verify()
