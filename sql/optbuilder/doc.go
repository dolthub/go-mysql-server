package optbuilder

// The optbuilder package is responsible for:
// - converting an ast.SQLNode tree into a sql.Node tree
// - resolving column and table uses.
//
// In the future this package will absorb:
// - type checking and coercion
// - normalizing expressions into their simplest canonical forms
//
// The intermediate phase involves several complications. The first is that
// there are too many node types for a one-PR pass. Edge cases will fallback
// to standard name resolving:
// - DML (INSERT, UPDATE, DELETE, DELETE JOIN, ...)
// - DDL (ALTER TABLE [ADD/DROP/MODIFY] [INDEX/CHECK], ...)
// - TRIGGERS
// - STORED PROCEDURES
// - PREPARED STATEMENTS
//
// Name resolution works similarly to an attribute grammar. A simple attribute
// grammar walks an AST and populates a node's
// attributes either by inspecting its parents for top-down attributes, or
// children for bottom-up attributes. Type checking for expressions, for
// example, take initial hints from the parent and build types upwards,
// applying casts where appropriate. For example, the float_col below should
// cast (1+1) to a float, and 2/1 to an int:
//
// INSERT INTO table (float_col, int_col) values (1+1, 2/1)
//
// Variable resolution of SQL queries works similarly, but involves
// more branching logic and in postgres and cockroach are divided into two
// phases: analysis and building. Analysis walks the AST, resolves types,
// collects name definitions, and replaces certain nodes with tracker ASTs
// that make aggregations and subqueries easier to build. Building (transform
// in PG) initializes the optimizer IR by adding expression groups to the
// query memo. In our case we currently create the sql.Node tree.
//
// In the simplest case, a SELECT expression resolves column references
// using FROM scope definitions. The source columns below are x:1, y:2, z:3:
//
// select x, y from xy where x = 0
// Project
// ├─ columns: [xy.x:1!null, xy.y:2!null]
// └─ Filter
//     ├─ Eq
//     │   ├─ xy.x:1!null
//     │   └─ 0 (tinyint)
//     └─ Table
//         ├─ name: xy
//         └─ columns: [x y z]
//
// Tt is useful to assign unique ids to referencable expressions. It is more
// difficult to track symbols after substituting execution time indexes.
//
// Two main forms of complexity are immediate. The first is that
// aggregations are subject to a dizzying set of resolution rules. Resolving valid
// aggregation variable references is difficult, and tagging invalid aggregations
// is also difficult.
//
// On the happy path, accessory columns used by ORDER BY, HAVING and DISTINCT clauses
// can inject extra aggregations or passthrough columns in addition to those in
// the GROUP BY and SELECT target lists.
//
// In the example below, we identify and tag two aggregations that are assigned
// expression ids after x,y, and z: SUM(y):4, COUNT(y):5. The sort node and target
// list projections that use those aggregations resolve dependencies by id reference:
//
// select x, sum(y) from xy group by x order by x - count(y)
// =>
// Project
// ├─ columns: [xy.x:1!null, SUM(xy.y):4!null as sum(y)]
// └─ Sort((xy.x:1!null - COUNT(xy.y):5!null) ASC nullsFirst)
//     └─ GroupBy
//         ├─ select: xy.y:2!null, xy.x:1!null, SUM(xy.y:2!null), COUNT(xy.y:2!null)
//         ├─ group: xy.x:1!null
//         └─ Table
//             ├─ name: xy
//             └─ columns: [x y z]
//
// Passthrough columns not included in the SELECT target list are included in the
// aggregation outputs:
//
// select x from xy having z > 0
//
// Project
// ├─ columns: [xy.x:1!null]
// └─ Having
//     ├─ GreaterThan
//     │   ├─ xy.z:3!null
//     │   └─ 0 (tinyint)
//     └─ GroupBy
//         ├─ select: xy.x:1!null, xy.z:3!null
//         ├─ group:
//         └─ Table
//             ├─ name: xy
//             └─ columns: [x y z]
//
// Aggregations are probably a long-tail of testing to get this behavior right,
// particularly when aggregate functions are places out of their execution scope.
//
// The second difficulty is how to represent expressions and references while
// building the plan, and how low in the tree to execute expression logic. This
// is a secondary concern compared to generating unique ids for aggregation
// functions and source columns.
//
// For example, (x+z) is a target and grouping column below. The aggregation could
// return (x+z) which the target list passes through:
//
// SELECT count(xy.x) AS count_1, x + z AS lx FROM xy GROUP BY x + z
// =>
// Project
// ├─ columns: [COUNT(xy.x):4!null as count_1, (xy.x:1!null + xy.z:3!null) as lx]
// └─ GroupBy
//     ├─ select: xy.x:1!null, (xy.x:1!null + xy.z:3!null), COUNT(xy.x:1!null), xy.z:3!null
//     ├─ group: (xy.x:1!null + xy.z:3!null)
//     └─ Table
//         ├─ name: xy
//         └─ columns: [x y z]
//
// We do not have a good way of referencing expressions that are not aggregation
// functions or table columns. In other databases, expressions are interned when
// they are added to the memo. So an expression will be evaluated and available
// for reference at the lowest level of the tree it was built. If an aggregation
// builds an expression, the projection built later will find the reference and
// avoid re-computing the value. If a relation earlier in the tree built a
// subtree of an expression currently being built, it can input the reference
// rather than computing the subtree.
//
// Questions:
// - can all node types be resolved with this setup? do we run into problems
//   w/ subqueries?
// - should we intern expression strings soon or put off?
// - can type checking fit in this setup?
//
// TODO:
// - A lot of validation logic is missing. Ambiguous table names, column names.
//   Validating strict grouping columns.
// - Subqueries still a bit broken, it would be nice for expression ids to by
//   globally incremented.
// - CTEs and recursive CTEs a bit broken.
// - Windows (skip on first pass?)
// - Need to see whether indexing pass is necessary before launching plans into
//   regular analyzer.
// - Parser branching logic for falling back to current parser converter.
// - Analyzer branching logic for removing resolve rules for new path.
//
