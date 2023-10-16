package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/dolthub/go-mysql-server/enginetest/sqllogictest/utils"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	args := os.Args[1:]

	if len(args) != 2 {
		panic("expected 2 args")
	}

	infile, err := os.Open(args[0])
	if err != nil {
		panic(err)
	}
	defer infile.Close()

	outfile, err := os.Create(args[1])
	if err != nil {
		panic(err)
	}
	defer outfile.Close()

	scanner := bufio.NewScanner(infile)
	for {
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		if len(line) == 0 {
			outfile.WriteString("\n")
			continue
		}
		if strings.HasPrefix(line, "#") {
			outfile.WriteString(line + "\n")
			continue
		}
		if strings.HasPrefix(line, "statement ok") {
			rewriteStmt(scanner, outfile)
			continue
		}
		if strings.HasPrefix(line, "query error") || strings.HasPrefix(line, "statement error") {
			rewriteError(scanner, outfile)
			continue
		}
		if strings.HasPrefix(line, "query") {
			rewriteQuery(scanner, line, outfile)
			continue
		}
	}
}

func rewriteStmt(scanner *bufio.Scanner, outfile *os.File) {
	var stmt string
	once := true
	for {
		if !scanner.Scan() {
			panic("expected statement")
		}
		part := scanner.Text()
		if len(part) == 0 {
			break
		}

		parts := strings.Split(part, "; ")
		if len(parts) > 1 {
			// multiple statements in one line for some reason
			for _, p := range parts {
				writeStmt(p, outfile)
				once = true
			}
			continue
		}

		if once {
			once = false
			stmt += part
		} else {
			stmt += "\n" + part
		}
		if strings.HasSuffix(part, ";") {
			writeStmt(stmt, outfile)
			once = true
			stmt = ""
		}
	}

	if len(stmt) != 0 {
		writeStmt(stmt, outfile)
	}
}

func writeStmt(stmt string, outfile *os.File) {
	outfile.WriteString("statement ok\n")
	outfile.WriteString(stmt + "\n\n")
}

func rewriteError(scanner *bufio.Scanner, outfile *os.File) {
	outfile.WriteString("statement error\n")

	stmt := utils.ReadStmt(scanner)
	outfile.WriteString(stmt + "\n\n")
}

func rewriteQuery(scanner *bufio.Scanner, line string, outfile *os.File) {
	schema := strings.Split(line, " ")[1]
	hasColNames := strings.Contains(line, "colnames")
	hasRowSort := strings.Contains(line, "rowsort")

	if hasRowSort {
		// TODO: throw warning about putting order by in query
	}

	// expect query
	query := utils.ReadQuery(scanner)

	// expect colnames; drop them
	if hasColNames && !scanner.Scan() {
		panic("expected colnames")
	}

	// expect results
	rows := utils.ReadResults(scanner)

	// ignore queries with full outer join or full join
	if strings.Contains(strings.ToLower(query), "full outer join") || strings.Contains(strings.ToLower(query), "full join") {
		return
	}

	outfile.WriteString(fmt.Sprintf("query %s nosort\n", schema))
	outfile.WriteString(query + "\n")
	outfile.WriteString(utils.SEP + "\n")
	for _, row := range rows {
		outfile.WriteString(row + "\n")
	}
	outfile.WriteString("\n")
}
