package main

import (
	"bufio"
	"fmt"
	"github.com/dolthub/go-mysql-server/enginetest/tmp/utils"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	args := os.Args[1:]

	if len(args) != 1 {
		panic("expected 1 arg")
	}

	file, err := os.Open(args[0])
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for {
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		if len(line) == 0 {
			fmt.Println()
			continue
		}
		if strings.HasPrefix(line, "#") {
			fmt.Println(line)
			continue
		}
		if strings.HasPrefix(line, "statement ok") {
			rewriteStmt(scanner)
			continue
		}
		if strings.HasPrefix(line, "query error") {
			rewriteError(scanner)
			continue
		}
		if strings.HasPrefix(line, "query") {
			rewriteQuery(scanner, line)
			continue
		}
	}
}

func rewriteStmt(scanner *bufio.Scanner) {
	var stmt string
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
				writeStmt(p)
			}
			continue
		}

		stmt += part
		if strings.HasSuffix(part, ";") {
			writeStmt(stmt)
			stmt = ""
		}
	}

	if len(stmt) != 0 {
		writeStmt(stmt)
	}
}

func writeStmt(stmt string) {
	fmt.Println("statement ok")
	fmt.Println(stmt)
	fmt.Println()
}

func rewriteError(scanner *bufio.Scanner) {
	fmt.Println("statement error")

	// expect a query, then blank line
	if !scanner.Scan() {
		panic("expected error query")
	}
	line := scanner.Text()
	if len(line) == 0 {
		panic("expected error query, but got blank line")
	}
	fmt.Println(line)

	if !scanner.Scan() {
		panic("expected blank line")
	}
	line = scanner.Text()
	if len(line) != 0 {
		panic("expected blank line, but got query")
	}
	fmt.Println()
}

func rewriteQuery(scanner *bufio.Scanner, line string) {
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

	fmt.Printf("query %s nosort\n", schema)
	fmt.Println(query)
	fmt.Println(utils.SEP)
	for _, row := range rows {
		fmt.Println(row)
	}
	fmt.Println()
}