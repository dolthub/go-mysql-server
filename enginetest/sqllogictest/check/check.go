package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/dolthub/go-mysql-server/enginetest/sqllogictest/utils"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr/v2"
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

	conn, err := dbr.Open("mysql", fmt.Sprintf("root:root@tcp(localhost:3306)/"), nil)
	//conn, err := dbr.Open("mysql", fmt.Sprintf("dolt@tcp(localhost:3307)/"), nil)
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("drop database if exists tmp")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("create database tmp")
	if err != nil {
		panic(err)
	}
	_, err = conn.Exec("use tmp")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	scanner := bufio.NewScanner(file)
	for {
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		if line == "statement ok" {
			stmt := utils.ReadStmt(scanner)
			if _, err := conn.Exec(stmt); err != nil {
				panic(fmt.Sprintf("%s \nerr: %v", stmt, err))
			}
			continue
		}
		if strings.HasPrefix(line, "statement error") {
			stmt := utils.ReadStmt(scanner)
			if _, err := conn.Query(stmt); err == nil {
				panic(fmt.Sprintf("%s \nexpected error, but got none", stmt))
			}
			continue
		}
		if strings.HasPrefix(line, "query") {
			if err := handleQuery(scanner, conn); err != nil {
				panic(err)
			}
			continue
		}
	}

	fmt.Println("All tests passed")
}

func handleQuery(scanner *bufio.Scanner, conn *dbr.Connection) error {
	query := utils.ReadQuery(scanner)
	_, rows, err := executeQuery(conn, query)
	if err != nil {
		panic(fmt.Sprintf("%s \nerr: %v ", query, err))
	}
	expectedRows := utils.ReadResults(scanner)
	err = compareRows(rows, expectedRows)
	if err != nil {
		return fmt.Errorf("%s \nerr: %v ", query, err)
	}
	return nil
}

// executeQuery executes the given query and returns the columns and rows (flattened)
func executeQuery(conn *dbr.Connection, query string) ([]string, []string, error) {
	res, err := conn.Query(query)
	if err != nil {
		return nil, nil, err
	}

	cols, _ := res.Columns()
	numCols := len(cols)

	rows := make([]string, 0)
	rowBuf := make([]interface{}, numCols)
	for j := 0; j < numCols; j++ {
		rowBuf[j] = new(interface{})
	}
	for res.Next() {
		if err := res.Scan(rowBuf...); err != nil {
			return nil, nil, err
		}
		for i := 0; i < numCols; i++ {
			rawVal := *rowBuf[i].(*interface{})
			if rawVal == nil {
				rows = append(rows, "NULL")
			} else {
				rows = append(rows, fmt.Sprintf("%s", rawVal))
			}
		}
	}

	return cols, rows, nil
}

func compareCols(cols, expectedCols []string) {
	if len(cols) != len(expectedCols) {
		panic(fmt.Sprintf("column lengths not equal: actual: %v, expected %v", len(cols), len(expectedCols)))
	}
	for i := 0; i < len(cols); i++ {
		if expectedCols[i] != "" && cols[i] != expectedCols[i] {
			panic(fmt.Sprintf("column %d not equal: actual: %v, expected %v", i, cols[i], expectedCols[i]))
		}
	}
}

func compareRows(rows, expectedRows []string) error {
	if len(rows) != len(expectedRows) {
		return fmt.Errorf("row lengths not equal: actual: %v, expected %v", len(rows), len(expectedRows))
	}
	for i := 0; i < len(rows); i++ {
		if rows[i] != expectedRows[i] {
			return fmt.Errorf("row %d not equal: actual: %v, expected %v", i, rows, expectedRows)
		}
	}
	return nil
}

func printRows(rows [][]string) {
	for _, row := range rows {
		for _, col := range row {
			fmt.Printf("%s\t", col)
		}
		fmt.Println()
	}
}
