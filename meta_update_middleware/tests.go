package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("tests:init()")
}

func testGetCTCCount(conn CbConnection) {
	log.Println("testGetCTCCount()")

	fileContent, err := os.ReadFile("sqls/getCTCCount.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	log.Println(tmplSQL)

	queryWithSQLStringTest(conn.Scope, tmplSQL)
}

func testGetSingleCTC(conn CbConnection) {
	log.Println("testGetSingleCTC()")

	fileContent, err := os.ReadFile("sqls/getSingleCTC.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	log.Println(tmplSQL)

	queryWithSQLStringTest(conn.Scope, tmplSQL)
}

func queryWithSQLStringTest(scope *gocb.Scope, text string) {

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		log.Fatal(err)
	}

	rows := make([]interface{}, 0)

	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			log.Fatal(err)
		}
		m := row.(map[string]interface{})
		rows = append(rows, m)
		fmt.Println(rows)
	}
	for i := 0; i < len(rows); i++ {
		m := rows[i].(map[string]interface{})
		walkJsonMap(m, 0)
	}
	log.Println(jsonPrettyPrint(rows))
}
