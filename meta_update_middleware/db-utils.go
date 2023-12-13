package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("db-utils:init()")
}

func queryWithSQLFile(scope *gocb.Scope, file string) (jsonOut []string) {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	// Convert []byte to string
	text := string(fileContent)
	fmt.Println(text)

	return queryWithSQLString(scope, text)
}

func queryWithSQLStringTest(scope *gocb.Scope, text string) {

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		log.Fatal(err)
	}

	// Interfaces for handling streaming return values
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
		walkJsonMap(m)
	}
}

func walkJsonMap(val map[string]interface{}) {
	for k, v := range val {
		switch vv := v.(type) {
		case string:
			fmt.Println(k, "is string", vv)
		case float64:
			fmt.Println(k, "is float64", vv)
		case []interface{}:
			fmt.Println(k, "is an array:")
			for i, u := range vv {
				fmt.Println(i, u)
			}
		case map[string]interface{}:
			fmt.Println(k, "is of a type map")
			m := v.(map[string]interface{})
			walkJsonMap(m)
		default:
			fmt.Println(k, "is of a type I don't know how to handle")
		}
	}
}

func queryWithSQLString(scope *gocb.Scope, text string) (jsonOut []string) {

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		log.Fatal(err)
	}

	// Interfaces for handling streaming return values
	retValues := []string{}

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			log.Fatal(err)
		}
		retValues = append(retValues, row.(string))
	}

	return retValues
}

func queryWithSQLFileJustPrint(scope *gocb.Scope, file string) {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	// Convert []byte to string
	text := string(fileContent)
	fmt.Println(text)

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		log.Fatal(err)
	} else {
		printQueryResult(queryResult)
	}
}

func printQueryResult(queryResult *gocb.QueryResult) {
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
	}
}

func printStringArray(in []string) {
	for i := 0; i < len(in); i++ {
		fmt.Println(in[i])
	}
}

func jsonPrettyPrint(in []interface{}) string {
	jsonText, err := json.Marshal(in)
	if err != nil {
		fmt.Println("ERROR PROCESSING STREAMING OUTPUT:", err)
	}
	var out bytes.Buffer
	json.Indent(&out, jsonText, "", "\t")
	return out.String()
}
