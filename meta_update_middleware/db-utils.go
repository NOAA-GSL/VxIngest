package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("db-utils:init()")
}

func getDbConnection(conf ConfigJSON, dbIdx int) (conn  CbConnection) {
	log.Println(fmt.Sprintf("getDbConnection(%d)", dbIdx))

	conn = CbConnection{}
	connectionString := conf.Private.Databases[dbIdx].Host
	bucketName := conf.Private.Databases[dbIdx].Bucket
	username := conf.Private.Databases[dbIdx].User
	password := conf.Private.Databases[dbIdx].Password

	options := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	}

	cluster, err := gocb.Connect("couchbase://"+connectionString, options)
	if err != nil {
		log.Fatal(err)
		return
	}

	conn.Cluster = cluster
	conn.Bucket = conn.Cluster.Bucket(bucketName)
	conn.vxDBTARGET = conf.Private.Databases[dbIdx].Bucket + "._default." + conf.Private.Databases[dbIdx].Collection

	log.Println("vxDBTARGET:" + conn.vxDBTARGET)

	err = conn.Bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
		return
	}

	conn.Scope = conn.Bucket.Scope(conf.Private.Databases[dbIdx].Scope)
	return conn
}


func queryWithSQLFile(scope *gocb.Scope, file string) (jsonOut []string) {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	// Convert []byte to string
	text := string(fileContent)
	fmt.Println(text)

	return queryWithSQLStringSA(scope, text)
}

func queryWithSQLStringSA(scope *gocb.Scope, text string) (rv []string) {
	log.Println("queryWithSQLStringSA(\n" + text + "\n)")

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

func queryWithSQLStringFA(scope *gocb.Scope, text string) (rv []float64) {
	log.Println("queryWithSQLStringFA(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		log.Fatal(err)
	}

	retValues := make([]float64, 0)

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			log.Fatal(err)
		}
		retValues = append(retValues, row.(float64))
	}

	return retValues
}

func queryWithSQLStringMAP(scope *gocb.Scope, text string) (jsonOut []interface{}) {
	log.Println("queryWithSQLStringMAP(\n" + text + "\n)")

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
	}
	return rows
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
