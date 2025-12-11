package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"
)

type CbConnection struct {
	Cluster    *gocb.Cluster
	Bucket     *gocb.Bucket
	Scope      *gocb.Scope
	Collection *gocb.Collection
	vxDBTARGET string
}

// init runs before main() is evaluated
func init() {
	log.Println("db-utils:init()")
}

func getDbConnection(cred Credentials) (conn CbConnection, err error) {
	log.Println("getDbConnection()")

	conn = CbConnection{}
	connectionString := cred.Cb_host
	bucketName := cred.Cb_bucket
	collection := cred.Cb_collection
	username := cred.Cb_user
	password := cred.Cb_password

	options := gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	}

	cluster, err := gocb.Connect(connectionString, options)
	if err != nil {
		err = fmt.Errorf("failed to connect to cluster: %w", err)
		return conn, err
	}

	conn.Cluster = cluster
	conn.Bucket = conn.Cluster.Bucket(bucketName)
	conn.Collection = conn.Bucket.Collection(collection)
	conn.vxDBTARGET = cred.Cb_bucket + "." + cred.Cb_scope + "." + cred.Cb_collection

	log.Println("vxDBTARGET:" + conn.vxDBTARGET)

	err = conn.Bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		return conn, err
	}

	conn.Scope = conn.Bucket.Scope(cred.Cb_scope)
	return conn, nil
}

func queryWithSQLFile(scope *gocb.Scope, file string) (jsonOut []string, err error) {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		log.Println("Error reading file:", err)
		return nil, err
	}

	// Convert []byte to string
	text := string(fileContent)
	fmt.Println(text)
	result, err := queryWithSQLStringSA(scope, text)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func queryWithSQLStringSA(scope *gocb.Scope, text string) (rv []string, err error) {
	log.Println("queryWithSQLStringSA(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{
			Adhoc:   true,
			Timeout: 300 * time.Second, // Set an explicit timeout
		},
	)
	if err != nil {
		return nil, err
	}

	// Interfaces for handling streaming return values
	retValues := []string{}

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			return nil, err
		}
		retValues = append(retValues, row.(string))
	}

	return retValues, nil
}

func queryWithSQLStringFA(scope *gocb.Scope, text string) (rv []float64, err error) {
	log.Println("queryWithSQLStringFA(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true, Timeout: 300 * time.Second},
	)
	if err != nil {
		return nil, err
	}

	retValues := make([]float64, 0)

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			return nil, err
		}
		retValues = append(retValues, row.(float64))
	}

	return retValues, nil
}

func queryWithSQLStringIA(scope *gocb.Scope, text string) (rv []int, err error) {
	log.Println("queryWithSQLStringFA(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true, Timeout: 300 * time.Second},
	)
	if err != nil {
		return nil, err
	}

	retValues := make([]int, 0)

	// Stream the values returned from the query into an untyped and unstructred
	// array of interfaces
	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			return nil, err
		}
		switch v := row.(type) {
		case float64:
			retValues = append(retValues, int(v))
		case int:
			retValues = append(retValues, v)
		}
	}

	return retValues, nil
}

func queryWithSQLStringMAP(scope *gocb.Scope, text string) (jsonOut []interface{}, err error) {
	log.Println("queryWithSQLStringMAP(\n" + text + "\n)")

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true, Timeout: 300 * time.Second},
	)
	if err != nil {
		return nil, err
	}

	rows := make([]interface{}, 0)

	for queryResult.Next() {
		var row interface{}
		err := queryResult.Row(&row)
		if err != nil {
			return nil, err
		}
		m := row.(map[string]interface{})
		rows = append(rows, m)
	}
	return rows, nil
}

func queryWithSQLFileJustPrint(scope *gocb.Scope, file string) error {
	fileContent, err := os.ReadFile(file)
	if err != nil {
		return err
	}

	// Convert []byte to string
	text := string(fileContent)
	fmt.Println(text)

	queryResult, err := scope.Query(
		fmt.Sprintf(text),
		&gocb.QueryOptions{Adhoc: true, Timeout: 300 * time.Second},
	)
	if err != nil {
		return err
	}
	return printQueryResult(queryResult)
}

func printQueryResult(queryResult *gocb.QueryResult) error {
	for queryResult.Next() {
		var result interface{}
		err := queryResult.Row(&result)
		if err != nil {
			return err
		}
		fmt.Println(result)
	}
	return nil
}
