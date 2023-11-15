package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
)

type ConfigJSON struct {
	Private struct {
		Databases []struct {
			Role       string `json:"role"`
			Status     string `json:"status"`
			Host       string `json:"host"`
			Bucket     string `json:"bucket"`
			Scope      string `json:"scope"`
			Collection string `json:"collection"`
			Port       string `json:"port"`
			User       string `json:"user"`
			Password   string `json:"password"`
		} `json:"databases"`
	} `json:"private"`
}

type CbConnection struct {
	Cluster    *gocb.Cluster
	Bucket     *gocb.Bucket
	Scope      *gocb.Scope
	Collection *gocb.Collection
}

func main() {
	log.Print("meta-update:main()")

	conf := ConfigJSON{}
	conn := CbConnection{}

	conf, err := parseConfig("settings.json")
	if err != nil {
		log.Fatal("Unable to parse config")
		return
	}

	// Uncomment following line to enable logging
	// gocb.SetLogger(gocb.VerboseStdioLogger())

	connectionString := conf.Private.Databases[0].Host
	bucketName := conf.Private.Databases[0].Bucket
	username := conf.Private.Databases[0].User
	password := conf.Private.Databases[0].Password

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

	err = conn.Bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
		return
	}

	conn.Scope = conn.Bucket.Scope(conf.Private.Databases[0].Scope)

	// get needed models
	models_requiring_metadata := queryWithSQLFile(conn.Scope, "sqls/getModels.sql")
	log.Println("models_requiring_metadata:")
	printStringArray(models_requiring_metadata)
	for i := 0; i < len(models_requiring_metadata); i++ {
		log.Println(fmt.Sprintf("%d\t%v", i, models_requiring_metadata[i]))
	}

	// remove metadata for models with no data
	models_with_existing_metadata := queryWithSQLFile(conn.Scope, "sqls/getModelsWithMetadata.sql")
	log.Println("models_with_existing_metadata:")
	printStringArray(models_with_existing_metadata)
	for i := 0; i < len(models_with_existing_metadata); i++ {
		log.Println(fmt.Sprintf("%d\t%v", i, models_with_existing_metadata[i]))
	}

	fileContent, err := os.ReadFile("sqls/deleteModelsWithNoData.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplDeleteModelsSQL := string(fileContent)
	log.Println(tmplDeleteModelsSQL)

	remove_metadata_for_models := queryWithSQLFile(conn.Scope, "sqls/getModelsNoData.sql")
	log.Println("Inserting fake model:RAP_OOPS_130 to test SQL ...")
	remove_metadata_for_models = append(remove_metadata_for_models, "RAP_OOPS_130")
	log.Println("remove_metadata_for_models:")
	printStringArray(remove_metadata_for_models)
	for i := 0; i < len(remove_metadata_for_models); i++ {
		delModelSQL := strings.Replace(tmplDeleteModelsSQL, "{vxMODEL}", remove_metadata_for_models[i], 1)
		log.Println(fmt.Sprintf("%d\t%v", i, remove_metadata_for_models[i]))
		log.Println("delModelSQL:\n" + delModelSQL)
		queryResult, err := conn.Scope.Query(
			delModelSQL,
			&gocb.QueryOptions{Adhoc: true},
		)
		if err != nil {
			log.Fatal(err)
		} else {
			printQueryResult(queryResult)
		}
	}

	// initialize the metadata for the models for which the metadata does not exist
	fileContent, err = os.ReadFile("sqls/initializeMetadata.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplInitializeMetadataSQL := string(fileContent)
	log.Println(tmplDeleteModelsSQL)
	for i := 0; i < len(models_requiring_metadata); i++ {
		contains := slices.Contains(models_with_existing_metadata, models_requiring_metadata[i])
		log.Println(fmt.Printf("contains:%t\n", contains))
		if !contains {
			initializeMetadataSQL := strings.Replace(tmplInitializeMetadataSQL, "{vxMODEL}", models_requiring_metadata[i], -1)
			log.Println("initializeMetadataSQL:\n" + initializeMetadataSQL)
		}
		/*
			queryResult, err := conn.Scope.Query(
				initializeMetadataSQL,
				&gocb.QueryOptions{Adhoc: true},
			)
			if err != nil {
				log.Fatal(err)
			} else {
				printQueryResult(queryResult)
			}
		*/
	}

	// get a sorted list of all the models_requiring_metadata
	// now update all the metdata for all the models that require it
	fileContent, err = os.ReadFile("sqls/updateMetadata.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplUpdateMetadataSQL := string(fileContent)
	log.Println(tmplUpdateMetadataSQL)
	for i := 0; i < len(models_requiring_metadata); i++ {
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

func queryWithSQLFile(scope *gocb.Scope, file string) (jsonOut []string) {
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

func parseConfig(file string) (ConfigJSON, error) {
	log.Println("parseConfig(" + file + ")")

	conf := ConfigJSON{}
	configFile, err := os.Open(file)
	if err != nil {
		log.Print("opening config file", err.Error())
		configFile.Close()
		return conf, err
	}
	defer configFile.Close()

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&conf); err != nil {
		log.Fatalln("parsing config file", err.Error())
		return conf, err
	}

	return conf, nil
}
