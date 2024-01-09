package main

import (
	"encoding/json"
//	"fmt"
	"log"
	"os"
	"slices"
	"time"

	"github.com/couchbase/gocb/v2"
)

type StrArray []string

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
	Metadata []struct {
		Name       string   `json:"name"`
		App        string   `json:"app"`
		SubDocType string   `json:"subDocType"`
		DocType    StrArray `json:"docType"`
	} `json:"metadata"`
}

type CbConnection struct {
	Cluster    *gocb.Cluster
	Bucket     *gocb.Bucket
	Scope      *gocb.Scope
	Collection *gocb.Collection
	vxDBTARGET string
}

// init runs before main() is evaluated
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("meta-update:init()")
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

	// log.Println(conf.Datasets[0].Name)

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
	conn.vxDBTARGET = conf.Private.Databases[0].Bucket + "._default." + conf.Private.Databases[0].Collection

	log.Println("vxDBTARGET:" + conn.vxDBTARGET)

	err = conn.Bucket.WaitUntilReady(5*time.Second, nil)
	if err != nil {
		log.Fatal(err)
		return
	}

	conn.Scope = conn.Bucket.Scope(conf.Private.Databases[0].Scope)

	//testGetSingleCTC(conn)
	//testGetCTCCount(conn)

	for ds := 0; ds < len(conf.Metadata); ds++ {
		for dt := 0; dt < len(conf.Metadata[ds].DocType); dt++ {
			log.Println("Metadata:" + conf.Metadata[ds].Name + ",DocType:" + conf.Metadata[ds].DocType[dt])
			updateMedataForAppDocType(conn, conf.Metadata[ds].Name, conf.Metadata[ds].App, conf.Metadata[ds].DocType[dt], conf.Metadata[ds].SubDocType)
			// TODO - remove break for after testing
			break
		}
		// TODO - remove break for after testing
		break
	}
}

func updateMedataForAppDocType(conn CbConnection, dataset string, app string, doctype string, subDocType string) {
	log.Println("updateMedataForAppDocType(" + dataset + "," + doctype + ")")

	// get needed models
	models := getModels(conn, dataset, app, doctype, subDocType)
	log.Println("models:")
	printStringArray(models)

	// get models having metadata but no data (remove metadata for these)
	// (note 'like %' is changed to 'like %25')
	models_with_metatada_but_no_data := getModelsNoData(conn, dataset, app, doctype, subDocType)
	log.Println("models_with_metatada_but_no_data:")
	printStringArray(models_with_metatada_but_no_data)

	//log.Println("Inserting fake model:RAP_OOPS_130 to test SQL ...")
	// models_with_metatada_but_no_data = append(models_with_metatada_but_no_data, "RAP_OOPS_130")

	// remove metadata for models with no data
	removeMetadataForModelsWithNoData(conn, dataset, app, doctype, subDocType, models_with_metatada_but_no_data)

	// get models with existing metadada
	models_with_existing_metadata := getModelsWithExistingMetadata(conn, dataset, app, doctype, subDocType)
	log.Println("models_with_existing_metadata:")
	printStringArray(models_with_existing_metadata)

	// initialize the metadata for the models for which the metadata does not exist
	for i := 0; i < len(models); i++ {
		contains := slices.Contains(models_with_existing_metadata, models[i])
		// log.Println(fmt.Printf("contains:%t\n", contains))
		if !contains {
			initializeMetadataForModel(conn, dataset, app, doctype, subDocType, models[i])
		}
	}

	for i := 0; i < len(models); i++ {
		thresholds := getDistinctThresholds(conn, dataset, app, doctype, subDocType, models[i])
		log.Println(thresholds)
	}

	/*
		// get a sorted list of all the models
		// now update all the metdata for all the models that require it
		fileContent, err = os.ReadFile("sqls/updateMetadata.sql")
		if err != nil {
			log.Fatal(err)
		}
		tmplUpdateMetadataSQL := string(fileContent)
		log.Println(tmplUpdateMetadataSQL)
		for i := 0; i < len(models); i++ {
			model := models[i]
			log.Println(model)
		}
	*/
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
