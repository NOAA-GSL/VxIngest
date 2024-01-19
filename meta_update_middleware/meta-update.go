package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
	// "github.com/couchbase/gocb/v2"
)

type StrArray []string

type ConfigJSON struct {
	Metadata []struct {
		Name       string   `json:"name"`
		App        string   `json:"app"`
		SubDocType string   `json:"subDocType"`
		DocType    StrArray `json:"docType"`
	} `json:"metadata"`
}

type Credentials struct {
	Cb_host       string `yaml:"cb_host"`
	Cb_user       string `yaml:"cb_user"`
	Cb_password   string `yaml:"cb_password"`
	Cb_bucket     string `yaml:"cb_bucket"`
	Cb_scope      string `yaml:"cb_scope"`
	Cb_collection string `yaml:"cb_collection"`
}

// init runs before main() is evaluated
func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Println("meta-update:init()")
}

func main() {
	// Uncomment following line to enable logging
	// gocb.SetLogger(gocb.VerboseStdioLogger())

	start := time.Now()
	log.Print("meta-update:main()")

	home, _ := os.UserHomeDir()
	var credentialsFilePath string
	flag.StringVar(&credentialsFilePath, "c", home+"/credentials", "path to credentials file")

	var settingsFilePath string
	flag.StringVar(&settingsFilePath, "s", "./settings.json", "path to settings.json file")

	var app string
	flag.StringVar(&app, "a", "", "app name")

	flag.Parse()

	if len(app) > 0 {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",app:" + app)
	} else {
		log.Println("meta-update, settings file:" + settingsFilePath + ",credentials file:" + credentialsFilePath + ",app:[all apps in settings file]")
	}

	conf := ConfigJSON{}

	conf, err := parseConfig(settingsFilePath)
	if err != nil {
		log.Fatal("Unable to parse config")
		return
	}

	credentials := getCredentials(credentialsFilePath)

	conn := getDbConnection(credentials)

	//testGetSingleCTC(conn)
	//testGetCTCCount(connSrc)

	for ds := 0; ds < len(conf.Metadata); ds++ {
		if len(app) > 0 {
			if app != conf.Metadata[ds].Name {
				continue
			}
		}
		for dt := 0; dt < len(conf.Metadata[ds].DocType); dt++ {
			log.Println("Metadata:" + conf.Metadata[ds].Name + ",DocType:" + conf.Metadata[ds].DocType[dt])
			updateMedataForAppDocType(conn, conf.Metadata[ds].Name, conf.Metadata[ds].App, conf.Metadata[ds].DocType[dt], conf.Metadata[ds].SubDocType)
		}
	}
	log.Println(fmt.Sprintf("\tmeta update finished in %v", time.Since(start)))
}

func updateMedataForAppDocType(conn CbConnection, name string, app string, doctype string, subDocType string) {
	log.Println("updateMedataForAppDocType(" + name + "," + doctype + ")")

	// get needed models
	models := getModels(conn, name, app, doctype, subDocType)
	log.Println("models:")
	printStringArray(models)

	// get models having metadata but no data (remove metadata for these)
	// (note 'like %' is changed to 'like %25')
	models_with_metatada_but_no_data := getModelsNoData(conn, name, app, doctype, subDocType)
	log.Println("models_with_metatada_but_no_data:")
	printStringArray(models_with_metatada_but_no_data)

	/*
		//log.Println("Inserting fake model:RAP_OOPS_130 to test SQL ...")
		// models_with_metatada_but_no_data = append(models_with_metatada_but_no_data, "RAP_OOPS_130")

		// remove metadata for models with no data
		removeMetadataForModelsWithNoData(connDst, name, app, doctype, subDocType, models_with_metatada_but_no_data)

		// get models with existing metadada
		models_with_existing_metadata := getModelsWithExistingMetadata(connSrc, name, app, doctype, subDocType)
		log.Println("models_with_existing_metadata:")
		printStringArray(models_with_existing_metadata)

		// initialize the metadata for the models for which the metadata does not exist
		for i := 0; i < len(models); i++ {
			contains := slices.Contains(models_with_existing_metadata, models[i])
			// log.Println(fmt.Printf("contains:%t\n", contains))
			if !contains {
				initializeMetadataForModel(connDst, name, app, doctype, subDocType, models[i])
			}
		}
	*/

	metadata := MetadataJSON{ID: "MD:matsGui:" + name + ":COMMON:V01", Name: name, App: app}
	metadata.Updated = 0

	for i, m := range models {
		model := Model{Name: m}
		thresholds := getDistinctThresholds(conn, name, app, doctype, subDocType, m)
		log.Println(thresholds)
		fcstLen := getDistinctFcstLen(conn, name, app, doctype, subDocType, m)
		log.Println(fcstLen)
		region := getDistinctRegion(conn, name, app, doctype, subDocType, m)
		log.Println(region)
		displayText := getDistinctDisplayText(conn, name, app, doctype, subDocType, m)
		log.Println(displayText)
		displayCategory := getDistinctDisplayCategory(conn, name, app, doctype, subDocType, m)
		log.Println(displayCategory)
		displayOrder := getDistinctDisplayOrder(conn, name, app, doctype, subDocType, m, i)
		log.Println(displayOrder)
		minMaxCountFloor := getMinMaxCountFloor(conn, name, app, doctype, subDocType, m)
		log.Println(jsonPrettyPrintStruct(minMaxCountFloor[0].(map[string]interface{})))

		// ./sqls/getDistinctThresholds.sql returns list of variables for SUMS DocType, like in Surface
		if doctype == "SUMS" {
			model.Variables = thresholds
		} else {
			model.Thresholds = thresholds
		}
		model.Model = models[i]
		model.FcstLens = fcstLen
		model.Regions = region
		model.DisplayText = displayText[0]
		model.DisplayCategory = displayCategory[0]
		model.DisplayOrder = displayOrder[0]
		model.Mindate = int(minMaxCountFloor[0].(map[string]interface{})["mindate"].(float64))
		model.Maxdate = int(minMaxCountFloor[0].(map[string]interface{})["maxdate"].(float64))
		model.Numrecs = int(minMaxCountFloor[0].(map[string]interface{})["numrecs"].(float64))
		metadata.Updated = int(minMaxCountFloor[0].(map[string]interface{})["updated"].(float64))
		metadata.Models = append(metadata.Models, model)
	}
	log.Println(jsonPrettyPrintStruct(metadata))
	writeMetadataToDb(conn, metadata)
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

func getCredentials(credentialsFilePath string) Credentials {
	creds := Credentials{}
	yamlFile, err := os.ReadFile(credentialsFilePath)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, &creds)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}
	return creds
}
