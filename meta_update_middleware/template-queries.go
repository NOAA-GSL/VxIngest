package main

import (
	"log"
	"os"
	"strings"

	"github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("template-queries:init()")
}

func getModels(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string) {
	log.Println("getModels(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")

	fileContent, err := os.ReadFile("sqls/getModels.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplGetModelsSQL := string(fileContent)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	log.Println(tmplGetModelsSQL)

	models_requiring_metadata := queryWithSQLString(conn.Scope, tmplGetModelsSQL)
	return models_requiring_metadata
}

func getModelsNoData(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string) {
	log.Println("getModelsNoData(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")

	fileContent, err := os.ReadFile("sqls/getModelsNoData.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplgetModelsNoDataSQL := string(fileContent)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxAPP}}", app, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxSUBDOCTYPE}}", doctype, -1)
	models_with_metatada_but_no_data := queryWithSQLString(conn.Scope, tmplgetModelsNoDataSQL)

	return models_with_metatada_but_no_data
}

func removeMetadataForModelsWithNoData(conn CbConnection, dataset string, app string, doctype string, subDocType string, models_with_metatada_but_no_data []string) {
	log.Println("removeMetadataForModelsWithNoData(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")

	fileContent, err := os.ReadFile("sqls/deleteModelMetadata.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplDeleteModelMetadataSQL := string(fileContent)
	tmplDeleteModelMetadataSQL = strings.Replace(tmplDeleteModelMetadataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplDeleteModelMetadataSQL = strings.Replace(tmplDeleteModelMetadataSQL, "{{vxAPP}}", doctype, -1)

	for i := 0; i < len(models_with_metatada_but_no_data); i++ {
		delModelSQL := strings.Replace(tmplDeleteModelMetadataSQL, "{{vxMODEL}}", models_with_metatada_but_no_data[i], 1)
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
}

func getModelsWithExistingMetadata(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string) {
	log.Println("getModelsWithExistingMetadata(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	fileContent, err := os.ReadFile("sqls/getModelsWithMetadata.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplgetModelsWithMetadataSQL := string(fileContent)
	tmplgetModelsWithMetadataSQL = strings.Replace(tmplgetModelsWithMetadataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, 1)
	tmplgetModelsWithMetadataSQL = strings.Replace(tmplgetModelsWithMetadataSQL, "{{vxAPP}}", app, -1)
	log.Println(tmplgetModelsWithMetadataSQL)

	models_with_existing_metadata := queryWithSQLString(conn.Scope, tmplgetModelsWithMetadataSQL)
	return models_with_existing_metadata
}

func initializeMetadataForModel(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) {
	log.Println("initializeMetadataForModel(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")

	fileContent, err := os.ReadFile("sqls/initializeMetadata.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplInitializeMetadataSQL := string(fileContent)
	tmplInitializeMetadataSQL = strings.Replace(tmplInitializeMetadataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplInitializeMetadataSQL = strings.Replace(tmplInitializeMetadataSQL, "{{vxAPP}}", doctype, -1)
	tmplInitializeMetadataSQL = strings.Replace(tmplInitializeMetadataSQL, "{{vxMODEL}}", model, -1)
	log.Println(tmplInitializeMetadataSQL)
	queryResult, err := conn.Scope.Query(
		tmplInitializeMetadataSQL, &gocb.QueryOptions{Adhoc: true},
	)
	if err != nil {
		log.Fatal(err)
	} else {
		printQueryResult(queryResult)
	}
}
