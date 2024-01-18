package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/gocb/v2"
)

// init runs before main() is evaluated
func init() {
	log.Println("template-queries:init()")
}

func getModels(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string) {
	log.Println("getModels(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getModels.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplGetModelsSQL := string(fileContent)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)

	models_requiring_metadata := queryWithSQLStringSA(conn.Scope, tmplGetModelsSQL)

	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return models_requiring_metadata
}

func getModelsNoData(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string) {
	log.Println("getModelsNoData(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getModelsNoData.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplgetModelsNoDataSQL := string(fileContent)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxAPP}}", app, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxSUBDOCTYPE}}", doctype, -1)
	models_with_metatada_but_no_data := queryWithSQLStringSA(conn.Scope, tmplgetModelsNoDataSQL)

	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return models_with_metatada_but_no_data
}

func removeMetadataForModelsWithNoData(conn CbConnection, dataset string, app string, doctype string, subDocType string, models_with_metatada_but_no_data []string) {
	log.Println("removeMetadataForModelsWithNoData(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	start := time.Now()

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
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
}

func getModelsWithExistingMetadata(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string) {
	log.Println("getModelsWithExistingMetadata(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getModelsWithMetadata.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplgetModelsWithMetadataSQL := string(fileContent)
	tmplgetModelsWithMetadataSQL = strings.Replace(tmplgetModelsWithMetadataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, 1)
	tmplgetModelsWithMetadataSQL = strings.Replace(tmplgetModelsWithMetadataSQL, "{{vxAPP}}", app, -1)

	models_with_existing_metadata := queryWithSQLStringSA(conn.Scope, tmplgetModelsWithMetadataSQL)
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return models_with_existing_metadata
}

func initializeMetadataForModel(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) {
	log.Println("initializeMetadataForModel(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/initializeMetadata.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplInitializeMetadataSQL := string(fileContent)
	tmplInitializeMetadataSQL = strings.Replace(tmplInitializeMetadataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplInitializeMetadataSQL = strings.Replace(tmplInitializeMetadataSQL, "{{vxAPP}}", app, -1)
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
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
}

func getDistinctThresholds(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []string) {
	log.Println("getDistinctThresholds(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctThresholds.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result := queryWithSQLStringMAP(conn.Scope, tmplSQL)

	m := result[0].(map[string]interface{})
	if len(result) > 1 {
		log.Println("Empty {}, using second result in array ...")
		m = result[1].(map[string]interface{})
	}

	// fmt.Printf("m[thresholds]: %T\n", m["thresholds"])
	tarr := ConvertSlice[string](m["thresholds"].([]interface{}))

	/*
		rv = make([]float64, 0)
		log.Println(tarr)
		for k := 0; k < len(tarr); k++ {
			// fmt.Printf("%T\n", tarr[k])
			// log.Println(tarr[k])
			val, err := strconv.ParseFloat(tarr[k], 64)
			if err != nil {
				panic(err)
			}
			// log.Println(val)
			rv = append(rv, val)
		}
	*/

	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return tarr
}

func getDistinctFcstLen(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []int) {
	log.Println("getDistinctFcstLen(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctFcstLen.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result := queryWithSQLStringIA(conn.Scope, tmplSQL)
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return result
}

func getDistinctRegion(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []string) {
	log.Println("getDistinctRegion(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctRegion.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result := queryWithSQLStringSA(conn.Scope, tmplSQL)
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return result
}

func getDistinctDisplayText(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []string) {
	log.Println("getDistinctDisplayText(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctDisplayText.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result := queryWithSQLStringSA(conn.Scope, tmplSQL)
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return result
}

func getDistinctDisplayCategory(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []int) {
	log.Println("getDistinctDisplayCategory(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctDisplayCategory.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result := queryWithSQLStringIA(conn.Scope, tmplSQL)
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return result
}

func getDistinctDisplayOrder(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string, mindx int) (rv []int) {
	log.Println("getDistinctDisplayOrder(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctDisplayOrder.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{mindx}}", strconv.Itoa(mindx), -1)

	result := queryWithSQLStringIA(conn.Scope, tmplSQL)
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return result
}

func getMinMaxCountFloor(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (jsonOut []interface{}) {
	log.Println("getMinMaxCountFloor(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getMinMaxCountFloor.sql")
	if err != nil {
		log.Fatal(err)
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result := queryWithSQLStringMAP(conn.Scope, tmplSQL)
	log.Println(fmt.Sprintf("\tin %v", time.Since(start)))
	return result
}
