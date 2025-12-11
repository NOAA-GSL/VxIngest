package main

import (
	"github.com/couchbase/gocb/v2"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// init runs before main() is evaluated
func init() {
	log.Println("template-queries:init()")
}

func getModels(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string, err error) {
	log.Println("getModels(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getModels.sql")
	if err != nil {
		return nil, err
	}
	tmplGetModelsSQL := string(fileContent)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplGetModelsSQL = strings.Replace(tmplGetModelsSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)

	models_requiring_metadata, err := queryWithSQLStringSA(conn.Scope, tmplGetModelsSQL)
	if err != nil {
		return nil, err
	}

	log.Printf("\tin %v", time.Since(start))
	return models_requiring_metadata, nil
}

func getModelsNoData(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string, err error) {
	log.Println("getModelsNoData(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getModelsNoData.sql")
	if err != nil {
		return nil, err
	}
	tmplgetModelsNoDataSQL := string(fileContent)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxAPP}}", app, -1)
	tmplgetModelsNoDataSQL = strings.Replace(tmplgetModelsNoDataSQL, "{{vxSUBDOCTYPE}}", doctype, -1)
	models_with_metatada_but_no_data, err := queryWithSQLStringSA(conn.Scope, tmplgetModelsNoDataSQL)
	if err != nil {
		return nil, err
	}

	log.Printf("\tin %v", time.Since(start))
	return models_with_metatada_but_no_data, nil
}

func removeMetadataForModelsWithNoData(conn CbConnection, dataset string, app string, doctype string, subDocType string, models_with_metatada_but_no_data []string) (err error) {
	log.Println("removeMetadataForModelsWithNoData(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/deleteModelMetadata.sql")
	if err != nil {
		return err
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
			return err
		} else {
			printQueryResult(queryResult)
		}
	}
	log.Printf("\tin %v", time.Since(start))
	return nil
}

func getModelsWithExistingMetadata(conn CbConnection, dataset string, app string, doctype string, subDocType string) (jsonOut []string, err error) {
	log.Println("getModelsWithExistingMetadata(" + dataset + "," + app + "," + doctype + "," + subDocType + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getModelsWithMetadata.sql")
	if err != nil {
		return nil, err
	}
	tmplgetModelsWithMetadataSQL := string(fileContent)
	tmplgetModelsWithMetadataSQL = strings.Replace(tmplgetModelsWithMetadataSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, 1)
	tmplgetModelsWithMetadataSQL = strings.Replace(tmplgetModelsWithMetadataSQL, "{{vxAPP}}", app, -1)

	models_with_existing_metadata, err := queryWithSQLStringSA(conn.Scope, tmplgetModelsWithMetadataSQL)
	if err != nil {
		return nil, err
	}
	log.Printf("\tin %v", time.Since(start))
	return models_with_existing_metadata, nil
}

func initializeMetadataForModel(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (err error) {
	log.Println("initializeMetadataForModel(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/initializeMetadata.sql")
	if err != nil {
		return err
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
		return err
	} else {
		printQueryResult(queryResult)
	}
	log.Printf("\tin %v", time.Since(start))
	return nil
}

func getDistinctThresholds(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []string, err error) {
	log.Println("getDistinctThresholds(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctThresholds.sql")
	if err != nil {
		return nil, err
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result, err := queryWithSQLStringMAP(conn.Scope, tmplSQL)
	if err != nil {
		return nil, err
	}

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

	log.Printf("\tin %v", time.Since(start))
	return tarr, err
}

func getDistinctFcstLen(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []int, err error) {
	log.Println("getDistinctFcstLen(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctFcstLen.sql")
	if err != nil {
		return nil, err
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result, err := queryWithSQLStringIA(conn.Scope, tmplSQL)
	if err != nil {
		return nil, err
	}
	log.Printf("\tin %v", time.Since(start))
	return result, nil
}

func getDistinctRegion(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []string, err error) {
	log.Println("getDistinctRegion(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctRegion.sql")
	if err != nil {
		return nil, err
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result, err := queryWithSQLStringSA(conn.Scope, tmplSQL)
	if err != nil {
		return nil, err
	}
	log.Printf("\tin %v", time.Since(start))
	return result, nil
}

func getDistinctDisplayText(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []string, err error) {
	log.Println("getDistinctDisplayText(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctDisplayText.sql")
	if err != nil {
		return nil, err
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result, err := queryWithSQLStringSA(conn.Scope, tmplSQL)
	if err != nil {
		return nil, err
	}
	log.Printf("\tin %v", time.Since(start))
	return result, nil
}

func getDistinctDisplayCategory(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (rv []int, err error) {
	log.Println("getDistinctDisplayCategory(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctDisplayCategory.sql")
	if err != nil {
		return nil, err
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result, err := queryWithSQLStringIA(conn.Scope, tmplSQL)
	if err != nil {
		return nil, err
	}
	log.Printf("\tin %v", time.Since(start))
	return result, nil
}

func getDistinctDisplayOrder(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string, mindx int) (rv []int, err error) {
	log.Println("getDistinctDisplayOrder(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getDistinctDisplayOrder.sql")
	if err != nil {
		return nil, err
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{mindx}}", strconv.Itoa(mindx), -1)

	result, err := queryWithSQLStringIA(conn.Scope, tmplSQL)
	if err != nil {
		return nil, err
	}
	log.Printf("\tin %v", time.Since(start))
	return result, nil
}

func getMinMaxCountFloor(conn CbConnection, dataset string, app string, doctype string, subDocType string, model string) (jsonOut []interface{}, err error) {
	log.Println("getMinMaxCountFloor(" + dataset + "," + app + "," + doctype + "," + subDocType + "," + model + ")")
	start := time.Now()

	fileContent, err := os.ReadFile("sqls/getMinMaxCountFloor.sql")
	if err != nil {
		return nil, err
	}
	tmplSQL := string(fileContent)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDBTARGET}}", conn.vxDBTARGET, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxDOCTYPE}}", doctype, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxSUBDOCTYPE}}", subDocType, -1)
	tmplSQL = strings.Replace(tmplSQL, "{{vxMODEL}}", model, -1)

	result, err := queryWithSQLStringMAP(conn.Scope, tmplSQL)
	if err != nil {
		return nil, err
	}
	log.Printf("\tin %v", time.Since(start))
	return result, nil
}
