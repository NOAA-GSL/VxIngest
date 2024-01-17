package main

import (
	//	"fmt"
	"log"
	//	"os"
	//	"time"
	// "github.com/couchbase/gocb/v2"
)

type Model struct {
	Name            string   `json:"name"`
	DisplayCategory int      `json:"displayCategory"`
	DisplayOrder    int      `json:"displayOrder"`
	DisplayText     string   `json:"displayText"`
	DocType         string   `json:"docType"`
	FcstLens        []int    `json:"fcstLens"`
	Maxdate         int      `json:"maxdate"`
	Mindate         int      `json:"mindate"`
	Model           string   `json:"model"`
	Numrecs         int      `json:"numrecs"`
	Regions         []string `json:"regions"`
	Subset          string   `json:"subset"`
	Thresholds      []string `json:"thresholds"`
	Variables       []string `json:"variables"`
	Type            string   `json:"type"`
	Version         string   `json:"version"`
}

type MetadataJSON struct {
	ID      string  `json:"id"`
	Name    string  `json:"name"`
	App     string  `json:"app"`
	Updated int     `json:"updated"`
	Models  []Model `json:"models"`
}

// init runs before main() is evaluated
func init() {
	log.Println("write-to-db:init()")
}

func writeMetadataToDb(conn CbConnection, metadata MetadataJSON) {
	_, err := conn.Collection.Upsert(metadata.ID, metadata, nil)
	if err != nil {
		log.Fatal(err)
	}
}
