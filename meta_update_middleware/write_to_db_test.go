package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func sampleMetadata() MetadataJSON {
	return MetadataJSON{
		ID:        "MD:matsGui:ceiling:COMMON:V01",
		Name:      "ceiling",
		App:       "cb-ceiling",
		Type:      "MD",
		Version:   "V01",
		Subset:    "COMMON",
		DocType:   "matsGui",
		Generated: true,
		Updated:   20260430,
		Models: []Model{
			{
				Name:            "HRRR",
				DisplayCategory: 1,
				DisplayOrder:    2,
				DisplayText:     "HRRR Ops",
				Model:           "HRRR_OPS",
				Thresholds:      []string{"1000", "2000"},
				FcstLens:        []int{0, 3, 6},
				Regions:         []string{"CONUS"},
				Mindate:         20240101,
				Maxdate:         20240131,
				Numrecs:         123,
			},
		},
	}
}

func TestWriteStructToFile_WritesValidPrettyJSON(t *testing.T) {
	meta := sampleMetadata()
	path := filepath.Join(t.TempDir(), "metadata.json")

	writeStructToFile(meta, path)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read output file: %v", err)
	}
	if !strings.Contains(string(data), "\n") {
		t.Fatalf("expected pretty-printed JSON with newlines")
	}

	var decoded MetadataJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("output was not valid JSON: %v", err)
	}

	if decoded.ID != meta.ID {
		t.Fatalf("expected id %q, got %q", meta.ID, decoded.ID)
	}
	if len(decoded.Models) != 1 {
		t.Fatalf("expected 1 model, got %d", len(decoded.Models))
	}
	if decoded.Models[0].Model != "HRRR_OPS" {
		t.Fatalf("expected model HRRR_OPS, got %q", decoded.Models[0].Model)
	}
}

func TestWriteMetadata_WithPath_WritesFileAndSkipsDB(t *testing.T) {
	meta := sampleMetadata()
	path := filepath.Join(t.TempDir(), "metadata_from_writeMetadata.json")

	writeMetadata(CbConnection{}, meta, path)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("expected file to be written by writeMetadata: %v", err)
	}

	var decoded MetadataJSON
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("expected valid JSON output: %v", err)
	}
	if decoded.Name != meta.Name {
		t.Fatalf("expected name %q, got %q", meta.Name, decoded.Name)
	}
}
