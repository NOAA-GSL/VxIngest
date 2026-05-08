package main

import (
	"strings"
	"testing"
)

func TestGetTabbedString_ZeroDepth(t *testing.T) {
	result := getTabbedString(0)
	if result != "" {
		t.Errorf("expected empty string for depth 0, got %q", result)
	}
}

func TestGetTabbedString_OneDepth(t *testing.T) {
	result := getTabbedString(1)
	if result != "\t" {
		t.Errorf("expected single tab for depth 1, got %q", result)
	}
}

func TestGetTabbedString_MultipleDepth(t *testing.T) {
	result := getTabbedString(4)
	if result != "\t\t\t\t" {
		t.Errorf("expected 4 tabs for depth 4, got %q", result)
	}
}

func TestJsonPrettyPrintStruct_SimpleMap(t *testing.T) {
	input := map[string]interface{}{
		"key": "value",
	}
	result := jsonPrettyPrintStruct(input)
	if !strings.Contains(result, "key") {
		t.Errorf("expected 'key' in pretty-printed output, got %q", result)
	}
	if !strings.Contains(result, "value") {
		t.Errorf("expected 'value' in pretty-printed output, got %q", result)
	}
	// Should be indented (contain newlines and tabs)
	if !strings.Contains(result, "\n") {
		t.Errorf("expected newlines in pretty-printed output, got %q", result)
	}
}

func TestJsonPrettyPrint_Array(t *testing.T) {
	input := []interface{}{"alpha", "beta", "gamma"}
	result := jsonPrettyPrint(input)
	if !strings.Contains(result, "alpha") {
		t.Errorf("expected 'alpha' in pretty-printed output, got %q", result)
	}
	if !strings.Contains(result, "\n") {
		t.Errorf("expected newlines in pretty-printed output, got %q", result)
	}
}

func TestJsonPrettyPrint_EmptyArray(t *testing.T) {
	input := []interface{}{}
	result := jsonPrettyPrint(input)
	if result == "" {
		t.Error("expected non-empty output for empty array, got empty string")
	}
}

func TestConvertSlice_Strings(t *testing.T) {
	input := []interface{}{"HRRR_OPS", "RAP_OPS_130", "RRFS_A"}
	result := ConvertSlice[string](input)
	if len(result) != 3 {
		t.Errorf("expected length 3, got %d", len(result))
	}
	if result[0] != "HRRR_OPS" {
		t.Errorf("expected 'HRRR_OPS', got %q", result[0])
	}
	if result[1] != "RAP_OPS_130" {
		t.Errorf("expected 'RAP_OPS_130', got %q", result[1])
	}
	if result[2] != "RRFS_A" {
		t.Errorf("expected 'RRFS_A', got %q", result[2])
	}
}

func TestConvertSlice_Empty(t *testing.T) {
	input := []interface{}{}
	result := ConvertSlice[string](input)
	if len(result) != 0 {
		t.Errorf("expected empty slice, got length %d", len(result))
	}
}
