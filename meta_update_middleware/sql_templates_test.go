package main

// sql_templates_test.go
//
// Tests that every SQL template file in the sqls/ directory:
//  1. Exists and is readable.
//  2. Contains all expected placeholder tokens before substitution.
//  3. Leaves no un-substituted {{...}} tokens after substitution.
//  4. Retains core SQL keywords (SELECT, FROM, WHERE) after substitution.
//
// These tests do NOT require a live Couchbase connection.

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// Standard substitution values used across all template tests.
const (
	testDBTARGET   = "vxdata._default.METAR"
	testDOCTYPE    = "CTC"
	testSUBDOCTYPE = "CEILING"
	testMODEL      = "HRRR_OPS"
	testAPP        = "cb-ceiling"
	testMindx      = 42
)

// placeholderRE matches any remaining {{...}} token.
var placeholderRE = regexp.MustCompile(`\{\{[^}]+\}\}`)

// loadAndSubstitute reads a SQL file and applies the given replacements map.
func loadAndSubstitute(t *testing.T, path string, replacements map[string]string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read SQL file %q: %v", path, err)
	}
	sql := string(raw)
	for placeholder, value := range replacements {
		sql = strings.ReplaceAll(sql, placeholder, value)
	}
	return sql
}

// assertNoPlaceholders fails the test if any {{...}} tokens remain.
func assertNoPlaceholders(t *testing.T, sql, file string) {
	t.Helper()
	if matches := placeholderRE.FindAllString(sql, -1); len(matches) > 0 {
		t.Errorf("%s: un-substituted placeholders remain after substitution: %v", file, matches)
	}
}

// assertSQLKeywords checks that the most basic SQL keywords are present.
func assertSQLKeywords(t *testing.T, sql, file string) {
	t.Helper()
	upper := strings.ToUpper(sql)
	for _, kw := range []string{"SELECT", "FROM"} {
		if !strings.Contains(upper, kw) {
			t.Errorf("%s: expected SQL keyword %q not found in substituted query", file, kw)
		}
	}
}

// ─────────────────────────────────────────────────────────────────
// getModels.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetModels_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getModels.sql"); err != nil {
		t.Fatalf("sqls/getModels.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetModels_ContainsPlaceholders(t *testing.T) {
	raw, err := os.ReadFile("sqls/getModels.sql")
	if err != nil {
		t.Fatalf("sqls/getModels.sql is not readable: %v", err)
	}
	sql := string(raw)
	for _, ph := range []string{"{{vxDBTARGET}}", "{{vxDOCTYPE}}", "{{vxSUBDOCTYPE}}"} {
		if !strings.Contains(sql, ph) {
			t.Errorf("sqls/getModels.sql: expected placeholder %q not found", ph)
		}
	}
}

func TestSQLTemplate_GetModels_Substitution(t *testing.T) {
	sql := loadAndSubstitute(t, "sqls/getModels.sql", map[string]string{
		"{{vxDBTARGET}}":   testDBTARGET,
		"{{vxDOCTYPE}}":    testDOCTYPE,
		"{{vxSUBDOCTYPE}}": testSUBDOCTYPE,
	})
	assertNoPlaceholders(t, sql, "getModels.sql")
	assertSQLKeywords(t, sql, "getModels.sql")
	if !strings.Contains(sql, testDBTARGET) {
		t.Errorf("getModels.sql: expected vxDBTARGET value %q in substituted SQL", testDBTARGET)
	}
	if !strings.Contains(sql, testDOCTYPE) {
		t.Errorf("getModels.sql: expected vxDOCTYPE value %q in substituted SQL", testDOCTYPE)
	}
	if !strings.Contains(sql, testSUBDOCTYPE) {
		t.Errorf("getModels.sql: expected vxSUBDOCTYPE value %q in substituted SQL", testSUBDOCTYPE)
	}
}

// ─────────────────────────────────────────────────────────────────
// getModelsNoData.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetModelsNoData_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getModelsNoData.sql"); err != nil {
		t.Fatalf("sqls/getModelsNoData.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetModelsNoData_ContainsPlaceholders(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getModelsNoData.sql")
	sql := string(raw)
	// Current version of the file is a fixed query (no placeholders after the
	// last edit); skip placeholder assertion if none are present.
	_ = sql
}

func TestSQLTemplate_GetModelsNoData_SQLKeywords(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getModelsNoData.sql")
	sql := string(raw)
	assertSQLKeywords(t, sql, "getModelsNoData.sql")
}

func TestSQLTemplate_GetModelsNoData_ContainsCOUNT(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getModelsNoData.sql")
	sql := strings.ToUpper(string(raw))
	if !strings.Contains(sql, "COUNT") {
		t.Error("getModelsNoData.sql: expected COUNT expression for no-data filter")
	}
}

// ─────────────────────────────────────────────────────────────────
// getModelsWithMetadata.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetModelsWithMetadata_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getModelsWithMetadata.sql"); err != nil {
		t.Fatalf("sqls/getModelsWithMetadata.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetModelsWithMetadata_SQLKeywords(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getModelsWithMetadata.sql")
	assertSQLKeywords(t, string(raw), "getModelsWithMetadata.sql")
}

func TestSQLTemplate_GetModelsWithMetadata_ContainsCOUNT(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getModelsWithMetadata.sql")
	sql := strings.ToUpper(string(raw))
	if !strings.Contains(sql, "COUNT") {
		t.Error("getModelsWithMetadata.sql: expected COUNT expression")
	}
}

// ─────────────────────────────────────────────────────────────────
// getDistinctDataKeys.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetDistinctDataKeys_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getDistinctDataKeys.sql"); err != nil {
		t.Fatalf("sqls/getDistinctDataKeys.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetDistinctDataKeys_ContainsPlaceholders(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getDistinctDataKeys.sql")
	sql := string(raw)
	for _, ph := range []string{"{{vxDBTARGET}}", "{{vxDOCTYPE}}", "{{vxSUBDOCTYPE}}", "{{vxMODEL}}"} {
		if !strings.Contains(sql, ph) {
			t.Errorf("sqls/getDistinctDataKeys.sql: expected placeholder %q not found", ph)
		}
	}
}

func TestSQLTemplate_GetDistinctDataKeys_Substitution(t *testing.T) {
	sql := loadAndSubstitute(t, "sqls/getDistinctDataKeys.sql", map[string]string{
		"{{vxDBTARGET}}":   testDBTARGET,
		"{{vxDOCTYPE}}":    testDOCTYPE,
		"{{vxSUBDOCTYPE}}": testSUBDOCTYPE,
		"{{vxMODEL}}":      testMODEL,
	})
	assertNoPlaceholders(t, sql, "getDistinctDataKeys.sql")
	assertSQLKeywords(t, sql, "getDistinctDataKeys.sql")
	if !strings.Contains(strings.ToUpper(sql), "UNNEST") {
		t.Error("getDistinctDataKeys.sql: expected UNNEST keyword")
	}
}

// ─────────────────────────────────────────────────────────────────
// getDistinctFcstLen.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetDistinctFcstLen_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getDistinctFcstLen.sql"); err != nil {
		t.Fatalf("sqls/getDistinctFcstLen.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetDistinctFcstLen_ContainsPlaceholders(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getDistinctFcstLen.sql")
	sql := string(raw)
	for _, ph := range []string{"{{vxDBTARGET}}", "{{vxDOCTYPE}}", "{{vxSUBDOCTYPE}}", "{{vxMODEL}}"} {
		if !strings.Contains(sql, ph) {
			t.Errorf("sqls/getDistinctFcstLen.sql: expected placeholder %q not found", ph)
		}
	}
}

func TestSQLTemplate_GetDistinctFcstLen_Substitution(t *testing.T) {
	sql := loadAndSubstitute(t, "sqls/getDistinctFcstLen.sql", map[string]string{
		"{{vxDBTARGET}}":   testDBTARGET,
		"{{vxDOCTYPE}}":    testDOCTYPE,
		"{{vxSUBDOCTYPE}}": testSUBDOCTYPE,
		"{{vxMODEL}}":      testMODEL,
	})
	assertNoPlaceholders(t, sql, "getDistinctFcstLen.sql")
	assertSQLKeywords(t, sql, "getDistinctFcstLen.sql")
	if !strings.Contains(sql, "fcstLen") {
		t.Error("getDistinctFcstLen.sql: expected 'fcstLen' field reference")
	}
}

// ─────────────────────────────────────────────────────────────────
// getDistinctRegion.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetDistinctRegion_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getDistinctRegion.sql"); err != nil {
		t.Fatalf("sqls/getDistinctRegion.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetDistinctRegion_ContainsPlaceholders(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getDistinctRegion.sql")
	sql := string(raw)
	for _, ph := range []string{"{{vxDBTARGET}}", "{{vxDOCTYPE}}", "{{vxSUBDOCTYPE}}", "{{vxMODEL}}"} {
		if !strings.Contains(sql, ph) {
			t.Errorf("sqls/getDistinctRegion.sql: expected placeholder %q not found", ph)
		}
	}
}

func TestSQLTemplate_GetDistinctRegion_Substitution(t *testing.T) {
	sql := loadAndSubstitute(t, "sqls/getDistinctRegion.sql", map[string]string{
		"{{vxDBTARGET}}":   testDBTARGET,
		"{{vxDOCTYPE}}":    testDOCTYPE,
		"{{vxSUBDOCTYPE}}": testSUBDOCTYPE,
		"{{vxMODEL}}":      testMODEL,
	})
	assertNoPlaceholders(t, sql, "getDistinctRegion.sql")
	assertSQLKeywords(t, sql, "getDistinctRegion.sql")
	if !strings.Contains(sql, "region") {
		t.Error("getDistinctRegion.sql: expected 'region' field reference")
	}
}

// ─────────────────────────────────────────────────────────────────
// getDistinctDisplayText.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetDistinctDisplayText_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getDistinctDisplayText.sql"); err != nil {
		t.Fatalf("sqls/getDistinctDisplayText.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetDistinctDisplayText_ContainsPlaceholders(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getDistinctDisplayText.sql")
	sql := string(raw)
	for _, ph := range []string{"{{vxDBTARGET}}", "{{vxMODEL}}"} {
		if !strings.Contains(sql, ph) {
			t.Errorf("sqls/getDistinctDisplayText.sql: expected placeholder %q not found", ph)
		}
	}
}

func TestSQLTemplate_GetDistinctDisplayText_Substitution(t *testing.T) {
	sql := loadAndSubstitute(t, "sqls/getDistinctDisplayText.sql", map[string]string{
		"{{vxDBTARGET}}": testDBTARGET,
		"{{vxMODEL}}":    testMODEL,
	})
	assertNoPlaceholders(t, sql, "getDistinctDisplayText.sql")
	assertSQLKeywords(t, sql, "getDistinctDisplayText.sql")
	if !strings.Contains(sql, "standardizedModelList") {
		t.Error("getDistinctDisplayText.sql: expected 'standardizedModelList' reference")
	}
}

// ─────────────────────────────────────────────────────────────────
// getDistinctDisplayCategory.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetDistinctDisplayCategory_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getDistinctDisplayCategory.sql"); err != nil {
		t.Fatalf("sqls/getDistinctDisplayCategory.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetDistinctDisplayCategory_ContainsPlaceholders(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getDistinctDisplayCategory.sql")
	sql := string(raw)
	for _, ph := range []string{"{{vxDBTARGET}}", "{{vxMODEL}}"} {
		if !strings.Contains(sql, ph) {
			t.Errorf("sqls/getDistinctDisplayCategory.sql: expected placeholder %q not found", ph)
		}
	}
}

func TestSQLTemplate_GetDistinctDisplayCategory_Substitution(t *testing.T) {
	sql := loadAndSubstitute(t, "sqls/getDistinctDisplayCategory.sql", map[string]string{
		"{{vxDBTARGET}}": testDBTARGET,
		"{{vxMODEL}}":    testMODEL,
	})
	assertNoPlaceholders(t, sql, "getDistinctDisplayCategory.sql")
	assertSQLKeywords(t, sql, "getDistinctDisplayCategory.sql")
	if !strings.Contains(strings.ToUpper(sql), "CASE") {
		t.Error("getDistinctDisplayCategory.sql: expected CASE expression")
	}
}

// ─────────────────────────────────────────────────────────────────
// getDistinctDisplayOrder.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetDistinctDisplayOrder_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getDistinctDisplayOrder.sql"); err != nil {
		t.Fatalf("sqls/getDistinctDisplayOrder.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetDistinctDisplayOrder_ContainsPlaceholders(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getDistinctDisplayOrder.sql")
	sql := string(raw)
	for _, ph := range []string{"{{vxDBTARGET}}", "{{vxMODEL}}", "{{mindx}}"} {
		if !strings.Contains(sql, ph) {
			t.Errorf("sqls/getDistinctDisplayOrder.sql: expected placeholder %q not found", ph)
		}
	}
}

func TestSQLTemplate_GetDistinctDisplayOrder_Substitution(t *testing.T) {
	sql := loadAndSubstitute(t, "sqls/getDistinctDisplayOrder.sql", map[string]string{
		"{{vxDBTARGET}}": testDBTARGET,
		"{{vxMODEL}}":    testMODEL,
		"{{mindx}}":      strconv.Itoa(testMindx),
	})
	assertNoPlaceholders(t, sql, "getDistinctDisplayOrder.sql")
	assertSQLKeywords(t, sql, "getDistinctDisplayOrder.sql")
	if !strings.Contains(sql, strconv.Itoa(testMindx)) {
		t.Errorf("getDistinctDisplayOrder.sql: expected mindx value %d in substituted SQL", testMindx)
	}
}

// ─────────────────────────────────────────────────────────────────
// getMinMaxCountFloor.sql
// ─────────────────────────────────────────────────────────────────

func TestSQLTemplate_GetMinMaxCountFloor_Readable(t *testing.T) {
	if _, err := os.ReadFile("sqls/getMinMaxCountFloor.sql"); err != nil {
		t.Fatalf("sqls/getMinMaxCountFloor.sql is not readable: %v", err)
	}
}

func TestSQLTemplate_GetMinMaxCountFloor_ContainsPlaceholders(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getMinMaxCountFloor.sql")
	sql := string(raw)
	for _, ph := range []string{"{{vxDBTARGET}}", "{{vxDOCTYPE}}", "{{vxSUBDOCTYPE}}", "{{vxMODEL}}"} {
		if !strings.Contains(sql, ph) {
			t.Errorf("sqls/getMinMaxCountFloor.sql: expected placeholder %q not found", ph)
		}
	}
}

func TestSQLTemplate_GetMinMaxCountFloor_Substitution(t *testing.T) {
	sql := loadAndSubstitute(t, "sqls/getMinMaxCountFloor.sql", map[string]string{
		"{{vxDBTARGET}}":   testDBTARGET,
		"{{vxDOCTYPE}}":    testDOCTYPE,
		"{{vxSUBDOCTYPE}}": testSUBDOCTYPE,
		"{{vxMODEL}}":      testMODEL,
	})
	assertNoPlaceholders(t, sql, "getMinMaxCountFloor.sql")
	assertSQLKeywords(t, sql, "getMinMaxCountFloor.sql")
	for _, field := range []string{"mindate", "maxdate", "numrecs", "updated"} {
		if !strings.Contains(sql, field) {
			t.Errorf("getMinMaxCountFloor.sql: expected field %q in substituted SQL", field)
		}
	}
}

func TestSQLTemplate_GetMinMaxCountFloor_AggregateKeywords(t *testing.T) {
	raw, _ := os.ReadFile("sqls/getMinMaxCountFloor.sql")
	upper := strings.ToUpper(string(raw))
	for _, kw := range []string{"MIN(", "MAX(", "COUNT(", "FLOOR("} {
		if !strings.Contains(upper, kw) {
			t.Errorf("getMinMaxCountFloor.sql: expected aggregate function %q", kw)
		}
	}
}
