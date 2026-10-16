// Package dashboards validates the Grafana dashboard artifacts of the service
// (CBL-11.5): the dashboards are code, so the repository unit checks guard them
// no less than the service Go code. The test reads deploy/grafana/dashboards
// and enforces the artifact rules of the spec: identity, datasource, read-only
// FINAL SQL against the reporting schema, the template variable contract and
// the English-only rule.
package dashboards

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// dashboard mirrors the subset of the Grafana dashboard JSON used by the checks.
type dashboard struct {
	UID        string `json:"uid"`
	Title      string `json:"title"`
	Editable   *bool  `json:"editable"`
	Templating struct {
		List []variable `json:"list"`
	} `json:"templating"`
	Panels []panel `json:"panels"`
}

// variable mirrors a Grafana template variable.
type variable struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Query      string `json:"query"`
	Regex      string `json:"regex"`
	Hide       int    `json:"hide"`
	Multi      bool   `json:"multi"`
	IncludeAll bool   `json:"includeAll"`
	AllValue   string `json:"allValue"`
}

// panel mirrors a Grafana panel.
type panel struct {
	Type     string   `json:"type"`
	Title    string   `json:"title"`
	Targets  []target `json:"targets"`
	Datasrc  datasrc  `json:"datasource"`
	GridPos  any      `json:"gridPos"`
	FieldCfg any      `json:"fieldConfig"`
}

// target mirrors a panel query target.
type target struct {
	RefID   string  `json:"refId"`
	RawSQL  string  `json:"rawSql"`
	Datasrc datasrc `json:"datasource"`
}

// datasrc is the datasource reference object.
type datasrc struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

// wantUIDs is the closed dashboard set of the task.
var wantUIDs = map[string]bool{
	"cbl-payments": true,
	"cbl-payouts":  true,
	"cbl-turnover": true,
}

// cyrillic detects Cyrillic text (the artifacts are English-only).
var cyrillic = regexp.MustCompile(`[\x{0400}-\x{04FF}]`)

// forbidden detects write/DDL keywords in panel SQL (read-only contract).
var forbidden = regexp.MustCompile(`(?i)\b(insert|update|delete|alter|drop|truncate|create|optimize|system|attach|detach|rename|grant|revoke)\b`)

// scaledMoney matches the exact money scaling pattern of the spec (FR-3):
// divideDecimal(toDecimal64(<sum>, $decimals), toDecimal64(pow(10, $decimals), 0), $decimals).
var scaledMoney = regexp.MustCompile(`divideDecimal\(toDecimal64\(.*, \$decimals\), toDecimal64\(pow\(10, \$decimals\), 0\), \$decimals\)`)

// loadDashboards reads and parses every dashboard JSON of the repository.
func loadDashboards(t *testing.T) map[string]dashboard {
	t.Helper()
	dirFS := os.DirFS(filepath.Join("..", "..", "deploy", "grafana", "dashboards"))
	entries, err := fs.ReadDir(dirFS, ".")
	if err != nil {
		t.Fatalf("read dashboards dir: %v", err)
	}
	found := map[string]dashboard{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		raw, err := fs.ReadFile(dirFS, entry.Name())
		if err != nil {
			t.Fatalf("read %s: %v", entry.Name(), err)
		}
		if cyrillic.Match(raw) {
			t.Errorf("%s: Cyrillic text found (artifacts are English-only)", entry.Name())
		}
		var d dashboard
		if err := json.Unmarshal(raw, &d); err != nil {
			t.Fatalf("%s: invalid JSON: %v", entry.Name(), err)
		}
		wantName := d.UID + ".json"
		if entry.Name() != wantName {
			t.Errorf("%s: file name must match the uid (%s)", entry.Name(), wantName)
		}
		found[d.UID] = d
	}
	return found
}

// TestDashboardSet checks the identity of the dashboard set (FR-3).
func TestDashboardSet(t *testing.T) {
	found := loadDashboards(t)
	if len(found) != len(wantUIDs) {
		t.Fatalf("dashboard count = %d, want %d", len(found), len(wantUIDs))
	}
	for uid, d := range found {
		if !wantUIDs[uid] {
			t.Errorf("unexpected dashboard uid %q", uid)
		}
		if strings.TrimSpace(d.Title) == "" {
			t.Errorf("%s: empty title", uid)
		}
		if d.Editable == nil || *d.Editable {
			t.Errorf("%s: dashboards are code: editable must be false", uid)
		}
		if len(d.Panels) < 3 {
			t.Errorf("%s: %d panels, want at least 3", uid, len(d.Panels))
		}
	}
	for uid := range wantUIDs {
		if _, ok := found[uid]; !ok {
			t.Errorf("dashboard %q is missing", uid)
		}
	}
}

// TestDashboardPanels checks the read-only FINAL SQL and the datasource binding (FR-3).
func TestDashboardPanels(t *testing.T) {
	for uid, d := range loadDashboards(t) {
		for _, p := range d.Panels {
			if len(p.Targets) == 0 {
				t.Errorf("%s: panel %q has no targets", uid, p.Title)
			}
			for _, target := range p.Targets {
				if target.Datasrc.UID != "cbl-clickhouse" {
					t.Errorf("%s: panel %q target datasource uid = %q, want cbl-clickhouse",
						uid, p.Title, target.Datasrc.UID)
				}
				sql := strings.TrimSpace(target.RawSQL)
				if !strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
					t.Errorf("%s: panel %q SQL is not SELECT-only: %.40q", uid, p.Title, sql)
				}
				if strings.Contains(sql, ";") {
					t.Errorf("%s: panel %q SQL must be a single statement", uid, p.Title)
				}
				if !strings.Contains(sql, "FROM reporting.") {
					t.Errorf("%s: panel %q reads outside the reporting schema", uid, p.Title)
				}
				if !strings.Contains(sql, "FINAL") {
					t.Errorf("%s: panel %q must read with FINAL", uid, p.Title)
				}
				if forbidden.MatchString(sql) {
					t.Errorf("%s: panel %q SQL contains a forbidden keyword", uid, p.Title)
				}
				freshness := strings.Contains(sql, "dateDiff('minute'")
				if !freshness {
					if !strings.Contains(sql, "$__timeFilter") {
						t.Errorf("%s: panel %q must scope the period with $__timeFilter", uid, p.Title)
					}
					if !strings.Contains(sql, "$asset") {
						t.Errorf("%s: panel %q must filter by the asset variable", uid, p.Title)
					}
				}
				if strings.Contains(sql, "$merchant") {
					t.Errorf("%s: panel %q uses a bare $merchant; the multi variable requires ${merchant:pipe} in match()", uid, p.Title)
				}
				if strings.Contains(sql, "match(merchant_id") && !strings.Contains(sql, "${merchant:pipe}") {
					t.Errorf("%s: panel %q filters by merchant without ${merchant:pipe}", uid, p.Title)
				}
				if strings.Contains(sql, "sumIf(") && !scaledMoney.MatchString(sql) {
					t.Errorf("%s: panel %q must scale money exactly: divideDecimal(toDecimal64(<sum>, $decimals), toDecimal64(pow(10, $decimals), 0), $decimals)",
						uid, p.Title)
				}
				if strings.Contains(sql, "toFloat64") {
					t.Errorf("%s: panel %q must not use floats for money", uid, p.Title)
				}
			}
		}
	}
}

// TestDashboardVariables checks the template variable contract (FR-2).
func TestDashboardVariables(t *testing.T) {
	for uid, d := range loadDashboards(t) {
		vars := map[string]variable{}
		for _, v := range d.Templating.List {
			vars[v.Name] = v
		}
		asset, ok := vars["asset"]
		if !ok || asset.Type != "query" || !strings.Contains(asset.Query, "FROM reporting.dim_accounts") {
			t.Errorf("%s: asset variable must be a dim_accounts query", uid)
		}
		if asset.Multi || asset.IncludeAll {
			t.Errorf("%s: asset variable must be single: the decimals query depends on it", uid)
		}
		if asset.Regex == "" {
			t.Errorf("%s: asset variable must be guarded by a regex (URL sync input)", uid)
		}
		decimals, ok := vars["decimals"]
		if !ok || !strings.Contains(decimals.Query, "if(count() = 0, 6,") || !strings.Contains(decimals.Query, "any(decimals)") {
			t.Errorf("%s: decimals variable must resolve any(decimals) with a 6 fallback on an empty vocabulary", uid)
		}
		if decimals.Multi {
			t.Errorf("%s: decimals variable must be single", uid)
		}
		if decimals.Hide != 2 {
			t.Errorf("%s: decimals variable must be hidden", uid)
		}
		merchant, ok := vars["merchant"]
		if !ok || !merchant.Multi || !merchant.IncludeAll || merchant.AllValue != ".*" {
			t.Errorf("%s: merchant variable must be multi with Include All and allValue .*", uid)
		}
		if !strings.Contains(merchant.Query, "FROM reporting.dim_accounts") {
			t.Errorf("%s: merchant variable must be a dim_accounts query", uid)
		}
		if merchant.Regex == "" {
			t.Errorf("%s: merchant variable must be guarded by a regex (URL sync input)", uid)
		}
	}
}
