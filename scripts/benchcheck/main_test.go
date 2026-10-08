package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The CSV in testdata and in the rows below is the format of `benchstat -format csv` at the
// version pinned in scripts/bench.sh: benchmark names lose their "Benchmark" prefix, the
// confidence interval is "∞" below 6 samples, and the P cell reads "p=0.003 n=10" ("n=10+3"
// when the sides differ). The files are benchstat's output for synthetic results.
const (
	protocolPkg = "github.com/maltemindedal/stash/internal/protocol"
	storagePkg  = "github.com/maltemindedal/stash/internal/storage"

	// pingRow is the PING row of testdata/regression-n10.csv: 20.61% slower at p=0.000.
	pingRow  = "Encode/PING-4,9.000000000000001e-08,1%,1.0855000000000002e-07,2%,+20.61%,p=0.000 n=10"
	pingFail = protocolPkg + " Encode/PING-4: sec/op +20.61% (p=0.000 n=10), more than 5% slower"
)

// table renders one benchstat CSV table the way `benchstat -format csv` prints it.
func table(pkg, unit string, rows ...string) string {
	lines := []string{
		"pkg: " + pkg,
		",base,,head,,,",
		"," + unit + ",CI," + unit + ",CI,vs base,P",
	}
	lines = append(lines, rows...)
	return strings.Join(lines, "\n") + "\n\n"
}

func readFixture(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	return string(raw)
}

func TestCheckFailsASlowdownBeyondTheThresholdOnlyWhenItIsSignificant(t *testing.T) {
	cases := []struct {
		name      string
		rows      []string
		threshold float64
		wantFails []string
	}{
		{
			name:      "slower than the threshold at p<0.05",
			rows:      []string{pingRow},
			threshold: 5,
			wantFails: []string{pingFail},
		},
		{
			name:      "slower than the threshold at n=5 with infinite confidence intervals",
			rows:      []string{"Encode/PING-4,8.92e-08,∞,1.076e-07,∞,+20.63%,p=0.008 n=5"},
			threshold: 5,
			wantFails: []string{protocolPkg + " Encode/PING-4: sec/op +20.63% (p=0.008 n=5), more than 5% slower"},
		},
		{
			name:      "slower than the threshold at the smallest n that can be significant",
			rows:      []string{"Encode/PING-4,8.92e-08,∞,1.076e-07,∞,+20.63%,p=0.029 n=4"},
			threshold: 5,
			wantFails: []string{protocolPkg + " Encode/PING-4: sec/op +20.63% (p=0.029 n=4), more than 5% slower"},
		},
		{
			name:      "slower but inside the threshold",
			rows:      []string{"Store/set-4,1.00455e-06,1%,1.02135e-06,1%,+1.67%,p=0.002 n=10"},
			threshold: 5,
		},
		{
			name:      "slower beyond the threshold but not significant",
			rows:      []string{"Encode/SET_with_PX-4,2.0e-07,10%,2.4e-07,10%,~,p=0.210 n=10"},
			threshold: 5,
		},
		{
			name:      "faster",
			rows:      []string{"Encode/PING-4,9e-08,1%,7e-08,1%,-22.22%,p=0.000 n=10"},
			threshold: 5,
		},
		{
			name:      "a raised threshold accepts 8% slower",
			rows:      []string{"Encode/PING-4,1e-06,1%,1.08e-06,1%,+8.00%,p=0.000 n=10"},
			threshold: 10,
		},
		{
			name:      "a benchmark name with a comma is quoted and still named",
			rows:      []string{`"Parse/a,b-4",1e-06,1%,1.5e-06,1%,+50.00%,p=0.000 n=10`},
			threshold: 5,
			wantFails: []string{protocolPkg + " Parse/a,b-4: sec/op +50.00% (p=0.000 n=10), more than 5% slower"},
		},
		{
			name:      "the geomean summary is not a benchmark",
			rows:      []string{"Encode/SET_with_PX-4,2.0e-07,1%,2.0e-07,1%,~,p=0.900 n=10", "geomean,1e-06,,1.4e-06,,+40.00%,"},
			threshold: 5,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			v, err := check(strings.NewReader(table(protocolPkg, "sec/op", tc.rows...)), tc.threshold)
			if err != nil {
				t.Fatalf("check() error = %v", err)
			}
			assertLines(t, "failures", v.failures, tc.wantFails)
		})
	}
}

func TestCheckFailsAnyRiseInAllocsPerOp(t *testing.T) {
	const pairedSecPerOp = "Encode/RESP3_nested_array-4,1.5985e-07,1%,1.5965e-07,1%,~,p=0.424 n=10"
	cases := []struct {
		name      string
		allocs    string
		wantFails []string
	}{
		{
			name:      "one more allocation",
			allocs:    "Encode/RESP3_nested_array-4,1,0%,2,0%,+100.00%,p=0.000 n=10",
			wantFails: []string{protocolPkg + " Encode/RESP3_nested_array-4: allocs/op 1 -> 2"},
		},
		{
			name:      "one more allocation out of ten",
			allocs:    "Encode/RESP3_nested_array-4,10,0%,11,0%,+10.00%,p=0.000 n=10",
			wantFails: []string{protocolPkg + " Encode/RESP3_nested_array-4: allocs/op 10 -> 11"},
		},
		{
			name:   "the same allocations",
			allocs: "Encode/RESP3_nested_array-4,1,0%,1,0%,~,p=1.000 n=10",
		},
		{
			name:   "fewer allocations",
			allocs: "Encode/RESP3_nested_array-4,2,0%,1,0%,-50.00%,p=0.000 n=10",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			csv := table(protocolPkg, "sec/op", pairedSecPerOp) + table(protocolPkg, "allocs/op", tc.allocs)
			v, err := check(strings.NewReader(csv), 5)
			if err != nil {
				t.Fatalf("check() error = %v", err)
			}
			assertLines(t, "failures", v.failures, tc.wantFails)
		})
	}
}

func TestCheckJudgesBenchstatsOwnOutputPerPackage(t *testing.T) {
	cases := []struct {
		name         string
		fixture      string
		threshold    float64
		wantCompared int
		wantFails    []string
	}{
		{
			name:         "a regression and an extra allocation, with a significant 1.67% inside the threshold",
			fixture:      "regression-n10.csv",
			threshold:    5,
			wantCompared: 4,
			wantFails: []string{
				pingFail,
				protocolPkg + " Encode/RESP3_nested_array-4: allocs/op 1 -> 2",
			},
		},
		{
			name:         "a tighter threshold also fails the 1.67% in the second package",
			fixture:      "regression-n10.csv",
			threshold:    1,
			wantCompared: 4,
			wantFails: []string{
				protocolPkg + " Encode/PING-4: sec/op +20.61% (p=0.000 n=10), more than 1% slower",
				protocolPkg + " Encode/RESP3_nested_array-4: allocs/op 1 -> 2",
				storagePkg + " Store/set-4: sec/op +1.67% (p=0.002 n=10), more than 1% slower",
			},
		},
		{
			name:         "nothing breaks the bar",
			fixture:      "clean-n10.csv",
			threshold:    5,
			wantCompared: 4,
		},
		{
			name:         "five samples a side are enough",
			fixture:      "regression-n5.csv",
			threshold:    5,
			wantCompared: 4,
			wantFails:    []string{protocolPkg + " Encode/PING-4: sec/op +20.63% (p=0.008 n=5), more than 5% slower"},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			v, err := check(strings.NewReader(readFixture(t, tc.fixture)), tc.threshold)
			if err != nil {
				t.Fatalf("check() error = %v", err)
			}
			if v.compared != tc.wantCompared {
				t.Fatalf("compared = %d, want %d", v.compared, tc.wantCompared)
			}
			assertLines(t, "failures", v.failures, tc.wantFails)
		})
	}
}

func TestCheckIgnoresBytesPerOpAndNamesTheFailingPackage(t *testing.T) {
	csv := table(protocolPkg, "sec/op", "Encode/SET_with_PX-4,2.0e-07,1%,2.0e-07,1%,~,p=0.900 n=10") +
		table(protocolPkg, "B/op", "Encode/SET_with_PX-4,64,0%,128,0%,+100.00%,p=0.000 n=10") +
		table(storagePkg, "sec/op", "Encode/SET_with_PX-4,1e-06,1%,1.5e-06,1%,+50.00%,p=0.000 n=10")

	v, err := check(strings.NewReader(csv), 5)
	if err != nil {
		t.Fatalf("check() error = %v", err)
	}
	assertLines(t, "failures", v.failures, []string{
		storagePkg + " Encode/SET_with_PX-4: sec/op +50.00% (p=0.000 n=10), more than 5% slower",
	})
}

func TestCheckFailsABenchmarkMeasuredOnOneSideOnly(t *testing.T) {
	v, err := check(strings.NewReader(readFixture(t, "one-sided-n10.csv")), 5)
	if err != nil {
		t.Fatalf("check() error = %v", err)
	}
	assertLines(t, "failures", v.failures, []string{
		protocolPkg + " Encode/Gone-4: measured on base only (removed or renamed at head)",
		protocolPkg + " Encode/Added-4: measured on head only (new or renamed); compare against a base ref that has it",
	})

	text, code := report(v, 5)
	if code != 1 || strings.Contains(text, "BENCH BAR MET") {
		t.Fatalf("report() = %q, code %d, want the bar failed with exit 1", text, code)
	}
}

func TestCheckCannotJudgeASecPerOpRowWithoutAUsablePValue(t *testing.T) {
	fixture := readFixture(t, "regression-n10.csv")
	if !strings.Contains(fixture, pingRow+"\n") {
		t.Fatalf("testdata/regression-n10.csv no longer holds pingRow %q", pingRow)
	}
	const withoutP = "Encode/PING-4,9.000000000000001e-08,1%,1.0855000000000002e-07,2%,+20.61%"
	cases := map[string]string{
		"no P cell":                withoutP,
		"an empty P cell":          withoutP + ",",
		"a P cell without a count": withoutP + ",p=0.000",
		"a count without a p":      withoutP + ",n=10",
		"an unparseable p":         withoutP + ",p=zero n=10",
		"a p that is NaN":          withoutP + ",p=NaN n=10",
		"a p above 1":              withoutP + ",p=1.5 n=10",
		"an unparseable count":     withoutP + ",p=0.000 n=ten",
		"an empty count part":      withoutP + ",p=0.000 n=10+",
	}
	for name, row := range cases {
		row := row
		t.Run(name, func(t *testing.T) {
			csv := strings.Replace(fixture, pingRow+"\n", row+"\n", 1)
			_, err := check(strings.NewReader(csv), 5)
			if err == nil {
				t.Fatal("check() error = nil, want an error: a row without a usable p-value must not pass")
			}
			if !strings.Contains(err.Error(), "Encode/PING-4 sec/op cannot be judged") {
				t.Fatalf("check() error = %v, want it to say the PING row cannot be judged", err)
			}
		})
	}
}

func TestCheckCannotJudgeFewerThanFourSamplesASide(t *testing.T) {
	cases := []struct {
		name    string
		fixture string
		wantErr string
	}{
		{
			name:    "three samples a side, where p cannot go below 0.1",
			fixture: "too-few-n3.csv",
			wantErr: "3 samples on one side, at least 4 are needed",
		},
		{
			name:    "ten base samples against three head samples",
			fixture: "uneven-n10-n3.csv",
			wantErr: "3 samples on one side, at least 4 are needed",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			_, err := check(strings.NewReader(readFixture(t, tc.fixture)), 5)
			if err == nil {
				t.Fatal("check() error = nil, want an error: a 20% slowdown at n=3 shows as ~ and must not pass")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("check() error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestCheckRejectsInputItCannotJudge(t *testing.T) {
	cases := map[string]string{
		"empty output":             "",
		"only one side measured":   table(protocolPkg, "sec/op", "Encode/Added-4,,,5.024e-07,1%"),
		"a row before any header":  pingRow + "\n",
		"a value that is no float": table(protocolPkg, "sec/op", "Encode/PING-4,fast,1%,1.0855e-07,2%,+20.61%,p=0.000 n=10"),
	}
	for name, csv := range cases {
		csv := csv
		t.Run(name, func(t *testing.T) {
			if _, err := check(strings.NewReader(csv), 5); err == nil {
				t.Fatal("check() error = nil, want an error so the bar is never met by default")
			}
		})
	}
}

func TestReportExitsNonZeroOnlyWhenTheBarFails(t *testing.T) {
	cases := []struct {
		name     string
		v        verdict
		wantCode int
		wantText []string
	}{
		{
			name:     "met",
			v:        verdict{compared: 3},
			wantCode: 0,
			wantText: []string{"BENCH BAR MET: 3 benchmark(s) compared, none more than 5% slower at p<0.05"},
		},
		{
			name:     "failed",
			v:        verdict{compared: 3, failures: []string{"pkg Encode/PING-4: allocs/op 3 -> 4"}},
			wantCode: 1,
			wantText: []string{"BENCH BAR FAILED: 1 row(s)", "  pkg Encode/PING-4: allocs/op 3 -> 4"},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			text, code := report(tc.v, 5)
			if code != tc.wantCode {
				t.Fatalf("report() code = %d, want %d", code, tc.wantCode)
			}
			for _, want := range tc.wantText {
				if !strings.Contains(text, want) {
					t.Fatalf("report() text = %q, want it to contain %q", text, want)
				}
			}
		})
	}
}

func assertLines(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %q, want %q", what, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %q, want %q", what, i, got[i], want[i])
		}
	}
}
