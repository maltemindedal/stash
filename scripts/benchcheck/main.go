// Command benchcheck applies the benchmark merge bar to a benchstat comparison.
//
// It reads the CSV that `benchstat -format csv base=<file> head=<file>` prints and
// reports every benchmark that breaks the bar: more than -threshold percent slower in
// sec/op at p < 0.05, a higher allocs/op median on head than on base, or a measurement on
// one side only (a benchmark that was renamed or removed has nothing to compare). It exits
// 1 when any row breaks the bar and 2 when the input cannot be judged: a sec/op row
// without a usable p-value or with fewer than 4 samples a side, or nothing compared at all.
// scripts/bench.sh runs it.
package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
)

// alpha is benchstat's default significance level, the one its text tables mark "~" at.
const alpha = 0.05

// minSamples is the fewest samples a side can have and still reach p < alpha: the
// smallest p benchstat's Mann-Whitney test can report is 0.1 with 3 samples a side and
// 0.029 with 4. With fewer, "not significant" says nothing about the benchmark.
const minSamples = 4

const (
	unitSeconds = "sec/op"
	unitAllocs  = "allocs/op"
)

// verdict is what a benchstat comparison says about the bar.
type verdict struct {
	compared int      // benchmarks with a sec/op result on both sides
	failures []string // one line per row that breaks the bar
}

func main() {
	threshold := flag.Float64("threshold", 5, "percent a benchmark may be slower in sec/op at p < 0.05")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: benchcheck [-threshold PCT] <benchstat.csv>")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 || *threshold < 0 {
		flag.Usage()
		os.Exit(2)
	}
	os.Exit(run(flag.Arg(0), *threshold))
}

func run(path string, threshold float64) int {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "benchcheck:", err)
		return 2
	}
	defer func() { _ = f.Close() }()

	v, err := check(f, threshold)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	text, code := report(v, threshold)
	fmt.Print(text)
	return code
}

// check judges the CSV benchstat printed for two inputs: the first is the base, the second
// the head. Only the sec/op and allocs/op tables count; other units are ignored. It returns
// an error, never a passing verdict, for input it cannot judge.
func check(r io.Reader, threshold float64) (verdict, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // rows with a missing side are shorter
	cr.LazyQuotes = true    // config lines such as "cpu: ..." are not CSV
	records, err := cr.ReadAll()
	if err != nil {
		return verdict{}, fmt.Errorf("benchcheck: reading benchstat CSV: %w", err)
	}

	var v verdict
	pkg, unit := "", ""
	for _, rec := range records {
		switch {
		case len(rec) == 1 && strings.HasPrefix(rec[0], "pkg: "):
			pkg = strings.TrimPrefix(rec[0], "pkg: ")
		case rec[0] == "" && len(rec) >= 3 && rec[2] == "CI":
			unit = rec[1] // the ",sec/op,CI,sec/op,CI,vs base,P" header opens a table
		case len(rec) == 1 || rec[0] == "" || rec[0] == "geomean":
			// config lines, the column-label header and the geomean summary
		default:
			if unit == "" {
				return verdict{}, fmt.Errorf("benchcheck: row %q comes before any unit header", rec[0])
			}
			if err := v.addRow(pkg, unit, rec, threshold); err != nil {
				return verdict{}, err
			}
		}
	}
	if v.compared == 0 {
		return verdict{}, errors.New("benchcheck: no benchmark has a sec/op result on both base and head (does the regex match, and does it match on both?)")
	}
	return v, nil
}

// addRow judges one benchmark row: name, base, base CI, head, head CI, delta, p.
func (v *verdict) addRow(pkg, unit string, rec []string, threshold float64) error {
	if unit != unitSeconds && unit != unitAllocs {
		return nil
	}
	name := rec[0]
	if pkg != "" {
		name = pkg + " " + name
	}
	if len(rec) < 5 || rec[1] == "" || rec[3] == "" {
		if unit == unitSeconds { // each unit's table repeats the row; judge it once
			if len(rec) >= 2 && rec[1] == "" {
				v.failures = append(v.failures, name+": measured on head only (new or renamed); compare against a base ref that has it")
			} else {
				v.failures = append(v.failures, name+": measured on base only (removed or renamed at head)")
			}
		}
		return nil
	}
	base, err := strconv.ParseFloat(rec[1], 64)
	if err != nil {
		return fmt.Errorf("benchcheck: %s %s base value %q: %w", name, unit, rec[1], err)
	}
	head, err := strconv.ParseFloat(rec[3], 64)
	if err != nil {
		return fmt.Errorf("benchcheck: %s %s head value %q: %w", name, unit, rec[3], err)
	}

	switch unit {
	case unitSeconds:
		if base <= 0 {
			return fmt.Errorf("benchcheck: %s sec/op base value %q is not positive", name, rec[1])
		}
		pCell := ""
		if len(rec) > 6 {
			pCell = rec[6]
		}
		p, n, err := significance(pCell)
		if err != nil {
			return fmt.Errorf("benchcheck: %s sec/op cannot be judged: %w", name, err)
		}
		if n < minSamples {
			return fmt.Errorf("benchcheck: %s sec/op cannot be judged: %d samples on one side, at least %d are needed to reach p<%g (use -count %d or more)", name, n, minSamples, alpha, minSamples)
		}
		v.compared++
		if slower := (head/base - 1) * 100; p < alpha && slower > threshold {
			v.failures = append(v.failures, fmt.Sprintf("%s: sec/op %+.2f%% (%s), more than %g%% slower", name, slower, pCell, threshold))
		}
	case unitAllocs:
		if head > base {
			v.failures = append(v.failures, fmt.Sprintf("%s: allocs/op %s -> %s", name, rec[1], rec[3]))
		}
	}
	return nil
}

// significance reads the p-value and the smaller side's sample count from benchstat's
// "p=0.003 n=10" cell, or "p=0.007 n=10+3" when the sides have different counts.
func significance(cell string) (p float64, n int, err error) {
	fields := strings.Fields(cell)
	if len(fields) < 2 || !strings.HasPrefix(fields[0], "p=") || !strings.HasPrefix(fields[1], "n=") {
		return 0, 0, fmt.Errorf("no p-value and sample count in %q", cell)
	}
	p, err = strconv.ParseFloat(strings.TrimPrefix(fields[0], "p="), 64)
	if err != nil || math.IsNaN(p) || p < 0 || p > 1 {
		return 0, 0, fmt.Errorf("unusable p-value in %q", cell)
	}
	n = -1
	for _, part := range strings.Split(strings.TrimPrefix(fields[1], "n="), "+") {
		count, err := strconv.Atoi(part)
		if err != nil || count < 0 {
			return 0, 0, fmt.Errorf("unusable sample count in %q", cell)
		}
		if n < 0 || count < n {
			n = count
		}
	}
	return p, n, nil
}

// report renders the verdict and the exit status that goes with it.
func report(v verdict, threshold float64) (string, int) {
	var b strings.Builder
	if len(v.failures) > 0 {
		fmt.Fprintf(&b, "BENCH BAR FAILED: %d row(s)\n", len(v.failures))
		for _, f := range v.failures {
			fmt.Fprintf(&b, "  %s\n", f)
		}
		return b.String(), 1
	}
	fmt.Fprintf(&b, "BENCH BAR MET: %d benchmark(s) compared, none more than %g%% slower at p<%g, none with more allocs/op\n", v.compared, threshold, alpha)
	return b.String(), 0
}
