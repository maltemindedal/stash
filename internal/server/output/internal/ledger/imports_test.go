package ledger

import (
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// TestTheLedgerImportsNothingButErrors keeps the Ledger a pure value: with no
// lock, clock, socket, timer or logger to import, a decision can only come
// from the positions it is told, which is what lets model_test.go walk every
// state. A Ledger that needs one of those needs it from its caller, as an
// argument (Take's now) or an Effects field.
func TestTheLedgerImportsNothingButErrors(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, spec := range file.Imports {
			path, err := strconv.Unquote(spec.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if path != "errors" {
				t.Errorf("%s imports %q: the Ledger may import errors and nothing else", name, path)
			}
		}
	}
	if checked == 0 {
		t.Fatal("found no Go file of the Ledger to check")
	}
}
