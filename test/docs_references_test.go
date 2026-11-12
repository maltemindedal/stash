package test

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// repoRoot is the module root, one directory above this package.
const repoRoot = ".."

var (
	backticked = regexp.MustCompile("`([^`\\s]+)`")
	repoPath   = regexp.MustCompile(`^(internal|cmd|test|scripts|docs)/[\w./-]+$`)
	// goName matches what a doc names as code: `name`, `name()`, `pkg.Name`
	// or `pkg.Type.Method`, with at least one inner capital so that plain
	// words are not taken for identifiers.
	goName = regexp.MustCompile(`^(?:([a-z]\w*)\.)?([A-Za-z]\w*(?:\.\w+)?)(?:\(\))?$`)
	camel  = regexp.MustCompile(`[a-z][A-Z]|^[A-Z][a-z]+[A-Z]`)
	// infoField is a lowercase word with one trailing capital, an INFO
	// field pattern such as `slaveN`, not a Go name.
	infoField = regexp.MustCompile(`^[a-z]+[A-Z]$`)
)

// declaredNames returns every top-level Go declaration in the module, the
// methods and struct fields included, and the names of the module's packages.
func declaredNames(t *testing.T) (names, packages map[string]bool) {
	t.Helper()
	names, packages = map[string]bool{}, map[string]bool{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != repoRoot && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		packages[file.Name.Name] = true
		ast.Inspect(file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.FuncDecl:
				names[n.Name.Name] = true
			case *ast.TypeSpec:
				names[n.Name.Name] = true
			case *ast.ValueSpec:
				for _, id := range n.Names {
					names[id.Name] = true
				}
			case *ast.Field:
				for _, id := range n.Names {
					names[id.Name] = true
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return names, packages
}

// TestDocsNameOnlyCodeThatExists fails when a Markdown file names, in
// backticks, a repository path that is gone or a Go identifier that nothing in
// the module declares. Agents follow these names: AGENTS.md sent them to the
// removed snapshotAllLocked for seven commits after 0d2824b, and
// docs/agents/domain.md pointed at a docs/adr directory that never existed
// until accb62c removed the guide.
func TestDocsNameOnlyCodeThatExists(t *testing.T) {
	names, packages := declaredNames(t)

	var stale []string
	err := filepath.WalkDir(repoRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && path != repoRoot && strings.HasPrefix(d.Name(), ".") {
			return filepath.SkipDir
		}
		// A design proposal names code that does not exist yet.
		if d.IsDir() && path == filepath.Join(repoRoot, "design") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".md") {
			return nil
		}
		rel, err := filepath.Rel(repoRoot, path)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for line := 1; scanner.Scan(); line++ {
			for _, m := range backticked.FindAllStringSubmatch(scanner.Text(), -1) {
				ref := strings.TrimRight(m[1], "/.,:;")
				if repoPath.MatchString(ref) {
					if _, err := os.Stat(filepath.Join(repoRoot, ref)); err != nil {
						stale = append(stale, rel+":"+strconv.Itoa(line)+": path "+ref+" does not exist")
					}
					continue
				}
				g := goName.FindStringSubmatch(ref)
				if g == nil || (g[1] != "" && !packages[g[1]]) {
					continue
				}
				parts := strings.Split(g[2], ".")
				name := parts[len(parts)-1]
				if !camel.MatchString(name) || infoField.MatchString(name) || names[name] {
					continue
				}
				stale = append(stale, rel+":"+strconv.Itoa(line)+": "+ref+" is not declared anywhere in the module")
			}
		}
		return scanner.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(stale)
	for _, s := range stale {
		t.Error(s)
	}
}
