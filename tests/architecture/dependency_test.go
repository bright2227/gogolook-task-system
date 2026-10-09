package architecture_test

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The dependency rule is an architectural contract: business code must not
// depend on transport, storage, messaging, or framework implementations.
func TestDependencyRule(t *testing.T) {
	const module = "github.com/bright2227/vsx-task-system"
	root := filepath.Join("..", "..")
	layers := []struct {
		name    string
		dir     string
		allowed map[string]bool
	}{
		{"entities", "internal/task", nil},
		{"use cases", "internal/app", map[string]bool{module + "/internal/task": true}},
		{"HTTP boundary", "internal/adapter/httpapi", map[string]bool{module + "/internal/app": true}},
	}
	for _, layer := range layers {
		t.Run(layer.name, func(t *testing.T) {
			files := 0
			err := filepath.WalkDir(filepath.Join(root, layer.dir), func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
					return nil
				}
				files++
				file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
				if err != nil {
					return err
				}
				for _, dependency := range file.Imports {
					name, err := strconv.Unquote(dependency.Path.Value)
					if err != nil {
						return err
					}
					first, _, _ := strings.Cut(name, "/")
					if !strings.Contains(first, ".") {
						continue // Standard library import.
					}
					if !layer.allowed[name] {
						t.Errorf("%s imports %s, crossing the %s boundary", path, name, layer.name)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if files == 0 {
				t.Fatalf("no production Go files found in %s", layer.dir)
			}
		})
	}
}
