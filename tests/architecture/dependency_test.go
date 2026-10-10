package architecture_test

import (
	"go/ast"
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
	const module = "github.com/bright2227/gogolook-task-system"
	root := filepath.Join("..", "..")
	layers := []struct {
		name    string
		dir     string
		allowed map[string]bool
	}{
		{"entities", "internal/domain", nil},
		{"use cases", "internal/usecase", map[string]bool{module + "/internal/domain": true, module + "/internal/usecase/port": true}},
		{"use-case ports", "internal/usecase/port", map[string]bool{module + "/internal/domain": true}},
		{"HTTP boundary", "internal/delivery/http", map[string]bool{module + "/internal/usecase": true, module + "/internal/domain": true, module + "/internal/delivery/http/controller": true}},
		{"AMQP delivery", "internal/delivery/amqp", map[string]bool{module + "/internal/usecase": true, module + "/internal/domain": true, module + "/internal/delivery": true, module + "/internal/infrastructure/rabbitmq": true, "github.com/rabbitmq/amqp091-go": true}},
		{"broker resource", "internal/infrastructure/rabbitmq", map[string]bool{module + "/internal/delivery": true, module + "/internal/domain": true, module + "/internal/usecase": true, "github.com/rabbitmq/amqp091-go": true}},
	}
	layerDirs := make(map[string]bool, len(layers))
	for _, layer := range layers {
		layerDirs[filepath.Join(root, layer.dir)] = true
	}
	for _, layer := range layers {
		t.Run(layer.name, func(t *testing.T) {
			files := 0
			layerRoot := filepath.Join(root, layer.dir)
			err := filepath.WalkDir(layerRoot, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() && path != layerRoot && layerDirs[path] {
					return filepath.SkipDir // A nested directory is checked as its own layer.
				}
				if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
					return nil
				}
				files++
				file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
				if err != nil {
					return err
				}
				// Channel adapters return errors to the connection owner. The shared
				// pool may open channels, but it must not control connection lifetime.
				sharedPool := filepath.Base(path) == "shared_task_queue.go"
				if layer.name == "AMQP delivery" || (layer.name == "broker resource" && (filepath.Base(path) == "task_queue.go" || sharedPool)) {
					ast.Inspect(file, func(node ast.Node) bool {
						call, ok := node.(*ast.CallExpr)
						if !ok {
							return true
						}
						var name string
						switch fn := call.Fun.(type) {
						case *ast.SelectorExpr:
							name = fn.Sel.Name
						case *ast.Ident:
							name = fn.Name
						}
						if name == "CloseConnectionOnCancel" || name == "CloseDeadline" || name == "Connection" || name == "ConnectionContext" || (name == "Channel" && !sharedPool) {
							t.Errorf("%s manages connection lifetime; role adapters must return errors to their owner", path)
						}
						return true
					})
				}
				for _, dependency := range file.Imports {
					name, err := strconv.Unquote(dependency.Path.Value)
					if err != nil {
						return err
					}
					if name == "database/sql" || strings.HasPrefix(name, "database/sql/") {
						t.Errorf("%s imports %s; database transactions belong in repository implementations", path, name)
						continue
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
