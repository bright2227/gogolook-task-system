package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAPISpecIsServedAndDocumentsTaskRoutes(t *testing.T) {
	server := NewServer(nil, nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("OpenAPI response status=%d content-type=%q", w.Code, w.Header().Get("Content-Type"))
	}
	var spec struct {
		OpenAPI    string                                `json:"openapi"`
		Paths      map[string]map[string]json.RawMessage `json:"paths"`
		Components struct {
			Schemas map[string]struct {
				Properties map[string]json.RawMessage `json:"properties"`
				Required   []string                   `json:"required"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &spec); err != nil {
		t.Fatalf("invalid embedded OpenAPI JSON: %v", err)
	}
	if spec.OpenAPI != "3.0.3" {
		t.Fatalf("OpenAPI version=%q want 3.0.3", spec.OpenAPI)
	}
	for _, path := range []string{"/tasks", "/tasks/{id}", "/health/live", "/health/ready", "/metrics"} {
		if len(spec.Paths[path]) == 0 {
			t.Errorf("OpenAPI path %q is missing", path)
		}
	}
	for _, method := range []string{"get", "put", "delete"} {
		if _, ok := spec.Paths["/tasks/{id}"][method]; !ok {
			t.Errorf("OpenAPI task route is missing %s", method)
		}
	}
	if _, ok := spec.Paths["/tasks/{id}:cancel"]; ok {
		t.Error("OpenAPI still documents removed cancellation")
	}
	list := spec.Components.Schemas["TaskList"]
	if _, ok := list.Properties["items"]; !ok {
		t.Error("TaskList schema is missing items")
	}
	if _, ok := list.Properties["next_cursor"]; !ok {
		t.Error("TaskList schema is missing next_cursor")
	}
	taskSchema := spec.Components.Schemas["Task"]
	for _, field := range []string{"error", "name", "version", "is_running", "is_submitted"} {
		if _, ok := taskSchema.Properties[field]; !ok {
			t.Errorf("Task schema is missing %s", field)
		}
	}
	if len(taskSchema.Required) == 0 {
		t.Error("Task schema is missing required fields")
	}
	if _, ok := taskSchema.Properties["submit"]; ok {
		t.Error("Task schema still documents the old submit field")
	}
	var submittedRequired bool
	for _, field := range taskSchema.Required {
		submittedRequired = submittedRequired || field == "is_submitted"
	}
	if !submittedRequired {
		t.Error("Task schema does not require is_submitted")
	}
	var status struct {
		Type string `json:"type"`
		Enum []int  `json:"enum"`
	}
	if err := json.Unmarshal(taskSchema.Properties["status"], &status); err != nil {
		t.Fatalf("invalid Task status schema: %v", err)
	}
	if status.Type != "integer" || len(status.Enum) != 2 || status.Enum[0] != 0 || status.Enum[1] != 1 {
		t.Errorf("Task status schema=%+v, want integer enum [0,1]", status)
	}
}

func TestSwaggerDocsPageReferencesEmbeddedSpec(t *testing.T) {
	server := NewServer(nil, nil)
	w := httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/docs", nil))
	if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), "text/html") {
		t.Fatalf("Swagger docs status=%d content-type=%q", w.Code, w.Header().Get("Content-Type"))
	}
	for _, want := range []string{"swagger-ui-dist@5.33.1/swagger-ui.css", "swagger-ui-dist@5.33.1/swagger-ui-bundle.js", `url: "/openapi.json"`, "requires network access"} {
		if !strings.Contains(w.Body.String(), want) {
			t.Errorf("Swagger page does not reference %q", want)
		}
	}
}
