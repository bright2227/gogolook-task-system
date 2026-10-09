package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bright2227/vsx-task-system/internal/adapter/memory"
	"github.com/bright2227/vsx-task-system/internal/app"
	"github.com/bright2227/vsx-task-system/internal/task"
)

func TestTaskHTTPFlowAndValidation(t *testing.T) {
	store := memory.New()
	api := (&API{Tasks: app.Tasks{Store: store, Clock: func() time.Time { return time.Now().UTC() }}, Ready: func(context.Context) error { return nil }}).Handler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		return rec
	}
	created := call(http.MethodPost, "/tasks", `{"payload":{"n":1}}`)
	var item struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &item); err != nil || item.ID == "" || item.Status != "pending" {
		t.Fatalf("response=%s err=%v", created.Body, err)
	}
	if got := call(http.MethodGet, "/tasks/"+item.ID, ""); got.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", got.Code, got.Body)
	}
	if got := call(http.MethodPost, "/tasks/"+item.ID+":cancel", ""); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"status":"canceled"`) {
		t.Fatalf("cancel status=%d body=%s", got.Code, got.Body)
	}
	completedPayload, _ := task.NewPayload([]byte(`{}`))
	completed, err := (app.Tasks{Store: store, Clock: time.Now}).Submit(context.Background(), completedPayload)
	if err != nil {
		t.Fatal(err)
	}
	completion := app.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: funcProcessor(func(context.Context, task.Payload) (string, error) { return "ok", nil })}
	if err = completion.Process(context.Background(), completed.ID); err != nil {
		t.Fatal(err)
	}
	if got := call(http.MethodPost, "/tasks/"+string(completed.ID)+":cancel", ""); got.Code != http.StatusConflict {
		t.Fatalf("completed cancellation status=%d body=%s", got.Code, got.Body)
	}
	for _, body := range []string{`{"payload":{} ,"surprise":1}`, `{"payload":{}} {}`} {
		if got := call(http.MethodPost, "/tasks", body); got.Code != http.StatusBadRequest {
			t.Errorf("body=%q status=%d", body, got.Code)
		}
	}
	if got := call(http.MethodPost, "/tasks", `{}`); got.Code != http.StatusUnprocessableEntity {
		t.Errorf("missing payload status=%d", got.Code)
	}
	if got := call(http.MethodPost, "/tasks", `{"payload":`+strings.Repeat("x", 65540)+`}`); got.Code != http.StatusBadRequest {
		t.Errorf("oversize status=%d", got.Code)
	}
}

type funcProcessor func(context.Context, task.Payload) (string, error)

func (f funcProcessor) Execute(ctx context.Context, p task.Payload) (string, error) { return f(ctx, p) }
