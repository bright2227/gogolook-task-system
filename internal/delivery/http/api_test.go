package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/repository/task/memory"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

type fakeTasks struct {
	submit func(context.Context, usecase.SubmitTaskRequest) (usecase.TaskResponse, error)
	get    func(context.Context, usecase.TaskRequest) (usecase.TaskResponse, error)
	cancel func(context.Context, usecase.TaskRequest) (usecase.TaskResponse, error)
}

func TestTaskHTTPFlowAndValidation(t *testing.T) {
	store := memory.New()
	service := usecase.Tasks{Store: store, Clock: time.Now}
	api := (&API{Tasks: service, Ready: func(context.Context) error { return nil }}).Handler()
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
	if err := json.Unmarshal(created.Body.Bytes(), &item); err != nil || item.ID == "" || item.Status != "pending" || created.Code != http.StatusAccepted {
		t.Fatalf("response=%s status=%d err=%v", created.Body, created.Code, err)
	}
	if got := call(http.MethodGet, "/tasks/"+item.ID, ""); got.Code != http.StatusOK {
		t.Fatalf("get status=%d body=%s", got.Code, got.Body)
	}
	if got := call(http.MethodPost, "/tasks/"+item.ID+":cancel", ""); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"status":"canceled"`) {
		t.Fatalf("cancel status=%d body=%s", got.Code, got.Body)
	}
	if got := call(http.MethodPost, "/tasks/"+item.ID+":cancel", ""); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"status":"canceled"`) {
		t.Fatalf("repeated cancel status=%d body=%s", got.Code, got.Body)
	}
	completed, err := service.Submit(context.Background(), usecase.SubmitTaskRequest{Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	completion := usecase.Consumer{Store: store, Clock: time.Now, Lease: time.Second, Timeout: time.Second, Processor: funcProcessor(func(context.Context, domain.Payload) (string, error) { return "ok", nil })}
	task, err := store.Get(context.Background(), domain.ID(completed.ID))
	if err != nil {
		t.Fatal(err)
	}
	var eventID domain.ID
	for _, event := range store.Events {
		if event.TaskID == domain.ID(completed.ID) && event.Name == "TaskSubmitted" {
			eventID = event.ID
			break
		}
	}
	if err = completion.Process(context.Background(), usecase.ProcessTaskRequest{ID: completed.ID, EventID: string(eventID), ExecutionID: string(task.Snapshot().ExecutionID)}); err != nil {
		t.Fatal(err)
	}
	if got := call(http.MethodPost, "/tasks/"+completed.ID+":cancel", ""); got.Code != http.StatusConflict {
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
	tooLargePayload := `{"payload":"` + strings.Repeat("a", 65537) + `"}`
	if got := call(http.MethodPost, "/tasks", tooLargePayload); got.Code != http.StatusUnprocessableEntity {
		t.Errorf("invalid payload status=%d", got.Code)
	}
	if got := call(http.MethodPost, "/tasks", `{"payload":`+strings.Repeat("x", 65540)+`}`); got.Code != http.StatusBadRequest {
		t.Errorf("oversize status=%d", got.Code)
	}
}

type funcProcessor func(context.Context, domain.Payload) (string, error)

func (f funcProcessor) Execute(ctx context.Context, _ domain.ID, p domain.Payload) (string, error) {
	return f(ctx, p)
}

func (f fakeTasks) Submit(c context.Context, r usecase.SubmitTaskRequest) (usecase.TaskResponse, error) {
	return f.submit(c, r)
}
func (f fakeTasks) Get(c context.Context, r usecase.TaskRequest) (usecase.TaskResponse, error) {
	return f.get(c, r)
}
func (f fakeTasks) Cancel(c context.Context, r usecase.TaskRequest) (usecase.TaskResponse, error) {
	return f.cancel(c, r)
}

func TestHTTPTranslatesRequestsAndPresentsResponses(t *testing.T) {
	var gotPayload []byte
	var gotID string
	fake := fakeTasks{
		submit: func(_ context.Context, req usecase.SubmitTaskRequest) (usecase.TaskResponse, error) {
			gotPayload = append([]byte(nil), req.Payload...)
			return usecase.TaskResponse{ID: "abc", Payload: req.Payload, Status: "pending", CreatedAt: time.Unix(1, 0)}, nil
		},
		get: func(_ context.Context, req usecase.TaskRequest) (usecase.TaskResponse, error) {
			gotID = req.ID
			return usecase.TaskResponse{ID: req.ID, Payload: []byte(`{"n":1}`), Status: "completed", Attempts: 2}, nil
		},
		cancel: func(_ context.Context, req usecase.TaskRequest) (usecase.TaskResponse, error) {
			gotID = req.ID
			return usecase.TaskResponse{ID: req.ID, Status: "canceled"}, nil
		},
	}
	api := (&API{Tasks: fake, Ready: func(context.Context) error { return nil }}).Handler()
	call := func(method, path, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		w := httptest.NewRecorder()
		api.ServeHTTP(w, r)
		return w
	}
	created := call(http.MethodPost, "/tasks", `{"payload":{"n":1}}`)
	if created.Code != http.StatusAccepted || string(gotPayload) != `{"n":1}` {
		t.Fatalf("submit status=%d payload=%q", created.Code, gotPayload)
	}
	var out response
	if err := json.Unmarshal(created.Body.Bytes(), &out); err != nil || out.ID != "abc" || out.Payload == nil {
		t.Fatalf("response=%s err=%v", created.Body, err)
	}
	got := call(http.MethodGet, "/tasks/abc", "")
	if got.Code != 200 || gotID != "abc" || !strings.Contains(got.Body.String(), `"attempts":2`) {
		t.Fatalf("get status=%d id=%q body=%s", got.Code, gotID, got.Body)
	}
	canceled := call(http.MethodPost, "/tasks/abc:cancel", "")
	if canceled.Code != 200 || gotID != "abc" {
		t.Fatalf("cancel status=%d id=%q", canceled.Code, gotID)
	}
}

func TestHTTPMapsApplicationErrorsAndRetainsBodyValidation(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{{usecase.ErrInvalidID, 400}, {usecase.ErrInvalidPayload, 422}, {usecase.ErrCompleted, 409}, {usecase.ErrConflict, 409}, {usecase.ErrNotFound, 404}, {errors.New("db unavailable"), 503}} {
		fake := fakeTasks{submit: func(context.Context, usecase.SubmitTaskRequest) (usecase.TaskResponse, error) {
			return usecase.TaskResponse{}, tc.err
		}, get: func(context.Context, usecase.TaskRequest) (usecase.TaskResponse, error) {
			return usecase.TaskResponse{}, tc.err
		}, cancel: func(context.Context, usecase.TaskRequest) (usecase.TaskResponse, error) {
			return usecase.TaskResponse{}, tc.err
		}}
		api := (&API{Tasks: fake, Ready: func(context.Context) error { return nil }}).Handler()
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/tasks/id", nil))
		if w.Code != tc.want {
			t.Errorf("error %v: status=%d want %d", tc.err, w.Code, tc.want)
		}
	}
	fake := fakeTasks{submit: func(context.Context, usecase.SubmitTaskRequest) (usecase.TaskResponse, error) {
		t.Fatal("invalid body reached application")
		return usecase.TaskResponse{}, nil
	}}
	api := (&API{Tasks: fake, Ready: func(context.Context) error { return nil }}).Handler()
	for _, body := range []string{`{"payload":{} ,"surprise":1}`, `{"payload":{}} {}`} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(body)))
		if w.Code != 400 {
			t.Errorf("body %q status=%d", body, w.Code)
		}
	}
}
