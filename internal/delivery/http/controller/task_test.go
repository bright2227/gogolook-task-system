package controller

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase/port"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/repository/task"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

func TestTaskBodiesRejectInvalidUTF8WithoutChangingStorage(t *testing.T) {
	ctx := context.Background()
	repo := task.NewMockTaskRepository()
	svc := usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now)
	item, err := svc.Submit(ctx, "original", []byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	api := NewTaskController(svc, func(context.Context) error { return nil }).Handler()
	for _, body := range []string{
		"{\"name\":\"n\",\"payload\":\"\xff\"}",
		"{\"name\":\"\xff\",\"payload\":\"p\"}",
		"{\"name\":\"n\",\"payload\":\"\xc3\"}",
		"{\"name\":\"n\",\"payload\":\"\xed\xa0\x80\"}",
	} {
		for _, method := range []string{http.MethodPost, http.MethodPut} {
			path := "/tasks"
			if method == http.MethodPut {
				path += "/" + string(item.ID)
			}
			w := httptest.NewRecorder()
			api.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
			if w.Code != http.StatusUnprocessableEntity {
				t.Errorf("%s invalid UTF-8=%d %s", method, w.Code, w.Body)
			}
			assertProblemMessage(t, w)
		}
	}
	stored, err := svc.Get(ctx, string(item.ID))
	if err != nil || stored.Name != item.Name || stored.Payload != item.Payload || stored.Version != item.Version {
		t.Fatalf("invalid PUT changed storage=%+v,%v", stored, err)
	}
	if items, _, err := svc.List(ctx, 100, time.Time{}, ""); err != nil || len(items) != 1 {
		t.Fatalf("invalid POST created tasks=%d,%v", len(items), err)
	}
	// Real replacement characters and multibyte text are valid input, including
	// a JSON surrogate pair that encodes a Unicode character.
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		path := "/tasks"
		if method == http.MethodPut {
			path += "/" + string(item.ID)
		}
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(`{"name":"你�","payload":"\ud83d\ude42�"}`)))
		var got response
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || w.Code != http.StatusOK || got.Name != "你�" || got.Payload != "🙂�" {
			t.Fatalf("%s valid Unicode=%d %s,%v", method, w.Code, w.Body, err)
		}
	}
	// The body cap still takes precedence over malformed byte sequences.
	w := httptest.NewRecorder()
	body := append(bytes.Repeat([]byte("a"), maxBodyBytes), 0xff)
	api.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewReader(body)))
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized invalid UTF-8=%d %s", w.Code, w.Body)
	}
}

func TestTaskHTTPFlowAndValidation(t *testing.T) {
	repo := task.NewMockTaskRepository()
	taskUseCase := usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now)
	api := NewTaskController(taskUseCase, func(context.Context) error { return nil }).Handler()
	call := func(method, path, body, version string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if version != "" {
			req.Header.Set("If-Match", version)
		}
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		return rec
	}
	created := call(http.MethodPost, "/tasks", `{"name":"first","payload":"hello"}`, "")
	var item response
	if err := json.Unmarshal(created.Body.Bytes(), &item); err != nil || item.ID == "" || item.Name != "first" || item.Version != 1 || item.IsRunning || item.Status != 0 || !item.IsSubmitted || created.Code != 200 || created.Header().Get("ETag") != `"1"` {
		t.Fatalf("create=%s status=%d err=%v", created.Body, created.Code, err)
	}
	path := "/tasks/" + item.ID
	got := call(http.MethodGet, path, "", "")
	if err := json.Unmarshal(got.Body.Bytes(), &item); err != nil || got.Code != 200 || item.Status != 0 {
		t.Fatalf("GET=%d %s err=%v", got.Code, got.Body, err)
	}
	if got := call(http.MethodPost, path+":cancel", "", ""); got.Code != http.StatusMethodNotAllowed {
		t.Fatalf("removed cancellation=%d", got.Code)
	}
	updated := call(http.MethodPut, path, `{"name":"second","payload":"new"}`, `"1"`)
	if err := json.Unmarshal(updated.Body.Bytes(), &item); err != nil || updated.Code != 200 || item.Name != "second" || item.Payload != "new" || item.Version != 2 || item.IsSubmitted || item.IsRunning || item.Status != 0 || updated.Header().Get("ETag") != `"2"` {
		t.Fatalf("PUT=%s status=%d err=%v", updated.Body, updated.Code, err)
	}
	if got := call(http.MethodPut, path, `{"name":"lost","payload":"lost"}`, `"1"`); got.Code != 409 {
		t.Fatalf("stale PUT=%d %s", got.Code, got.Body)
	}
	if got := call(http.MethodDelete, path, "", `"1"`); got.Code != 409 {
		t.Fatalf("stale DELETE=%d %s", got.Code, got.Body)
	}
	if err := repo.UpdateByID(context.Background(), domain.ID(item.ID), item.Version, func(task *domain.Task) (bool, error) { return task.Start(time.Now()) }); err != nil {
		t.Fatal(err)
	}
	got = call(http.MethodGet, path, "", "")
	if err := json.Unmarshal(got.Body.Bytes(), &item); err != nil || item.Status != 0 || !item.IsRunning {
		t.Fatalf("running response=%s err=%v", got.Body, err)
	}
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		if got := call(method, path, `{"name":"busy","payload":"busy"}`, ""); got.Code != 409 {
			t.Fatalf("running %s=%d %s", method, got.Code, got.Body)
		}
	}
	result := "finished"
	if err := repo.UpdateByID(context.Background(), domain.ID(item.ID), item.Version, func(task *domain.Task) (bool, error) { return task.Finish(time.Now(), &result, nil) }); err != nil {
		t.Fatal(err)
	}
	got = call(http.MethodGet, path, "", "")
	if err := json.Unmarshal(got.Body.Bytes(), &item); err != nil || item.Status != 1 || item.IsRunning || item.Result == nil || *item.Result != result {
		t.Fatalf("completed response=%s err=%v", got.Body, err)
	}
	var page listResponse
	listed := call(http.MethodGet, "/tasks", "", "")
	if err := json.Unmarshal(listed.Body.Bytes(), &page); err != nil || len(page.Items) != 1 || page.Items[0].Status != 1 {
		t.Fatalf("completed list=%s err=%v", listed.Body, err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(got.Body.Bytes(), &fields); err != nil {
		t.Fatal(err)
	}
	if _, ok := fields["is_submitted"]; !ok {
		t.Fatal("response is missing is_submitted")
	}
	if _, ok := fields["submit"]; ok {
		t.Fatal("response still exposes submit")
	}
	// Start and Finish each revise the row: version 2 became 4 in the background.
	if got := call(http.MethodDelete, path, "", `"2"`); got.Code != 409 {
		t.Fatalf("stale DELETE after execution=%d %s", got.Code, got.Body)
	}
	// Without If-Match, PUT and DELETE act on the current row.
	renamed := call(http.MethodPut, path, `{"name":"renamed","payload":"new"}`, "")
	if err := json.Unmarshal(renamed.Body.Bytes(), &item); err != nil || renamed.Code != 200 || item.Name != "renamed" || item.Version != 5 || item.Status != 1 {
		t.Fatalf("PUT without If-Match=%s status=%d err=%v", renamed.Body, renamed.Code, err)
	}
	if got := call(http.MethodDelete, path, "", ""); got.Code != 204 || got.Body.Len() != 0 {
		t.Fatalf("DELETE=%d %s", got.Code, got.Body)
	}
	for _, method := range []string{http.MethodGet, http.MethodDelete, http.MethodPut} {
		if got := call(method, path, `{"name":"missing","payload":"missing"}`, ""); got.Code != 404 {
			t.Fatalf("missing %s=%d %s", method, got.Code, got.Body)
		}
	}
	for _, body := range []string{`{"payload":{}}`, `{"payload":"x"} {}`, `{}`} {
		if got := call(http.MethodPost, "/tasks", body, ""); got.Code != 422 {
			t.Errorf("body=%q status=%d", body, got.Code)
		}
	}
	if got := call(http.MethodPost, "/tasks", `{"name":"test task","payload":"`+strings.Repeat("a", domain.MaxPayloadBytes)+`"}`, ""); got.Code != 200 {
		t.Errorf("max payload=%d", got.Code)
	}
	for _, body := range []string{`{"name":"test task","payload":"` + strings.Repeat("a", domain.MaxPayloadBytes+1) + `"}`, `{"name":"test task","payload":"a\u0000b"}`} {
		if got := call(http.MethodPost, "/tasks", body, ""); got.Code != 422 {
			t.Errorf("invalid payload of %d bytes=%d", len(body), got.Code)
		}
	}
	// 1024 escaped bytes fit; a body past 8 KiB is refused before validation.
	if got := call(http.MethodPost, "/tasks", `{"name":"test task","payload":"`+strings.Repeat(`\u00e9`, 512)+`"}`, ""); got.Code != 200 {
		t.Errorf("escaped max payload=%d", got.Code)
	}
	// The largest valid body, fully escaped, fits under the cap.
	if got := call(http.MethodPost, "/tasks", `{"name":"`+strings.Repeat(`\u0001`, domain.MaxNameBytes)+`","payload":"`+strings.Repeat(`\u0001`, domain.MaxPayloadBytes)+`"}`, ""); got.Code != 200 {
		t.Errorf("escaped max name and payload=%d %s", got.Code, got.Body)
	}
	maxName := call(http.MethodPost, "/tasks", `{"name":"`+strings.Repeat("a", domain.MaxNameBytes)+`","payload":"x"}`, "")
	if err := json.Unmarshal(maxName.Body.Bytes(), &item); err != nil || maxName.Code != 200 {
		t.Errorf("max name=%d %v", maxName.Code, err)
	}
	longName := `{"name":"` + strings.Repeat("a", domain.MaxNameBytes+1) + `","payload":"x"}`
	if got := call(http.MethodPost, "/tasks", longName, ""); got.Code != 422 {
		t.Errorf("POST oversized name=%d", got.Code)
	}
	if got := call(http.MethodPut, "/tasks/"+item.ID, longName, ""); got.Code != 422 {
		t.Errorf("PUT oversized name=%d", got.Code)
	}
	if got := call(http.MethodPost, "/tasks", `{"name":"test task","payload":"`+strings.Repeat("a", 9<<10)+`"}`, ""); got.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized body=%d", got.Code)
	}
	if got := call(http.MethodPut, "/tasks/"+string(domain.NewID()), `{"name":"n","payload":"`+strings.Repeat("a", 9<<10)+`"}`, ""); got.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversized PUT body=%d", got.Code)
	}
}

func TestTaskUpdateRejectsInvalidBodiesAndPreconditions(t *testing.T) {
	taskUseCase := usecase.NewTaskUseCase(task.NewMockTaskRepository(), acceptedQueue{}, time.Now)
	item, err := taskUseCase.Submit(context.Background(), "name", []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	api := NewTaskController(taskUseCase, func(context.Context) error { return nil }).Handler()
	for _, body := range []string{`{}`, `{"name":"n"}`, `{"payload":"p"}`, `{"name":null,"payload":"p"}`, `{"name":"before\u0000after","payload":"p"}`, `{"name":"n","payload":{}}`, `{"name":"n","payload":"p","status":"pending"}`, `{"name":"n","payload":"p","status":2}`, `{"name":"n","payload":"p","status":-1}`, `{"name":"n","payload":"p","status":"1"}`, `{"name":"n","payload":"p","status":1.5}`, `{"name":"n","payload":"p","status":null}`, `{"name":"n","payload":"p","version":1}`, `{"name":"n","payload":"p"} {}`} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/tasks/"+string(item.ID), strings.NewReader(body)))
		if w.Code != 422 {
			t.Errorf("invalid PUT %q=%d %s", body, w.Code, w.Body)
		}
		assertProblemMessage(t, w)
	}
	stored, err := taskUseCase.Get(context.Background(), string(item.ID))
	if err != nil || stored.Name != item.Name || stored.Payload != item.Payload || stored.Version != item.Version {
		t.Fatalf("invalid PUT changed task=%+v err=%v", stored, err)
	}
	for _, version := range []string{"1", `"0"`, `"-1"`, `"bad"`, `"1", "2"`, "*"} {
		for _, method := range []string{http.MethodPut, http.MethodDelete} {
			req := httptest.NewRequest(method, "/tasks/"+string(item.ID), strings.NewReader(`{"name":"n","payload":"p"}`))
			req.Header.Set("If-Match", version)
			w := httptest.NewRecorder()
			api.ServeHTTP(w, req)
			if w.Code != 422 {
				t.Errorf("invalid If-Match %q %s=%d", version, method, w.Code)
			}
		}
	}
}

func TestTaskNamesAreRequiredWithoutChangingStorage(t *testing.T) {
	ctx := context.Background()
	repo := task.NewMockTaskRepository()
	svc := usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now)
	original, err := svc.Submit(ctx, "original", []byte("original"))
	if err != nil {
		t.Fatal(err)
	}
	api := NewTaskController(svc, func(context.Context) error { return nil }).Handler()
	for _, body := range []string{
		`{"payload":"changed"}`,
		`{"name":null,"payload":"changed"}`,
		`{"name":"","payload":"changed"}`,
		`{"name":" \t\r\n","payload":"changed"}`,
		`{"name":"\u00a0\u3000","payload":"changed"}`,
		`{"name":1,"payload":"changed"}`,
	} {
		for _, method := range []string{http.MethodPost, http.MethodPut} {
			path := "/tasks"
			if method == http.MethodPut {
				path += "/" + string(original.ID)
			}
			w := httptest.NewRecorder()
			api.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
			if w.Code != http.StatusUnprocessableEntity {
				t.Errorf("%s body %s=%d %s", method, body, w.Code, w.Body)
			}
			assertProblemMessage(t, w)
		}
	}
	items, _, err := svc.List(ctx, 100, time.Time{}, "")
	if err != nil || len(items) != 1 || items[0].Name != original.Name || items[0].Payload != original.Payload || items[0].Version != original.Version {
		t.Fatalf("rejected names changed storage=%+v err=%v", items, err)
	}
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		path := "/tasks"
		if method == http.MethodPut {
			path += "/" + string(original.ID)
		}
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(`{"name":" 你好🙂 ","payload":""}`)))
		var item response
		if err := json.Unmarshal(w.Body.Bytes(), &item); err != nil || w.Code != http.StatusOK || item.Name != " 你好🙂 " || item.Payload != "" {
			t.Fatalf("%s valid name=%d %s err=%v", method, w.Code, w.Body, err)
		}
	}
}

type failingGetRepository struct {
	port.TaskRepository
	err error
}

func (r failingGetRepository) Get(context.Context, domain.ID) (*domain.Task, error) {
	return nil, r.err
}

type failingListRepository struct {
	port.TaskRepository
	err error
}

func (r failingListRepository) List(context.Context, int, time.Time, domain.ID) ([]*domain.Task, error) {
	return nil, r.err
}

func TestHTTPMapsApplicationErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{
		{domain.ErrInvalidID, http.StatusNotFound},
		{domain.ErrPayload, http.StatusUnprocessableEntity},
		{domain.ErrBusy, http.StatusConflict}, {domain.ErrName, http.StatusUnprocessableEntity},
		{domain.ErrConflict, http.StatusConflict},
		{domain.ErrNotFound, http.StatusNotFound},
		{errors.New("db unavailable"), http.StatusInternalServerError},
	} {
		repo := failingGetRepository{TaskRepository: task.NewMockTaskRepository(), err: tc.err}
		taskUseCase := usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now)
		api := NewTaskController(taskUseCase, func(context.Context) error { return nil }).Handler()
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/tasks/00000000-0000-4000-8000-000000000000", nil))
		if w.Code != tc.want {
			t.Errorf("error %v: status=%d want %d", tc.err, w.Code, tc.want)
		}
	}
}

func TestHTTPRejectsInvalidBodies(t *testing.T) {
	api := NewTaskController(usecase.NewTaskUseCase(task.NewMockTaskRepository(), acceptedQueue{}, time.Now), func(context.Context) error { return nil }).Handler()
	for _, tc := range []struct {
		body string
		want int
	}{
		{`{"name":"test task","payload":"x" ,"surprise":1}`, http.StatusOK}, {`{"payload":"x"} {}`, http.StatusUnprocessableEntity}, {`{"payload":{}}`, http.StatusUnprocessableEntity}, {`{"payload":null}`, http.StatusUnprocessableEntity}, {`{}`, http.StatusUnprocessableEntity},
		{`{"payload":"x","name":null}`, http.StatusUnprocessableEntity},
		{`{"payload":"x","name":3}`, http.StatusUnprocessableEntity},
		{`{"payload":"x","name":"before\u0000after"}`, http.StatusUnprocessableEntity},
	} {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/tasks", strings.NewReader(tc.body)))
		if w.Code != tc.want {
			t.Errorf("body %q status=%d want %d", tc.body, w.Code, tc.want)
		}
		if tc.want == http.StatusUnprocessableEntity {
			assertProblemMessage(t, w)
		}
	}
}

func TestTaskListMapsPaginationAndMasksStorageErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
		body string
	}{
		{domain.ErrPagination, http.StatusBadRequest, "invalid pagination"},
		{errors.New("private database detail"), http.StatusInternalServerError, "Something went wrong. Please notify the maintainer"},
	} {
		repo := failingListRepository{TaskRepository: task.NewMockTaskRepository(), err: tc.err}
		api := NewTaskController(usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now), func(context.Context) error { return nil }).Handler()
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/tasks", nil))
		if w.Code != tc.want || !strings.Contains(w.Body.String(), tc.body) || strings.Contains(w.Body.String(), "private database detail") {
			t.Errorf("error %v: status=%d body=%s want status=%d", tc.err, w.Code, w.Body, tc.want)
		}
	}
}

func TestHTTPReturnsSourceTaskError(t *testing.T) {
	repo := task.NewMockTaskRepository()
	taskUseCase := usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now)
	created, err := taskUseCase.Submit(context.Background(), "test task", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	const detail = "permission denied for internal credential"
	if err = repo.UpdateByID(context.Background(), created.ID, created.Version, func(t *domain.Task) (bool, error) {
		if _, err := t.Start(time.Now()); err != nil {
			return false, err
		}
		failure := detail
		return t.Finish(time.Now(), nil, &failure)
	}); err != nil {
		t.Fatal(err)
	}
	api := NewTaskController(taskUseCase, func(context.Context) error { return nil }).Handler()
	w := httptest.NewRecorder()
	api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/tasks/"+string(created.ID), nil))
	var response struct {
		Error  string `json:"error"`
		Status int    `json:"status"`
	}
	if err = json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != http.StatusOK || response.Error != detail || response.Status != 1 {
		t.Fatalf("response=%s status=%d err=%v", w.Body, w.Code, err)
	}
	stored, err := repo.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if failure := stored.Error; failure == nil || *failure != detail {
		t.Fatalf("private diagnostic changed: %v", failure)
	}
}

func TestTaskListCursorKeepsTimestampTiesStable(t *testing.T) {
	repo := task.NewMockTaskRepository()
	now := time.Date(2026, 5, 12, 9, 30, 0, 123, time.UTC)
	taskUseCase := usecase.NewTaskUseCase(repo, acceptedQueue{}, func() time.Time { return now })
	for i := 0; i < 3; i++ {
		if _, err := taskUseCase.Submit(context.Background(), "test task", []byte(`{"n":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	api := NewTaskController(taskUseCase, func(context.Context) error { return nil }).Handler()
	call := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w
	}
	var first struct {
		Items      []response `json:"items"`
		NextCursor *string    `json:"next_cursor"`
	}
	w := call("/tasks?page_size=2")
	if err := json.Unmarshal(w.Body.Bytes(), &first); err != nil || w.Code != http.StatusOK || len(first.Items) != 2 || first.NextCursor == nil {
		t.Fatalf("first page=%s status=%d err=%v", w.Body, w.Code, err)
	}
	if first.Items[0].Status != 0 || first.Items[1].Status != 0 || first.Items[0].CreatedAt != first.Items[1].CreatedAt || first.Items[0].ID <= first.Items[1].ID {
		t.Fatalf("timestamp tie order is not stable descending ID: %+v", first.Items)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(*first.NextCursor)
	if err != nil || string(decoded) != first.Items[1].CreatedAt.Format(time.RFC3339Nano)+"|"+first.Items[1].ID {
		t.Fatalf("cursor=%q decoded=%q err=%v", *first.NextCursor, decoded, err)
	}
	var second struct {
		Items      []response `json:"items"`
		NextCursor *string    `json:"next_cursor"`
	}
	w = call("/tasks?page_size=2&cursor=" + *first.NextCursor)
	if err := json.Unmarshal(w.Body.Bytes(), &second); err != nil || w.Code != http.StatusOK || len(second.Items) != 1 || second.NextCursor != nil {
		t.Fatalf("second page=%s status=%d err=%v", w.Body, w.Code, err)
	}
	if second.Items[0].CreatedAt != now.Truncate(time.Microsecond) || second.Items[0].ID >= first.Items[1].ID {
		t.Fatalf("second page does not continue timestamp-tie order: %+v", second.Items)
	}
}

func TestTaskListEmptyPageAndRejectsInvalidPagination(t *testing.T) {
	api := NewTaskController(usecase.NewTaskUseCase(task.NewMockTaskRepository(), acceptedQueue{}, time.Now), func(context.Context) error { return nil }).Handler()
	call := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w
	}
	var empty struct {
		Items      []json.RawMessage `json:"items"`
		NextCursor *string           `json:"next_cursor"`
	}
	w := call("/tasks")
	if err := json.Unmarshal(w.Body.Bytes(), &empty); err != nil || w.Code != http.StatusOK || empty.Items == nil || len(empty.Items) != 0 || empty.NextCursor != nil {
		t.Fatalf("empty page=%s status=%d err=%v", w.Body, w.Code, err)
	}

	id := string(domain.NewID())
	validTime := "2026-05-12T09:30:00.000000123Z"
	badCursorPayloads := []string{"!", "not-a-time|" + id, validTime + "|bad-id", validTime + "|" + id + "|extra", "0001-01-01T00:00:00Z|" + id}
	for _, payload := range badCursorPayloads {
		cursor := base64.RawURLEncoding.EncodeToString([]byte(payload))
		if got := call("/tasks?cursor=" + cursor); got.Code != http.StatusBadRequest {
			t.Errorf("cursor %q status=%d body=%s", payload, got.Code, got.Body)
		} else {
			assertProblemMessage(t, got)
		}
	}
	for _, path := range []string{
		"/tasks?page_size=0", "/tasks?page_size=-1", "/tasks?page_size=101", "/tasks?page_size=nope",
		"/tasks?page_size=2&page_size=3", "/tasks?offset=0", "/tasks?limit=10", "/tasks?cursor=abc&cursor=def",
	} {
		if got := call(path); got.Code != http.StatusBadRequest {
			t.Errorf("path %q status=%d body=%s", path, got.Code, got.Body)
		} else {
			assertProblemMessage(t, got)
		}
	}
}

func assertProblemMessage(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	var fields map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &fields); err != nil || fields["message"] == "" || len(fields) != 1 {
		t.Fatalf("error response does not match OpenAPI message schema: %s, %v", w.Body, err)
	}
}

// acceptedQueue confirms every Submit fast-path publication.
type acceptedQueue struct{}

func (acceptedQueue) Enqueue(_ context.Context, id domain.ID) (usecase.Publication, error) {
	return usecase.Publication{ID: id}, nil
}

// The assignment's create shape includes status, so POST and PUT accept 0 or 1
// and ignore it: status is derived from processing, never set by clients.
func TestTaskStatusInRequestBodyIsValidatedAndIgnored(t *testing.T) {
	repo := task.NewMockTaskRepository()
	taskUseCase := usecase.NewTaskUseCase(repo, acceptedQueue{}, time.Now)
	api := NewTaskController(taskUseCase, func(context.Context) error { return nil }).Handler()
	call := func(method, path, body string) (*httptest.ResponseRecorder, response) {
		w := httptest.NewRecorder()
		api.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
		var item response
		_ = json.Unmarshal(w.Body.Bytes(), &item)
		return w, item
	}
	for _, status := range []string{"0", "1"} {
		w, item := call(http.MethodPost, "/tasks", `{"name":"n","payload":"p","status":`+status+`}`)
		if w.Code != 200 || item.Status != 0 || item.Version != 1 {
			t.Fatalf("POST status %s=%d %s", status, w.Code, w.Body)
		}
		stored, err := repo.Get(context.Background(), domain.ID(item.ID))
		if err != nil || stored.Finished() || stored.Status != domain.Pending {
			t.Fatalf("POST status %s changed task=%+v,%v", status, stored, err)
		}
		w, updated := call(http.MethodPut, "/tasks/"+item.ID, `{"name":"edited","payload":"p","status":`+status+`}`)
		if w.Code != 200 || updated.Status != 0 || updated.Name != "edited" || updated.Version != 2 {
			t.Fatalf("PUT status %s=%d %s", status, w.Code, w.Body)
		}
		stored, err = repo.Get(context.Background(), domain.ID(item.ID))
		if err != nil || stored.Finished() || stored.Status != domain.Pending || stored.Name != "edited" {
			t.Fatalf("PUT status %s changed task=%+v,%v", status, stored, err)
		}
	}
	for _, status := range []string{"2", "-1", `"1"`, "1.5", "null", `"x"`} {
		if w, _ := call(http.MethodPost, "/tasks", `{"name":"test task","payload":"p","status":`+status+`}`); w.Code != 422 {
			t.Errorf("POST status %s=%d %s", status, w.Code, w.Body)
		} else {
			assertProblemMessage(t, w)
		}
	}
	if items, _, err := taskUseCase.List(context.Background(), 100, time.Time{}, ""); err != nil || len(items) != 2 {
		t.Fatalf("invalid status created tasks=%d,%v", len(items), err)
	}
}
