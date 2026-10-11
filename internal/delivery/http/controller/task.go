package controller

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

type TaskController struct {
	useCase  *usecase.TaskUseCase
	ready    func(context.Context) error
	requests atomic.Int64
	failures atomic.Int64
}

func NewTaskController(taskUseCase *usecase.TaskUseCase, ready func(context.Context) error) *TaskController {
	return &TaskController{useCase: taskUseCase, ready: ready}
}

type response struct {
	Name        string    `json:"name"`
	Version     int64     `json:"version"`
	IsRunning   bool      `json:"is_running"`
	ID          string    `json:"id"`
	Payload     string    `json:"payload"`
	Status      int       `json:"status"`
	Result      *string   `json:"result"`
	Error       *string   `json:"error"`
	CreatedAt   time.Time `json:"created_time"`
	UpdatedAt   time.Time `json:"updated_time"`
	IsSubmitted bool      `json:"is_submitted"`
}

type listResponse struct {
	Items      []response `json:"items"`
	NextCursor *string    `json:"next_cursor"`
}

func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func taskResponse(task *domain.Task) response {
	status := 0
	if task.Finished() {
		status = 1
	}
	return response{ID: string(task.ID), Name: task.Name, Version: task.Version, IsRunning: task.IsRunning, Payload: task.Payload.String(), Status: status, Result: task.Result, Error: task.Error, CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt, IsSubmitted: task.IsSubmitted}
}
func (a *TaskController) problem(w http.ResponseWriter, err error) {
	code, message := http.StatusInternalServerError, "Something went wrong. Please notify the maintainer"
	switch {
	case errors.Is(err, domain.ErrNotFound):
		code, message = http.StatusNotFound, err.Error()
	case errors.Is(err, domain.ErrInvalidID):
		code, message = http.StatusNotFound, domain.ErrNotFound.Error()
	case errors.Is(err, domain.ErrPagination):
		code, message = http.StatusBadRequest, err.Error()
	case errors.Is(err, domain.ErrPayload), errors.Is(err, domain.ErrName):
		code, message = http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrBusy):
		code, message = http.StatusConflict, err.Error()
	default:
		a.failures.Add(1)
		slog.Error("request failed", "error", err)
	}
	write(w, code, map[string]string{"message": message})
}
func (a *TaskController) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", a.submit)
	mux.HandleFunc("GET /tasks", a.list)
	mux.HandleFunc("GET /tasks/{id}", a.get)
	mux.HandleFunc("PUT /tasks/{id}", a.update)
	mux.HandleFunc("DELETE /tasks/{id}", a.delete)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := a.ready(ctx); err != nil {
			a.problem(w, err)
			return
		}
		write(w, 200, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = io.WriteString(w, "# TYPE gogolook_http_requests_total counter\ngogolook_http_requests_total "+jsonNumber(a.requests.Load())+"\n# TYPE gogolook_storage_failures_total counter\ngogolook_storage_failures_total "+jsonNumber(a.failures.Load())+"\n")
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.requests.Add(1)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func jsonNumber(n int64) string { b, _ := json.Marshal(n); return string(b) }

func (a *TaskController) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	if _, exists := query["offset"]; exists {
		write(w, http.StatusBadRequest, map[string]string{"message": "offset is not supported"})
		return
	}
	if _, exists := query["limit"]; exists {
		write(w, http.StatusBadRequest, map[string]string{"message": "limit is not supported"})
		return
	}

	pageSize := 20
	if values, exists := query["page_size"]; exists {
		if len(values) != 1 {
			write(w, http.StatusBadRequest, map[string]string{"message": "invalid page_size"})
			return
		}
		parsed, err := strconv.Atoi(values[0])
		if err != nil || parsed < 1 || parsed > 100 {
			write(w, http.StatusBadRequest, map[string]string{"message": "page_size must be between 1 and 100"})
			return
		}
		pageSize = parsed
	}

	var before time.Time
	var beforeID domain.ID
	if values, exists := query["cursor"]; exists {
		if len(values) != 1 {
			write(w, http.StatusBadRequest, map[string]string{"message": "invalid cursor"})
			return
		}
		decoded, err := base64.RawURLEncoding.DecodeString(values[0])
		parts := strings.Split(string(decoded), "|")
		if err != nil || len(parts) != 2 {
			write(w, http.StatusBadRequest, map[string]string{"message": "invalid cursor"})
			return
		}
		before, err = time.Parse(time.RFC3339Nano, parts[0])
		if err != nil || before.IsZero() {
			write(w, http.StatusBadRequest, map[string]string{"message": "invalid cursor"})
			return
		}
		beforeID, err = domain.ParseID(parts[1])
		if err != nil {
			write(w, http.StatusBadRequest, map[string]string{"message": "invalid cursor"})
			return
		}
	}

	tasks, hasMore, err := a.useCase.List(r.Context(), pageSize, before, beforeID)
	if err != nil {
		a.problem(w, err)
		return
	}
	items := make([]response, 0, len(tasks))
	for _, item := range tasks {
		items = append(items, taskResponse(item))
	}
	var nextCursor *string
	if hasMore && len(tasks) > 0 {
		last := tasks[len(tasks)-1]
		value := base64.RawURLEncoding.EncodeToString([]byte(last.CreatedAt.Format(time.RFC3339Nano) + "|" + string(last.ID)))
		nextCursor = &value
	}
	write(w, http.StatusOK, listResponse{Items: items, NextCursor: nextCursor})
}

// maxBodyBytes fits the largest valid body with maximal JSON escaping: \uXXXX
// takes 6 bytes per control character, so a payload of domain.MaxPayloadBytes
// takes up to 6 KiB and a name of domain.MaxNameBytes up to 1.5 KiB.
const maxBodyBytes = 8 << 10

// bodyTooLarge answers 413 when reading stopped at maxBodyBytes.
func bodyTooLarge(w http.ResponseWriter, err error) bool {
	var tooLarge *http.MaxBytesError
	if !errors.As(err, &tooLarge) {
		return false
	}
	write(w, http.StatusRequestEntityTooLarge, map[string]string{"message": "request body exceeds 8 KiB"})
	return true
}

// Read the bounded body before decoding: encoding/json replaces invalid UTF-8
// inside strings, which would otherwise hide malformed input from the domain.
func readJSONBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	defer r.Body.Close()
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		if !bodyTooLarge(w, err) {
			write(w, 422, map[string]string{"message": "could not read request body"})
		}
		return nil, false
	}
	if !utf8.Valid(body) {
		write(w, 422, map[string]string{"message": "request body must be valid UTF-8"})
		return nil, false
	}
	return body, true
}

func (a *TaskController) submit(w http.ResponseWriter, r *http.Request) {
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	var in struct {
		Payload *string         `json:"payload"`
		Name    *string         `json:"name"`
		Status  json.RawMessage `json:"status"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&in); err != nil {
		write(w, 422, map[string]string{"message": "expected a JSON object with name and payload strings"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		write(w, 422, map[string]string{"message": "expected one JSON object"})
		return
	}
	if !validStatus(in.Status) {
		write(w, 422, map[string]string{"message": "status must be 0 or 1"})
		return
	}
	if in.Payload == nil {
		a.problem(w, domain.ErrPayload)
		return
	}
	if in.Name == nil {
		a.problem(w, domain.ErrName)
		return
	}
	s, err := a.useCase.Submit(r.Context(), *in.Name, []byte(*in.Payload))
	if err != nil {
		a.problem(w, err)
		return
	}
	w.Header().Set("Location", "/tasks/"+string(s.ID))
	writeTask(w, s)
}
func (a *TaskController) get(w http.ResponseWriter, r *http.Request) {
	s, err := a.useCase.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		a.taskProblem(w, err, r.PathValue("id"))
		return
	}
	writeTask(w, s)
}
func writeTask(w http.ResponseWriter, item *domain.Task) {
	w.Header().Set("ETag", strconv.Quote(strconv.FormatInt(item.Version, 10)))
	write(w, http.StatusOK, taskResponse(item))
}

// validStatus accepts an absent status or 0/1. The assignment's create shape
// includes status, so clients may send it; the server derives it and ignores it.
func validStatus(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return true
	}
	var status *int
	if err := json.Unmarshal(raw, &status); err != nil || status == nil {
		return false
	}
	return *status == 0 || *status == 1
}
func expectedVersion(r *http.Request) (int64, error) {
	values := r.Header.Values("If-Match")
	if len(values) == 0 {
		return 0, nil
	}
	if len(values) != 1 {
		return 0, errors.New("If-Match must contain one quoted task version")
	}
	value, err := strconv.Unquote(values[0])
	if err != nil {
		return 0, errors.New("If-Match must contain one quoted task version")
	}
	version, err := strconv.ParseInt(value, 10, 64)
	if err != nil || version < 1 {
		return 0, errors.New("If-Match must contain a positive task version")
	}
	return version, nil
}
func (a *TaskController) update(w http.ResponseWriter, r *http.Request) {
	body, ok := readJSONBody(w, r)
	if !ok {
		return
	}
	var in struct {
		Name    *string         `json:"name"`
		Payload *string         `json:"payload"`
		Status  json.RawMessage `json:"status"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil || in.Name == nil || in.Payload == nil {
		write(w, 422, map[string]string{"message": "expected name and payload strings only"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		write(w, 422, map[string]string{"message": "expected one JSON object"})
		return
	}
	if !validStatus(in.Status) {
		write(w, 422, map[string]string{"message": "status must be 0 or 1"})
		return
	}
	version, err := expectedVersion(r)
	if err != nil {
		write(w, 422, map[string]string{"message": err.Error()})
		return
	}
	item, err := a.useCase.Update(r.Context(), r.PathValue("id"), *in.Name, []byte(*in.Payload), version)
	if err != nil {
		a.taskProblem(w, err, r.PathValue("id"))
		return
	}
	writeTask(w, item)
}
func (a *TaskController) delete(w http.ResponseWriter, r *http.Request) {
	version, err := expectedVersion(r)
	if err != nil {
		write(w, 422, map[string]string{"message": err.Error()})
		return
	}
	if err := a.useCase.Delete(r.Context(), r.PathValue("id"), version); err != nil {
		a.taskProblem(w, err, r.PathValue("id"))
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *TaskController) taskProblem(w http.ResponseWriter, err error, id string) {
	switch {
	case errors.Is(err, domain.ErrNotFound), errors.Is(err, domain.ErrInvalidID):
		write(w, 404, map[string]string{"message": "Task: " + id + " not found."})
	default:
		a.problem(w, err)
	}
}
