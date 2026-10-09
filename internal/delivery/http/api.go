package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

type API struct {
	Tasks    usecase.TaskUseCases
	Ready    func(context.Context) error
	requests atomic.Int64
	failures atomic.Int64
}

type response struct {
	ID        string          `json:"id"`
	Payload   json.RawMessage `json:"payload"`
	Status    string          `json:"status"`
	Result    *string         `json:"result"`
	Error     *string         `json:"error"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
	Attempts  int             `json:"attempts"`
}

func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func taskResponse(s usecase.TaskResponse) response {
	return response{s.ID, json.RawMessage(s.Payload), s.Status, s.Result, s.Error, s.CreatedAt, s.UpdatedAt, s.Attempts}
}
func (a *API) problem(w http.ResponseWriter, err error) {
	code, message := http.StatusServiceUnavailable, "storage temporarily unavailable"
	switch {
	case errors.Is(err, usecase.ErrNotFound):
		code, message = http.StatusNotFound, err.Error()
	case errors.Is(err, usecase.ErrInvalidID):
		code, message = http.StatusBadRequest, err.Error()
	case errors.Is(err, usecase.ErrInvalidPayload):
		code, message = http.StatusUnprocessableEntity, err.Error()
	case errors.Is(err, usecase.ErrCompleted), errors.Is(err, usecase.ErrConflict):
		code, message = http.StatusConflict, err.Error()
	default:
		a.failures.Add(1)
		slog.Error("request failed", "error", err)
	}
	write(w, code, map[string]string{"error": message})
}
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tasks", a.submit)
	mux.HandleFunc("GET /tasks/{id}", a.get)
	// Go's ServeMux cannot use a wildcard embedded in a path segment.
	mux.HandleFunc("POST /tasks/{action}", a.cancel)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := a.Ready(ctx); err != nil {
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
func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 256*1024)
	defer r.Body.Close()
	var in struct {
		Payload json.RawMessage `json:"payload"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&in); err != nil {
		write(w, 400, map[string]string{"error": "expected a JSON object with payload"})
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		write(w, 400, map[string]string{"error": "expected one JSON object"})
		return
	}
	s, err := a.Tasks.Submit(r.Context(), usecase.SubmitTaskRequest{Payload: in.Payload})
	if err != nil {
		a.problem(w, err)
		return
	}
	w.Header().Set("Location", "/tasks/"+s.ID)
	write(w, http.StatusAccepted, taskResponse(s))
}
func (a *API) get(w http.ResponseWriter, r *http.Request) {
	s, err := a.Tasks.Get(r.Context(), usecase.TaskRequest{ID: r.PathValue("id")})
	if err != nil {
		a.problem(w, err)
		return
	}
	write(w, 200, taskResponse(s))
}
func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	const suffix = ":cancel"
	if len(action) < len(suffix) || !strings.HasSuffix(action, suffix) {
		http.NotFound(w, r)
		return
	}
	s, err := a.Tasks.Cancel(r.Context(), usecase.TaskRequest{ID: strings.TrimSuffix(action, suffix)})
	if err != nil {
		a.problem(w, err)
		return
	}
	write(w, 200, taskResponse(s))
}
