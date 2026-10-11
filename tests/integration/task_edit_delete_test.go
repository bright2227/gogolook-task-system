//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	httpapi "github.com/bright2227/gogolook-task-system/internal/delivery/http"
	"github.com/bright2227/gogolook-task-system/internal/domain"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

type integrationPublisherFunc func(context.Context, domain.ID) error

func (f integrationPublisherFunc) Enqueue(ctx context.Context, id domain.ID) (usecase.Publication, error) {
	err := f(ctx, id)
	if err != nil {
		return usecase.Publication{}, err
	}
	return usecase.Publication{ID: id}, nil
}

func TestUpdateByIDTransactionGuardsPostgres(t *testing.T) {
	s := newPostgresStack(t, true)
	taskUseCase := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
	created, err := taskUseCase.Submit(s.ctx, "original", []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := taskUseCase.Submit(s.ctx, "other", []byte("other payload"))
	if err != nil {
		t.Fatal(err)
	}
	callbackErr := errors.New("callback failed")
	for _, test := range []struct {
		name   string
		update func(*domain.Task) (bool, error)
		want   error
	}{
		{"unchanged", func(item *domain.Task) (bool, error) { item.Name = "discarded"; return false, nil }, nil},
		{"callback error", func(item *domain.Task) (bool, error) { item.Name = "discarded"; return true, callbackErr }, callbackErr},
		{"invalid content", func(item *domain.Task) (bool, error) { item.Name = "\x00"; return true, nil }, domain.ErrName},
		{"changed ID", func(item *domain.Task) (bool, error) { item.ID = other.ID; item.Name = "discarded"; return true, nil }, domain.ErrConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := s.repo.UpdateByID(s.ctx, created.ID, created.Version, test.update); !errors.Is(err, test.want) {
				t.Fatalf("update error=%v want=%v", err, test.want)
			}
			for _, before := range []*domain.Task{created, other} {
				stored, err := s.repo.Get(s.ctx, before.ID)
				if err != nil || *stored != *before {
					t.Fatalf("discarded update changed task=%+v err=%v", stored, err)
				}
			}
		})
	}
	for _, test := range []struct {
		id      domain.ID
		version int64
		want    error
	}{
		{created.ID, created.Version + 1, domain.ErrConflict},
		{domain.NewID(), 1, domain.ErrNotFound},
	} {
		called := false
		err := s.repo.UpdateByID(s.ctx, test.id, test.version, func(*domain.Task) (bool, error) { called = true; return true, nil })
		if !errors.Is(err, test.want) || called {
			t.Fatalf("guard error=%v want=%v callback=%v", err, test.want, called)
		}
	}
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	err = s.repo.UpdateByID(ctx, created.ID, created.Version, func(item *domain.Task) (bool, error) {
		item.Name = "discarded"
		cancel()
		return true, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled update=%v", err)
	}
	stored, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || *stored != *created {
		t.Fatalf("canceled update changed task=%+v err=%v", stored, err)
	}
	if err := s.repo.UpdateByID(s.ctx, created.ID, created.Version, func(item *domain.Task) (bool, error) {
		return item.Edit("committed", item.Payload, time.Now())
	}); err != nil {
		t.Fatal(err)
	}
	stored, err = s.repo.Get(s.ctx, created.ID)
	if err != nil || stored.Name != "committed" || stored.Version != created.Version+1 {
		t.Fatalf("successful update=%+v err=%v", stored, err)
	}
}

func TestTaskNamesRejectNULBeforePersistence(t *testing.T) {
	s := newPostgresStack(t, true)
	taskUseCase := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
	created, err := taskUseCase.Submit(s.ctx, "original", []byte("payload"))
	if err != nil {
		t.Fatal(err)
	}
	api := httpapi.NewServer(taskUseCase, func(ctx context.Context) error { return s.db.PingContext(ctx) })
	for _, request := range []struct{ method, path string }{
		{http.MethodPost, "/tasks"},
		{http.MethodPut, "/tasks/" + string(created.ID)},
	} {
		for _, body := range []string{
			`{"payload":"changed"}`,
			`{"name":null,"payload":"changed"}`,
			`{"name":"","payload":"changed"}`,
			`{"name":" \t\u3000","payload":"changed"}`,
			`{"name":"before\u0000after","payload":"changed"}`,
		} {
			req := httptest.NewRequest(request.method, request.path, strings.NewReader(body)).WithContext(s.ctx)
			response := httptest.NewRecorder()
			api.ServeHTTP(response, req)
			if response.Code != http.StatusUnprocessableEntity {
				t.Fatalf("%s invalid name %s response=%d %s", request.method, body, response.Code, response.Body)
			}
		}
	}
	stored, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || stored.Name != created.Name || stored.Payload != created.Payload || stored.Version != created.Version {
		t.Fatalf("rejected names changed task=%+v err=%v", stored, err)
	}
	var count int
	if err := s.db.QueryRowContext(s.ctx, `SELECT count(*) FROM tasks`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("rejected name created a task: count=%d err=%v", count, err)
	}
}

func TestTaskEditDeleteClaimsAndVersionedPublication(t *testing.T) {
	s := newPostgresStack(t, true)
	svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, time.Now)
	created, err := svc.Submit(s.ctx, "original name", []byte("original payload"))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	releaseWork := sync.OnceFunc(func() { close(release) })
	defer releaseWork()
	var execute usecase.Work = func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	done := make(chan error, 1)
	go func() { done <- svc.ProcessTasks(s.ctx, []domain.ID{created.ID}, execute, retryDelays) }()
	select {
	case <-started:
	case <-s.ctx.Done():
		t.Fatal("worker never claimed task")
	}
	server := httptest.NewServer(httpapi.NewServer(svc, func(ctx context.Context) error { return s.db.PingContext(ctx) }))
	defer server.Close()
	request := func(method, body string, want int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(s.ctx, method, server.URL+"/tasks/"+string(created.ID), strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != want {
			t.Fatalf("%s response=%d %s err=%v", method, response.StatusCode, data, err)
		}
		return data
	}
	request(http.MethodPut, `{"name":"new name","payload":"new payload"}`, http.StatusConflict)
	request(http.MethodDelete, "", http.StatusConflict)
	running, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || !running.IsRunning || running.Version != 2 || running.Payload.String() != "original payload" {
		t.Fatalf("running mutation changed task=%+v,%v", running, err)
	}
	releaseWork()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-s.ctx.Done():
		t.Fatal("worker did not commit result")
	}
	var renamed struct {
		Name        string  `json:"name"`
		Version     int64   `json:"version"`
		IsRunning   bool    `json:"is_running"`
		Status      *int    `json:"status"`
		IsSubmitted *bool   `json:"is_submitted"`
		Result      *string `json:"result"`
	}
	data := request(http.MethodPut, `{"name":"renamed","payload":"original payload"}`, http.StatusOK)
	if err := json.Unmarshal(data, &renamed); err != nil || renamed.Name != "renamed" || renamed.Version != 4 || renamed.IsRunning || renamed.Status == nil || *renamed.Status != 1 || renamed.IsSubmitted == nil || !*renamed.IsSubmitted || renamed.Result == nil || *renamed.Result != "16" {
		t.Fatalf("name-only edit lost completed result: %s err=%v", data, err)
	}
	data = request(http.MethodPut, `{"name":"new content","payload":"new payload"}`, http.StatusOK)
	var reset struct {
		Status      *int    `json:"status"`
		IsSubmitted *bool   `json:"is_submitted"`
		Result      *string `json:"result"`
		Error       *string `json:"error"`
	}
	if err := json.Unmarshal(data, &reset); err != nil || reset.Status == nil || *reset.Status != 0 || reset.IsSubmitted == nil || *reset.IsSubmitted || reset.Result != nil || reset.Error != nil {
		t.Fatalf("payload edit did not reset public task state: %s err=%v", data, err)
	}
	edited, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || edited.Version != 5 || edited.Status != domain.Pending || edited.IsRunning || edited.IsSubmitted || edited.Result != nil || edited.Error != nil || edited.RetryCount != 0 || edited.CreatedAt != created.CreatedAt {
		t.Fatalf("payload edit did not reset task=%+v,%v", edited, err)
	}
	called := false
	err = s.repo.UpdateByID(s.ctx, created.ID, created.Version, func(item *domain.Task) (bool, error) {
		called = true
		item.IsSubmitted = true
		return true, nil
	})
	if !errors.Is(err, domain.ErrConflict) || called {
		t.Fatalf("stale SQL update invoked callback=%v err=%v", called, err)
	}
	if err := s.repo.DeleteByID(s.ctx, created.ID, created.Version); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("stale SQL delete=%v", err)
	}

	// Versions so far: create 1, claim 2, finish 3, rename 4, payload edit 5.
	// Hold broker confirmation after the sweeper has read version 5. An edit
	// wins before its marker transaction, which must leave version 6 unpublished.
	publishing := make(chan struct{})
	confirm := make(chan struct{})
	releaseConfirm := sync.OnceFunc(func() { close(confirm) })
	defer releaseConfirm()
	publisher := integrationPublisherFunc(func(ctx context.Context, id domain.ID) error {
		if id != created.ID {
			return errors.New("unexpected publication ID")
		}
		close(publishing)
		select {
		case <-confirm:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	publication := usecase.NewTaskUseCase(s.repo, publisher, time.Now)
	publication.RepublishAfter = republishAfter
	publication.StaleAfter = staleAfter
	// The edited row was created moments ago; sweep past PublishGrace.
	publication.Clock = func() time.Time { return time.Now().Add(publication.PublishGrace) }
	type publicationOutcome struct {
		count int
		err   error
	}
	published := make(chan publicationOutcome, 1)
	go func() { n, err := publication.PublishPending(s.ctx); published <- publicationOutcome{n, err} }()
	select {
	case <-publishing:
	case <-s.ctx.Done():
		t.Fatal("publisher never reached confirmation")
	}
	newRevision, err := svc.Update(s.ctx, string(created.ID), "latest", []byte("latest payload"), edited.Version)
	if err != nil {
		t.Fatal(err)
	}
	releaseConfirm()
	select {
	case result := <-published:
		if result.err != nil || result.count != 0 {
			t.Fatalf("stale publication marker=%d,%v", result.count, result.err)
		}
	case <-s.ctx.Done():
		t.Fatal("publisher did not finish")
	}
	latest, err := s.repo.Get(s.ctx, created.ID)
	if err != nil || latest.Version != newRevision.Version || latest.Version != 6 || latest.IsSubmitted || latest.Payload.String() != "latest payload" {
		t.Fatalf("stale confirmation changed new revision=%+v,%v", latest, err)
	}
}

func TestDeletedTaskDeliveryIsAcknowledged(t *testing.T) {
	s := newIntegrationStack(t)
	client := s.client(t, "integration.deleted")
	repo := s.repo
	svc := newPublishingTaskUseCase(s, client, 8)
	created := submitTask(t, s.ctx, svc, "deleted payload")
	if count, err := svc.PublishPending(s.ctx); err != nil || count != 0 || !created.IsSubmitted {
		t.Fatalf("publish before deletion=%d,%v", count, err)
	}
	if err := svc.Delete(s.ctx, string(created.ID), created.Version); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRowContext(s.ctx, "SELECT count(*) FROM tasks").Scan(&count); err != nil || count != 0 {
		t.Fatalf("deletion left task rows=%d err=%v", count, err)
	}
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	processed := make(chan struct{}, 1)
	settled := 0
	executions := 0
	var execute usecase.Work = func(context.Context) error {
		executions++
		return nil
	}
	consumer := startBatchConsumer(t, ctx, client, usecase.NewTaskUseCase(repo, nil, time.Now), execute, retryDelays, func(ctx context.Context) error {
		if _, err := repo.Get(ctx, created.ID); !errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("deleted delivery still exists: %v", err)
		}
		settled++
		processed <- struct{}{}
		return nil
	})
	select {
	case <-processed:
	case <-time.After(time.Second):
		t.Fatal("deleted delivery did not finish")
	}
	waitForQueueDrained(t, ctx, s, s.queue("integration.deleted.tasks"))
	if err := consumer.Stop(); err != nil {
		t.Fatalf("deleted-message consumer=%v", err)
	}
	if settled != 1 || executions != 0 {
		t.Fatalf("deleted delivery batches=%d executions=%d", settled, executions)
	}
	assertQueueCount(t, s.ctx, s, s.queue("integration.deleted.tasks"), 0)
}
