//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	httpapi "github.com/bright2227/gogolook-task-system/internal/delivery/http"
	"github.com/bright2227/gogolook-task-system/internal/usecase"
)

func TestTaskListKeysetPaginationPostgresAndHTTP(t *testing.T) {
	s := newPostgresStack(t, true)
	const count = 5
	fixedTime := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	now := fixedTime
	svc := usecase.NewTaskUseCase(s.repo, sweeperOnly, func() time.Time { return now })
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		created := submitTask(t, s.ctx, svc, fmt.Sprintf(`{"item":%d}`, i))
		ids = append(ids, string(created.ID))
		if !created.CreatedAt.Equal(fixedTime) {
			t.Fatalf("task timestamp=%s, want fixed timestamp %s", created.CreatedAt, fixedTime)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(ids)))

	direct, more, err := svc.List(s.ctx, 2, time.Time{}, "")
	if err != nil {
		t.Fatalf("use case keyset list: %v", err)
	}
	if !more || len(direct) != 2 || string(direct[0].ID) != ids[0] || string(direct[1].ID) != ids[1] {
		t.Fatalf("use case first page=(%v, hasMore=%v), want IDs=%v", direct, more, ids[:2])
	}

	server := httptest.NewServer(httpapi.NewServer(svc, func(ctx context.Context) error { return s.db.PingContext(ctx) }))
	defer server.Close()
	type item struct {
		ID        string    `json:"id"`
		CreatedAt time.Time `json:"created_time"`
	}
	type page struct {
		Items      []item  `json:"items"`
		NextCursor *string `json:"next_cursor"`
	}
	getPage := func(path string, status int) page {
		t.Helper()
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != status {
			t.Fatalf("GET %s status=%d, want %d", path, resp.StatusCode, status)
		}
		var result page
		if status == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				t.Fatalf("decode GET %s: %v", path, err)
			}
		}
		return result
	}
	pagePath := func(cursor *string) string {
		values := url.Values{"page_size": []string{"2"}}
		if cursor != nil {
			values.Set("cursor", *cursor)
		}
		return "/tasks?" + values.Encode()
	}

	first := getPage(pagePath(nil), http.StatusOK)
	if len(first.Items) != 2 || first.NextCursor == nil || *first.NextCursor == "" {
		t.Fatalf("first page=%+v, want two items and a cursor", first)
	}
	for i, it := range first.Items {
		if it.ID != ids[i] || !it.CreatedAt.Equal(fixedTime) {
			t.Fatalf("first page[%d]=%+v, want ID=%s timestamp=%s", i, it, ids[i], fixedTime)
		}
	}

	// A newer insert between requests sorts before the cursor. It must not
	// shift or duplicate any task in the traversal already in progress.
	now = fixedTime.Add(time.Second)
	inserted := submitTask(t, s.ctx, svc, `{"item":"inserted-between-pages"}`)
	if !inserted.CreatedAt.After(fixedTime) {
		t.Fatalf("interleaved task timestamp=%s, want after %s", inserted.CreatedAt, fixedTime)
	}

	second := getPage(pagePath(first.NextCursor), http.StatusOK)
	if len(second.Items) != 2 || second.NextCursor == nil || *second.NextCursor == "" {
		t.Fatalf("second page=%+v, want two items and a cursor", second)
	}
	third := getPage(pagePath(second.NextCursor), http.StatusOK)
	if len(third.Items) != 1 || third.NextCursor != nil {
		t.Fatalf("third page=%+v, want one item and no cursor", third)
	}
	seen := make(map[string]bool, count)
	var traversed []string
	for _, pg := range []page{first, second, third} {
		for _, it := range pg.Items {
			if seen[it.ID] {
				t.Fatalf("task %s appeared more than once across pages", it.ID)
			}
			seen[it.ID] = true
			traversed = append(traversed, it.ID)
		}
	}
	if fmt.Sprint(traversed) != fmt.Sprint(ids) {
		t.Fatalf("keyset traversal IDs=%v, want original tasks in created_at DESC, id DESC order %v", traversed, ids)
	}

	defaultPage := getPage("/tasks", http.StatusOK)
	if len(defaultPage.Items) != count+1 {
		t.Fatalf("default page items=%d, want %d", len(defaultPage.Items), count+1)
	}
	for _, query := range []string{
		"page_size=abc", "page_size=0", "page_size=-1", "page_size=101",
		"cursor=bad-cursor", "limit=2", "offset=0",
	} {
		getPage("/tasks?"+query, http.StatusBadRequest)
	}
}
