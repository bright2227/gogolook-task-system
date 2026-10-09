// Command load creates tasks concurrently and polls each through its terminal state.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type taskResponse struct {
	ID     string  `json:"id"`
	Status string  `json:"status"`
	Error  *string `json:"error"`
}

func main() {
	base := flag.String("url", "http://localhost:8080", "task API base URL")
	duration := flag.Duration("duration", 30*time.Second, "load generation duration")
	concurrency := flag.Int("concurrency", 8, "concurrent create-and-poll clients")
	poll := flag.Duration("poll", 200*time.Millisecond, "terminal-state polling interval")
	flag.Parse()
	if *duration <= 0 || *concurrency < 1 || *poll <= 0 {
		fmt.Fprintln(os.Stderr, "duration, concurrency, and poll must be positive")
		os.Exit(2)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	var submitted, completed, failed, requestErrors atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				body := []byte(`{"payload":{"operation":"simulated-work"}}`)
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(*base, "/")+"/tasks", bytes.NewReader(body))
				if err != nil {
					return
				}
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					if ctx.Err() == nil {
						requestErrors.Add(1)
					}
					continue
				}
				var item taskResponse
				decodeErr := json.NewDecoder(resp.Body).Decode(&item)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusAccepted || decodeErr != nil || item.ID == "" {
					requestErrors.Add(1)
					continue
				}
				submitted.Add(1)
				for ctx.Err() == nil {
					select {
					case <-ctx.Done():
						return
					case <-time.After(*poll):
					}
					r, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(*base, "/")+"/tasks/"+item.ID, nil)
					if err != nil {
						return
					}
					rr, err := client.Do(r)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						requestErrors.Add(1)
						continue
					}
					var state taskResponse
					e := json.NewDecoder(rr.Body).Decode(&state)
					_ = rr.Body.Close()
					if rr.StatusCode != http.StatusOK || e != nil {
						requestErrors.Add(1)
						continue
					}
					if state.Status == "completed" {
						if state.Error == nil {
							completed.Add(1)
						} else {
							failed.Add(1)
						}
						break
					}
					if state.Status == "canceled" {
						failed.Add(1)
						break
					}
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	fmt.Printf("duration=%.1fs submitted=%d terminal_completed=%d terminal_failed=%d request_errors=%d create_rate=%.2f/s completion_rate=%.2f/s\n", elapsed, submitted.Load(), completed.Load(), failed.Load(), requestErrors.Load(), float64(submitted.Load())/elapsed, float64(completed.Load())/elapsed)
}
