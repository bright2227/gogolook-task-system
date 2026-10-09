package processor

import (
	"context"
	"time"

	"github.com/bright2227/gogolook-task-system/internal/domain"
)

type SimulatedProcessor struct{}

func (SimulatedProcessor) Execute(ctx context.Context, _ domain.ID, _ domain.Payload) (string, error) {
	t := time.NewTimer(3 * time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-t.C:
		return "processed", nil
	}
}
