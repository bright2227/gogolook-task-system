package processor

import (
	"context"
	"time"

	"github.com/bright2227/vsx-task-system/internal/task"
)

type SimulatedProcessor struct{}

func (SimulatedProcessor) Execute(ctx context.Context, _ task.Payload) (string, error) {
	t := time.NewTimer(3 * time.Second)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-t.C:
		return "processed", nil
	}
}
