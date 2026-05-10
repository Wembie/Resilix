package resilix

import (
	"context"
	"time"
)

type BeforeExecuteHook func(context.Context, Operation) error
type AfterExecuteHook func(context.Context, Operation, time.Duration, error)

type HookSet struct {
	BeforeExecute []BeforeExecuteHook
	AfterExecute  []AfterExecuteHook
}
