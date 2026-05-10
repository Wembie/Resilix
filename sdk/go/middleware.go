package resilix

import "context"

type Operation struct {
	Name       string
	Kind       string
	Keys       []string
	Attributes map[string]string
}

type Handler func(context.Context, Operation) (any, error)

type Middleware func(Handler) Handler

func chain(middlewares []Middleware, terminal Handler) Handler {
	if len(middlewares) == 0 {
		return terminal
	}

	wrapped := terminal
	for i := len(middlewares) - 1; i >= 0; i-- {
		wrapped = middlewares[i](wrapped)
	}
	return wrapped
}

type ReleaseFunc func()

type AdmissionController interface {
	Acquire(context.Context, Operation) (ReleaseFunc, error)
}
