package resilix

import (
	"errors"
	"fmt"

	"github.com/Wembie/Resilix/sdk/go/internal/contract"
)

var (
	ErrInvalidConfiguration = errors.New("resilix: invalid configuration")
	ErrBackpressure         = errors.New("resilix: backpressure limit exceeded")
	ErrKeyNotFound          = contract.ErrKeyNotFound
)

type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("resilix: validation failed for %s: %s", e.Field, e.Message)
}

type OperationError struct {
	Operation string
	Keys      []string
	Cause     error
}

func (e OperationError) Error() string {
	return fmt.Sprintf("resilix: operation %s failed: %v", e.Operation, e.Cause)
}

func (e OperationError) Unwrap() error {
	return e.Cause
}

func wrapOperationError(name string, keys []string, err error) error {
	if err == nil {
		return nil
	}

	var operationError OperationError
	if errors.As(err, &operationError) {
		return err
	}

	return OperationError{
		Operation: name,
		Keys:      keys,
		Cause:     err,
	}
}
