package process

import "context"

// Process contains context used in query execution
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
	Ctx context.Context
}

func (p Process) Mp() any {
	return nil
}
