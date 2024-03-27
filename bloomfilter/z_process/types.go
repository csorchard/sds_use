package process

// Process contains context used in query execution
// one or more pipeline will be generated for one query,
// and one pipeline has one process instance.
type Process struct {
}

func (p Process) Mp() any {
	return nil
}
