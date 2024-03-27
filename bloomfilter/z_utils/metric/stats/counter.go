package stats

import "sync/atomic"

// Counter represents a combination of global & current_window counter.
type Counter struct {
	window atomic.Int64
	global atomic.Int64
}

// Add adds to global and window counter
func (c *Counter) Add(delta int64) (new int64) {
	c.window.Add(delta)
	return c.global.Add(delta)
}
