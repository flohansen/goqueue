package goqueue

// EnqueueHandler defines the function signature for handling job enqueuing.
type EnqueueHandler func(ctx JobEnqueueContext) error

// ProcessHandler defines the function signature for handling job processing.
type ProcessHandler func(ctx JobProcessContext) error

// Middleware allows wrapping EnqueueHandler and ProcessHandler with custom
// logic.
type Middleware interface {
	// Enqueue is called when a job is being enqueued.
	Enqueue(next EnqueueHandler) EnqueueHandler

	// Process is called when a job is being processed.
	Process(next ProcessHandler) ProcessHandler
}

// UnimplementedMiddleware can be embedded to have a
// Middleware implementation with no methods.
type UnimplementedMiddleware struct {
	Middleware
}

// Enqueue returns the next EnqueueHandler unchanged.
func (m *UnimplementedMiddleware) Enqueue(next EnqueueHandler) EnqueueHandler {
	return next
}

// Process returns the next ProcessHandler unchanged.
func (m *UnimplementedMiddleware) Process(next ProcessHandler) ProcessHandler {
	return next
}
