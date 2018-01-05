package gexe

// Executor executes the submitted tasks
type Executor interface {
	Execute(r Runnable)
}
