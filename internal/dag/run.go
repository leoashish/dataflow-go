package dag

import (
	"context"

	"dataflow/internal/todo"
)

// Run is documented on Graph. (It lives in its own file so you can build New/Order/Levels first and Run afterwards.)
func (g *Graph) Run(ctx context.Context, maxParallel int) (map[string]Result, error) {
	return nil, todo.NotImplemented("Ch21: Run")
}
