// Package processor defines the interface and implementations for log transformation.
package processor

import (
	"context"

	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// Processor defines the contract for log transformations.
// Processors modify LogEntry in place, enriching or parsing the data.
type Processor interface {
	// Process transforms a LogEntry in place.
	// Returns an error if processing fails critically (entry should be dropped).
	Process(ctx context.Context, entry *model.LogEntry) error

	// Name returns a unique identifier for this processor.
	Name() string
}

// Chain composes multiple processors into a sequential pipeline.
type Chain struct {
	processors []Processor
}

// NewChain creates a new processor chain.
func NewChain(processors ...Processor) *Chain {
	return &Chain{processors: processors}
}

// Process applies all processors in sequence.
func (c *Chain) Process(ctx context.Context, entry *model.LogEntry) error {
	for _, p := range c.processors {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := p.Process(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

// Name returns the chain identifier.
func (c *Chain) Name() string {
	return "chain"
}

// Add appends a processor to the chain.
func (c *Chain) Add(p Processor) {
	c.processors = append(c.processors, p)
}

// Len returns the number of processors in the chain.
func (c *Chain) Len() int {
	return len(c.processors)
}
