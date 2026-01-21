// Package pipeline orchestrates the log collection flow.
package pipeline

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/emitter"
	"github.com/GabrielNunesIT/log-collector/internal/ingestor"
	"github.com/GabrielNunesIT/log-collector/internal/model"
	"github.com/GabrielNunesIT/log-collector/internal/processor"
)

// managedIngestor wraps an ingestor with its lifecycle management.
type managedIngestor struct {
	ingestor  ingestor.Ingestor
	processor *processor.Chain
	cancel    context.CancelFunc
	done      chan struct{}
}

// managedEmitter wraps an emitter with its lifecycle management.
type managedEmitter struct {
	emitter emitter.Emitter
	cancel  context.CancelFunc
	done    chan struct{}
}

// Pipeline coordinates ingestors, processors, and emitters.
type Pipeline struct {
	cfg    *config.Config
	logger logger.ILogger
	mu     sync.RWMutex

	ingestors map[string]*managedIngestor
	emitters  map[string]*managedEmitter

	// fanoutChan receives processed entries for distribution to emitters.
	fanoutChan chan *model.LogEntry

	// runCtx is the main run context
	runCtx    context.Context
	runCancel context.CancelFunc
}

// New creates a new pipeline from configuration.
func New(cfg *config.Config, log logger.ILogger) (*Pipeline, error) {
	p := &Pipeline{
		cfg:        cfg,
		logger:     log.SubLogger("Pipeline"),
		ingestors:  make(map[string]*managedIngestor),
		emitters:   make(map[string]*managedEmitter),
		fanoutChan: make(chan *model.LogEntry, cfg.Pipeline.BufferSize),
	}

	if err := p.buildIngestors(); err != nil {
		return nil, fmt.Errorf("building ingestors: %w", err)
	}

	if err := p.buildEmitters(); err != nil {
		return nil, fmt.Errorf("building emitters: %w", err)
	}

	return p, nil
}

// buildIngestors creates enabled ingestors with their processor chains.
func (p *Pipeline) buildIngestors() error {
	// File ingestor
	if p.cfg.Ingestors.File.Enabled {
		chain, err := p.buildProcessorChain(p.cfg.Ingestors.File.Processor)
		if err != nil {
			return err
		}
		p.ingestors["file"] = &managedIngestor{
			ingestor:  ingestor.NewFileIngestor(p.cfg.Ingestors.File, p.logger),
			processor: chain,
			done:      make(chan struct{}),
		}
	}

	// Syslog ingestor
	if p.cfg.Ingestors.Syslog.Enabled {
		chain, err := p.buildProcessorChain(p.cfg.Ingestors.Syslog.Processor)
		if err != nil {
			return err
		}
		p.ingestors["syslog"] = &managedIngestor{
			ingestor:  ingestor.NewSyslogIngestor(p.cfg.Ingestors.Syslog, p.logger),
			processor: chain,
			done:      make(chan struct{}),
		}
	}

	// Journal ingestor
	if p.cfg.Ingestors.Journal.Enabled {
		chain, err := p.buildProcessorChain(p.cfg.Ingestors.Journal.Processor)
		if err != nil {
			return err
		}
		p.ingestors["journal"] = &managedIngestor{
			ingestor:  ingestor.NewJournalIngestor(p.cfg.Ingestors.Journal, p.logger),
			processor: chain,
			done:      make(chan struct{}),
		}
	}

	// Stdin ingestor
	if p.cfg.Ingestors.Stdin.Enabled {
		chain, err := p.buildProcessorChain(p.cfg.Ingestors.Stdin.Processor)
		if err != nil {
			return err
		}
		p.ingestors["stdin"] = &managedIngestor{
			ingestor:  ingestor.NewStdinIngestor(p.cfg.Ingestors.Stdin, p.logger),
			processor: chain,
			done:      make(chan struct{}),
		}
	}

	if len(p.ingestors) == 0 {
		return fmt.Errorf("no ingestors enabled")
	}

	p.logger.Debugf("built %d ingestors", len(p.ingestors))
	return nil
}

// buildProcessorChain creates a processor chain from config.
func (p *Pipeline) buildProcessorChain(cfg config.ProcessorConfig) (*processor.Chain, error) {
	chain := processor.NewChain()

	if cfg.Parser.Enabled {
		parser, err := processor.NewParser(cfg.Parser)
		if err != nil {
			return nil, fmt.Errorf("creating parser: %w", err)
		}
		chain.Add(parser)
	}

	if cfg.Enricher.Enabled {
		enricher := processor.NewEnricher(cfg.Enricher)
		chain.Add(enricher)
	}

	return chain, nil
}

// buildEmitters creates enabled emitters.
func (p *Pipeline) buildEmitters() error {
	if p.cfg.Emitters.Stdout.Enabled {
		p.emitters["stdout"] = &managedEmitter{
			emitter: emitter.NewStdoutEmitter(p.cfg.Emitters.Stdout, p.logger),
			done:    make(chan struct{}),
		}
	}

	if p.cfg.Emitters.File.Enabled {
		p.emitters["file"] = &managedEmitter{
			emitter: emitter.NewFileEmitter(p.cfg.Emitters.File, p.logger),
			done:    make(chan struct{}),
		}
	}

	if p.cfg.Emitters.Elasticsearch.Enabled {
		p.emitters["elasticsearch"] = &managedEmitter{
			emitter: emitter.NewElasticsearchEmitter(p.cfg.Emitters.Elasticsearch, p.logger),
			done:    make(chan struct{}),
		}
	}

	if p.cfg.Emitters.Loki.Enabled {
		p.emitters["loki"] = &managedEmitter{
			emitter: emitter.NewLokiEmitter(p.cfg.Emitters.Loki, p.logger),
			done:    make(chan struct{}),
		}
	}

	if p.cfg.Emitters.VictoriaLogs.Enabled {
		p.emitters["victorialogs"] = &managedEmitter{
			emitter: emitter.NewVictoriaLogsEmitter(p.cfg.Emitters.VictoriaLogs, p.logger),
			done:    make(chan struct{}),
		}
	}

	if len(p.emitters) == 0 {
		return fmt.Errorf("no emitters enabled")
	}

	p.logger.Debugf("built %d emitters", len(p.emitters))
	return nil
}

// Run starts the pipeline and blocks until context is cancelled.
func (p *Pipeline) Run(ctx context.Context) error {
	p.runCtx, p.runCancel = context.WithCancel(ctx)

	// Start all emitters
	p.mu.RLock()
	for name, me := range p.emitters {
		if err := me.emitter.Start(p.runCtx); err != nil {
			p.mu.RUnlock()
			return fmt.Errorf("starting emitter %s: %w", name, err)
		}
		p.logger.Debugf("started emitter: %s", name)
	}
	p.mu.RUnlock()

	g, gCtx := errgroup.WithContext(p.runCtx)

	// Start fanout goroutine
	g.Go(func() error {
		return p.runFanout(gCtx)
	})

	// Start each ingestor with its own context
	p.mu.Lock()
	for name, mi := range p.ingestors {
		mi := mi
		name := name
		ingestorCtx, cancel := context.WithCancel(gCtx)
		mi.cancel = cancel

		g.Go(func() error {
			defer close(mi.done)
			p.logger.Debugf("started ingestor: %s", name)
			return p.runIngestorPipeline(ingestorCtx, name, mi)
		})
	}
	p.mu.Unlock()

	// Wait for all goroutines to complete
	err := g.Wait()

	// Graceful shutdown
	p.shutdown()

	return err
}

// shutdown gracefully stops all emitters.
func (p *Pipeline) shutdown() {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), p.cfg.Pipeline.ShutdownTimeout)
	defer cancel()

	p.mu.RLock()
	defer p.mu.RUnlock()

	for name, me := range p.emitters {
		if stopErr := me.emitter.Stop(shutdownCtx); stopErr != nil {
			p.logger.Warningf("emitter stop error: name=%s, error=%v", name, stopErr)
		}
	}
	p.logger.Debug("all emitters stopped")
}

// runIngestorPipeline runs a single ingestor and its processor chain.
func (p *Pipeline) runIngestorPipeline(ctx context.Context, name string, mi *managedIngestor) error {
	rawChan := make(chan *model.LogEntry, p.cfg.Pipeline.BufferSize)

	var wg sync.WaitGroup

	// Start processor goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for entry := range rawChan {
			if err := mi.processor.Process(ctx, entry); err != nil {
				p.logger.Debugf("processor error: ingestor=%s, error=%v", name, err)
				continue
			}

			select {
			case p.fanoutChan <- entry:
			case <-ctx.Done():
				return
			default:
				if p.cfg.Pipeline.DropOnFullBuffer {
					p.logger.Debug("buffer full, dropping entry")
					continue
				}
				select {
				case p.fanoutChan <- entry:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	// Run the ingestor
	err := mi.ingestor.Start(ctx, rawChan)

	// Wait for processor to drain
	wg.Wait()

	p.logger.Debugf("ingestor stopped: name=%s", name)
	return err
}

// runFanout distributes entries to all emitters.
func (p *Pipeline) runFanout(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			// Drain remaining entries
			for {
				select {
				case entry := <-p.fanoutChan:
					p.emitToAll(ctx, entry)
				default:
					return ctx.Err()
				}
			}
		case entry, ok := <-p.fanoutChan:
			if !ok {
				return nil
			}
			p.emitToAll(ctx, entry)
		}
	}
}

// emitToAll sends an entry to all enabled emitters.
func (p *Pipeline) emitToAll(ctx context.Context, entry *model.LogEntry) {
	p.mu.RLock()
	emitters := make([]emitter.Emitter, 0, len(p.emitters))
	for _, me := range p.emitters {
		emitters = append(emitters, me.emitter)
	}
	p.mu.RUnlock()

	var wg sync.WaitGroup
	for _, e := range emitters {
		e := e
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.Emit(ctx, entry.Clone()); err != nil {
				p.logger.Debugf("emit error: emitter=%s, error=%v", e.Name(), err)
			}
		}()
	}
	wg.Wait()
}

// Reconfigure applies a new configuration, adding/removing components as needed.
func (p *Pipeline) Reconfigure(newCfg *config.Config) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldCfg := p.cfg
	p.cfg = newCfg

	// Handle ingestor changes
	if err := p.reconfigureIngestors(oldCfg, newCfg); err != nil {
		return fmt.Errorf("reconfiguring ingestors: %w", err)
	}

	// Handle emitter changes
	if err := p.reconfigureEmitters(oldCfg, newCfg); err != nil {
		return fmt.Errorf("reconfiguring emitters: %w", err)
	}

	p.logger.Infof("configuration applied: ingestors=%d, emitters=%d",
		len(p.ingestors), len(p.emitters))

	return nil
}

// reconfigureIngestors handles adding/removing ingestors.
func (p *Pipeline) reconfigureIngestors(oldCfg, newCfg *config.Config) error {
	type ingestorDef struct {
		name    string
		enabled bool
		cfg     any
		procCfg config.ProcessorConfig
	}

	getIngestors := func(cfg *config.Config) map[string]ingestorDef {
		return map[string]ingestorDef{
			"file":    {"file", cfg.Ingestors.File.Enabled, cfg.Ingestors.File, cfg.Ingestors.File.Processor},
			"syslog":  {"syslog", cfg.Ingestors.Syslog.Enabled, cfg.Ingestors.Syslog, cfg.Ingestors.Syslog.Processor},
			"journal": {"journal", cfg.Ingestors.Journal.Enabled, cfg.Ingestors.Journal, cfg.Ingestors.Journal.Processor},
			"stdin":   {"stdin", cfg.Ingestors.Stdin.Enabled, cfg.Ingestors.Stdin, cfg.Ingestors.Stdin.Processor},
		}
	}

	oldIngestors := getIngestors(oldCfg)
	newIngestors := getIngestors(newCfg)

	// Remove disabled ingestors
	for name, oldDef := range oldIngestors {
		newDef := newIngestors[name]
		if oldDef.enabled && !newDef.enabled {
			if err := p.removeIngestor(name); err != nil {
				return err
			}
		}
	}

	// Add newly enabled ingestors
	for name, newDef := range newIngestors {
		oldDef := oldIngestors[name]
		if newDef.enabled && !oldDef.enabled {
			if err := p.addIngestor(name, newCfg); err != nil {
				return err
			}
		}
	}

	return nil
}

// addIngestor adds a new ingestor at runtime.
func (p *Pipeline) addIngestor(name string, cfg *config.Config) error {
	var ing ingestor.Ingestor
	var procCfg config.ProcessorConfig

	switch name {
	case "file":
		ing = ingestor.NewFileIngestor(cfg.Ingestors.File, p.logger)
		procCfg = cfg.Ingestors.File.Processor
	case "syslog":
		ing = ingestor.NewSyslogIngestor(cfg.Ingestors.Syslog, p.logger)
		procCfg = cfg.Ingestors.Syslog.Processor
	case "journal":
		ing = ingestor.NewJournalIngestor(cfg.Ingestors.Journal, p.logger)
		procCfg = cfg.Ingestors.Journal.Processor
	case "stdin":
		ing = ingestor.NewStdinIngestor(cfg.Ingestors.Stdin, p.logger)
		procCfg = cfg.Ingestors.Stdin.Processor
	default:
		return fmt.Errorf("unknown ingestor: %s", name)
	}

	chain, err := p.buildProcessorChain(procCfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(p.runCtx)
	mi := &managedIngestor{
		ingestor:  ing,
		processor: chain,
		cancel:    cancel,
		done:      make(chan struct{}),
	}

	p.ingestors[name] = mi

	// Start the ingestor in background
	go func() {
		defer close(mi.done)
		if err := p.runIngestorPipeline(ctx, name, mi); err != nil {
			p.logger.Warningf("ingestor error: name=%s, error=%v", name, err)
		}
	}()

	p.logger.Infof("ingestor added: %s", name)
	return nil
}

// removeIngestor stops and removes an ingestor.
func (p *Pipeline) removeIngestor(name string) error {
	mi, ok := p.ingestors[name]
	if !ok {
		return nil
	}

	// Cancel the ingestor context
	if mi.cancel != nil {
		mi.cancel()
	}

	// Wait for it to stop
	<-mi.done

	delete(p.ingestors, name)
	p.logger.Infof("ingestor removed: %s", name)
	return nil
}

// reconfigureEmitters handles adding/removing emitters.
func (p *Pipeline) reconfigureEmitters(oldCfg, newCfg *config.Config) error {
	type emitterDef struct {
		name    string
		enabled bool
	}

	getEmitters := func(cfg *config.Config) map[string]emitterDef {
		return map[string]emitterDef{
			"stdout":        {"stdout", cfg.Emitters.Stdout.Enabled},
			"file":          {"file", cfg.Emitters.File.Enabled},
			"elasticsearch": {"elasticsearch", cfg.Emitters.Elasticsearch.Enabled},
			"loki":          {"loki", cfg.Emitters.Loki.Enabled},
			"victorialogs":  {"victorialogs", cfg.Emitters.VictoriaLogs.Enabled},
		}
	}

	oldEmitters := getEmitters(oldCfg)
	newEmitters := getEmitters(newCfg)

	// Remove disabled emitters
	for name, oldDef := range oldEmitters {
		newDef := newEmitters[name]
		if oldDef.enabled && !newDef.enabled {
			if err := p.removeEmitter(name); err != nil {
				return err
			}
		}
	}

	// Add newly enabled emitters
	for name, newDef := range newEmitters {
		oldDef := oldEmitters[name]
		if newDef.enabled && !oldDef.enabled {
			if err := p.addEmitter(name, newCfg); err != nil {
				return err
			}
		}
	}

	return nil
}

// addEmitter adds a new emitter at runtime.
func (p *Pipeline) addEmitter(name string, cfg *config.Config) error {
	var em emitter.Emitter

	switch name {
	case "stdout":
		em = emitter.NewStdoutEmitter(cfg.Emitters.Stdout, p.logger)
	case "file":
		em = emitter.NewFileEmitter(cfg.Emitters.File, p.logger)
	case "elasticsearch":
		em = emitter.NewElasticsearchEmitter(cfg.Emitters.Elasticsearch, p.logger)
	case "loki":
		em = emitter.NewLokiEmitter(cfg.Emitters.Loki, p.logger)
	case "victorialogs":
		em = emitter.NewVictoriaLogsEmitter(cfg.Emitters.VictoriaLogs, p.logger)
	default:
		return fmt.Errorf("unknown emitter: %s", name)
	}

	if err := em.Start(p.runCtx); err != nil {
		return fmt.Errorf("starting emitter %s: %w", name, err)
	}

	p.emitters[name] = &managedEmitter{
		emitter: em,
		done:    make(chan struct{}),
	}

	p.logger.Infof("emitter added: %s", name)
	return nil
}

// removeEmitter stops and removes an emitter.
func (p *Pipeline) removeEmitter(name string) error {
	me, ok := p.emitters[name]
	if !ok {
		return nil
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), p.cfg.Pipeline.ShutdownTimeout)
	defer cancel()

	if err := me.emitter.Stop(shutdownCtx); err != nil {
		p.logger.Warningf("emitter stop error: name=%s, error=%v", name, err)
	}

	delete(p.emitters, name)
	p.logger.Infof("emitter removed: %s", name)
	return nil
}

// IngestorCount returns the number of enabled ingestors.
func (p *Pipeline) IngestorCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.ingestors)
}

// EmitterCount returns the number of enabled emitters.
func (p *Pipeline) EmitterCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.emitters)
}
