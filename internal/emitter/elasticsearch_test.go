package emitter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/GabrielNunesIT/go-libs/logger"
	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
	"github.com/GabrielNunesIT/log-collector/tests/mocks"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// testLogger returns a logger for tests that discards output.
func testLogger() logger.ILogger {
	return logger.NewConsoleLogger(io.Discard)
}

func TestElasticsearchEmitter_Start(t *testing.T) {
	tests := []struct {
		name          string
		cfg           config.ElasticsearchEmitterConfig
		factoryMock   func(*testing.T) IndexerFactory
		expectedError string
	}{
		{
			name: "Success",
			cfg: config.ElasticsearchEmitterConfig{
				Enabled:   true,
				Addresses: []string{"http://localhost:9200"},
				Index:     "test-index",
			},
			factoryMock: func(t *testing.T) IndexerFactory {
				mockIndexer := mocks.NewBulkIndexer(t)
				return func(c config.ElasticsearchEmitterConfig) (esutil.BulkIndexer, error) {
					return mockIndexer, nil
				}
			},
			expectedError: "",
		},
		{
			name: "Factory Error",
			cfg:  config.ElasticsearchEmitterConfig{Enabled: true},
			factoryMock: func(t *testing.T) IndexerFactory {
				return func(c config.ElasticsearchEmitterConfig) (esutil.BulkIndexer, error) {
					return nil, errors.New("factory failure")
				}
			},
			expectedError: "factory failure",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := tt.factoryMock(t)
			e := NewElasticsearchEmitter(tt.cfg, testLogger(), WithIndexerFactory(factory))
			err := e.Start(context.Background())
			if tt.expectedError != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestElasticsearchEmitter_Emit(t *testing.T) {
	entry := &model.LogEntry{
		Timestamp: time.Now(),
		Source:    "test-source",
		Raw:       []byte(`{"foo":"bar"}`),
		Parsed:    map[string]any{"foo": "bar"},
		Metadata:  map[string]string{"host": "test-host"},
	}

	tests := []struct {
		name        string
		cfg         config.ElasticsearchEmitterConfig
		setupMock   func(*mocks.BulkIndexer)
		entry       *model.LogEntry
		expectError bool
	}{
		{
			name: "Success",
			cfg: config.ElasticsearchEmitterConfig{
				Enabled: true,
				Index:   "test-index",
			},
			setupMock: func(m *mocks.BulkIndexer) {
				m.On("Add", mock.Anything, mock.MatchedBy(func(item esutil.BulkIndexerItem) bool {
					return item.Action == "index"
				})).Return(nil).Run(func(args mock.Arguments) {
					item := args.Get(1).(esutil.BulkIndexerItem)
					bodyBytes, _ := io.ReadAll(item.Body)
					var bodyMap map[string]any
					_ = json.Unmarshal(bodyBytes, &bodyMap)
					assert.Equal(t, "test-source", bodyMap["source"])
					assert.Equal(t, "bar", bodyMap["foo"])
					assert.Equal(t, "test-host", bodyMap["host"])
				})
			},
			entry:       entry,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockIndexer := mocks.NewBulkIndexer(t)
			if tt.setupMock != nil {
				tt.setupMock(mockIndexer)
			}

			factory := func(c config.ElasticsearchEmitterConfig) (esutil.BulkIndexer, error) {
				return mockIndexer, nil
			}

			e := NewElasticsearchEmitter(tt.cfg, testLogger(), WithIndexerFactory(factory))
			_ = e.Start(context.Background())

			err := e.Emit(context.Background(), tt.entry)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			mockIndexer.AssertExpectations(t)
		})
	}
}

func TestElasticsearchEmitter_Stop(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"Success"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockIndexer := mocks.NewBulkIndexer(t)
			mockIndexer.On("Close", mock.Anything).Return(nil)

			factory := func(c config.ElasticsearchEmitterConfig) (esutil.BulkIndexer, error) {
				return mockIndexer, nil
			}

			e := NewElasticsearchEmitter(config.ElasticsearchEmitterConfig{Enabled: true}, testLogger(), WithIndexerFactory(factory))
			_ = e.Start(context.Background())

			err := e.Stop(context.Background())
			assert.NoError(t, err)

			mockIndexer.AssertExpectations(t)
		})
	}
}
