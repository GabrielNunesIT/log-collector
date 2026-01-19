package emitter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/model"
	"github.com/user/log-collector/tests/mocks"
)

func TestFileEmitter_Start(t *testing.T) {
	cfg := config.FileEmitterConfig{
		Enabled: true,
		Path:    "/tmp/test.log",
	}

	t.Run("success", func(t *testing.T) {
		mockWriter := mocks.NewWriteCloser(t)
		factory := func(c config.FileEmitterConfig) (io.WriteCloser, error) {
			return mockWriter, nil
		}

		e := NewFileEmitter(cfg, WithWriterFactory(factory))
		err := e.Start(context.Background())
		assert.NoError(t, err)
	})

	t.Run("factory error", func(t *testing.T) {
		factory := func(c config.FileEmitterConfig) (io.WriteCloser, error) {
			return nil, errors.New("factory error")
		}

		e := NewFileEmitter(cfg, WithWriterFactory(factory))
		err := e.Start(context.Background())
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "factory error")
	})
}

func TestFileEmitter_Emit(t *testing.T) {
	cfg := config.FileEmitterConfig{Enabled: true}
	entry := &model.LogEntry{
		Timestamp: time.Now(),
		Source:    "test-source",
		Raw:       []byte(`{"foo":"bar"}`),
		Parsed:    map[string]any{"foo": "bar"},
		Metadata:  map[string]string{"host": "localhost"},
	}

	t.Run("success", func(t *testing.T) {
		mockWriter := mocks.NewWriteCloser(t)
		factory := func(c config.FileEmitterConfig) (io.WriteCloser, error) {
			return mockWriter, nil
		}

		mockWriter.On("Write", mock.MatchedBy(func(p []byte) bool {
			// Verify content
			var output map[string]any
			err := json.Unmarshal(p, &output)
			return err == nil &&
				output["source"] == "test-source" &&
				output["foo"] == "bar" &&
				output["host"] == "localhost"
		})).Return(len(entry.Raw), nil)

		e := NewFileEmitter(cfg, WithWriterFactory(factory))
		_ = e.Start(context.Background())

		err := e.Emit(context.Background(), entry)
		assert.NoError(t, err)
		mockWriter.AssertExpectations(t)
	})

	t.Run("write error", func(t *testing.T) {
		mockWriter := mocks.NewWriteCloser(t)
		factory := func(c config.FileEmitterConfig) (io.WriteCloser, error) {
			return mockWriter, nil
		}

		mockWriter.On("Write", mock.Anything).Return(0, errors.New("disk full"))

		e := NewFileEmitter(cfg, WithWriterFactory(factory))
		_ = e.Start(context.Background())

		err := e.Emit(context.Background(), entry)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "disk full")
	})
}

func TestFileEmitter_Stop(t *testing.T) {
	mockWriter := mocks.NewWriteCloser(t)
	factory := func(c config.FileEmitterConfig) (io.WriteCloser, error) {
		return mockWriter, nil
	}
	
	mockWriter.On("Close").Return(nil)

	e := NewFileEmitter(config.FileEmitterConfig{}, WithWriterFactory(factory))
	_ = e.Start(context.Background())

	err := e.Stop(context.Background())
	assert.NoError(t, err)
	mockWriter.AssertExpectations(t)
}
