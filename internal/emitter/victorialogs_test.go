package emitter

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

func TestVictoriaLogsEmitter_Name(t *testing.T) {
	cfg := config.VictoriaLogsEmitterConfig{}
	emitter := NewVictoriaLogsEmitter(cfg, testLogger())

	if emitter.Name() != "victorialogs" {
		t.Errorf("expected name 'victorialogs', got %q", emitter.Name())
	}
}

func TestVictoriaLogsEmitter_Emit_BatchFlush(t *testing.T) {
	var capturedReq *http.Request
	var capturedBody []byte

	mock := &mockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			body, _ := io.ReadAll(req.Body)
			capturedBody = body
			return &http.Response{
				StatusCode: 204,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		},
	}

	cfg := config.VictoriaLogsEmitterConfig{
		URL:           "http://localhost:9428",
		BatchSize:     1, // Flush immediately
		FlushInterval: time.Hour,
	}

	emitter := NewVictoriaLogsEmitter(cfg, testLogger(), WithVictoriaLogsHTTPClient(mock))

	entry := &model.LogEntry{
		Timestamp: time.Date(2026, 1, 18, 12, 0, 0, 0, time.UTC),
		Source:    "test-source",
		Raw:       []byte("test message"),
		Parsed:    map[string]any{"level": "info"},
		Metadata:  map[string]string{"host": "localhost"},
	}

	err := emitter.Emit(context.Background(), entry)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	if capturedReq == nil {
		t.Fatal("expected HTTP request to be made")
	}

	// Verify URL
	if capturedReq.URL.Path != "/insert/jsonline" {
		t.Errorf("unexpected path: %s", capturedReq.URL.Path)
	}

	// Verify content type
	if capturedReq.Header.Get("Content-Type") != "application/x-ndjson" {
		t.Errorf("unexpected content-type: %s", capturedReq.Header.Get("Content-Type"))
	}

	// Verify body contains expected fields
	bodyStr := string(capturedBody)
	if !strings.Contains(bodyStr, `"_source":"test-source"`) {
		t.Errorf("body should contain _source field: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `"_msg":"test message"`) {
		t.Errorf("body should contain _msg field: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `"level":"info"`) {
		t.Errorf("body should contain level field: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, `"host":"localhost"`) {
		t.Errorf("body should contain host field: %s", bodyStr)
	}
}

func TestVictoriaLogsEmitter_Emit_HTTPError(t *testing.T) {
	mock := &mockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 503,
				Body:       io.NopCloser(strings.NewReader("service unavailable")),
			}, nil
		},
	}

	cfg := config.VictoriaLogsEmitterConfig{
		URL:           "http://localhost:9428",
		BatchSize:     1,
		FlushInterval: time.Hour,
	}

	emitter := NewVictoriaLogsEmitter(cfg, testLogger(), WithVictoriaLogsHTTPClient(mock))

	entry := &model.LogEntry{
		Timestamp: time.Now(),
		Source:    "test",
		Raw:       []byte("test"),
	}

	err := emitter.Emit(context.Background(), entry)
	if err == nil {
		t.Fatal("expected error for 503 response")
	}

	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error should contain status code: %v", err)
	}
}

func TestVictoriaLogsEmitter_Emit_NoBatchFlush(t *testing.T) {
	callCount := 0
	mock := &mockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			callCount++
			return &http.Response{
				StatusCode: 204,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		},
	}

	cfg := config.VictoriaLogsEmitterConfig{
		URL:           "http://localhost:9428",
		BatchSize:     10, // High batch size - won't flush
		FlushInterval: time.Hour,
	}

	emitter := NewVictoriaLogsEmitter(cfg, testLogger(), WithVictoriaLogsHTTPClient(mock))

	entry := &model.LogEntry{
		Timestamp: time.Now(),
		Source:    "test",
		Raw:       []byte("test"),
	}

	_ = emitter.Emit(context.Background(), entry)

	if callCount != 0 {
		t.Errorf("expected no HTTP call before batch is full, got %d", callCount)
	}
}

func TestVictoriaLogsEmitter_MultipleBatchesSameStream(t *testing.T) {
	callCount := 0
	var capturedBody []byte

	mock := &mockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			callCount++
			body, _ := io.ReadAll(req.Body)
			capturedBody = body
			return &http.Response{
				StatusCode: 204,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		},
	}

	cfg := config.VictoriaLogsEmitterConfig{
		URL:           "http://localhost:9428",
		BatchSize:     3, // Flush after 3 entries
		FlushInterval: time.Hour,
	}

	emitter := NewVictoriaLogsEmitter(cfg, testLogger(), WithVictoriaLogsHTTPClient(mock))

	for i := 0; i < 3; i++ {
		entry := &model.LogEntry{
			Timestamp: time.Now(),
			Source:    "test",
			Raw:       []byte("message"),
		}
		_ = emitter.Emit(context.Background(), entry)
	}

	if callCount != 1 {
		t.Errorf("expected 1 HTTP call after batch full, got %d", callCount)
	}

	// Verify body has 3 lines (jsonline format)
	lines := strings.Split(strings.TrimSpace(string(capturedBody)), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines in body, got %d", len(lines))
	}
}
