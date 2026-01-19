package emitter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// mockHTTPClient implements HTTPDoer for testing.
type mockHTTPClient struct {
	DoFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.DoFunc(req)
}

func TestLokiEmitter_Name(t *testing.T) {
	cfg := config.LokiEmitterConfig{}
	emitter := NewLokiEmitter(cfg)

	if emitter.Name() != "loki" {
		t.Errorf("expected name 'loki', got %q", emitter.Name())
	}
}

func TestLokiEmitter_Emit_BatchFlush(t *testing.T) {
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

	cfg := config.LokiEmitterConfig{
		URL:           "http://localhost:3100",
		BatchSize:     1, // Flush immediately on first entry
		FlushInterval: time.Hour,
		Labels:        map[string]string{"app": "test"},
	}

	emitter := NewLokiEmitter(cfg, WithLokiHTTPClient(mock))

	entry := &model.LogEntry{
		Timestamp: time.Date(2026, 1, 18, 12, 0, 0, 0, time.UTC),
		Source:    "test-source",
		Raw:       []byte("test message"),
		Parsed:    map[string]any{},
		Metadata:  map[string]string{},
	}

	err := emitter.Emit(context.Background(), entry)
	if err != nil {
		t.Fatalf("Emit failed: %v", err)
	}

	if capturedReq == nil {
		t.Fatal("expected HTTP request to be made")
	}

	// Verify URL
	if capturedReq.URL.Path != "/loki/api/v1/push" {
		t.Errorf("unexpected path: %s", capturedReq.URL.Path)
	}

	// Verify content type
	if capturedReq.Header.Get("Content-Type") != "application/json" {
		t.Errorf("unexpected content-type: %s", capturedReq.Header.Get("Content-Type"))
	}

	// Verify body structure
	var pushReq lokiPushRequest
	if err := json.Unmarshal(capturedBody, &pushReq); err != nil {
		t.Fatalf("failed to parse request body: %v", err)
	}

	if len(pushReq.Streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(pushReq.Streams))
	}

	stream := pushReq.Streams[0]
	if stream.Stream["app"] != "test" {
		t.Errorf("expected app=test label, got %v", stream.Stream)
	}
	if stream.Stream["source"] != "test-source" {
		t.Errorf("expected source=test-source label, got %v", stream.Stream)
	}
	if len(stream.Values) != 1 {
		t.Fatalf("expected 1 value, got %d", len(stream.Values))
	}
	if stream.Values[0][1] != "test message" {
		t.Errorf("expected message 'test message', got %q", stream.Values[0][1])
	}
}

func TestLokiEmitter_Emit_TenantHeader(t *testing.T) {
	var capturedReq *http.Request

	mock := &mockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{
				StatusCode: 204,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		},
	}

	cfg := config.LokiEmitterConfig{
		URL:           "http://localhost:3100",
		BatchSize:     1,
		FlushInterval: time.Hour,
		TenantID:      "my-tenant",
	}

	emitter := NewLokiEmitter(cfg, WithLokiHTTPClient(mock))

	entry := &model.LogEntry{
		Timestamp: time.Now(),
		Source:    "test",
		Raw:       []byte("test"),
	}

	_ = emitter.Emit(context.Background(), entry)

	if capturedReq.Header.Get("X-Scope-OrgID") != "my-tenant" {
		t.Errorf("expected X-Scope-OrgID header, got %q", capturedReq.Header.Get("X-Scope-OrgID"))
	}
}

func TestLokiEmitter_Emit_HTTPError(t *testing.T) {
	mock := &mockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: 500,
				Body:       io.NopCloser(strings.NewReader("internal error")),
			}, nil
		},
	}

	cfg := config.LokiEmitterConfig{
		URL:           "http://localhost:3100",
		BatchSize:     1,
		FlushInterval: time.Hour,
	}

	emitter := NewLokiEmitter(cfg, WithLokiHTTPClient(mock))

	entry := &model.LogEntry{
		Timestamp: time.Now(),
		Source:    "test",
		Raw:       []byte("test"),
	}

	err := emitter.Emit(context.Background(), entry)
	if err == nil {
		t.Fatal("expected error for 500 response")
	}

	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should contain status code: %v", err)
	}
}

func TestLokiEmitter_Emit_NoBatchFlush(t *testing.T) {
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

	cfg := config.LokiEmitterConfig{
		URL:           "http://localhost:3100",
		BatchSize:     10, // High batch size - won't flush
		FlushInterval: time.Hour,
	}

	emitter := NewLokiEmitter(cfg, WithLokiHTTPClient(mock))

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

func TestLokiEmitter_Emit_ParsedFieldsAsJSON(t *testing.T) {
	var capturedBody []byte

	mock := &mockHTTPClient{
		DoFunc: func(req *http.Request) (*http.Response, error) {
			body, _ := io.ReadAll(req.Body)
			capturedBody = body
			return &http.Response{
				StatusCode: 204,
				Body:       io.NopCloser(strings.NewReader("")),
			}, nil
		},
	}

	cfg := config.LokiEmitterConfig{
		URL:           "http://localhost:3100",
		BatchSize:     1,
		FlushInterval: time.Hour,
	}

	emitter := NewLokiEmitter(cfg, WithLokiHTTPClient(mock))

	entry := &model.LogEntry{
		Timestamp: time.Now(),
		Source:    "test",
		Raw:       []byte("original message"),
		Parsed:    map[string]any{"level": "info", "user": "alice"},
	}

	_ = emitter.Emit(context.Background(), entry)

	var pushReq lokiPushRequest
	_ = json.Unmarshal(capturedBody, &pushReq)

	line := pushReq.Streams[0].Values[0][1]

	// Line should be JSON when parsed fields are present
	var lineData map[string]any
	if err := json.Unmarshal([]byte(line), &lineData); err != nil {
		t.Fatalf("line should be JSON when parsed fields present: %v", err)
	}

	if lineData["level"] != "info" {
		t.Errorf("expected level=info in JSON line, got %v", lineData)
	}
	if lineData["message"] != "original message" {
		t.Errorf("expected message in JSON line, got %v", lineData)
	}
}
