package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
)

// Parser extracts structured fields from log entries.
type Parser struct {
	cfg      config.ParserConfig
	patterns []*regexp.Regexp
}

// NewParser creates a new parsing processor.
func NewParser(cfg config.ParserConfig) (*Parser, error) {
	p := &Parser{cfg: cfg}

	// Compile regex patterns
	for _, pattern := range cfg.Patterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, err
		}
		p.patterns = append(p.patterns, re)
	}

	return p, nil
}

// Name returns the processor identifier.
func (p *Parser) Name() string {
	return "parser"
}

// Process parses the log entry and populates the Parsed field.
func (p *Parser) Process(ctx context.Context, entry *model.LogEntry) error {
	if !p.cfg.Enabled {
		return nil
	}

	// Try JSON parsing first if enabled
	if p.cfg.JSONAutoDetect && p.tryParseJSON(entry) {
		return nil
	}

	// Try regex patterns
	for _, re := range p.patterns {
		if p.tryParseRegex(entry, re) {
			return nil
		}
	}

	return nil
}

// tryParseJSON attempts to parse the raw log as JSON.
func (p *Parser) tryParseJSON(entry *model.LogEntry) bool {
	raw := bytes.TrimSpace(entry.Raw)
	if len(raw) == 0 {
		return false
	}

	// Quick check for JSON-like content
	if raw[0] != '{' && raw[0] != '[' {
		return false
	}

	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return false
	}

	// Merge parsed JSON into entry.Parsed
	for k, v := range data {
		entry.Parsed[k] = v
	}

	entry.Parsed["_parsed_format"] = "json"
	return true
}

// tryParseRegex attempts to extract named groups from a regex pattern.
func (p *Parser) tryParseRegex(entry *model.LogEntry, re *regexp.Regexp) bool {
	names := re.SubexpNames()
	if len(names) <= 1 {
		return false // No named groups
	}

	matches := re.FindSubmatch(entry.Raw)
	if matches == nil {
		return false
	}

	for i, name := range names {
		if i == 0 || name == "" {
			continue // Skip full match and unnamed groups
		}
		if i < len(matches) {
			entry.Parsed[name] = string(matches[i])
		}
	}

	entry.Parsed["_parsed_format"] = "regex"
	entry.Parsed["_parsed_pattern"] = re.String()
	return true
}

// CommonLogPatterns provides pre-built regex patterns for common log formats.
var CommonLogPatterns = map[string]string{
	// Apache/Nginx Combined Log Format
	"combined": `^(?P<remote_addr>\S+) - (?P<remote_user>\S+) \[(?P<time_local>[^\]]+)\] "(?P<request>[^"]*)" (?P<status>\d+) (?P<body_bytes>\d+) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)"`,

	// Syslog (RFC 3164)
	"syslog": `^<(?P<priority>\d+)>(?P<timestamp>\w{3}\s+\d+\s+\d+:\d+:\d+)\s+(?P<hostname>\S+)\s+(?P<program>[^\[:]+)(?:\[(?P<pid>\d+)\])?:\s*(?P<message>.*)`,

	// Key-Value pairs
	"kv": `(?P<key>\w+)=(?P<value>"[^"]*"|\S+)`,

	// Log level detection
	"level": `(?i)\b(?P<level>DEBUG|INFO|WARN(?:ING)?|ERROR|FATAL|CRITICAL|TRACE)\b`,
}

// ParseLevel extracts log level from common patterns.
func ParseLevel(raw string) string {
	raw = strings.ToUpper(raw)
	levels := []string{"FATAL", "CRITICAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG", "TRACE"}
	for _, level := range levels {
		if strings.Contains(raw, level) {
			if level == "WARNING" {
				return "WARN"
			}
			return level
		}
	}
	return ""
}
