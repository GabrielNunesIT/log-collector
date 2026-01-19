package ingestor

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/user/log-collector/internal/config"
	"github.com/user/log-collector/internal/model"
)

// UDPListenerFactory creates a UDP connection.
type UDPListenerFactory func(network, address string) (net.PacketConn, error)

// TCPListenerFactory creates a TCP listener.
type TCPListenerFactory func(network, address string) (net.Listener, error)

// SyslogOption configures the SyslogIngestor.
type SyslogOption func(*SyslogIngestor)

// WithUDPListenerFactory sets a custom UDP listener factory.
func WithUDPListenerFactory(f UDPListenerFactory) SyslogOption {
	return func(s *SyslogIngestor) {
		s.udpFactory = f
	}
}

// WithTCPListenerFactory sets a custom TCP listener factory.
func WithTCPListenerFactory(f TCPListenerFactory) SyslogOption {
	return func(s *SyslogIngestor) {
		s.tcpFactory = f
	}
}

// SyslogIngestor receives syslog messages over UDP or TCP.
type SyslogIngestor struct {
	cfg        config.SyslogIngestorConfig
	name       string
	udpFactory UDPListenerFactory
	tcpFactory TCPListenerFactory
}

// NewSyslogIngestor creates a new syslog ingestor.
func NewSyslogIngestor(cfg config.SyslogIngestorConfig, opts ...SyslogOption) *SyslogIngestor {
	s := &SyslogIngestor{
		cfg:  cfg,
		name: "syslog",
	}

	// Default UDP factory
	s.udpFactory = func(network, address string) (net.PacketConn, error) {
		addr, err := net.ResolveUDPAddr(network, address)
		if err != nil {
			return nil, err
		}
		return net.ListenUDP(network, addr)
	}

	// Default TCP factory
	s.tcpFactory = net.Listen

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Name returns the ingestor identifier.
func (s *SyslogIngestor) Name() string {
	return s.name
}

// Start begins listening for syslog messages.
func (s *SyslogIngestor) Start(ctx context.Context, out chan<- *model.LogEntry) error {
	defer close(out)

	switch strings.ToLower(s.cfg.Protocol) {
	case "udp":
		return s.startUDP(ctx, out)
	case "tcp":
		return s.startTCP(ctx, out)
	default:
		return fmt.Errorf("unsupported syslog protocol: %s", s.cfg.Protocol)
	}
}

// startUDP listens for syslog messages over UDP.
func (s *SyslogIngestor) startUDP(ctx context.Context, out chan<- *model.LogEntry) error {
	conn, err := s.udpFactory("udp", s.cfg.Address)
	if err != nil {
		return fmt.Errorf("listening on UDP: %w", err)
	}
	defer conn.Close()

	// Handle context cancellation
	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	buf := make([]byte, 65535) // Max UDP packet size
	for {
		n, remoteAddr, err := conn.ReadFrom(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// Log error and continue
				continue
			}
		}

		message := make([]byte, n)
		copy(message, buf[:n])

		entry := model.NewLogEntry(s.name, message)
		entry.Metadata["protocol"] = "udp"
		entry.Metadata["remote_addr"] = remoteAddr.String()

		// Parse syslog priority and facility if present
		s.parseSyslogHeader(entry)

		select {
		case out <- entry:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// startTCP listens for syslog messages over TCP.
func (s *SyslogIngestor) startTCP(ctx context.Context, out chan<- *model.LogEntry) error {
	listener, err := s.tcpFactory("tcp", s.cfg.Address)
	if err != nil {
		return fmt.Errorf("listening on TCP: %w", err)
	}
	defer listener.Close()

	// Handle context cancellation
	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				continue
			}
		}

		go s.handleTCPConnection(ctx, conn, out)
	}
}

// handleTCPConnection reads syslog messages from a TCP connection.
func (s *SyslogIngestor) handleTCPConnection(ctx context.Context, conn net.Conn, out chan<- *model.LogEntry) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	scanner := bufio.NewScanner(conn)

	// Increase buffer size for long syslog messages
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		entry := model.NewLogEntry(s.name, []byte(scanner.Text()))
		entry.Metadata["protocol"] = "tcp"
		entry.Metadata["remote_addr"] = remoteAddr

		s.parseSyslogHeader(entry)

		select {
		case out <- entry:
		case <-ctx.Done():
			return
		}
	}
}

// parseSyslogHeader extracts priority, facility, and severity from syslog messages.
// Supports RFC 3164 and RFC 5424 formats.
func (s *SyslogIngestor) parseSyslogHeader(entry *model.LogEntry) {
	raw := string(entry.Raw)
	if len(raw) == 0 || raw[0] != '<' {
		return
	}

	// Find closing bracket
	end := strings.Index(raw, ">")
	if end < 2 || end > 4 {
		return
	}

	// Parse priority
	var priority int
	if _, err := fmt.Sscanf(raw[1:end], "%d", &priority); err != nil {
		return
	}

	facility := priority / 8
	severity := priority % 8

	entry.Parsed["syslog_priority"] = priority
	entry.Parsed["syslog_facility"] = facility
	entry.Parsed["syslog_severity"] = severity
	entry.Parsed["syslog_facility_name"] = facilityName(facility)
	entry.Parsed["syslog_severity_name"] = severityName(severity)

	// Remove priority from raw for cleaner message
	entry.Parsed["syslog_message"] = strings.TrimSpace(raw[end+1:])
}

// facilityName returns the human-readable facility name.
func facilityName(facility int) string {
	names := []string{
		"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
		"uucp", "cron", "authpriv", "ftp", "ntp", "audit", "alert", "clock",
		"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
	}
	if facility >= 0 && facility < len(names) {
		return names[facility]
	}
	return "unknown"
}

// severityName returns the human-readable severity name.
func severityName(severity int) string {
	names := []string{
		"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
	}
	if severity >= 0 && severity < len(names) {
		return names[severity]
	}
	return "unknown"
}

// SyslogTimestampFormat is the standard syslog timestamp format.
var SyslogTimestampFormat = time.Stamp
