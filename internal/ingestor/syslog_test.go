package ingestor

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/GabrielNunesIT/log-collector/internal/config"
	"github.com/GabrielNunesIT/log-collector/internal/model"
	"github.com/GabrielNunesIT/log-collector/tests/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestSyslogIngestor_UDP(t *testing.T) {
	tests := []struct {
		name           string
		cfg            config.SyslogIngestorConfig
		message        string
		remoteAddr     *net.UDPAddr
		expectedRaw    string
		expectedProto  string
		expectedFac    string
		expectedSev    string
	}{
		{
			name: "RFC3164 Message",
			cfg: config.SyslogIngestorConfig{
				Enabled:  true,
				Protocol: "udp",
				Address:  ":5140",
			},
			message:       "<134>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8",
			remoteAddr:    &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
			expectedRaw:   "<134>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8",
			expectedProto: "udp",
			expectedFac:   "local0",
			expectedSev:   "info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockPC := mocks.NewPacketConn(t)
			
			// Synchronization channel
			blockRead := make(chan struct{})
			msgBytes := []byte(tt.message)

			// First call: Return message
			mockPC.On("ReadFrom", mock.Anything).Return(len(msgBytes), tt.remoteAddr, nil).Run(func(args mock.Arguments) {
				p := args.Get(0).([]byte)
				copy(p, msgBytes)
			}).Once()

			// Second call: Block until context done
			mockPC.On("ReadFrom", mock.Anything).Run(func(args mock.Arguments) {
				close(blockRead)
				<-time.After(500 * time.Millisecond) 
			}).Return(0, nil, io.EOF)

			mockPC.On("Close").Return(nil)

			factory := func(network, address string) (net.PacketConn, error) {
				return mockPC, nil
			}

			out := make(chan *model.LogEntry, 1)
			ingestor := NewSyslogIngestor(tt.cfg, WithUDPListenerFactory(factory))

			ctx, cancel := context.WithCancel(context.Background())
			startDone := make(chan error)
			
			go func() {
				startDone <- ingestor.Start(ctx, out)
			}()

			select {
			case entry := <-out:
				assert.Equal(t, tt.expectedRaw, string(entry.Raw))
				assert.Equal(t, tt.expectedProto, entry.Metadata["protocol"])
				if tt.expectedFac != "" {
					assert.Equal(t, tt.expectedFac, entry.Parsed["syslog_facility_name"])
				}
				if tt.expectedSev != "" {
					assert.Equal(t, tt.expectedSev, entry.Parsed["syslog_severity_name"])
				}
			case <-time.After(100 * time.Millisecond):
				cancel()
				t.Fatal("timeout waiting for log entry")
			}
			
			// Wait until we hit the blocking read
			select {
			case <-blockRead:
			case <-time.After(100 * time.Millisecond):
			}

			cancel()
			<-startDone
		})
	}
}

func TestSyslogIngestor_TCP(t *testing.T) {
	tests := []struct {
		name           string
		cfg            config.SyslogIngestorConfig
		message        string
		remoteAddr     *net.TCPAddr
		expectedRaw    string
		expectedProto  string
	}{
		{
			name: "Standard Message",
			cfg: config.SyslogIngestorConfig{
				Enabled:  true,
				Protocol: "tcp",
				Address:  ":5140",
			},
			message:       "<13>Oct 11 22:14:15 mymachine test\n",
			remoteAddr:    &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1234},
			expectedRaw:   "<13>Oct 11 22:14:15 mymachine test",
			expectedProto: "tcp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockL := mocks.NewListener(t)
			mockConn := mocks.NewConn(t)
			
			// Synchronization for Accept loop
			blockAccept := make(chan struct{})

			// Setup Listener Accept
			mockL.On("Accept").Return(mockConn, nil).Once()
			
			// Second accept blocks
			mockL.On("Accept").Run(func(args mock.Arguments) {
				close(blockAccept)
				<-time.After(500 * time.Millisecond)
			}).Return(nil, io.EOF)
			
			mockL.On("Close").Return(nil)

			// Setup Conn Read
			mockConn.On("Read", mock.Anything).Return(func(b []byte) int {
				msg := []byte(tt.message)
				copy(b, msg)
				return len(msg)
			}, nil).Once()

			mockConn.On("Read", mock.Anything).Return(0, io.EOF)
			
			mockConn.On("RemoteAddr").Return(tt.remoteAddr)
			mockConn.On("Close").Return(nil)

			factory := func(network, address string) (net.Listener, error) {
				return mockL, nil
			}

			out := make(chan *model.LogEntry, 1)
			ingestor := NewSyslogIngestor(tt.cfg, WithTCPListenerFactory(factory))

			ctx, cancel := context.WithCancel(context.Background())
			startDone := make(chan error)

			go func() {
				startDone <- ingestor.Start(ctx, out)
			}()

			select {
			case entry := <-out:
				assert.Equal(t, tt.expectedRaw, string(entry.Raw))
				assert.Equal(t, tt.expectedProto, entry.Metadata["protocol"])
			case <-time.After(100 * time.Millisecond):
				cancel()
				t.Fatal("timeout waiting for log entry")
			}
			
			// Wait for blocking Accept
			select {
			case <-blockAccept:
			case <-time.After(100 * time.Millisecond):
			}

			cancel()
			<-startDone
		})
	}
}
