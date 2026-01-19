package testutil

import (
	"io"
	"net"

	"github.com/elastic/go-elasticsearch/v8/esutil"
)

// WriteCloser wraps io.WriteCloser for mock generation
type WriteCloser interface {
	io.WriteCloser
}

// PacketConn wraps net.PacketConn for mock generation
type PacketConn interface {
	net.PacketConn
}

// Listener wraps net.Listener for mock generation
type Listener interface {
	net.Listener
}

// Conn wraps net.Conn for mock generation
type Conn interface {
	net.Conn
}

// BulkIndexer wraps esutil.BulkIndexer for mock generation
type BulkIndexer interface {
	esutil.BulkIndexer
}
