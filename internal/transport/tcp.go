package transport

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/utils/netutil"
	"github.com/LiuzhouChan/go-paxos/internal/utils/syncutil"
	"github.com/LiuzhouChan/go-paxos/paxosio"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

var (
	// ErrBadMessage is the error returned to indicate the incoming message is
	// corrupted.
	ErrBadMessage       = errors.New("invalid message")
	magicNumber         = [2]byte{0xAE, 0x7D}
	payloadBufferSize   = settings.SnapshotChunkSize + 1024*128
	magicNumberDuration = 1 * time.Second
	headerDuration      = 2 * time.Second
	readDuration        = 5 * time.Second
	writeDuration       = 5 * time.Second
	keepAlivePeriod     = 30 * time.Second
	perConnBufSize      = settings.Soft.PerConnectionBufferSize
	recvBufSize         = settings.Soft.PerConnectionRecvBufSize
)

const (
	// TCPPaxosRPCName is the name of the tcp RPC module.
	TCPPaxosRPCName          = "go-tcp-transport"
	requestHeaderSize        = 14
	paxosType         uint16 = 100
	snapshotType      uint16 = 200
)

type requestHeader struct {
	method uint16
	size   uint32
	crc    uint32
}

func (h *requestHeader) encode(buf []byte) []byte {
	if len(buf) < requestHeaderSize {
		panic("input buf too small")
	}
	binary.BigEndian.PutUint16(buf, h.method)
	binary.BigEndian.PutUint32(buf[2:], h.size)
	binary.BigEndian.PutUint32(buf[6:], 0)
	binary.BigEndian.PutUint32(buf[10:], h.crc)
	v := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	binary.BigEndian.PutUint32(buf[6:], v)
	return buf[:requestHeaderSize]
}
func (h *requestHeader) decode(buf []byte) bool {
	if len(buf) < requestHeaderSize {
		return false
	}
	incoming := binary.BigEndian.Uint32(buf[6:])
	binary.BigEndian.PutUint32(buf[6:], 0)
	expected := crc32.ChecksumIEEE(buf[:requestHeaderSize])
	if incoming != expected {
		plog.Errorf("header crc check failed")
		return false
	}
	binary.BigEndian.PutUint32(buf[6:], incoming)
	method := binary.BigEndian.Uint16(buf)
	if method != paxosType && method != snapshotType {
		plog.Errorf("invalid method type")
		return false
	}
	h.method = method
	h.size = binary.BigEndian.Uint32(buf[2:])
	h.crc = binary.BigEndian.Uint32(buf[10:])
	return true
}

// Marshaler is the interface for types that can be Marshaled.
type Marshaler interface {
	MarshalTo([]byte) (int, error)
	Size() int
}

func writeMessage(conn net.Conn,
	header requestHeader, buf []byte, headerBuf []byte) error {
	crc := crc32.ChecksumIEEE(buf)
	header.size = uint32(len(buf))
	header.crc = crc
	headerBuf = header.encode(headerBuf)
	tt := time.Now().Add(magicNumberDuration).Add(headerDuration)
	if err := conn.SetWriteDeadline(tt); err != nil {
		return err
	}
	if _, err := conn.Write(magicNumber[:]); err != nil {
		return err
	}
	if _, err := conn.Write(headerBuf); err != nil {
		return err
	}
	sent := 0
	bufSize := int(recvBufSize)
	for sent < len(buf) {
		if sent+bufSize > len(buf) {
			bufSize = len(buf) - sent
		}
		tt = time.Now().Add(writeDuration)
		if err := conn.SetWriteDeadline(tt); err != nil {
			return err
		}
		if _, err := conn.Write(buf[sent : sent+bufSize]); err != nil {
			return err
		}
		sent += bufSize
	}
	if sent != len(buf) {
		plog.Panicf("sent %d, buf len %d", sent, len(buf))
	}
	return nil
}

func readMessage(conn net.Conn,
	header []byte, rbuf []byte) (requestHeader, []byte, error) {
	tt := time.Now().Add(headerDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return requestHeader{}, nil, err
	}
	if _, err := io.ReadFull(conn, header); err != nil {
		plog.Errorf("failed to get the header")
		return requestHeader{}, nil, err
	}
	rheader := requestHeader{}
	if !rheader.decode(header) {
		plog.Errorf("invalid header")
		return requestHeader{}, nil, ErrBadMessage
	}
	if rheader.size == 0 {
		plog.Errorf("invalid payload length")
		return requestHeader{}, nil, ErrBadMessage
	}
	var buf []byte
	if rheader.size > uint32(len(rbuf)) {
		buf = make([]byte, rheader.size)
	} else {
		buf = rbuf[:rheader.size]
	}
	received := 0
	var recvBuf []byte
	if rheader.size < uint32(recvBufSize) {
		recvBuf = buf[:rheader.size]
	} else {
		recvBuf = buf[:recvBufSize]
	}
	toRead := rheader.size
	for toRead > 0 {
		tt = time.Now().Add(readDuration)
		if err := conn.SetReadDeadline(tt); err != nil {
			return requestHeader{}, nil, err
		}
		if _, err := io.ReadFull(conn, recvBuf); err != nil {
			return requestHeader{}, nil, err
		}
		toRead -= uint32(len(recvBuf))
		received += len(recvBuf)
		if toRead < uint32(recvBufSize) {
			recvBuf = buf[received : uint32(received)+toRead]
		} else {
			recvBuf = buf[received : received+int(recvBufSize)]
		}
	}
	if uint32(received) != rheader.size {
		panic("unexpected size")
	}
	if crc32.ChecksumIEEE(buf) != rheader.crc {
		plog.Errorf("invalid payload checksum")
		return requestHeader{}, nil, ErrBadMessage
	}
	return rheader, buf, nil
}

func readMagicNumber(conn net.Conn, magicNum []byte) error {
	tt := time.Now().Add(magicNumberDuration)
	if err := conn.SetReadDeadline(tt); err != nil {
		return err
	}
	if _, err := io.ReadFull(conn, magicNum); err != nil {
		return err
	}
	if !bytes.Equal(magicNum, magicNumber[:]) {
		plog.Errorf("invalid magic number")
		return ErrBadMessage
	}
	return nil
}

// TCPConnection is the connection used for sending paxos messages to remote
// nodes.
type TCPConnection struct {
	conn    net.Conn
	header  []byte
	payload []byte
}

// NewTCPConnection creates and returns a new TCPConnection instance.
func NewTCPConnection(conn net.Conn) *TCPConnection {
	return &TCPConnection{
		conn:    conn,
		header:  make([]byte, requestHeaderSize),
		payload: make([]byte, perConnBufSize),
	}
}

// Close closes the TCPConnection instance.
func (c *TCPConnection) Close() {
	if err := c.conn.Close(); err != nil {
		plog.Errorf("failed to close the connection %v", err)
	}
}

// SendMessageBatch sends a paxos message batch to remote node.
func (c *TCPConnection) SendMessageBatch(batch paxospb.MessageBatch) error {
	header := requestHeader{method: paxosType}
	var buf []byte
	sz := batch.SizeUpperLimit()
	if len(c.payload) < sz {
		buf = make([]byte, sz)
	} else {
		buf = c.payload
	}
	n, err := batch.MarshalTo(buf)
	if err != nil {
		panic(err)
	}
	return writeMessage(c.conn, header, buf[:n], c.header)
}

// TCPTransport is a TCP based RPC module for exchanging paxos messages and
// snapshots between NodeHost instances.
type TCPTransport struct {
	nhConfig       config.NodeHostConfig
	stopper        *syncutil.Stopper
	requestHandler paxosio.RequestHandler
}

// NewTCPTransport creates and returns a new TCP transport module.
func NewTCPTransport(nhConfig config.NodeHostConfig,
	requestHandler paxosio.RequestHandler) paxosio.IPaxosRPC {
	return &TCPTransport{
		nhConfig:       nhConfig,
		stopper:        syncutil.NewStopper(),
		requestHandler: requestHandler,
	}
}

// Start starts the TCP transport module.
func (g *TCPTransport) Start() error {
	address := g.nhConfig.PaxosAddress
	listener, err := netutil.NewStoppableListener(address,
		nil, g.stopper.ShouldStop())
	if err != nil {
		plog.Panicf("failed to new a stoppable listener, %v", err)
	}
	g.stopper.RunWorker(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				if err == netutil.ErrListenerStopped {
					return
				}
				panic(err)
			}
			var once sync.Once
			closeFn := func() {
				once.Do(func() {
					if err := conn.Close(); err != nil {
						plog.Errorf("failed to close the connection %v", err)
					}
				})
			}
			g.stopper.RunWorker(func() {
				<-g.stopper.ShouldStop()
				closeFn()
			})
			g.stopper.RunWorker(func() {
				g.serveConn(conn)
				closeFn()
			})
		}
	})
	return nil
}

// Stop stops the TCP transport module.
func (g *TCPTransport) Stop() {
	g.stopper.Stop()
}

// GetConnection returns a new paxosio.IConnection for sending raft messages.
func (g *TCPTransport) GetConnection(ctx context.Context,
	target string) (paxosio.IConnection, error) {
	conn, err := g.getConnection(ctx, target)
	if err != nil {
		return nil, err
	}
	return NewTCPConnection(conn), nil
}

// Name returns a human readable name of the TCP transport module.
func (g *TCPTransport) Name() string {
	return TCPPaxosRPCName
}

func (g *TCPTransport) serveConn(conn net.Conn) {
	magicNum := make([]byte, len(magicNumber))
	header := make([]byte, requestHeaderSize)
	tbuf := make([]byte, payloadBufferSize)
	var chunks paxosio.IChunkSink
	stopper := syncutil.NewStopper()
	defer func() {
		if chunks != nil {
			chunks.Close()
		}
	}()
	defer stopper.Stop()
	for {
		err := readMagicNumber(conn, magicNum)
		if err != nil {
			if err == ErrBadMessage {
				return
			}
			operr, ok := err.(net.Error)
			if ok && operr.Timeout() {
				continue
			} else {
				return
			}
		}
		rheader, buf, err := readMessage(conn, header, tbuf)
		if err != nil {
			return
		}
		if rheader.method == paxosType {
			batch := paxospb.MessageBatch{}
			if err := batch.Unmarshal(buf); err != nil {
				return
			}
			g.requestHandler(batch)
		} else {
			plog.Warningf("snapshot msg todo")
		}
	}
}

func setTCPConn(conn *net.TCPConn) error {
	if err := conn.SetLinger(0); err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		return err
	}
	return conn.SetKeepAlivePeriod(keepAlivePeriod)
}

func (g *TCPTransport) getConnection(ctx context.Context,
	target string) (net.Conn, error) {
	timeout := time.Duration(getDialTimeoutSecond()) * time.Second
	conn, err := net.DialTimeout("tcp", target, timeout)
	if err != nil {
		return nil, err
	}
	tcpconn, ok := conn.(*net.TCPConn)
	if ok {
		if err := setTCPConn(tcpconn); err != nil {
			return nil, err
		}
	}
	return conn, nil
}
