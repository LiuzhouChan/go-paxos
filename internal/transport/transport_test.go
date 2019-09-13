package transport

import (
	"sync"
	"testing"
	"time"

	"github.com/LiuzhouChan/go-paxos/config"
	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/internal/utils/random"
	"github.com/LiuzhouChan/go-paxos/internal/utils/syncutil"
	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const (
	grpcServerURL = "localhost:5601"
)

type testMessageHandler struct {
	mu               sync.Mutex
	requestCount     map[paxosio.NodeInfo]uint64
	unreachableCount map[paxosio.NodeInfo]uint64
}

func newTestMessageHandler() *testMessageHandler {
	return &testMessageHandler{
		requestCount:     make(map[paxosio.NodeInfo]uint64),
		unreachableCount: make(map[paxosio.NodeInfo]uint64),
	}
}

func (h *testMessageHandler) HandleMessageBatch(reqs paxospb.MessageBatch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, req := range reqs.Requests {
		epk := paxosio.GetNodeInfo(req.GroupID, req.To)
		v, ok := h.requestCount[epk]
		if ok {
			h.requestCount[epk] = v + 1
		} else {
			h.requestCount[epk] = 1
		}
	}
}

func (h *testMessageHandler) getRequestCount(groupID uint64,
	nodeID uint64) uint64 {
	return h.getMessageCount(h.requestCount, groupID, nodeID)
}

func (h *testMessageHandler) getMessageCount(m map[paxosio.NodeInfo]uint64,
	groupID uint64, nodeID uint64) uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	epk := paxosio.GetNodeInfo(groupID, nodeID)
	v, ok := m[epk]
	if ok {
		return v
	}
	return 0
}

func newNOOPTestTransport() (*Transport,
	*Nodes, *NOOPTransport, *noopRequest, *noopConnectRequest) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	c := config.NodeHostConfig{
		PaxosAddress:    "localhost:9876",
		PaxosRPCFactory: NewNOOPTransport,
	}
	ctx := server.NewContext(c)
	transport := NewTransport(c, ctx, nodes)
	paxosRPC, ok := transport.paxosRPC.(*NOOPTransport)
	if !ok {
		panic("not a noop transport")
	}
	return transport, nodes, paxosRPC, paxosRPC.req, paxosRPC.connReq
}

func newTestTransport() (*Transport, *Nodes,
	*syncutil.Stopper) {
	stopper := syncutil.NewStopper()
	nodes := NewNodes(settings.Soft.StreamConnections)
	c := config.NodeHostConfig{
		PaxosAddress:    grpcServerURL,
		PaxosRPCFactory: nil,
	}
	ctx := server.NewContext(c)
	transport := NewTransport(c, ctx, nodes)

	return transport, nodes, stopper
}

func testMessageCanBeSent(t *testing.T, sz uint64) {
	trans, nodes, stopper := newTestTransport()
	defer trans.serverCtx.Stop()
	defer trans.Stop()
	defer stopper.Stop()
	handler := newTestMessageHandler()
	trans.SetMessageHandler(handler)
	nodes.AddNode(100, 2, grpcServerURL)
	for i := 0; i < 20; i++ {
		msg := paxospb.PaxosMsg{
			MsgType: paxospb.PaxosPrepare,
			To:      2,
			GroupID: 100,
		}
		done := trans.ASyncSend(msg)
		if !done {
			t.Errorf("failed to send message")
		}
	}
	done := false
	for i := 0; i < 200; i++ {
		time.Sleep(100 * time.Millisecond)
		if handler.getRequestCount(100, 2) == 20 {
			done = true
			break
		}
	}
	if !done {
		t.Errorf("failed to get all 20 sent messages")
	}
	// test to ensure a single big message can be sent/received.
	payload := []byte(random.String(int(sz)))
	m := paxospb.PaxosMsg{
		MsgType: paxospb.PaxosPrepare,
		To:      2,
		GroupID: 100,
		Value:   payload,
	}
	ok := trans.ASyncSend(m)
	if !ok {
		t.Errorf("failed to send the large msg")
	}
	received := false
	for i := 0; i < 200; i++ {
		time.Sleep(100 * time.Millisecond)
		if handler.getRequestCount(100, 2) == 21 {
			received = true
			break
		}
	}
	if !received {
		t.Errorf("got %d, want %d", handler.getRequestCount(100, 2), 21)
	}
}

func TestMessageCanBeSent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testMessageCanBeSent(t, settings.MaxProposalPayloadSize*10)
	testMessageCanBeSent(t, recvBufSize/2)
	testMessageCanBeSent(t, recvBufSize+1)
	testMessageCanBeSent(t, perConnBufSize+1)
	testMessageCanBeSent(t, perConnBufSize/2)
	testMessageCanBeSent(t, 1)
}

// add some latency to localhost
// sudo tc qdisc add dev lo root handle 1:0 netem delay 100msec
// remove latency
// sudo tc qdisc del dev lo root
// don't forget to change your TCP window size if necessary
// e.g. in our dev environment, we have -
// net.core.wmem_max = 25165824
// net.core.rmem_max = 25165824
// net.ipv4.tcp_rmem = 4096 87380 25165824
// net.ipv4.tcp_wmem = 4096 87380 25165824
func testMessageCanBeSentWithLargeLatency(t *testing.T) {
	trans, nodes, stopper := newTestTransport()
	defer trans.serverCtx.Stop()
	defer trans.Stop()
	defer stopper.Stop()
	handler := newTestMessageHandler()
	trans.SetMessageHandler(handler)
	nodes.AddNode(100, 2, grpcServerURL)
	for i := 0; i < 128; i++ {
		msg := paxospb.PaxosMsg{
			MsgType: paxospb.PaxosPrepare,
			To:      2,
			GroupID: 100,
			Value:   make([]byte, 2*1024*1024),
		}
		done := trans.ASyncSend(msg)
		if !done {
			t.Errorf("failed to send message")
		}
	}
	done := false
	for i := 0; i < 400; i++ {
		time.Sleep(100 * time.Millisecond)
		if handler.getRequestCount(100, 2) == 128 {
			done = true
			break
		}
	}
	if !done {
		t.Errorf("failed to send/receive all messages")
	}
}

// latency need to be simulated by configuring your environment
func TestMessageCanBeSentWithLargeLatency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testMessageCanBeSentWithLargeLatency(t)
}
