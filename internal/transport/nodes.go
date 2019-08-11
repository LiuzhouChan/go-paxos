package transport

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/LiuzhouChan/go-paxos/internal/server"
	"github.com/LiuzhouChan/go-paxos/internal/utils/logutil"
	"github.com/LiuzhouChan/go-paxos/paxosio"
)

// INodeRegistry is the local registry interface used to keep all known
// nodes in the multi paxos system.
type INodeRegistry interface {
	AddNode(groupID uint64, nodeID uint64, url string)
	RemoveNode(groupID uint64, nodeID uint64)
	RemoveCluster(groupID uint64)
	Resolve(groupID uint64, nodeID uint64) (string, string, error)
}

type addr struct {
	network string
	address string
	port    int
}

func newAddr(v string) (*addr, error) {
	in := strings.TrimSpace(v)
	parts := strings.Split(in, ":")
	if len(parts) != 2 || len(parts[0]) == 0 || len(parts[1]) == 0 {
		plog.Panicf("invalid address, %s", v)
	}
	p, err := strconv.Atoi(parts[1])
	if err != nil || p < 0 || p > 65535 {
		plog.Panicf("invalid port %s", parts[1])
	}
	a := &addr{
		network: "tcp",
		address: parts[0],
		port:    p,
	}
	return a, nil
}

func (a *addr) String() string {
	return fmt.Sprintf("%s:%d", a.address, a.port)
}

type record struct {
	address string
	key     string
}

// Nodes is used to manage all known node addresses in the multi paxos system.
// The transport layer uses this address registry to locate nodes.
type Nodes struct {
	partitioner server.IPartitioner
	mu          struct {
		sync.Mutex
		addr map[paxosio.NodeInfo]record
	}
	nmu struct {
		sync.Mutex
		nodes map[paxosio.NodeInfo]string
	}
}

// NewNodes returns a new Nodes object.
func NewNodes(streamConnections uint64) *Nodes {
	n := &Nodes{}
	if streamConnections > 1 {
		n.partitioner = server.NewFixedPartitioner(streamConnections)
	}
	n.mu.addr = make(map[paxosio.NodeInfo]record)
	n.nmu.nodes = make(map[paxosio.NodeInfo]string)
	return n
}

// AddRemoteAddress remembers the specified address obtained from the source
// of the incoming message.
func (n *Nodes) AddRemoteAddress(groupID uint64,
	nodeID uint64, address string) {
	if len(address) == 0 {
		panic("empty address")
	}
	if nodeID == 0 {
		panic("invalid node id")
	}
	n.nmu.Lock()
	key := paxosio.GetNodeInfo(groupID, nodeID)
	v, ok := n.nmu.nodes[key]
	if !ok {
		n.nmu.nodes[key] = address
	} else {
		if v != address {
			plog.Panicf("inconsistent addr for %s, %s:%s",
				logutil.DescribeNode(groupID, nodeID), v, address)
		}
	}
	n.nmu.Unlock()
}
