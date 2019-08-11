package transport

import (
	"testing"

	"github.com/LiuzhouChan/go-paxos/internal/settings"
	"github.com/LiuzhouChan/go-paxos/paxosio"
)

func TestPeerCanBeAdded(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	_, _, err := nodes.Resolve(100, 2)
	if err == nil {
		t.Fatalf("error not reported")
	}
	nodes.AddNode(100, 2, "a2:2")
	url, _, err := nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to resolve address")
	}
	if url != "a2:2" {
		t.Errorf("got %s, want %s", url, "a2:2")
	}
}

func TestPeerAddressCanNotBeUpdated(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	nodes.AddNode(100, 2, "a2:2")
	nodes.AddNode(100, 2, "a2:3")
	nodes.AddNode(100, 2, "a2:4")
	url, _, err := nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to resolve address")
	}
	if url != "a2:2" {
		t.Errorf("got %s, want %s", url, "a2:2")
	}
}

func TestPeerCanBeRemoved(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	nodes.AddNode(100, 2, "a2:2")
	url, _, err := nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to resolve address")
	}
	if url != "a2:2" {
		t.Errorf("got %s, want %s", url, "a2:2")
	}
	nodes.RemoveNode(100, 2)
	_, _, err = nodes.Resolve(100, 2)
	if err == nil {
		t.Fatalf("error not reported")
	}
}

func TestRemoveGroup(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	nodes.AddNode(100, 2, "a2:2")
	nodes.AddNode(100, 3, "a2:3")
	nodes.AddNode(200, 2, "a3:2")
	nodes.RemoveGroup(100)
	_, _, err := nodes.Resolve(100, 2)
	if err == nil {
		t.Errorf("cluster not removed")
	}
	_, _, err = nodes.Resolve(200, 2)
	if err != nil {
		t.Errorf("failed to get node")
	}
}

func TestRemoteAddressCanBeUsed(t *testing.T) {
	nodes := NewNodes(settings.Soft.StreamConnections)
	_, _, err := nodes.Resolve(100, 2)
	if err == nil {
		t.Errorf("unexpected result")
	}
	nodes.AddRemoteAddress(100, 2, "a3:2")
	v, _, err := nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to return the remote address")
	}
	if v != "a3:2" {
		t.Errorf("v %s, want a3:2", v)
	}
	nodes.nmu.nodes = make(map[paxosio.NodeInfo]string)
	v, _, err = nodes.Resolve(100, 2)
	if err != nil {
		t.Errorf("failed to return the remote address")
	}
	if v != "a3:2" {
		t.Errorf("v %s, want a3:2", v)
	}
}

func testInvalidAddressWillPanic(t *testing.T, addr string) {
	po := false
	nodes := NewNodes(settings.Soft.StreamConnections)
	defer func() {
		if r := recover(); r != nil {
			po = true
		}
		if !po {
			t.Errorf("failed to panic on invalid address")
		}
	}()
	nodes.AddNode(100, 2, addr)
}

func TestInvalidAddressWillPanic(t *testing.T) {
	testInvalidAddressWillPanic(t, "a3")
	testInvalidAddressWillPanic(t, "3")
	testInvalidAddressWillPanic(t, "abc:")
	testInvalidAddressWillPanic(t, ":")
	testInvalidAddressWillPanic(t, ":1243")
	testInvalidAddressWillPanic(t, "abc")
	testInvalidAddressWillPanic(t, "abc:67890")
}
