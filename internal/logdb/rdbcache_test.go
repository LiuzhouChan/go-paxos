package logdb

import (
	"reflect"
	"testing"

	"github.com/LiuzhouChan/go-paxos/paxosio"
	"github.com/LiuzhouChan/go-paxos/paxospb"
)

func TestCachedNodeInfoCanBeSet(t *testing.T) {
	c := newRDBCache()
	if len(c.nodeInfo) != 0 {
		t.Errorf("unexpected map len")
	}
	toSet := c.setNodeInfo(100, 2)
	if !toSet {
		t.Errorf("unexpected return value %t", toSet)
	}
	toSet = c.setNodeInfo(100, 2)
	if toSet {
		t.Errorf("unexpected return value %t", toSet)
	}
	if len(c.nodeInfo) != 1 {
		t.Errorf("unexpected map len")
	}
	ni := paxosio.NodeInfo{
		GroupID: 100,
		NodeID:  2,
	}
	_, ok := c.nodeInfo[ni]
	if !ok {
		t.Errorf("node info not set")
	}
}

func TestCachedStateCanBeSet(t *testing.T) {
	c := newRDBCache()
	if len(c.ps) != 0 {
		t.Errorf("unexpected saveState len %d", len(c.ps))
	}
	st := paxospb.State{
		Commit: 3,
	}
	toSet := c.setState(100, 2, st)
	if !toSet {
		t.Errorf("unexpected return value")
	}
	toSet = c.setState(100, 2, st)
	if toSet {
		t.Errorf("unexpected return value")
	}
	st.Commit = 4
	toSet = c.setState(100, 2, st)
	if !toSet {
		t.Errorf("unexpected return value")
	}
	if len(c.ps) != 1 {
		t.Errorf("unexpected savedState len %d", len(c.ps))
	}
	ni := paxosio.NodeInfo{
		GroupID: 100,
		NodeID:  2,
	}
	v, ok := c.ps[ni]
	if !ok {
		t.Errorf("unexpected savedState map value")
	}
	if !reflect.DeepEqual(&v, &st) {
		t.Errorf("unexpected persistent state values")
	}
}

func TestMaxInstanceIDCanBeSetAndGet(t *testing.T) {
	c := newRDBCache()
	c.setMaxInstance(10, 10, 100)
	v, ok := c.getMaxInstance(10, 10)
	if !ok {
		t.Fatalf("failed to get max instance id")
	}
	if v != 100 {
		t.Errorf("unexpected max instance id, got %d, want 100", v)
	}
}
