package rsm

import "testing"

func TestOffloadedStatusReadyToDestroy(t *testing.T) {
	o := OffloadedStatus{}
	if o.ReadyToDestroy() {
		t.Errorf("ready to destroy, not expected")
	}
	if o.Destroyed() {
		t.Errorf("destroyed, not expected")
	}
	o.SetDestroyed()
	if !o.Destroyed() {
		t.Errorf("not destroyed, not expected")
	}
}

func TestOffloadedStatusAllOffloadedWillMakeItReadyToDestroy(t *testing.T) {
	o := OffloadedStatus{}
	o.SetOffloaded(FromStepWorker)
	o.SetOffloaded(FromCommitWorker)
	o.SetOffloaded(FromSnapshotWorker)
	if o.ReadyToDestroy() {
		t.Errorf("ready to destroy, not expected")
	}
	o.SetOffloaded(FromNodeHost)
	if !o.ReadyToDestroy() {
		t.Errorf("not ready to destroy, not expected")
	}
}

func TestOffloadedStatusOffloadedFromNodeHostIsHandled(t *testing.T) {
	o := OffloadedStatus{}
	o.SetOffloaded(FromNodeHost)
	if !o.ReadyToDestroy() {
		t.Errorf("not ready to destroy, not expected")
	}
	o1 := OffloadedStatus{}
	o1.SetLoaded(FromStepWorker)
	if !o1.loadedByStepWorker {
		t.Errorf("set loaded didn't set component as loaded")
	}
	o1.SetOffloaded(FromNodeHost)
	if o1.ReadyToDestroy() {
		t.Errorf("ready to destroy, not expected")
	}
	o1.SetOffloaded(FromStepWorker)
	if !o1.ReadyToDestroy() {
		t.Errorf("not ready to destroy, not expected")
	}
}
