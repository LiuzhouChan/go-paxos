package paxoskv

import (
	context "context"

	"github.com/LiuzhouChan/go-paxos"
	"github.com/LiuzhouChan/go-paxos/internal/utils/netutil"
	"github.com/LiuzhouChan/go-paxos/internal/utils/stringutil"
	"github.com/LiuzhouChan/go-paxos/internal/utils/syncutil"
	grpc "google.golang.org/grpc"
)

// PaxosKV is the main struct used pb the kv
type PaxosKV struct {
	nh         *paxos.NodeHost
	stopper    *syncutil.Stopper
	server     *server
	grpcHost   string
	grpcServer *grpc.Server
	ctx        context.Context
	cancel     context.CancelFunc
}

//NewPaxosKV ...
func NewPaxosKV(nh *paxos.NodeHost, grpcHost string) *PaxosKV {
	if !stringutil.IsValidAddress(grpcHost) {
		plog.Panicf("invalid drummer grpc address %s", grpcHost)
	}
	server := &server{
		nh: nh,
	}
	stopper := syncutil.NewStopper()

	kv := &PaxosKV{
		nh:       nh,
		stopper:  stopper,
		server:   server,
		grpcHost: grpcHost,
	}
	kv.ctx, kv.cancel = context.WithCancel(context.Background())
	return kv
}

//Start ...
func (kv *PaxosKV) Start() {
	kv.stopper.RunWorker(func() {
		kv.kvWorker()
	})
}

//Stop ...
func (kv *PaxosKV) Stop() {
	plog.Infof("paxoskv's server ctx is going to be stopped")
	kv.cancel()
	kv.stopper.Stop()
	if kv.grpcServer != nil {
		plog.Infof("paxoskv grpc server going to be stopped %s", kv.nh.PaxosAddress())
		kv.grpcServer.Stop()
	}
}

func (kv *PaxosKV) kvWorker() {
	select {
	case <-kv.stopper.ShouldStop():
		return
	default:
	}
	plog.Infof("going to start the kv API server")
	kv.startKVRPCServer()
}

func (kv *PaxosKV) startKVRPCServer() {
	stoppableListener, err := netutil.NewStoppableListener(kv.grpcHost, nil,
		kv.stopper.ShouldStop())
	if err != nil {
		plog.Panicf("failed to create a listener %v", err)
	}
	grpcServer := grpc.NewServer()
	RegisterPhxKVServerServer(grpcServer, kv.server)
	kv.stopper.RunWorker(func() {
		if err := grpcServer.Serve(stoppableListener); err != nil {
			plog.Errorf("serve returned %v", err)
		}
		plog.Infof("server's gRPC serve function returned, nh %s", kv.nh.PaxosAddress())
	})
	kv.grpcServer = grpcServer
}
