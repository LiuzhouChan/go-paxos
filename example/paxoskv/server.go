package paxoskv

import (
	context "context"
	fmt "fmt"
	"os"
	"time"

	"github.com/LiuzhouChan/go-paxos"
)

type server struct {
	nh *paxos.NodeHost
}

func (s *server) Put(ctx context.Context,
	req *KVRequest) (*KVResponse, error) {
	return s.propose(ctx, req)
}

func (s *server) Delete(ctx context.Context,
	req *KVRequest) (*KVResponse, error) {
	return s.propose(ctx, req)
}

func (s *server) GetLocal(ctx context.Context,
	req *KVRequest) (*KVResponse, error) {
	query, err := req.Marshal()
	if err != nil {
		panic(err)
	}
	res, err := s.nh.ReadLocalNode(128, query)
	if err != nil {
		panic(err)
	}
	var resp KVResponse
	err = resp.Unmarshal(res)
	return &resp, err
}

func (s *server) propose(ctx context.Context, req *KVRequest) (*KVResponse, error) {
	query, err := req.Marshal()
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	code, err := s.nh.SyncPropose(ctx, 128, query)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
		return nil, err
	}
	resp := &KVResponse{}
	resp.Code = KVResponse_Code(code)
	return resp, nil
}
