package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/LiuzhouChan/go-paxos/example/paxoskv"
	"github.com/sirupsen/logrus"
	grpc "google.golang.org/grpc"
)

func usage(argv []string) {
	fmt.Printf("%s <server address ip:port> <put> <key> <value> <version>\n", argv[0])
	fmt.Printf("%s <server address ip:port> <getlocal> <key>\n", argv[0])
	fmt.Printf("%s <server address ip:port> <delete> <key> <version>\n", argv[0])

}

type client struct {
	address string
	conn    *grpc.ClientConn
	c       paxoskv.PhxKVServerClient
}

func newClient(address string) *client {
	return &client{
		address: address,
	}
}

func (c *client) Dial() {
	conn, err := grpc.Dial(c.address, grpc.WithInsecure())
	if err != nil {
		logrus.Panic(err)
	}
	c.conn = conn
	client := paxoskv.NewPhxKVServerClient(conn)
	c.c = client
}

func (c *client) Stop() {
	c.conn.Close()
}

func (c *client) Put(key string, value []byte, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	req := &paxoskv.KVRequest{
		Key:      key,
		Value:    []byte(value),
		Version:  uint64(version),
		Operator: paxoskv.KVRequest_WRITE,
	}
	defer cancel()
	rs, err := c.c.Put(ctx, req)
	if err != nil {
		panic(err)
	}
	logrus.Info(rs.Code)
}

func (c *client) GetLocal(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	req := &paxoskv.KVRequest{
		Key:      key,
		Operator: paxoskv.KVRequest_READ,
	}
	defer cancel()
	rs, err := c.c.GetLocal(ctx, req)
	if err != nil {
		panic(err)
	}
	logrus.Infof("%+v", rs)
}

func (c *client) Del(key string, version uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	req := &paxoskv.KVRequest{
		Key:      key,
		Version:  uint64(version),
		Operator: paxoskv.KVRequest_DELETE,
	}
	defer cancel()
	rs, err := c.c.Delete(ctx, req)
	if err != nil {
		panic(err)
	}
	logrus.Info(rs.Code)
}

func main() {
	args := os.Args
	if len(args) < 4 {
		usage(args)
		return
	}
	serverAddr := args[1]
	c := newClient(serverAddr)
	c.Dial()

	function := args[2]
	key := args[3]
	if function == "put" {
		if len(args) < 6 {
			usage(args)
			return
		}
		value := args[4]
		version, err := strconv.Atoi(args[5])
		if err != nil {
			panic(err)
		}
		c.Put(key, []byte(value), uint64(version))
	} else if function == "getlocal" {
		c.GetLocal(key)
	} else if function == "delete" {
		if len(args) < 5 {
			usage(args)
			return
		}
		version, err := strconv.Atoi(args[4])
		if err != nil {
			logrus.Panic(err)
		}
		c.Del(key, uint64(version))
	} else {
		usage(args)
	}
}
