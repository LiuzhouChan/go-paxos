#!/bin/bash

echo "paxospb contains code generated by gogoproto and gencode..."
/usr/bin/protoc --proto_path=..:$GOPATH/src:. --gogofaster_out=plugins=grpc:. paxos.proto
