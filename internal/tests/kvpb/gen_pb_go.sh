#!/bin/bash

protoc --go_out=. --proto_path=../..:$GOPATH/src:. kv.proto
