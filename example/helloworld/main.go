package main

import (
	"github.com/LiuzhouChan/go-paxos/logger"
)

func main() {
	log := logger.GetLogger("test")
	log.Infof("test")
	log.Errorf("e test")
}
