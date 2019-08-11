package paxosio

//IPaxosRPC ...
type IPaxosRPC interface {
	Name() string
	Start() error
	Stop()
}
