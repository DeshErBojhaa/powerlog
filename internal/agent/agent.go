package agent

import (
	"github.com/DeshErBojhaa/powerlog/internal/discovery"
	"github.com/DeshErBojhaa/powerlog/internal/log"
	"google.golang.org/grpc"
	"sync"
)

type Agent struct {
	Config

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator log.Replicator

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	DataDir        string
	BindAddr       string
	RPCPort        int
	NodeName       string
	StartJoinAddrs []string
}
