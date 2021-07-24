package agent

import (
	"bytes"
	"fmt"
	"github.com/DeshErBojhaa/powerlog/internal/discovery"
	"github.com/DeshErBojhaa/powerlog/internal/log"
	"github.com/DeshErBojhaa/powerlog/internal/server"
	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
	"net"
	"sync"
	"time"
)

// Agent runs on every service instance, setting
// up and connecting all the different components
type Agent struct {
	Config Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0
	})
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(raftLn)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitFOrLeader(3 * time.Second)
	}
	return err
}

func (a *Agent) setupServer() error {
	serverConfig := &server.Config{
		CommitLog: a.log,
	}
	var opts []grpc.ServerOption
	var err error
	a.server, err = server.NewgRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	grpcLn := a.mux.Match(cmux.Any())

	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdownCh)

	serviceClosers := []func() error{
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}

	for _, fn := range serviceClosers {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	//var opts []grpc.DialOption
	//conn, err := grpc.Dial(rpcAddr, grpc.WithInsecure())
	//if err != nil {
	//	return err
	//}
	//client := api.NewLogClient(conn)
	//a.replicator = &log.Replicator{
	//	DialOptions: opts,
	//	LocalServer: client,
	//}
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}

type Config struct {
	DataDir        string
	BindAddr       string
	RPCPort        int
	NodeName       string
	StartJoinAddrs []string
	// Bootstrap should be set to true when starting
	// the first node of the cluster.
	Bootstrap bool
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:     config,
		shutdownCh: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go func() {
		_ = a.serve()
	}()
	return a, nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}
