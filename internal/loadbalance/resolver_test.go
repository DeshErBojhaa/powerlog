package loadbalance_test

import (
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"github.com/DeshErBojhaa/powerlog/internal/loadbalance"
	"github.com/DeshErBojhaa/powerlog/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"net"
	"testing"
)

func TestResolver(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	sev, err := server.NewgRPCServer(&server.Config{
		GetServer: &getServer{},
	})
	require.NoError(t, err)

	go func() {
		_ = sev.Serve(l)
	}()
	conn := &clientConn{}
	r := loadbalance.Resolver{}
	_, err = r.Build(resolver.Target{Endpoint: l.Addr().String()}, conn, resolver.BuildOptions{})
	require.NoError(t, err)

	want := resolver.State{
		Addresses: []resolver.Address{{
			Addr:       "localhost:9001",
			Attributes: attributes.New("is_leader", true),
		}, {
			Addr:       "localhost:9002",
			Attributes: attributes.New("is_leader", false),
		}},
	}
	require.Equal(t, want, conn.state)
}

type getServer struct{}

func (g getServer) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:       "leader",
			RpcAddr:  "localhost:9001",
			IsLeader: true,
		}, {
			Id:       "follower",
			RpcAddr:  "localhost:9002",
			IsLeader: false,
		},
	}, nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(err error) {}

func (c *clientConn) NewAddress(addrs []resolver.Address) {}

func (c *clientConn) NewServiceConfig(config string) {}

func (c *clientConn) ParseServiceConfig(config string) *serviceconfig.ParseResult {
	return nil
}
