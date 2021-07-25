package agent_test

import (
	"context"
	"fmt"
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"github.com/DeshErBojhaa/powerlog/internal/agent"
	"github.com/DeshErBojhaa/powerlog/internal/loadbalance"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"
)

func TestAgent(t *testing.T) {
	var agents = make([]*agent.Agent, 0, 3)
	for i := 0; i < 3; i++ {
		ports := getPort(i, 2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(
				startJoinAddrs,
				agents[0].Config.BindAddr,
			)
		}
		a, err := agent.New(agent.Config{
			NodeName:       fmt.Sprintf("%d", i),
			StartJoinAddrs: startJoinAddrs,
			BindAddr:       bindAddr,
			RPCPort:        rpcPort,
			DataDir:        dataDir,
			Bootstrap:      i == 0,
		})
		require.NoError(t, err)
		agents = append(agents, a)
	}
	defer func() {
		for _, a := range agents {
			err := a.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(a.Config.DataDir))
		}
	}()
	time.Sleep(3 * time.Second)
	leader := client(t, agents[0])
	produceResp, err := leader.Produce(
		context.Background(),
		&api.ProduceRequest{Record: &api.Record{Value: []byte("foo")}})
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	consumeResp, err := leader.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResp.Offset})
	require.NoError(t, err)
	require.Equal(t, consumeResp.Record.Value, []byte("foo"))

	time.Sleep(3 * time.Second)
	follower := client(t, agents[1])
	consumeResp, err = follower.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResp.Offset})
	require.NoError(t, err)
	require.Equal(t, consumeResp.Record.Value, []byte("foo"))

	consumeResp, err = leader.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResp.Offset + 1},
	)
	require.Nil(t, consumeResp)
	require.Error(t, err)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

func client(t *testing.T, agent *agent.Agent) api.LogClient {
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", loadbalance.Name, rpcAddr),
		grpc.WithInsecure(), // TODO : fix
	)
	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}

func getPort(idx, n int) []int {
	ret := make([]int, 0, n)
	start := idx * n
	for i, p := range []int{22221, 22222, 22223, 22224, 22225, 22226, 22227} {
		if i < start {
			continue
		}
		l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: p})
		if err != nil {
			continue
		}
		_ = l.Close()
		ret = append(ret, p)
		if len(ret) == n {
			break
		}
	}
	return ret
}
