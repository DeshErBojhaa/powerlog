package agent_test

import (
	"context"
	"fmt"
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"github.com/DeshErBojhaa/powerlog/internal/agent"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"
)

func TestAgent(t *testing.T) {
	var agents = make([]*agent.Agent, 0, 3)
	for i := 0; i < 3; i++ {
		ports := getPort(2)
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
		agent, err := agent.New(agent.Config{
			NodeName:       fmt.Sprintf("%d", i),
			StartJoinAddrs: startJoinAddrs,
			BindAddr:       bindAddr,
			RPCPort:        rpcPort,
			DataDir:        dataDir,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
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

	consumeResp, err := leader.Consume(
		context.Background(),
		&api.ConsumeRequest{Offset: produceResp.Offset})
	require.NoError(t, err)
	require.Equal(t, consumeResp.Record.Value, []byte("foo"))
}

func client(t *testing.T, agent *agent.Agent) api.LogClient {
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(rpcAddr, grpc.WithInsecure()) // TODO : fix
	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}

func getPort(n int) []int {
	ret := make([]int, 0, n)
	for _, p := range []int{34567, 23567, 27654, 37547, 44444, 36459} {
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
