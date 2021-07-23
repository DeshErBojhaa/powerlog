package log_test

import (
	"fmt"
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"github.com/DeshErBojhaa/powerlog/internal/log"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"
)

func TestMultipleNodes(t *testing.T) {
	nodeCount := 3
	logs := make([]*log.DistributedLog, 0, nodeCount)
	ports := getPort(nodeCount)

	for i := 0; i < nodeCount; i++ {
		dataDir, err := ioutil.TempDir("", "distribute-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := log.Config{}

		// Raft
		config.Raft.StreamLayer = log.NewStreamLayer(ln)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond

		if i == 0 {
			config.Raft.Bootstrap = true
		}

		l, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)
		if i != 0 {
			err = logs[0].Join(fmt.Sprintf("%d", i), ln.Addr().String())
			require.NoError(t, err)
		} else {
			err = l.WaitFOrLeader(3 * time.Second)
			require.NoError(t, err)
		}
		logs = append(logs, l)
	}
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	//for _, record := range records {
	//	off, err := logs[0].
	//}
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
