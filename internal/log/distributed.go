package log

import (
	"bytes"
	"fmt"
	"github.com/hashicorp/raft"
	raftBoltDB "github.com/hashicorp/raft-boltdb"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	dl := &DistributedLog{config: config}
	if err := dl.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := dl.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return dl, nil
}

func (dl *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	dl.log, err = NewLog(logDir, dl.config)
	return err
}

func (dl *DistributedLog) setupRaft(dataDir string) error {
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := dl.config
	logConfig.Segment.InitialOffset = 1 // Raft requirement!
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}
	stableStore, err := raftBoltDB.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr)
	if err != nil {
		return err
	}
	maxpool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		dl.config.Raft.StreamLayer,
		maxpool,
		timeout,
		os.Stderr)
	config := raft.DefaultConfig()
	config.LocalID = dl.config.Raft.LocalID
	if dl.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = dl.config.Raft.HeartbeatTimeout
	}
	if dl.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = dl.config.Raft.ElectionTimeout
	}
	if dl.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = dl.config.Raft.LeaderLeaseTimeout
	}
	if dl.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = dl.config.Raft.CommitTimeout
	}

	fsm := &fsm{log: dl.log}
	dl.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}
	if dl.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = dl.raft.BootstrapCluster(config).Error()
	}
	return err
}

type logStore struct {
	*Log
}

func newLogStore(dir string, config Config) (*logStore, error) {
	log, err := NewLog(dir, config)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l logStore) FirstIndex() (uint64, error) {
	panic("implement me")
}

func (l logStore) LastIndex() (uint64, error) {
	panic("implement me")
}

func (l logStore) GetLog(index uint64, log *raft.Log) error {
	panic("implement me")
}

func (l logStore) StoreLog(log *raft.Log) error {
	panic("implement me")
}

func (l logStore) StoreLogs(logs []*raft.Log) error {
	panic("implement me")
}

func (l logStore) DeleteRange(min, max uint64) error {
	panic("implement me")
}

type fsm struct {
	log *Log
}

func (f fsm) Apply(log *raft.Log) interface{} {
	panic("implement me")
}

func (f fsm) Snapshot() (raft.FSMSnapshot, error) {
	panic("implement me")
}

func (f fsm) Restore(closer io.ReadCloser) error {
	panic("implement me")
}

const RaftRPC = 1

type StreamLayer struct {
	ln net.Listener
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// identify to mux this as a raft rpc
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if bytes.Compare([]byte{byte(RaftRPC)}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}
	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
