package log

import (
	"bytes"
	"fmt"
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"github.com/hashicorp/raft"
	raftBoltDB "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
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
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		dl.config.Raft.StreamLayer,
		maxPool,
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

func (dl *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := dl.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

func (dl *DistributedLog) apply(reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	future := dl.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func (dl *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return dl.log.Read(offset)
}

func (dl *DistributedLog) Join(id, addr string) error {
	configFuture := dl.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID != serverID && srv.Address != serverAddr {
			continue
		}
		if srv.ID == serverID && srv.Address == serverAddr {
			// Already joined
			return nil
		}
		// Remove the existing server
		removeFuture := dl.raft.RemoveServer(serverID, 0, 0)
		if err := removeFuture.Error(); err != nil {
			return err
		}
	}
	addFuture := dl.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

func (dl *DistributedLog) Leave(id string) error {
	removeFuture := dl.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (dl *DistributedLog) WaitFOrLeader(timeout time.Duration) error {
	timeoutCh := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCh:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := dl.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (dl *DistributedLog) Close() error {
	future := dl.raft.Shutdown()
	if err := future.Error(); err != nil {
		return err
	}
	return dl.log.Close()
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
	return l.LowestOffset()
}

func (l logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l logStore) GetLog(index uint64, ret *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	ret.Data = in.Value
	ret.Index = in.Offset
	ret.Type = raft.LogType(in.Type)
	ret.Term = in.Term
	return nil
}

func (l logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(
			&api.Record{
				Value: record.Data,
				Term:  record.Term,
				Type:  uint32(record.Type),
			}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange removes the records between the offsets.
func (l logStore) DeleteRange(min, max uint64) error {
	_ = min
	return l.Truncate(max)
}

type fsm struct {
	log *Log
}

var _ raft.FSM = (*fsm)(nil)

type RequestType uint8

const AppendRequestType RequestType = 0

func (f fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

func (f fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

func (f fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(encoding.Uint64(b))
		if _, err = io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

func (f fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}

const RaftRPC = 1

type StreamLayer struct {
	ln net.Listener
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

func NewStreamLayer(ln net.Listener) *StreamLayer {
	return &StreamLayer{ln: ln}
}

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
