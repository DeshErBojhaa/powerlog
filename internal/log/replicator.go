package log

import (
	"context"
	"fmt"
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"sync"
)

type replicator struct {
	mu sync.Mutex

	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

func (r *replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])
	return nil
}

func (r *replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logger.Error(fmt.Sprintf("%v failed to dial %s", err, addr))
	}
	defer cc.Close()

	ctx := context.Background()
	client := api.NewLogClient(cc)
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		r.logger.Error(fmt.Sprintf("%v failed to consume %s", err, addr))
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logger.Error(fmt.Sprintf("%v failed to receive %s", err, addr))
				return
			}
			records <- recv.Record
		}
	}()

	for true {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				r.logger.Error(fmt.Sprintf("%v failed to produce %s", err, addr))
				return
			}

		}
	}
}

func (r *replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

func (r *replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

// Leave method handles the server leaving the cluster by removing
// the server from the list of servers to replicate and closes the
// server’s associated channel.
func (r *replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}
