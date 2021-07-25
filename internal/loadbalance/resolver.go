package loadbalance

import (
	"context"
	"fmt"
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"sync"
)

const Name = "powerlog"

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Builder = (*Resolver)(nil)

func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions) (
	resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalanceingConfig":[{"%s":{}}]}`, Name))
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

func (r *Resolver) Scheme() string {
	return Name
}

// ResolveNow to resolve the target, discover the servers,
// and update the client connection with the servers. How resolver
// will discover the servers depends on resolvers and the service
// they’re working with.
//
// For example, a resolver built for Kubernetes could call Kubernetes’
// API to get the list of endpoints. We create a gRPC client for our
// service and call the GetServers() API to get the cluster’s servers.
func (r *Resolver) ResolveNow(options resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()

	client := api.NewLogClient(r.resolverConn)
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServerRequest{})
	if err != nil {
		r.logger.Error("failed to resolve server", zap.Error(err))
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr:       server.RpcAddr,
			Attributes: attributes.New("is_leader", server.IsLeader),
		})
	}
	if err := r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	}); err != nil {
		r.logger.Error("failed to update state", zap.Error(err))
	}
}

func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error("railed to close conn", zap.Error(err))
	}
}
