package server

import api "github.com/DeshErBojhaa/powerlog/api/v1"

//CommitLog describes the functionality for the gRPC server.
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Config struct {
	CommitLog CommitLog
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

var _ api.LogServer = (*grpcServer)(nil)

func newgrpcServer(config *Config) (*grpcServer, error) {
	srv := &grpcServer{
		Config: config,
	}
	return srv, nil
}
