package server

import (
	"context"
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"github.com/DeshErBojhaa/powerlog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"testing"
)

func TestServer(t *testing.T) {
	for desc, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config){
		"produce/consume message to/from log succeeds": testProduceConsume,
		"consume past log boundary fails":              testConsumePastBoundary,
		"produce/consume stream succeeds":              testProduceConsumeStream,
	} {
		// Loop work
		t.Run(desc, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (api.LogClient, *Config, func()) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)

	plog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg := &Config{CommitLog: plog}
	if fn != nil {
		fn(cfg)
	}

	server, err := NewgRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	clientOptions := []grpc.DialOption{grpc.WithInsecure()} // Fix later
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)
	client := api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		_ = cc.Close()
		_ = l.Close()
		_ = plog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{Value: []byte("hello world")}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			}})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}

	// Produce stream
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset %d, want %d", res.Offset, offset)
			}
		}
	}

	// Consume stream
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	require.NoError(t, err)

	for offset, record := range records {
		res, err := stream.Recv()
		require.NoError(t, err)
		require.Equal(t, res.Record, &api.Record{
			Value:  record.Value,
			Offset: uint64(offset),
		})
	}
}
