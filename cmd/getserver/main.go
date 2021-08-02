package main

import (
	"context"
	"flag"
	"fmt"
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"google.golang.org/grpc"
	"log"
)

func main() {
	addr := flag.String("addr", ":8400", "service address")
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalln(err)
	}
	client := api.NewLogClient(conn)
	ctx := context.Background()

	res, err := client.GetServers(ctx, &api.GetServerRequest{})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("servers:")
	for _, server := range res.Servers {
		fmt.Printf(" - %v\n", server)
	}
}
