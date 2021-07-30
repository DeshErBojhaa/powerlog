package main

import (
	"github.com/DeshErBojhaa/powerlog/internal/agent"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"path"
)

func main() {
	cli := &cli{}

	cmd := &cobra.Command{
		Use:     "cli for powerlog",
		PreRunE: cli.setupConfig,
		RunE:    cli.run,
	}

	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	dataDir := path.Join(os.TempDir(), "powerlog")

	cmd.Flags().String("config-file", "", "Path to config file.")
	cmd.Flags().String("data-dir", dataDir, "Directory to store Log and Raft data.")
	cmd.Flags().String("node-name", hostname, "Unique server ID.")
	cmd.Flags().String("bind-addr", "127.0.0.1:8401", "Address to bind Serf on.")
	cmd.Flags().Int("rpc-port", 8400, "Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs", nil, "Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")
	return viper.BindPFlags(cmd.Flags())
}

type cfg struct {
	agent.Config
}

type cli struct {
	cfg cfg
}

func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	return nil
}
