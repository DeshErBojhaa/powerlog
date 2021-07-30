package main

import (
	"errors"
	"github.com/DeshErBojhaa/powerlog/internal/agent"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"
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
	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err

	}
	viper.SetConfigFile(configFile)

	if err = viper.ReadInConfig(); err != nil {
		// Only return if err is not due to missing config file
		if !errors.As(err, &viper.ConfigFileNotFoundError{}) {
			return err
		}
	}
	c.cfg.DataDir = viper.GetString("data-dir")
	c.cfg.NodeName = viper.GetString("node-name")
	c.cfg.BindAddr = viper.GetString("bind-addr")
	c.cfg.RPCPort = viper.GetInt("rpc-port")
	c.cfg.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.cfg.Bootstrap = viper.GetBool("bootstrap")
	return nil
}

func (c *cli) run(cmd *cobra.Command, args []string) error {
	a, err := agent.New(c.cfg.Config)
	if err != nil {
		return err
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	return a.Shutdown()
}
