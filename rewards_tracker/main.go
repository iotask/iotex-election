package main

import (
	"context"
	"flag"
	"io/ioutil"
	"log"

	"github.com/iotexproject/iotex-election/committee"
	"github.com/iotexproject/iotex-election/rewards_tracker/datastore"
	"github.com/iotexproject/iotex-election/rewards_tracker/epochscontroller"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	yaml "gopkg.in/yaml.v2"
)

// Config defines the config for server
type Config struct {
	DB               datastore.DBConfig      `yaml:"db"`
	Port             int                     `yaml:"port"`
	EpochsController epochscontroller.Config `yaml:"epochsController"`
	Committee        committee.Config        `yaml:"committee"`
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "config.yaml", "path of server config file")
	flag.Parse()

	zapCfg := zap.NewDevelopmentConfig()
	zapCfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	zapCfg.Level.SetLevel(zap.InfoLevel)
	l, err := zapCfg.Build()
	if err != nil {
		log.Panic("Failed to init zap global logger, no zap log will be shown till zap is properly initialized: ", err)
	}
	zap.ReplaceGlobals(l)

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		zap.L().Fatal("failed to load config file", zap.Error(err))
	}
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		zap.L().Fatal("failed to unmarshal config", zap.Error(err))
	}
	// DB Connection
	ds, err := datastore.NewDataStore(&config.DB)
	if err != nil {
		log.Fatalf("Error connecting to datastore: %v", err)
	}
	defer ds.Session.Close()

	// Create epochs controller
	epochsController, err := epochscontroller.NewServer(&config.EpochsController, &config.Committee, ds)
	if err != nil {
		zap.L().Fatal("failed to instantiate Epochs Controller", zap.Error(err))
	}
	zap.L().Info("New Epoch Controller created")
	if err := epochsController.Start(context.Background()); err != nil {
		zap.L().Fatal("failed to start ranking server", zap.Error(err))
	}
	zap.L().Info("Service started")
	defer epochsController.Stop(context.Background())

	select {}
}
