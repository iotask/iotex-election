package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"github.com/iotexproject/iotex-election/rewards_tracker/datastore"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	yaml "gopkg.in/yaml.v2"
)

var ds *datastore.DataStore

type Exception struct {
	Message string `json:"message"`
}

func setHeader(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
}

func getAccount(w http.ResponseWriter, r *http.Request) {
	session := ds.Session.Clone()
	defer session.Close()

	vars := mux.Vars(r)
	address := vars["address"]
	acc, err := ds.GetAccount(address)
	if err != nil {
		fmt.Fprintln(w, "Error...")
	} else {
		json.NewEncoder(w).Encode(acc)
	}
}

func getOpenBalance(w http.ResponseWriter, r *http.Request) {
	session := ds.Session.Clone()
	defer session.Close()

	total := big.NewInt(0)
	accounts, _ := ds.GetOpenBalances()
	for _, acc := range accounts {
		value, _ := new(big.Int).SetString(acc.Balance, 10)
		total.Add(total, value)
	}
	f := map[string]interface{}{
		"Total": total.String(),
	}
	json.NewEncoder(w).Encode(f)
}

func startWeb(webPort string) {
	r := mux.NewRouter()

	// Rewards
	r.HandleFunc("/api/open-balance", getOpenBalance).Methods("GET")
	r.HandleFunc("/api/account/{address}", getAccount).Methods("GET")

	if err := http.ListenAndServe(webPort, r); err != nil {
		log.Fatal(err)
	}
}

const (
	defaultHost = "localhost:27017"
)

// Config defines the config for server
type Config struct {
	DB   datastore.DBConfig `yaml:"db"`
	Port int                `yaml:"port"`
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
	ds, err = datastore.NewDataStore(&config.DB)
	if err != nil {
		log.Fatalf("Error connecting to datastore: %v", err)
	}
	defer ds.Session.Close()

	// start API
	startWeb(strconv.Itoa(config.Port))
}
