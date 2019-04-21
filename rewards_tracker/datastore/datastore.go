package datastore

import (
	"crypto/tls"
	"net"

	"gopkg.in/mgo.v2"
)

const (
	accountsCollection           = "accounts"
	rewardsCollection            = "rewards"
	epochsCollection             = "epochs"
	payoutTransactionsCollection = "payoutTransactions"
	payoutCollection             = "payout"
)

// DBConfig defines database config yaml
type DBConfig struct {
	Host        string `yaml:"host"`
	MongoURI    string `yaml:"mongoURI"`
	MongoTLS    bool   `yaml:"mongoTLS"`
	DatasetName string `yaml:"datasetName"`
}

// DataStore is used to implement datastore.
type DataStore struct {
	cfg          DBConfig
	Session      *mgo.Session
	EpochChannel chan uint64
}

// NewDataStore creates the main session to our mongodb instance
func NewDataStore(cfg *DBConfig) (*DataStore, error) {
	var session *mgo.Session
	var err error
	if len(cfg.MongoURI) > 0 {
		dialInfo, err := mgo.ParseURL(cfg.MongoURI)
		if err != nil {
			return nil, err
		}
		// TLS connection option
		if cfg.MongoTLS {
			tlsConfig := &tls.Config{}
			dialInfo.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
				conn, err := tls.Dial("tcp", addr.String(), tlsConfig)
				return conn, err
			}
		}
		session, err = mgo.DialWithInfo(dialInfo)
	} else {
		session, err = mgo.Dial(cfg.Host)
	}

	session.SetMode(mgo.Monotonic, true)
	s := &DataStore{
		cfg:          *cfg,
		Session:      session,
		EpochChannel: make(chan uint64),
	}
	if err == nil {
		s.CreateIndex()
	}
	return s, err
}

// CreateIndex initiate DB indexes
func (s *DataStore) CreateIndex() error {
	session := s.Session.Clone()
	defer session.Close()
	// Accounts Index
	for _, key := range []string{"ioaddress"} {
		index := mgo.Index{
			Key:        []string{key},
			Background: true,
		}
		if err := session.DB(s.cfg.DatasetName).C(accountsCollection).EnsureIndex(index); err != nil {
			return err
		}
	}

	// Epochs Index
	for _, key := range []string{"epoch"} {
		index := mgo.Index{
			Key:        []string{key},
			Background: true,
		}
		if err := session.DB(s.cfg.DatasetName).C(epochsCollection).EnsureIndex(index); err != nil {
			return err
		}
	}
	// Payout Index
	for _, key := range []string{"Epoch", "date"} {
		index := mgo.Index{
			Key:        []string{key},
			Background: true,
		}
		if err := session.DB(s.cfg.DatasetName).C(payoutCollection).EnsureIndex(index); err != nil {
			return err
		}
	}
	// PayoutTransactions Index
	for _, key := range []string{"payout_id", "hash", "ioaddress", "date"} {
		index := mgo.Index{
			Key:        []string{key},
			Background: true,
		}
		if err := session.DB(s.cfg.DatasetName).C(payoutTransactionsCollection).EnsureIndex(index); err != nil {
			return err
		}
	}
	return nil
}
