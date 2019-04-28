package datastore

import (
	"strings"
	"time"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"
)

// Payout set
type Payout struct {
	ID                bson.ObjectId `json:"id" bson:"_id"`
	Epoch             uint64
	Date              time.Time
	TotalTransactions uint64
	TotalRewards      string
}

// PayoutTransaction set
type PayoutTransaction struct {
	PayoutID   bson.ObjectId
	IOAddress  string
	ETHAddress string
	Amount     string
	Hash       string
	Network    string
}

// GetLastPayout get last epoch rewards
func (s *DataStore) GetLastPayout() (*Payout, error) {
	session := s.Session.Clone()
	defer session.Close()
	result := new(Payout)
	err := session.DB(s.cfg.DatasetName).C(payoutCollection).Find(bson.M{}).Limit(1).Sort("-epoch").One(&result)
	if err != nil {
		return nil, err
	}
	return result, err
}

// InsertPayout add payout summary to db
func (s *DataStore) InsertPayout(payout *Payout) (*bson.ObjectId, error) {
	session := s.Session.Clone()
	defer session.Close()
	id := bson.NewObjectId()
	payout.ID = id
	err := session.DB(s.cfg.DatasetName).C(payoutCollection).Insert(payout)
	if err != nil {
		zap.L().Error("err", zap.Error(err))
		return nil, err
	}
	return &id, nil
}

// InsertPayoutTransactions insert payout transactions into DB
func (s *DataStore) InsertPayoutTransactions(p []interface{}) error {
	zap.L().Info("Addding payout transactions to DB")
	session := s.Session.Clone()
	defer session.Close()

	if len(p) > 0 {
		bulk := session.DB(s.cfg.DatasetName).C(payoutTransactionsCollection).Bulk()
		bulk.Unordered()
		bulk.Insert(p...)
		_, err := bulk.Run()
		if err != nil {
			zap.L().Fatal("Error on bulk insert payout transactions", zap.Error(err))
			return err
		}
	}

	for k, _ := range p {
		info := p[k].(*PayoutTransaction)
		err := s.AddToBalance(info.IOAddress, "-"+info.Amount)
		if err != nil {
			zap.L().Fatal("Fail to add balance into account", zap.String("IOAddress", info.IOAddress),
				zap.String("Payout ID", info.PayoutID.String()))
		}
	}
	return nil
}

// GetPayout get last epoch rewards
func (s *DataStore) GetPayout(address string) ([]interface{}, error) {
	session := s.Session.Clone()
	defer session.Close()
	query := bson.M{}
	if strings.HasPrefix(address, "io") {
		query = bson.M{"ioaddress": address}
	} else {
		query = bson.M{"ethaddress": strings.ToLower(strings.TrimPrefix(address, "0x"))}
	}

	pipeline := []bson.M{
		{"$match": query},
		{"$lookup": bson.M{"from": "payout",
			"localField":   "payoutid",
			"foreignField": "_id",
			"as":           "payout",
		}},
		{"$unwind": "$payout"},
		{"$project": bson.M{"ethaddress": 1,
			"ioaddress":    1,
			"hash":         1,
			"network":      1,
			"amount":       1,
			"payout.date":  1,
			"payout.epoch": 1,
		}},
		{"$sort": bson.D{{"payout.epoch", -1}}},
	}
	var result []interface{}
	err := session.DB(s.cfg.DatasetName).C(payoutTransactionsCollection).Pipe(pipeline).All(&result)
	if err != nil {
		zap.L().Error("Fail to get address payout", zap.Error(err))
		return nil, err
	}
	return result, err
}
