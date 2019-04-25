package datastore

import (
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
