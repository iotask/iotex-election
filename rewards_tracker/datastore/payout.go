package datastore

import (
	"time"

	"gopkg.in/mgo.v2/bson"
)

// Payout set
type Payout struct {
	Epoch             uint64
	Date              time.Time
	TotalTransactions uint64
	TotalRewards      string
}

// PayoutHistory set
type PayoutHistory struct {
	PayoutID  bson.ObjectId
	IOAddress string
	Hash      string
	Amount    string
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
