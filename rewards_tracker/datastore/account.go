package datastore

import (
	"strings"
	"time"

	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"

	eutil "github.com/iotexproject/iotex-election/util"
)

// Account set
type Account struct {
	Name              string
	ETHAddress        string
	IOAddress         string
	LastStake         string
	LastParticipation float32
	Balance           string
	Update            time.Time
}

// AccountRewards set
type AccountRewards struct {
	Epoch         uint64
	ETHAddress    string
	IOAddress     string
	Stake         string
	Participation float32

	BlockPercentage uint64
	EpochPercentage uint64
	BonusPercentage uint64
	BlockRewards    string
	EpochRewards    string
	BonusRewards    string
	ExtraRewards    string
	Date            time.Time
	Update          time.Time
}

// AddToAccount value to Account valance
func (s *DataStore) AddToAccount(reward AccountRewards) error {
	session := s.Session.Clone()
	defer session.Close()
	totalRewards := reward.GetTotalRewards()
	if totalRewards == "0" {
		return nil
	}
	zap.L().Info("Adding rewards to account", zap.String("IOAddress", reward.IOAddress),
		zap.String("BlockRewards", reward.BlockRewards), zap.String("EpochRewards", reward.EpochRewards),
		zap.String("BonusRewards", reward.BonusRewards))

	result := new(Account)
	err := session.DB(s.cfg.DatasetName).C(accountsCollection).Find(bson.M{"ioaddress": reward.IOAddress}).One(&result)
	if err != nil {

		if err.Error() == "not found" {
			result = &Account{IOAddress: reward.IOAddress, ETHAddress: reward.ETHAddress,
				Balance: totalRewards, LastStake: reward.Stake, LastParticipation: reward.Participation,
				Update: time.Now()}
			return session.DB(s.cfg.DatasetName).C(accountsCollection).Insert(result)
		}
		zap.L().Fatal("Error adding to account", zap.Error(err))
	}

	totalRewards = eutil.AddStrs(totalRewards, result.Balance)
	return session.DB(s.cfg.DatasetName).C(accountsCollection).Update(bson.M{"ioaddress": reward.IOAddress},
		bson.M{"$set": bson.M{"balance": totalRewards, "laststake": reward.Stake,
			"lastparticipation": reward.Participation, "update": time.Now()}})
}

// AddToBalance add to existent account balance
func (s *DataStore) AddToBalance(ioAddr string, amount string) error {
	session := s.Session.Clone()
	defer session.Close()

	result := new(Account)
	err := session.DB(s.cfg.DatasetName).C(accountsCollection).Find(bson.M{"ioaddress": ioAddr}).One(&result)
	if err != nil {
		zap.L().Fatal("Error adding to account", zap.Error(err))
	}
	newAmount := eutil.AddStrs(amount, result.Balance)
	return session.DB(s.cfg.DatasetName).C(accountsCollection).Update(bson.M{"ioaddress": ioAddr},
		bson.M{"$set": bson.M{"balance": newAmount, "update": time.Now()}})
}

// GetTotalRewards return sum of Block,Epoch and Bonus rewards in string
func (reward *AccountRewards) GetTotalRewards() string {
	totalRewards := eutil.AddStrs(reward.BlockRewards, reward.EpochRewards)
	totalRewards = eutil.AddStrs(totalRewards, reward.BonusRewards)
	return totalRewards
}

// GetOpenBalances get accounts with open balance
func (s *DataStore) GetOpenBalances() ([]Account, error) {
	session := s.Session.Clone()
	defer session.Close()

	var result []Account
	err := session.DB(s.cfg.DatasetName).C(accountsCollection).Find(bson.M{"balance": bson.M{"$ne": "0"}}).All(&result)
	if err != nil {
		zap.L().Error("Error loading open balance accounts", zap.Error(err))
		return nil, err
	}
	return result, nil
}

// GetAccount get account by address
func (s *DataStore) GetAccount(address string) (Account, error) {
	session := s.Session.Clone()
	defer session.Close()
	query := bson.M{}
	if strings.HasPrefix(address, "io") {
		query = bson.M{"ioaddress": address}
	} else {
		query = bson.M{"ethaddress": strings.ToLower(strings.TrimPrefix(address, "0x"))}
	}

	var result Account
	err := session.DB(s.cfg.DatasetName).C(accountsCollection).Find(query).One(&result)
	if err != nil {
		zap.L().Error("Account not found", zap.Error(err))
	}
	return result, nil
}

// GetRewards get account by address
func (s *DataStore) GetRewards(address string) ([]interface{}, error) {
	session := s.Session.Clone()
	defer session.Close()
	query := bson.M{}
	if strings.HasPrefix(address, "io") {
		query = bson.M{"ioaddress": address}
	} else {
		query = bson.M{"ethaddress": strings.ToLower(strings.TrimPrefix(address, "0x"))}
	}

	var result []interface{}
	err := session.DB(s.cfg.DatasetName).C(rewardsCollection).Find(query).Sort("-epoch").Limit(100).All(&result)
	if err != nil {
		zap.L().Error("Account not found", zap.Error(err))
	}
	return result, nil
}
