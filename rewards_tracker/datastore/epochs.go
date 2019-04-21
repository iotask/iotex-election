package datastore

import (
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/iotexproject/iotex-address/address"
	"go.uber.org/zap"
	"gopkg.in/mgo.v2/bson"
)

// EpochRewards set
type EpochRewards struct {
	Epoch          uint64
	BPName         string
	TotalBPsVotes  string
	TotalVotes     string
	Producer       bool
	ProducedBlocks uint64

	TotalBlockRewards string
	TotalEpochRewards string
	TotalBonusRewards string
	BlockPercentage   uint64
	EpochPercentage   uint64
	BonusPercentage   uint64

	ChainBlockRewards string
	ChainBonusRewards string
	Date              time.Time
	Update            time.Time
}

func (e *EpochRewards) newReward() (reward AccountRewards) {
	reward.Epoch = e.Epoch
	reward.BlockPercentage = e.BlockPercentage
	reward.EpochPercentage = e.EpochPercentage
	reward.BonusPercentage = e.BonusPercentage
	reward.Date = e.Date
	return reward
}

// CalcRewards Calc staked rewards based on EpochRewards set and bp0 list
func (e *EpochRewards) CalcRewards(bp0 map[string]string) []interface{} {
	// calculate
	TotalBlockRewards, _ := new(big.Int).SetString(e.TotalBlockRewards, 10)
	TotalEpochRewards, _ := new(big.Int).SetString(e.TotalEpochRewards, 10)
	TotalBonusRewards, _ := new(big.Int).SetString(e.TotalBonusRewards, 10)
	blockPayout := new(big.Int).Div(new(big.Int).Mul(TotalBlockRewards, big.NewInt(int64(e.BlockPercentage))), big.NewInt(100))
	epochPayout := new(big.Int).Div(new(big.Int).Mul(TotalEpochRewards, big.NewInt(int64(e.EpochPercentage))), big.NewInt(100))
	bonusPayout := new(big.Int).Div(new(big.Int).Mul(TotalBonusRewards, big.NewInt(int64(e.BonusPercentage))), big.NewInt(100))

	//var accountRewards []AccountRewards
	accountRewards := make([]interface{}, 0, len(bp0))
	for k, v := range bp0 {
		votes, ok := new(big.Int).SetString(v, 10)
		if !ok {
			log.Panic("SetString: error")
		}
		if votes.Cmp(new(big.Int).SetInt64(0)) <= 0 {
			continue
		}
		reward := e.newReward()
		reward.ETHAddress = k
		reward.IOAddress = toIoAddr(k)

		TotalVotes, _ := new(big.Int).SetString(e.TotalVotes, 10)
		if TotalVotes.Cmp(new(big.Int).SetInt64(0)) > 0 {
			participation := new(big.Float).Quo(new(big.Float).SetInt(votes), new(big.Float).SetInt(TotalVotes))
			reward.Participation, _ = participation.Float32()
			reward.BlockRewards = new(big.Int).Div(new(big.Int).Mul(votes, blockPayout), TotalVotes).String()
			reward.EpochRewards = new(big.Int).Div(new(big.Int).Mul(votes, epochPayout), TotalVotes).String()
			reward.BonusRewards = new(big.Int).Div(new(big.Int).Mul(votes, bonusPayout), TotalVotes).String()
		}

		reward.Stake = votes.String()
		reward.Update = time.Now()
		accountRewards = append(accountRewards, reward)

	}
	return accountRewards
}

func toIoAddr(addr string) string {
	ethAddr := common.HexToAddress(addr)
	pkHash := ethAddr.Bytes()
	ioAddr, _ := address.FromBytes(pkHash)
	return ioAddr.String()
}

// InsertEpochRewards insert rewards into DB and update account balance
func (s *DataStore) InsertEpochRewards(e *EpochRewards, r []interface{}) error {
	// TODO: create transcantion to rollback if any error
	zap.L().Info("Addding rewards to DB")
	session := s.Session.Clone()
	defer session.Close()

	if len(r) > 0 {
		bulk := session.DB(s.cfg.DatasetName).C(rewardsCollection).Bulk()
		bulk.Unordered()
		bulk.Insert(r...)
		_, err := bulk.Run()
		if err != nil {
			zap.L().Error("Error on bulk insert rewards", zap.Error(err))
			return err
		}
	}

	e.Update = time.Now()
	err := session.DB(s.cfg.DatasetName).C(epochsCollection).Insert(e)
	if err != nil {
		zap.L().Error("Error on epoch result insert", zap.Error(err))
		return err
	}
	for _, data := range r {
		err = s.AddToAccount(data.(AccountRewards))
		if err != nil {
			zap.L().Fatal("Fail to add into account", zap.String("IOAddress", data.(AccountRewards).IOAddress))
		}
	}

	return nil
}

// GetLastEpoch get last epoch rewards
func (s *DataStore) GetLastEpoch() (*EpochRewards, error) {
	session := s.Session.Clone()
	defer session.Close()
	result := new(EpochRewards)
	err := session.DB(s.cfg.DatasetName).C(epochsCollection).Find(bson.M{}).Limit(1).Sort("-epoch").One(&result)
	if err != nil {
		return nil, err
	}
	return result, err
}
