package epochscontroller

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/iotexproject/iotex-core/action/protocol/rewarding/rewardingpb"
	"github.com/iotexproject/iotex-core/cli/ioctl/util"
	"github.com/iotexproject/iotex-core/protogen/iotexapi"
	"github.com/iotexproject/iotex-election/committee"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/iotexproject/iotex-election/rewards_tracker/datastore"
	eutil "github.com/iotexproject/iotex-election/util"
)

// Bucket of votes
type Bucket struct {
	ethAddr string
	stakes  string
	votes   string
	bpname  string
}

// RewardsConf defines rewards distribution by type of rewards
type RewardsConf struct {
	ChainBlockRewards string `yaml:"chainBlockRewards"`
	ChainBonusRewards string `yaml:"chainBonusRewards"`
	BlockPercentage   uint64 `yaml:"blockPercentage"`
	EpochPercentage   uint64 `yaml:"epochPercentage"`
	BonusPercentage   uint64 `yaml:"bonusPercentage"`
}

// Config defines the config for server
type Config struct {
	Interval       time.Duration `yaml:"interval"`
	Name           string        `yaml:"name"`
	StartEpoch     uint64        `yaml:"startEpoch"`
	BPAddress      string        `yaml:"bpAddress"`
	RewardsAddress string        `yaml:"rewardsAddress"`
	Rewards        RewardsConf   `yaml:"rewards"`
}

// Server defines the interface of the epochs server
type Server interface {
	Start(context.Context) error
	Stop(context.Context) error
}

// server is used to implement rewards server
type server struct {
	cfg          Config
	ctx          context.Context
	ds           *datastore.DataStore
	cli          iotexapi.APIServiceClient
	committee    committee.Committee
	terminate    chan bool
	currentEpoch uint64
}

// NewServer returns an implementation IoTeX epochs tracker
func NewServer(cfg *Config, cfgCommittee *committee.Config, ds *datastore.DataStore) (Server, error) {
	conn, err := util.ConnectToEndpoint(true)
	if err != nil {
		zap.L().Fatal(err.Error())
	}
	//defer conn.Close()
	committee, err := committee.NewCommittee(nil, *cfgCommittee)
	if err != nil {
		zap.L().Fatal("failed to create committee", zap.Error(err))
	}

	s := &server{
		cfg:          *cfg,
		ctx:          nil,
		ds:           ds,
		cli:          iotexapi.NewAPIServiceClient(conn),
		committee:    committee,
		terminate:    make(chan bool),
		currentEpoch: 0,
	}
	return s, nil
}

func (s *server) Start(ctx context.Context) error {

	s.ctx = ctx
	// Get last height from DB
	startEpoch, err := s.ds.GetLastEpoch()
	if err != nil {
		// use config file if not started yet
		s.currentEpoch = s.cfg.StartEpoch
	} else {
		s.currentEpoch = startEpoch.Epoch
	}

	zap.L().Info("Start syncing", zap.Uint64("Epoch", s.currentEpoch))
	zap.L().Info("catching up via network")
	// get last epoch
	tipEpoch, err := s.TipEpoch()
	zap.L().Info("Current Epoch Tip", zap.Uint64("CurrentTip", tipEpoch))
	if err != nil {
		return errors.Wrap(err, "failed to get tip height")
	}
	if err := s.sync(tipEpoch); err != nil {
		return errors.Wrap(err, "failed to catch up via network")
	}
	zap.L().Info("subscribing to new epoch")
	epochChan := make(chan uint64)
	reportChan := make(chan error)
	go func() {
		for {
			select {
			case <-s.terminate:
				s.terminate <- true
				return
			case epoch := <-epochChan:
				if err := s.Sync(epoch); err != nil {
					zap.L().Error("failed to sync", zap.Error(err))
				}
			case err := <-reportChan:
				zap.L().Error("something goes wrong", zap.Error(err))
			}
		}
	}()
	s.SubscribeNewEpoch(epochChan, reportChan, s.terminate, tipEpoch)
	return nil
}

func (s *server) SubscribeNewEpoch(epoch chan uint64, report chan error, unsubscribe chan bool, lastEpoch uint64) {
	// TODO: Calc timeout by block till next epoch
	ticker := time.NewTicker(s.cfg.Interval * time.Second)
	go func() {
		for {
			select {
			case <-unsubscribe:
				unsubscribe <- true
				return
			case <-ticker.C:
				if tipEpoch, err := s.tipEpoch(lastEpoch); err != nil {
					report <- err
				} else {
					zap.L().Warn("LOG INterval")
					epoch <- tipEpoch
				}
			}
		}
	}()
}

func (s *server) Stop(ctx context.Context) error {
	// TODO: Stop server
	s.terminate <- true
	return nil
}

func (s *server) Sync(epoch uint64) error {
	if err := s.sync(epoch); err != nil {
		return errors.Wrap(err, "failed to catch up via network")
	}
	return nil
}

func (s *server) TipEpoch() (uint64, error) {
	return s.tipEpoch(0)
}

func (s *server) tipEpoch(lastEpoch uint64) (uint64, error) {
	chainRequest := &iotexapi.GetChainMetaRequest{}
	chainResponse, err := s.cli.GetChainMeta(s.ctx, chainRequest)
	if err != nil {
		return 0, err
	}
	num := chainResponse.GetChainMeta().GetEpoch().GetNum()
	if num == 0 {
		return 0, errors.New("failed to get tip epoch")
	}
	if num >= lastEpoch {
		return num, nil
	}
	// Warning if tip lower than last epoch
	zap.L().Warn("Something wrong with last epoch", zap.Uint64("LastEpoch", lastEpoch), zap.Uint64("Epoch", num))
	return num, nil
}

func (s *server) sync(tipEpoch uint64) error {
	if tipEpoch <= s.currentEpoch {
		zap.L().Info("No new updates...")
		return nil
	}
	for nextEpoch := s.currentEpoch + 1; nextEpoch < tipEpoch; nextEpoch++ {
		zap.L().Info("Downloading...", zap.Uint64("Epoch", nextEpoch))
		epochMetaRequest := &iotexapi.GetEpochMetaRequest{EpochNumber: nextEpoch}
		epochMetaResponse, err := s.cli.GetEpochMeta(s.ctx, epochMetaRequest)
		if err != nil {
			zap.L().Fatal(err.Error())
		}
		//ret, _ := prettyjson.Marshal(epochMetaResponse)

		// Load config bases
		epochRewards := datastore.EpochRewards{
			Epoch:             nextEpoch,
			BPName:            s.cfg.Name,
			TotalBPsVotes:     "0",
			TotalVotes:        "0",
			Producer:          false,
			ProducedBlocks:    uint64(0),
			TotalBlockRewards: "0",
			TotalEpochRewards: "0",
			TotalBonusRewards: "0",

			ChainBlockRewards: s.cfg.Rewards.ChainBlockRewards,
			ChainBonusRewards: s.cfg.Rewards.ChainBonusRewards,
			BlockPercentage:   s.cfg.Rewards.BlockPercentage,
			EpochPercentage:   s.cfg.Rewards.EpochPercentage,
			BonusPercentage:   s.cfg.Rewards.BonusPercentage,
		}
		// TODO: get date from Epoch
		epochRewards.Date = time.Now()
		// get producers in epoch
		for _, b := range epochMetaResponse.GetBlockProducersInfo() {
			if b.GetAddress() == s.cfg.BPAddress {
				// if BP active, set blockrewards
				ChainBlockRewards, _ := new(big.Int).SetString(epochRewards.ChainBlockRewards, 10)
				if b.GetActive() {
					epochRewards.ProducedBlocks = b.GetProduction()
					epochRewards.TotalBlockRewards = new(big.Int).Mul(
						big.NewInt(int64(epochRewards.ProducedBlocks)),
						ChainBlockRewards).String()
				}
				// if BP in the 36, set fundation bonus rewards
				epochRewards.TotalBonusRewards = epochRewards.ChainBonusRewards
				epochRewards.Producer = true
				break
			}
		}
		// Get vote rewards from Epoch
		epRewards := s.getRewardsFromEpoch(nextEpoch)
		buckets := s.dump(epochMetaResponse.EpochData.GetGravityChainStartHeight())
		// process rewards
		bps, totalBPsVotes := process(buckets)
		bp0, totalVotes := getBP(s.cfg.Name, bps)
		epochRewards.TotalBPsVotes = totalBPsVotes.String()
		epochRewards.TotalVotes = totalVotes.String()
		epochRewards.TotalEpochRewards = epRewards.String()
		// compute
		accountRewards := epochRewards.CalcRewards(bp0)
		// Insert into DB
		s.ds.InsertEpochRewards(&epochRewards, accountRewards)
		s.currentEpoch = nextEpoch
		s.ds.EpochChannel <- nextEpoch
	}
	return nil
}

func (s *server) getRewardsFromEpoch(epoch uint64) *big.Int {
	// last block from epoch
	lastBlock := epoch * 24 * 15 // numDelegate: 24, subEpoch: 15 -> 360 blocks per epoch
	blockRequest := &iotexapi.GetBlockMetasRequest{
		Lookup: &iotexapi.GetBlockMetasRequest_ByIndex{
			ByIndex: &iotexapi.GetBlockMetasByIndexRequest{
				Start: lastBlock,
				Count: 1,
			},
		},
	}
	blockResponse, err := s.cli.GetBlockMetas(s.ctx, blockRequest)
	if err != nil {
		zap.L().Fatal(err.Error())
	}
	if len(blockResponse.BlkMetas) == 0 {
		zap.L().Fatal("failed to get last block in epoch", zap.Uint64("Epoch", epoch))
	}
	actionsRequest := &iotexapi.GetActionsRequest{
		Lookup: &iotexapi.GetActionsRequest_ByBlk{
			ByBlk: &iotexapi.GetActionsByBlockRequest{
				BlkHash: blockResponse.BlkMetas[0].Hash,
				Start:   uint64(blockResponse.BlkMetas[0].NumActions) - 1,
				Count:   1,
			},
		},
	}
	actionsResponse, err := s.cli.GetActions(s.ctx, actionsRequest)
	if err != nil {
		zap.L().Error("Error getting action", zap.Error(err))
		return big.NewInt(0)
	}
	if len(actionsResponse.ActionInfo) == 0 {
		zap.L().Fatal("failed to get last action in epoch", zap.Uint64("Epoch", epoch))
	}
	if actionsResponse.ActionInfo[0].Action.Core.GetGrantReward() == nil {
		zap.L().Fatal("Not grantReward action")
	}
	receiptRequest := &iotexapi.GetReceiptByActionRequest{
		ActionHash: actionsResponse.ActionInfo[0].ActHash,
	}
	zap.L().Info("Getting receipt of", zap.String("Hash", actionsResponse.ActionInfo[0].ActHash))
	receiptResponse, err := s.cli.GetReceiptByAction(s.ctx, receiptRequest)
	if err != nil {
		zap.L().Fatal(err.Error())
	}
	for _, receiptLog := range receiptResponse.ReceiptInfo.Receipt.Logs {
		var rewardLog rewardingpb.RewardLog
		if err := proto.Unmarshal(receiptLog.Data, &rewardLog); err != nil {
			zap.L().Fatal(err.Error())
		}
		if rewardLog.Type == rewardingpb.RewardLog_EPOCH_REWARD && rewardLog.Addr == s.cfg.RewardsAddress {
			epochReward, ok := new(big.Int).SetString(rewardLog.Amount, 10)
			if !ok {
				zap.L().Fatal("SetString: error")
			}
			return epochReward
		}
	}
	zap.L().Info(fmt.Sprintf("No epoch rewards for %s in epoch %d", s.cfg.RewardsAddress, epoch))
	return big.NewInt(0)
}

func (s *server) dump(height uint64) (buckets []Bucket) {
	result, err := s.committee.FetchResultByHeight(height)
	if err != nil {
		zap.L().Fatal("failed to fetch result")
	}
	for _, delegate := range result.Delegates() {
		for _, vote := range result.VotesByDelegate(delegate.Name()) {
			buckets = append(buckets, Bucket{
				ethAddr: hex.EncodeToString(vote.Voter()),
				stakes:  vote.WeightedAmount().String(),
				votes:   vote.Amount().String(),
				bpname:  string(vote.Candidate()),
			})
		}
	}
	return buckets
}

func process(buckets []Bucket) (bps map[string](map[string]string), totalBPsVotes *big.Int) {
	totalBPsVotes = big.NewInt(0)
	bps = make(map[string](map[string]string))
	for _, bucket := range buckets {
		vs, ok := bps[bucket.bpname]
		if ok {
			// Already have this BP
			_, ook := vs[bucket.ethAddr]
			if ook {
				// Already have this eth addr, need to combine the stakes
				vs[bucket.ethAddr] = eutil.AddStrs(vs[bucket.ethAddr], bucket.stakes)
			} else {
				vs[bucket.ethAddr] = bucket.stakes
			}
			votes, ok := new(big.Int).SetString(bucket.stakes, 10)
			if !ok {
				log.Panic("SetString: error")
			}
			totalBPsVotes.Add(totalBPsVotes, votes)
		} else {
			vs := make(map[string]string)
			vs[bucket.ethAddr] = bucket.stakes
			name := "UNVOTED"
			if len(bucket.bpname) > 0 {
				if strings.Contains(bucket.bpname, "iorobotbp") {
					continue
				}
				name = bucket.bpname

				votes, ok := new(big.Int).SetString(bucket.stakes, 10)
				if !ok {
					log.Panic("SetString: error")
				}
				totalBPsVotes.Add(totalBPsVotes, votes)
			}
			bps[name] = vs
		}
	}
	return bps, totalBPsVotes
}

func getBP(name string, bps map[string](map[string]string)) (map[string]string, *big.Int) {
	var bpByte []byte
	zeroByte := []byte{}
	for i := 0; i < 12-len(name); i++ {
		zeroByte = append(zeroByte, byte(0))
	}
	bpByte = append(zeroByte, []byte(name)...)
	bpHex := string(bpByte)

	bp0, ok := bps[string(bpHex)]
	if !ok {
		zap.L().Error("invalid bp name: " + name)
		return make(map[string]string, 0), new(big.Int).SetInt64(0)
	}
	totalVotes := big.NewInt(0)
	for _, v := range bp0 {
		votes, ok := new(big.Int).SetString(v, 10)
		if !ok {
			log.Panic("SetString: error")
		}
		totalVotes.Add(totalVotes, votes)
	}
	return bp0, totalVotes
}
