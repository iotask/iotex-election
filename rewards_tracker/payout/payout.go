package payout

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/golang/protobuf/proto"
	"github.com/iotexproject/iotex-address/address"
	"github.com/iotexproject/iotex-core/action"
	"github.com/iotexproject/iotex-core/action/protocol/rewarding"
	"github.com/iotexproject/iotex-core/cli/ioctl/cmd/account"
	"github.com/iotexproject/iotex-core/cli/ioctl/util"
	"github.com/iotexproject/iotex-core/pkg/hash"
	"github.com/iotexproject/iotex-core/pkg/keypair"
	"github.com/iotexproject/iotex-core/pkg/util/byteutil"
	"github.com/iotexproject/iotex-core/protogen/iotexapi"
	"github.com/iotexproject/iotex-election/rewards_tracker/datastore"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

// PayoutConf defines payout
type PayoutConf struct {
	Interval   uint64 `yaml:"interval"`
	MinAmount  string `yaml:"minAmount"`
	Claim      bool   `yaml:"claim"`
	PrivateKey string `yaml:"privateKey"`
}

// PayoutServer defines the interface of the epochs server
type PayoutServer interface {
	Start(context.Context) error
	Stop(context.Context) error
}

// payoutServer is used to implement payout server
type payoutServer struct {
	cfg             PayoutConf
	ctx             context.Context
	ds              *datastore.DataStore
	cli             iotexapi.APIServiceClient
	terminate       chan bool
	lastPayoutEpoch uint64
}

// NewServer returns an implementation IoTeX epochs tracker
func NewServer(cfg *PayoutConf, ds *datastore.DataStore) (PayoutServer, error) {
	conn, err := util.ConnectToEndpoint(true)
	if err != nil {
		zap.L().Fatal(err.Error())
	}
	s := &payoutServer{
		cfg:             *cfg,
		ctx:             nil,
		ds:              ds,
		cli:             iotexapi.NewAPIServiceClient(conn),
		terminate:       make(chan bool),
		lastPayoutEpoch: uint64(0),
	}
	return s, nil
}

func (s *payoutServer) Start(ctx context.Context) error {
	// Get last height from DB
	lastPayout, err := s.ds.GetLastPayout()
	if err != nil {
		// use config file if not started yet
		zap.L().Error("Cannot find last payout", zap.Error(err))
	} else {
		s.lastPayoutEpoch = lastPayout.Epoch
	}
	s.ctx = ctx

	go func() {
		for {
			select {
			case <-s.terminate:
				s.terminate <- true
				return
			case epoch := <-s.ds.EpochChannel:
				if (epoch - s.lastPayoutEpoch) > s.cfg.Interval {
					zap.L().Info("Staring payout", zap.Uint64("PayoutEpoch", epoch))
					s.RunPayout(epoch)
					s.lastPayoutEpoch = epoch
				}
			}
		}
	}()

	return nil
}
func (s *payoutServer) RunPayout(epoch uint64) error {
	accs, err := s.ds.GetOpenOpenBalance()
	if err != nil {
		return err
	}
	// Sum total
	total := new(big.Int)
	toPay := make([]datastore.PayoutHistory, len(accs))
	minPayput, _ := new(big.Int).SetString(s.cfg.MinAmount, 10)
	for _, acc := range accs {
		// check min amount
		amount, _ := new(big.Int).SetString(acc.Balance, 10)
		if amount.Cmp(minPayput) >= 0 {
			toPay = append(toPay, datastore.PayoutHistory{IOAddress: acc.IOAddress,
				Amount: acc.Balance})
			total.Add(total, amount)
		}
	}
	// load PK
	prvKey, err := keypair.HexStringToPrivateKey(s.cfg.PrivateKey)
	if err != nil {
		zap.L().Error("Error loading private key from string", zap.Error(err))
	}
	addr, err := address.FromBytes(prvKey.PublicKey().Hash())
	if err != nil {
		zap.L().Error("Error getting address private key from string", zap.Error(err))
	}
	zap.L().Info("Address", zap.String("Address", addr.String()))

	// Check Rewards Balance
	accountRequest := &iotexapi.GetAccountRequest{Address: addr.String()}
	accountResponse, err := s.cli.GetAccount(s.ctx, accountRequest)
	balance := accountResponse.GetAccountMeta().Balance
	// Check Claim Rewards
	if s.cfg.Claim {
		unclaimedReward, err := s.getReward(addr.String())
		if err != nil {
			zap.L().Error("Cannot check unclaimed rewards balance", zap.Error(err))
		}
		if unclaimedReward.Cmp(new(big.Int).SetInt64(0)) > 0 {
			zap.L().Info("Rewards claim", zap.Uint64("UnclaimedReward", unclaimedReward.Uint64()))
			shash, _ := s.claim(unclaimedReward, 350000, "", addr.String(), prvKey)
			// TODO: Insert claim to DB
			// TODO: Daley for rewards to relflat in balance
		} else {
			zap.L().Info("No rewards to claim", zap.Uint64("UnclaimedReward", unclaimedReward.Uint64()))
		}
	}
	zap.L().Info("Pay run", zap.Uint64("Total", total.Uint64()),
		zap.Int("Accounts", len(toPay)), zap.String("CurrentBalance", balance))
	// TODO: Add payout to DB

	// TODO: Do payout
	for _, p := range toPay {
		_ = p
	}

	s.lastPayoutEpoch = epoch
	return nil
}

func (s *payoutServer) Stop(ctx context.Context) error {
	// TODO: Stop server
	s.terminate <- true
	return nil
}

func (s *payoutServer) getReward(address string) (*big.Int, error) {
	request := &iotexapi.ReadStateRequest{
		ProtocolID: []byte(rewarding.ProtocolID),
		MethodName: []byte("UnclaimedBalance"),
		Arguments:  [][]byte{[]byte(address)},
	}
	response, err := s.cli.ReadState(s.ctx, request)
	if err != nil {
		sta, ok := status.FromError(err)
		if ok {
			return nil, fmt.Errorf(sta.Message())
		}
		return nil, err
	}
	rewardRau, ok := big.NewInt(0).SetString(string(response.Data), 10)
	if !ok {
		return nil, fmt.Errorf("failed to convert string into big int")
	}
	return rewardRau, nil
}

func (s *payoutServer) claim(amount *big.Int, gasLimit uint64, data string,
	signer string, prvKey keypair.PrivateKey) (string, error) {
	payload := []byte(data)
	if gasLimit == 0 {
		gasLimit = action.ClaimFromRewardingFundBaseGas +
			action.ClaimFromRewardingFundGasPerByte*uint64(len(payload))
	}

	request := &iotexapi.SuggestGasPriceRequest{}
	response, err := s.cli.SuggestGasPrice(s.ctx, request)
	if err != nil {
		return "", err
	}
	gasPriceRau := new(big.Int).SetUint64(response.GasPrice)

	accountMeta, err := account.GetAccountMeta(signer)
	if err != nil {
		return "", err
	}
	nonce := accountMeta.PendingNonce

	b := &action.ClaimFromRewardingFundBuilder{}
	act := b.SetAmount(amount).SetData(payload).Build()
	bd := &action.EnvelopeBuilder{}
	elp := bd.SetNonce(nonce).
		SetGasPrice(gasPriceRau).
		SetGasLimit(gasLimit).
		SetAction(&act).Build()

	sealed, err := action.Sign(elp, prvKey)
	if err != nil {
		zap.L().Error("failed to sign action", zap.Error(err))
		return "", err
	}
	selp := sealed.Proto()

	requestBroadcast := &iotexapi.SendActionRequest{Action: selp}
	_, err = s.cli.SendAction(s.ctx, requestBroadcast)
	if err != nil {
		sta, ok := status.FromError(err)
		if ok {
			return "", fmt.Errorf(sta.Message())
		}
		return "", err
	}
	shash := hash.Hash256b(byteutil.Must(proto.Marshal(selp)))
	return hex.EncodeToString(shash[:]), nil
}
