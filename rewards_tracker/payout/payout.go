package payout

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto/sha3"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/golang/protobuf/proto"
	"github.com/iotexproject/iotex-address/address"
	"github.com/iotexproject/iotex-core/action"
	"github.com/iotexproject/iotex-core/cli/ioctl/cmd/alias"
	"github.com/iotexproject/iotex-core/cli/ioctl/util"
	"github.com/iotexproject/iotex-core/pkg/hash"
	"github.com/iotexproject/iotex-core/pkg/keypair"
	"github.com/iotexproject/iotex-core/pkg/log"
	"github.com/iotexproject/iotex-core/pkg/util/byteutil"
	"github.com/iotexproject/iotex-core/protogen/iotexapi"
	"github.com/iotexproject/iotex-core/protogen/iotextypes"
	"github.com/iotexproject/iotex-election/rewards_tracker/datastore"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

// PayoutConf defines payout
type PayoutConf struct {
	Interval        uint64 `yaml:"interval"`
	MinAmount       string `yaml:"minAmount"`
	Claim           bool   `yaml:"claim"`
	GasLimit        uint64 `yaml:"gasLimit"`
	PrivateKey      string `yaml:"privateKey"`
	IOPayout        bool   `yaml:"ioPayout"`
	EthAPI          string `yaml:"ethAPI"`
	EthPrivateKey   string `yaml:"ethPrivateKey"`
	EthToken        string `yaml:"ethToken"`
	EthGasLimit     uint64 `yaml:"ethGasLimit"`
	EthSendInterval uint64 `yaml:"ethSendInterval"`
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
				if (s.cfg.Interval > 0) && ((epoch - s.lastPayoutEpoch) >= s.cfg.Interval) {
					zap.L().Info("Staring payout", zap.Uint64("PayoutEpoch", epoch))
					s.RunPayout(epoch)
					s.lastPayoutEpoch = epoch
				}
			}
		}
	}()

	return nil
}

func (s *payoutServer) Stop(ctx context.Context) error {
	// TODO: Stop server
	s.terminate <- true
	return nil
}

func (s *payoutServer) RunPayout(epoch uint64) error {
	accs, err := s.ds.GetOpenBalances()
	if err != nil {
		return err
	}
	// Sum total
	total := new(big.Int)
	toPay := make([]interface{}, 0, len(accs))
	minPayput, _ := new(big.Int).SetString(s.cfg.MinAmount, 10)
	for _, acc := range accs {
		// check min amount
		amount, _ := new(big.Int).SetString(acc.Balance, 10)
		if amount.Cmp(minPayput) >= 0 {
			toPay = append(toPay, &datastore.PayoutTransaction{IOAddress: acc.IOAddress,
				ETHAddress: acc.ETHAddress, Amount: acc.Balance})
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

	// Check Rewards Balance && load ETH keys if needed
	var balance string
	var ethClient *ethclient.Client
	var prvKeyETH keypair.PrivateKey
	if !s.cfg.IOPayout {
		ethClient, err = ethclient.Dial(s.cfg.EthAPI)
		if err != nil {
			zap.L().Fatal("Cannot dial to ETH", zap.Error(err))
		}
		prvKeyETH, _ = keypair.HexStringToPrivateKey(s.cfg.EthPrivateKey)
		// TODO: get ETH token balance
		balance = "1000000000000000000000000"
	} else {
		balance, err = s.getAccountBalance(addr.String())
		if err != nil {
			zap.L().Error("Error checking IO account current balance", zap.Error(err))
		}
	}
	zap.L().Info("Current Balance", zap.String("Balance", balance))

	// Check Claim Rewards
	if s.cfg.Claim && !s.cfg.IOPayout {
		unclaimedReward, err := s.unclaimedBalance(addr.String())
		if err != nil {
			zap.L().Error("Cannot check unclaimed rewards balance", zap.Error(err))

		} else {
			if unclaimedReward.Cmp(new(big.Int).SetInt64(0)) > 0 {
				zap.L().Info("Rewards claim", zap.String("UnclaimedReward", unclaimedReward.String()))
				cHash, _ := s.claim(unclaimedReward, s.cfg.GasLimit, "", addr.String(), prvKey)
				zap.L().Info("Claim Hash", zap.String("ClaimHash", cHash))
				// wait from transaction to get in
				claimReceipt, err := s.waitForReceipt(cHash)
				if err != nil {
					zap.L().Error("Could not confirm claim transaction", zap.Error(err))
				}
				// Insert claim into DB
				s.ds.InsertClaim(&datastore.Claim{Amount: unclaimedReward.String(), Hash: cHash,
					Receipt: claimReceipt.String(), Date: time.Now()})
				// Check if balance is updated
				balanceUpdate, err := s.getAccountBalance(addr.String())
				if err != nil {
					zap.L().Error("Error checking account current balance", zap.Error(err))
				} else {
					balance = balanceUpdate
				}
			} else {
				zap.L().Info("No rewards to claim", zap.Uint64("UnclaimedReward", unclaimedReward.Uint64()))
			}
		}
	}
	// Check if balance is enough
	balanceN, _ := new(big.Int).SetString(balance, 10)
	if total.Cmp(balanceN) >= 0 {
		zap.L().Error("Current balance smaller than total payout", zap.String("Balance", balanceN.String()),
			zap.String("TotalPay", total.String()), zap.String("NetBalance", new(big.Int).Sub(balanceN, total).String()))
		return errors.New("Current balance smaller than total payout")
	}
	zap.L().Info("Pay run", zap.String("Total", total.String()),
		zap.Int("Accounts", len(toPay)), zap.String("CurrentBalance", balanceN.String()),
		zap.String("NetBalance", new(big.Int).Sub(balanceN, total).String()))
	// Add payout summary into DB
	id, err := s.ds.InsertPayout(&datastore.Payout{Epoch: epoch, Date: time.Now(),
		TotalTransactions: uint64(len(toPay)), TotalRewards: total.String()})
	if err != nil {
		zap.L().Fatal("Cannot add Payout into DB", zap.Error(err))
	}

	for k := range toPay {
		p := toPay[k].(*datastore.PayoutTransaction)
		p.PayoutID = *id
		amount, _ := new(big.Int).SetString(p.Amount, 10)
		// Check if IoTeX or ETH payout
		if s.cfg.IOPayout {
			tHash, err := s.ioTransfer(p.IOAddress, "", amount, prvKey)
			if err != nil {
				zap.L().Error("Error transfering...", zap.Error(err))
				p.Amount = "0"
			}
			p.Hash = tHash
			p.Network = "IoTeX"
		} else {
			tHash, err := s.ethTransfer(ethClient, p.ETHAddress, amount, prvKeyETH)
			if err != nil {
				zap.L().Error("Error transfering...", zap.Error(err))
				p.Amount = "0"
			}
			p.Hash = tHash
			p.Network = "ETH"
			// sleep between transactions
			time.Sleep(time.Duration(s.cfg.EthSendInterval) * time.Second)

		}
	}
	// Save into DB
	err = s.ds.InsertPayoutTransactions(toPay)
	if err != nil {
		zap.L().Fatal("Error Inserting payout into DB", zap.Error(err))
	}
	s.lastPayoutEpoch = epoch

	return nil
}

func (s *payoutServer) getAccountBalance(addr string) (string, error) {
	accountRequest := &iotexapi.GetAccountRequest{Address: addr}
	accountResponse, err := s.cli.GetAccount(s.ctx, accountRequest)
	if err != nil {
		return "0", err
	}
	return accountResponse.GetAccountMeta().Balance, nil
}

func (s *payoutServer) unclaimedBalance(address string) (*big.Int, error) {
	request := &iotexapi.ReadStateRequest{
		ProtocolID: []byte("rewarding"),
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

	gasPriceRau, _ := s.getGasPrice()
	nonce, _ := s.getNonce(signer)

	b := &action.ClaimFromRewardingFundBuilder{}
	act := b.SetAmount(amount).SetData(payload).Build()
	bd := &action.EnvelopeBuilder{}
	elp := bd.SetNonce(nonce).
		SetGasPrice(gasPriceRau).
		SetGasLimit(gasLimit).
		SetAction(&act).Build()

	cHash, err := s.Sign(elp, prvKey)
	if err != nil {
		zap.L().Error("failed to sign action", zap.Error(err))
		return "", err
	}
	return cHash, nil
}

func (s *payoutServer) waitForReceipt(hash string) (*iotexapi.ReceiptInfo, error) {
	receiptRequest := &iotexapi.GetReceiptByActionRequest{ActionHash: hash}
	for i := 10; i <= 50; i += 10 {
		time.Sleep(time.Duration(i) * time.Second)
		receipt, err := s.cli.GetReceiptByAction(s.ctx, receiptRequest)
		if err != nil {
			zap.L().Error("GetReceipt error", zap.Error(err))
		} else {
			return receipt.ReceiptInfo, nil
		}
	}
	return nil, errors.New("GetReceipt timeout")
}

func printActionProto(action *iotextypes.Action) (string, error) {
	pubKey, err := keypair.BytesToPublicKey(action.SenderPubKey)
	if err != nil {
		log.L().Error("failed to convert pubkey")
		return "", err
	}
	senderAddress, err := address.FromBytes(pubKey.Hash())
	if err != nil {
		log.L().Error("failed to convert bytes into address")
		return "", err
	}
	output := fmt.Sprintf("\nversion: %d  ", action.Core.GetVersion()) +
		fmt.Sprintf("nonce: %d  ", action.Core.GetNonce()) +
		fmt.Sprintf("gasLimit: %d  ", action.Core.GasLimit) +
		fmt.Sprintf("gasPrice: %s Rau\n", action.Core.GasPrice) +
		fmt.Sprintf("senderAddress: %s %s\n", senderAddress.String(),
			Match(senderAddress.String(), "address"))
	switch {
	default:
		output += proto.MarshalTextString(action.Core)
	case action.Core.GetTransfer() != nil:
		transfer := action.Core.GetTransfer()
		output += "transfer: <\n" +
			fmt.Sprintf("  recipient: %s %s\n", transfer.Recipient,
				Match(transfer.Recipient, "address")) +
			fmt.Sprintf("  amount: %s Rau\n", transfer.Amount)
		if len(transfer.Payload) != 0 {
			output += fmt.Sprintf("  payload: %s\n", transfer.Payload)
		}
		output += ">\n"
	case action.Core.GetExecution() != nil:
		execution := action.Core.GetExecution()
		output += "execution: <\n" +
			fmt.Sprintf("  contract: %s %s\n", execution.Contract,
				Match(execution.Contract, "address"))
		if execution.Amount != "0" {
			output += fmt.Sprintf("  amount: %s Rau\n", execution.Amount)
		}
		output += fmt.Sprintf("  data: %x\n", execution.Data) + ">\n"
	case action.Core.GetPutPollResult() != nil:
		putPollResult := action.Core.GetPutPollResult()
		output += "putPollResult: <\n" +
			fmt.Sprintf("  height: %d\n", putPollResult.Height) +
			"  candidates: <\n"
		for _, candidate := range putPollResult.Candidates.Candidates {
			output += "    candidate: <\n" +
				fmt.Sprintf("      address: %s\n", candidate.Address)
			votes := big.NewInt(0).SetBytes(candidate.Votes)
			output += fmt.Sprintf("      votes: %s\n", votes.String()) +
				fmt.Sprintf("      rewardAdress: %s\n", candidate.RewardAddress) +
				"    >\n"
		}
		output += "  >\n" +
			">\n"
	}
	output += fmt.Sprintf("senderPubKey: %x\n", action.SenderPubKey) +
		fmt.Sprintf("signature: %x\n", action.Signature)

	return output, nil
}

// Match returns human readable expression
func Match(in string, matchType string) string {
	switch matchType {
	case "address":
		alias, err := alias.Alias(in)
		if err != nil {
			return ""
		}
		return "(" + alias + ")"
	case "status":
		if in == "0" {
			return "(Fail)"
		} else if in == "1" {
			return "(Success)"
		}
	}
	return ""
}

func (s *payoutServer) ioTransfer(recipient string, data string, amount *big.Int, prvKey keypair.PrivateKey) (string, error) {
	payload := make([]byte, 0)
	if len(data) > 0 {
		payload = []byte(data)
	}

	sender, err := address.FromBytes(prvKey.PublicKey().Hash())
	if err != nil {
		zap.L().Error("Error getting address private key from string", zap.Error(err))
	}
	if recipient == sender.String() {
		return "0000000000000000000000000000000000000000000000000000000000000000", nil
	}

	gasLimit := uint64(10000) + uint64(100)*uint64(len(payload))

	gasPriceRau, _ := s.getGasPrice()
	nonce, _ := s.getNonce(sender.String())

	tx, _ := action.NewTransfer(nonce, amount,
		recipient, []byte(payload), gasLimit, gasPriceRau)

	bd := &action.EnvelopeBuilder{}
	elp := bd.SetNonce(nonce).
		SetGasPrice(gasPriceRau).
		SetGasLimit(gasLimit).
		SetAction(tx).Build()

	tHash, err := s.Sign(elp, prvKey)
	if err != nil {
		zap.L().Error("failed to sign action", zap.Error(err))
		return "", err
	}
	return tHash, nil
}

func (s *payoutServer) getGasPrice() (*big.Int, error) {
	request := &iotexapi.SuggestGasPriceRequest{}
	response, err := s.cli.SuggestGasPrice(s.ctx, request)
	if err != nil {
		return nil, err
	}
	gasPriceRau := new(big.Int).SetUint64(response.GasPrice)
	if gasPriceRau.Cmp(new(big.Int).SetInt64(1)) == 0 {
		gasPriceRau.SetString("1000000000000", 10)
	}

	return gasPriceRau, nil
}

func (s *payoutServer) getNonce(signer string) (uint64, error) {
	accRequest := &iotexapi.GetAccountRequest{Address: signer}
	account, err := s.cli.GetAccount(s.ctx, accRequest)
	if err != nil {
		return 1, err
	}
	return account.AccountMeta.PendingNonce, nil
}

func (s *payoutServer) Sign(elp action.Envelope, prvKey keypair.PrivateKey) (string, error) {
	sealed, err := action.Sign(elp, prvKey)
	if err != nil {
		zap.L().Error("failed to sign action", zap.Error(err))
		return "", err
	}
	selp := sealed.Proto()
	actionInfo, err := printActionProto(selp)
	if err != nil {
		zap.L().Error("failed to sign action", zap.Error(err))
		return "", err
	}
	fmt.Println("\n" + actionInfo + "\n")

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

func (s *payoutServer) ethTransfer(client *ethclient.Client, recipient string, amount *big.Int, prvKey keypair.PrivateKey) (string, error) {
	sender := common.BytesToAddress(prvKey.PublicKey().Hash())

	nonce, err := client.PendingNonceAt(s.ctx, sender)
	if err != nil {
		zap.L().Error("failed to ger nonce action", zap.Error(err))
		return "", err
	}

	if recipient == sender.String() {
		return "0000000000000000000000000000000000000000000000000000000000000000", nil
	}

	toAddress := common.HexToAddress("0x" + recipient)
	tokenAddress := common.HexToAddress(s.cfg.EthToken)

	transferFnSignature := []byte("transfer(address,uint256)")
	hash := sha3.NewKeccak256()
	hash.Write(transferFnSignature)
	methodID := hash.Sum(nil)[:4]
	paddedAddress := common.LeftPadBytes(toAddress.Bytes(), 32)

	paddedAmount := common.LeftPadBytes(amount.Bytes(), 32)
	var data []byte
	data = append(data, methodID...)
	data = append(data, paddedAddress...)
	data = append(data, paddedAmount...)

	gasPrice, err := client.SuggestGasPrice(context.Background())
	if err != nil {
		zap.L().Error("failed to get gas price", zap.Error(err))
		return "", err
	}
	tmp := big.NewInt(20)
	tmp.Mul(tmp, gasPrice)
	tmp.Quo(tmp, big.NewInt(100))
	gasPrice.Add(gasPrice, tmp)
	zap.L().Info("Gas Prince", zap.String("Gas", gasPrice.String()))

	/*gasLimit, err := client.EstimateGas(s.ctx, ethereum.CallMsg{
		To:   &toAddress,
		Data: data,
	})
	if err != nil {
		zap.L().Error("failed to estimate gas", zap.Error(err))
		return "", err
	}*/
	gasLimit := s.cfg.EthGasLimit

	value := big.NewInt(0) // in wei (0 eth)
	tx := types.NewTransaction(nonce, tokenAddress, value, gasLimit, gasPrice, data)
	zap.L().Info("Sending...", zap.String("To", toAddress.String()),
		zap.String("Amount", amount.String()), zap.String("Hash", tx.Hash().Hex()))

	chainID, err := client.NetworkID(s.ctx)
	if err != nil {
		zap.L().Error("failed to get chain ID", zap.Error(err))
		return "", err
	}

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainID), prvKey.EcdsaPrivateKey())
	if err != nil {
		zap.L().Error("failed sign TX", zap.Error(err))
		return "", err
	}

	err = client.SendTransaction(s.ctx, signedTx)
	if err != nil {
		zap.L().Error("failed broadcast TX", zap.Error(err))
		return "", err
	}

	return signedTx.Hash().Hex(), nil
}
