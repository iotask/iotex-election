package datastore

import (
	"time"
)

// Account set
type Account struct {
	Name              string
	ETHAddress        string
	IOAddress         string
	LastStake         uint64
	LastParticipation float32
	Balance           float64
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
	Date            time.Time
	Update          time.Time
}
