package datastore

import (
	"time"
)

// Claim set
type Claim struct {
	Amount  string
	Hash    string
	Date    time.Time
	Receipt string
}

// InsertClaim add claim data to DB
func (s *DataStore) InsertClaim(claim *Claim) error {
	session := s.Session.Clone()
	defer session.Close()

	return session.DB(s.cfg.DatasetName).C(claimsCollection).Insert(claim)
}
