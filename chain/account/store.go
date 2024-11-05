package account

import (
	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
)

func getStorageIterator() []byte {
	return storageKeyPrefix
}

type accountStore struct {
	address types.Address
	handle  db.Handle
	db.DB
}

func (as *accountStore) Address() *types.Address {
	return &as.address
}
func (as *accountStore) Storage() db.DB {
	return db.DisableNotFound(as.Subset(getStorageIterator()))
}
func (as *accountStore) Snapshot() store.Account {
	return NewAccountStore(as.address, as.DB.Snapshot(), as.handle)
}
func (as *accountStore) Identifier() types.HashHeight {
	frontier, err := as.Frontier()
	common.DealWithErr(err)
	if frontier == nil {
		return types.ZeroHashHeight
	}
	return frontier.Identifier()
}
func (as *accountStore) Handle() db.Handle {
	return as.handle
}

func NewAccountStore(address types.Address, db db.DB, handle db.Handle) store.Account {
	if db == nil {
		panic("account store can't operate with nil db")
	}
	return &accountStore{
		address: address,
		handle:  handle,
		DB:      db,
	}
}
