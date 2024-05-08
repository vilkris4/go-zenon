package vm_context

import (
	"github.com/zenon-network/go-zenon/chain/account"
	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/consensus/api"
)

type accountVmContext struct {
	accountStoreSnapshot store.Account
	api.PillarReader
	store.Account
	momentumStore store.Momentum
	archiveStore  store.Archive
}

func (ctx *accountVmContext) MomentumStore() store.Momentum {
	return ctx.momentumStore
}

func (ctx *accountVmContext) ArchiveStore() store.Archive {
	return ctx.archiveStore
}

func NewAccountContext(momentumStore store.Momentum, accountBlock store.Account, archiveStore store.Archive, pillarReader api.PillarReader) AccountVmContext {
	return &accountVmContext{
		momentumStore: momentumStore,
		Account:       accountBlock,
		archiveStore:  archiveStore,
		PillarReader:  pillarReader,
	}
}

func NewGenesisAccountContext(address types.Address) AccountVmContext {
	return NewAccountContext(nil, account.NewAccountStore(address, db.NewMemDB()), nil, nil)
}
