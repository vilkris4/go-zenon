package vm_context

import (
	"github.com/zenon-network/go-zenon/chain"
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
}

func (ctx *accountVmContext) MomentumStore() store.Momentum {
	return ctx.momentumStore
}

func (ctx *accountVmContext) Release(chain chain.Chain) {
	if ctx.momentumStore != nil {
		chain.ReleaseMomentumStore(ctx.momentumStore)
	}
	if ctx.Account != nil {
		chain.ReleaseAccountStore(ctx.Account)
	}
}

func NewAccountContext(momentumStore store.Momentum, accountBlock store.Account, pillarReader api.PillarReader) AccountVmContext {
	return &accountVmContext{
		momentumStore: momentumStore,
		Account:       accountBlock,
		PillarReader:  pillarReader,
	}
}

func NewGenesisAccountContext(address types.Address) AccountVmContext {
	return NewAccountContext(nil, account.NewAccountStore(address, db.NewMemDB(), 0), nil)
}
