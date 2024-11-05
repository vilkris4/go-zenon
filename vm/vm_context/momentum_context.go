package vm_context

import (
	"github.com/zenon-network/go-zenon/chain"
	"github.com/zenon-network/go-zenon/chain/momentum"
	"github.com/zenon-network/go-zenon/chain/store"
)

type MomentumVMContext interface {
	store.Momentum
	Release(chain chain.Chain)
}

type momentumVMContext struct {
	store.Momentum
}

func (ctx *momentumVMContext) Release(chain chain.Chain) {
	chain.ReleaseMomentumStore(ctx.Momentum)
}

func NewMomentumVMContext(store store.Momentum) MomentumVMContext {
	return &momentumVMContext{
		Momentum: store,
	}
}

func NewGenesisMomentumVMContext() MomentumVMContext {
	return &momentumVMContext{
		Momentum: momentum.NewGenesisStore(),
	}
}
