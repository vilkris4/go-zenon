package pillar

import (
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/vm/embedded/implementation"
	"github.com/zenon-network/go-zenon/vm/vm_context"
)

func canPerformEmbeddedUpdate(momentumStore store.Momentum, accountStore store.Account, contract types.Address) error {
	context := vm_context.NewAccountContext(momentumStore, accountStore, nil)
	return implementation.CanPerformUpdate(context)
}

func (w *worker) updateContracts() error {
	momentumStore := w.chain.GetFrontierMomentumStore()
	defer w.chain.ReleaseMomentumStore(momentumStore)
	for _, address := range types.EmbeddedWUpdate {
		accountStore := w.chain.GetFrontierAccountStore(address)
		if err := canPerformEmbeddedUpdate(momentumStore, accountStore, address); err == nil {
			w.chain.ReleaseAccountStore(accountStore)
			w.log.Info("producing block to update embedded-contract", "contract-address", address)
			if block, err := w.supervisor.GenerateFromTemplate(&nom.AccountBlock{
				BlockType: nom.BlockTypeUserSend,
				Address:   w.coinbase.Address,
				ToAddress: address,
				Data:      definition.ABICommon.PackMethodPanic(definition.UpdateMethodName),
			}, w.coinbase.Signer); err != nil {
				return err
			} else {
				w.broadcaster.CreateAccountBlock(block)
			}
		} else if err == constants.ErrUpdateTooRecent || err == constants.ErrContractMethodNotFound {
			w.chain.ReleaseAccountStore(accountStore)
		} else {
			w.chain.ReleaseAccountStore(accountStore)
			return err
		}
	}
	return nil
}
