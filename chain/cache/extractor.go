package cache

import (
	"bytes"
	"fmt"

	"github.com/zenon-network/go-zenon/chain/account"
	"github.com/zenon-network/go-zenon/chain/momentum"
	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

type cacheExtractor struct {
	store  store.Cache
	height uint64
	patch  db.Patch
}

func (ce *cacheExtractor) Put(key []byte, value []byte) {
	if cacheKey := ce.tryToGetCacheKey(key, value); cacheKey != nil {
		ce.patch.Put(cacheKey, value)
	}
}

func (ce *cacheExtractor) Delete(key []byte) {
	if cacheKey := ce.tryToGetCacheKey(key, nil); cacheKey != nil {
		ce.patch.Delete(cacheKey)
	}
}

func (ce *cacheExtractor) tryToGetCacheKey(key []byte, value []byte) []byte {
	if bytes.HasPrefix(key, momentum.AccountStorePrefix) {
		key = bytes.TrimPrefix(key, momentum.AccountStorePrefix)
		address := key[:types.AddressSize]
		keyWithoutAddress := key[types.AddressSize:]

		// Cache fused plasma
		if bytes.Equal(address, types.PlasmaContract.Bytes()) {
			prefix := common.JoinBytes(account.StorageKeyPrefix, definition.FusedAmountKeyPrefix)
			if bytes.HasPrefix(keyWithoutAddress, prefix) {
				beneficiary := bytes.TrimPrefix(keyWithoutAddress, prefix)
				return ce.getHeightKey(getFusedAmountPrefix(beneficiary))
			}
		}

		// Cache sporks
		if bytes.Equal(address, types.SporkContract.Bytes()) {
			prefix := common.JoinBytes(account.StorageKeyPrefix, []byte{definition.SporkInfoPrefix})
			if bytes.HasPrefix(keyWithoutAddress, prefix) {
				sporkId := bytes.TrimPrefix(keyWithoutAddress, prefix)
				fmt.Printf("caching spork %x\n", sporkId)
				return ce.getHeightKey(getSporkInfoPrefix(sporkId))
			}
		}

		// Cache chain plasma
		if bytes.HasPrefix(keyWithoutAddress, account.ChainPlasmaKey) {
			if value == nil {
				return nil
			}
			// Verify that the state has changed
			a, err := types.BytesToAddress(address)
			common.DealWithErr(err)
			current, err := ce.store.GetChainPlasma(a)
			common.DealWithErr(err)
			valuebig := common.BytesToBigInt(value)
			if current.Cmp(valuebig) == 0 {
				return nil
			}
			return ce.getHeightKey(getChainPlasmaPrefix(address))
		}
	}
	return nil
}

func (ce *cacheExtractor) getHeightKey(prefix []byte) []byte {
	return common.JoinBytes(prefix, common.Uint64ToBytes(ce.height))
}
