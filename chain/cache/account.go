package cache

import (
	"math/big"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
)

var (
	fusedAmountKeyPrefix = []byte{0}
	chainPlasmaKeyPrefix = []byte{1}
)

func getFusedAmountPrefix(address []byte) []byte {
	return common.JoinBytes(getAccountCacheKey(address), fusedAmountKeyPrefix)
}

func getChainPlasmaPrefix(address []byte) []byte {
	return common.JoinBytes(getAccountCacheKey(address), chainPlasmaKeyPrefix)
}

func (cs *cacheStore) GetFusedPlasma(address types.Address) (*big.Int, error) {
	value, err := getByHeight(cs.identifier.Height, cs.DB.Subset(getFusedAmountPrefix(address.Bytes())))
	if err == leveldb.ErrNotFound {
		return big.NewInt(0), nil
	}
	if err != nil {
		return nil, err
	}
	return big.NewInt(0).SetBytes(value), nil
}

func (cs *cacheStore) GetChainPlasma(address types.Address) (*big.Int, error) {
	value, err := getByHeight(cs.identifier.Height, cs.DB.Subset(getChainPlasmaPrefix(address.Bytes())))
	if err == leveldb.ErrNotFound {
		return big.NewInt(0), nil
	}
	if err != nil {
		return nil, err
	}
	return big.NewInt(0).SetBytes(value), nil
}

func (cs *cacheStore) pruneAccountCache(blocks []*nom.AccountBlock) error {
	for _, block := range blocks {
		// Include descedants for consistency
		all := append([]*nom.AccountBlock{block}, block.DescendantBlocks...)
		for _, b := range all {
			prefix := getFusedAmountPrefix(b.Address.Bytes())
			dataSet := cs.DB.Subset(prefix)
			fusedPlasmaKeys, err := getKeysToPrune(dataSet, prefix, b.MomentumAcknowledged.Height)
			if err != nil {
				return err
			}
			prefix = getChainPlasmaPrefix(b.Address.Bytes())
			dataSet = cs.DB.Subset(prefix)
			chainPlasmaKeys, err := getKeysToPrune(dataSet, prefix, b.MomentumAcknowledged.Height)
			if err != nil {
				return err
			}
			for _, key := range append(fusedPlasmaKeys, chainPlasmaKeys...) {
				cs.DB.Delete(key)
			}
		}
	}
	return nil
}

func getKeysToPrune(dataSet db.DB, prefix []byte, toHeight uint64) ([][]byte, error) {
	if dataSet == nil {
		return nil, nil
	}
	iterator := dataSet.NewIterator([]byte{})
	defer iterator.Release()

	keys := [][]byte{}
	for {
		if !iterator.Next() {
			if iterator.Error() != nil {
				return nil, iterator.Error()
			}
			break
		}
		key := make([]byte, len(iterator.Key()))
		copy(key, iterator.Key())
		if common.BytesToUint64(key) > toHeight {
			break
		}
		keys = append(keys, common.JoinBytes(prefix, key))
	}

	// Remove key of the current state, so that it's not pruned
	if len(keys) > 0 {
		keys = keys[:len(keys)-1]
	}
	return keys, nil
}
