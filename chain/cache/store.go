package cache

import (
	"github.com/zenon-network/go-zenon/chain/cache/storage"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
)

type cacheStore struct {
	identifier types.HashHeight
	db.DB
}

func (ps *cacheStore) Identifier() types.HashHeight {
	return ps.identifier
}

func (cs *cacheStore) ApplyMomentum(detailed *nom.DetailedMomentum, changes db.Patch) error {
	if len(detailed.AccountBlocks) == 0 {
		return nil
	}
	extractor := &cacheExtractor{store: cs, height: detailed.Momentum.Height, patch: db.NewPatch()}
	if err := changes.Replay(extractor); err != nil {
		return err
	}
	if err := cs.DB.Apply(extractor.patch); err != nil {
		return err
	}
	return cs.pruneAccountCache(detailed.AccountBlocks)
}

func getByHeight(height uint64, dataSet db.DB) ([]byte, error) {
	if dataSet == nil {
		return nil, nil
	}
	iterator := dataSet.NewIterator([]byte{})
	defer iterator.Release()

	value := []byte{}
	for {
		if !iterator.Next() {
			if iterator.Error() != nil {
				return nil, iterator.Error()
			}
			break
		}
		//fmt.Printf("iterator key %d, value %d\n", common.BytesToUint64(iterator.Key()), big.NewInt(0).SetBytes(iterator.Value()))
		keyHeight := common.BytesToUint64(iterator.Key())
		if keyHeight > height {
			//fmt.Printf("breaking at value key %d, value %d\n", common.BytesToUint64(iterator.Key()), big.NewInt(0).SetBytes(result))
			break
		}
		value = make([]byte, len(iterator.Value()))
		copy(value, iterator.Value())
		if keyHeight == height {
			break
		}
	}
	return value, nil
}

func NewCacheStore(identifier types.HashHeight, db storage.CacheDB) store.Cache {
	if db == nil {
		panic("cache store can't operate with nil db")
	}
	frontier := storage.GetFrontierIdentifier(db.Get())
	if identifier.Height > frontier.Height {
		panic("cache store identifier height cannot be greater than db height")
	}
	return &cacheStore{
		identifier: identifier,
		DB:         db.Get(),
	}
}
