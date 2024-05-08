package archive

import (
	"math/big"

	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

type archiveStore struct {
	db.DB
}

func (as *archiveStore) Identifier() types.HashHeight {
	return db.GetFrontierIdentifier(as.DB)
}

func (as *archiveStore) GetIdentifierByHash(hash types.Hash) (*types.HashHeight, error) {
	identifier, err := db.GetIdentifierByHash(as.DB, hash)
	if err != nil {
		return nil, err
	}
	return identifier, nil
}

func (as *archiveStore) GetStakeBeneficialAmount(addr types.Address) (*big.Int, error) {
	key := common.JoinBytes(fusedAmountKeyPrefix, addr.Bytes())
	if data, err := db.DisableNotFound(as.DB).Get(key); err != nil {
		return nil, err
	} else {
		return common.BytesToBigInt(data), nil
	}
}

func (as *archiveStore) GetChainPlasma(addr types.Address) (*big.Int, error) {
	key := common.JoinBytes(chainPlasmaKeyPrefix, addr.Bytes())
	if data, err := db.DisableNotFound(as.DB).Get(key); err != nil {
		return nil, err
	} else {
		return common.BytesToBigInt(data), nil
	}
}

func (as *archiveStore) IsSporkActive(implemented *types.ImplementedSpork) (bool, error) {
	frontier := as.Identifier()
	if frontier.Height == 1 {
		return false, nil
	}

	storage := db.DisableNotFound(as.Subset(sporkStorageKeyPrefix))
	sporks := definition.GetAllSporks(storage)

	for _, spork := range sporks {
		if spork.Activated && spork.EnforcementHeight <= frontier.Height && spork.Id == implemented.SporkId {
			return true, nil
		}
	}

	return false, nil
}

func NewArchiveStore(db db.DB) store.Archive {
	if db == nil {
		panic("archive store can't operate with nil db")
	}
	return &archiveStore{
		DB: db,
	}
}
