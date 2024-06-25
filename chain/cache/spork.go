package cache

import (
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

var (
	sporkInfoKeyPrefix = []byte{0}
)

func getSporkInfoPrefix(id []byte) []byte {
	return common.JoinBytes(getSporkCacheKey(id), sporkInfoKeyPrefix)
}

func (cs *cacheStore) IsSporkActive(implemented *types.ImplementedSpork) (bool, error) {
	identifier := cs.Identifier()
	if identifier.Height == 1 {
		return false, nil
	}

	data, err := getByHeight(cs.identifier.Height, cs.DB.Subset(getSporkInfoPrefix(implemented.SporkId.Bytes())))
	if err != nil {
		return false, err
	}

	if len(data) == 0 {
		return false, nil
	}

	spork := definition.ParseSporkInfo(data)
	if spork.Activated && spork.EnforcementHeight <= identifier.Height && spork.Id == implemented.SporkId {
		return true, nil
	}

	return false, nil
}
