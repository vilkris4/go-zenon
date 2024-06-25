package cache

import (
	"github.com/zenon-network/go-zenon/common"
)

var (
	accountCacheKeyPrefix = []byte{3}
	sporkCacheKeyPrefix   = []byte{4}
)

func getAccountCacheKey(address []byte) []byte {
	return common.JoinBytes(accountCacheKeyPrefix, address)
}

func getSporkCacheKey(id []byte) []byte {
	return common.JoinBytes(sporkCacheKeyPrefix, id)
}
