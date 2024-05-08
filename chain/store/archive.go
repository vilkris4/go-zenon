package store

import (
	"math/big"

	"github.com/zenon-network/go-zenon/common/types"
)

type Archive interface {
	Identifier() types.HashHeight
	GetIdentifierByHash(types.Hash) (*types.HashHeight, error)
	GetStakeBeneficialAmount(types.Address) (*big.Int, error)
	GetChainPlasma(types.Address) (*big.Int, error)
	IsSporkActive(*types.ImplementedSpork) (bool, error)
}
