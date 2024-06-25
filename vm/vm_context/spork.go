package vm_context

import (
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
)

func (ctx *accountVmContext) IsAcceleratorSporkEnforced() bool {
	if ctx.cacheStore != nil {
		active, err := ctx.cacheStore.IsSporkActive(types.AcceleratorSpork)
		common.DealWithErr(err)
		return active
	} else {
		active, err := ctx.momentumStore.IsSporkActive(types.AcceleratorSpork)
		common.DealWithErr(err)
		return active
	}
}

func (ctx *accountVmContext) IsHtlcSporkEnforced() bool {
	if ctx.cacheStore != nil {
		active, err := ctx.cacheStore.IsSporkActive(types.HtlcSpork)
		common.DealWithErr(err)
		return active
	} else {
		active, err := ctx.momentumStore.IsSporkActive(types.HtlcSpork)
		common.DealWithErr(err)
		return active
	}
}

func (ctx *accountVmContext) IsBridgeAndLiquiditySporkEnforced() bool {
	if ctx.cacheStore != nil {
		active, err := ctx.cacheStore.IsSporkActive(types.BridgeAndLiquiditySpork)
		common.DealWithErr(err)
		return active
	} else {
		active, err := ctx.momentumStore.IsSporkActive(types.BridgeAndLiquiditySpork)
		common.DealWithErr(err)
		return active
	}
}
