package vm_context

import (
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
)

func (ctx *accountVmContext) IsAcceleratorSporkEnforced() bool {
	if ctx.archiveStore != nil {
		active, err := ctx.archiveStore.IsSporkActive(types.AcceleratorSpork)
		common.DealWithErr(err)
		return active
	} else {
		active, err := ctx.momentumStore.IsSporkActive(types.AcceleratorSpork)
		common.DealWithErr(err)
		return active
	}
}

func (ctx *accountVmContext) IsHtlcSporkEnforced() bool {
	if ctx.archiveStore != nil {
		active, err := ctx.archiveStore.IsSporkActive(types.HtlcSpork)
		common.DealWithErr(err)
		return active
	} else {
		active, err := ctx.momentumStore.IsSporkActive(types.HtlcSpork)
		common.DealWithErr(err)
		return active
	}
}

func (ctx *accountVmContext) IsBridgeAndLiquiditySporkEnforced() bool {
	if ctx.archiveStore != nil {
		active, err := ctx.archiveStore.IsSporkActive(types.BridgeAndLiquiditySpork)
		common.DealWithErr(err)
		return active
	} else {
		active, err := ctx.momentumStore.IsSporkActive(types.BridgeAndLiquiditySpork)
		common.DealWithErr(err)
		return active
	}
}
