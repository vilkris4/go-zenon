package db

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zenon-network/go-zenon/common"
)

type partitionConfig struct {
	size   uint64
	prefix []byte
}

var (
	partitionFrontierByte = []byte{0}
	partitionConfigs      = []partitionConfig{
		{size: uint64(1000), prefix: []byte{1}},
		{size: uint64(10000), prefix: []byte{2}},
		{size: uint64(100000), prefix: []byte{3}},
	}
)

func getPartitionPrefix(cfg partitionConfig, index uint64) []byte {
	return common.JoinBytes(cfg.prefix, common.Uint64ToBytes(index))
}

func getPartitionIndexByHeight(cfg partitionConfig, height uint64) uint64 {
	return height / cfg.size
}

func getPartitionByHeight(database DB, cfg partitionConfig, height uint64) DB {
	index := getPartitionIndexByHeight(cfg, height)
	return database.Subset(getPartitionPrefix(cfg, index))
}

func GetConfigForSmallestPartition() partitionConfig {
	return partitionConfigs[0]
}

func GetPartitionsFrontierHeight(database DB) uint64 {
	var expectedHeight uint64
	var actualHeight uint64
	data, err := database.Get(partitionFrontierByte)
	if err == leveldb.ErrNotFound {
		return 0
	} else if err != nil {
		common.DealWithErr(err)
	}
	expectedHeight = common.BytesToUint64(data)
	for i, cfg := range partitionConfigs {
		partition := getPartitionByHeight(database, cfg, expectedHeight)
		height := GetFrontierIdentifier(partition).Height
		if i == 0 || height < actualHeight {
			actualHeight = height
		}
	}
	return actualHeight
}

func GetPartitionsEndHeight(height uint64) uint64 {
	smallest := GetConfigForSmallestPartition()
	return getPartitionIndexByHeight(smallest, height)*smallest.size + smallest.size - 1
}

func GetPartitions(database db, height uint64) []db {
	var partitions []db
	for i, cfg := range partitionConfigs {
		isLast := i == len(partitionConfigs)-1
		endIndex := getPartitionIndexByHeight(cfg, height)
		startIndex := roundDownToTen(endIndex)
		if len(partitions) > 0 {
			if endIndex == 0 {
				break
			}
			endIndex = endIndex - 1
		}
		if isLast {
			startIndex = uint64(0)
		}
		if !isLast && endsWithNine(endIndex) {
			continue
		}
		for j := int(endIndex); j >= int(startIndex); j-- {
			partitions = append(partitions, newSubDB(getPartitionPrefix(cfg, uint64(j)), database))
		}
	}
	return partitions
}

func ApplyPatchToPartitions(database DB, patch Patch, height uint64, isRollback bool) error {
	for _, cfg := range partitionConfigs {
		if err := ApplyPatch(getPartitionByHeight(database, cfg, height), patch); err != nil {
			return err
		}
	}
	if isRollback {
		height = height - 1
	}
	if err := database.Put(partitionFrontierByte, common.Uint64ToBytes(height)); err != nil {
		return err
	}
	return nil
}

func roundDownToTen(x uint64) uint64 {
	return (x / 10) * 10
}

func endsWithNine(x uint64) bool {
	return (x+1)%10 == 0
}
