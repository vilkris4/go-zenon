package db

import (
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/zenon-network/go-zenon/common"
)

func applyMockTransactionsToDB(m Manager, db DB, amount int) {
	for i := 1; i <= amount; i++ {
		t := newMockTransaction(int64(i), m.Frontier())
		common.DealWithErr(m.Add(t))
		ApplyPatchToPartitions(db, m.GetPatch(t.commit.Identifier()), uint64(i), false)
	}
}

func TestGetPartitionsFrontierHeight(t *testing.T) {
	m := NewLevelDBManager(t.TempDir(), true)
	defer m.Stop()

	opts := &opt.Options{OpenFilesCacheCapacity: getOpenFilesCacheCapacity()}
	ldb, err := leveldb.OpenFile(t.TempDir(), opts)
	common.DealWithErr(err)
	defer ldb.Close()

	db := NewLevelDBWrapper(ldb)
	common.ExpectUint64(t, GetPartitionsFrontierHeight(db), 0)
	applyMockTransactionsToDB(m, db, 10)
	common.ExpectUint64(t, GetPartitionsFrontierHeight(db), 10)
}

func TestGetPartitionsEndHeight(t *testing.T) {
	partitionConfigs = []partitionConfig{
		{size: uint64(1000)},
		{size: uint64(10000)},
		{size: uint64(100000)},
	}
	common.ExpectUint64(t, GetPartitionsEndHeight(10632414), 10632999)
	common.ExpectUint64(t, GetPartitionsEndHeight(6502252), 6502999)
	common.ExpectUint64(t, GetPartitionsEndHeight(935120), 935999)
	common.ExpectUint64(t, GetPartitionsEndHeight(99999), 99999)
	common.ExpectUint64(t, GetPartitionsEndHeight(23946), 23999)
	common.ExpectUint64(t, GetPartitionsEndHeight(1000), 1999)
	common.ExpectUint64(t, GetPartitionsEndHeight(124), 999)
	common.ExpectUint64(t, GetPartitionsEndHeight(56), 999)
	common.ExpectUint64(t, GetPartitionsEndHeight(1), 999)
}
