package storage

import (
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
)

const (
	rollbackCacheSize = 100
)

var (
	storageByte  = []byte{85}
	rollbackByte = []byte{119}
)

type CacheDB interface {
	Get() db.DB

	Add(types.HashHeight, db.Patch) error
	Pop() error

	Stop() error
}

type cacheDB struct {
	ldb     *leveldb.DB
	changes sync.Mutex
	stopped bool
}

func NewCacheDB(_ db.DB, ldb *leveldb.DB) CacheDB {
	return &cacheDB{
		ldb: ldb,
	}
}

func GetFrontierIdentifier(db db.DB) types.HashHeight {
	data, err := db.Get(frontierIdentifierKey)
	if err == leveldb.ErrNotFound {
		return types.ZeroHashHeight
	}
	common.DealWithErr(err)
	hh, err := types.DeserializeHashHeight(data)
	common.DealWithErr(err)
	return *hh
}

func GetRollbackCacheSize() int {
	return rollbackCacheSize
}

func (c *cacheDB) Get() db.DB {
	c.changes.Lock()
	defer c.changes.Unlock()
	if c.stopped {
		return nil
	}
	snapshot, _ := c.ldb.GetSnapshot()
	return db.NewLevelDBSnapshotWrapper(snapshot).Subset(storageByte)
}

func (c *cacheDB) Add(identifier types.HashHeight, patch db.Patch) error {
	temp := db.NewMemDB()
	if err := temp.Put(frontierIdentifierKey, identifier.Serialize()); err != nil {
		return err
	}
	frontierPatch, err := temp.Changes()
	if err != nil {
		return err
	}
	if err := frontierPatch.Replay(patch); err != nil {
		return err
	}
	rollbackPatch := db.RollbackPatch(c.Get(), patch)

	c.changes.Lock()
	defer c.changes.Unlock()

	if err := c.ldb.Put(common.JoinBytes(rollbackByte, common.Uint64ToBytes(identifier.Height)), rollbackPatch.Dump(), nil); err != nil {
		return err
	}
	if identifier.Height > rollbackCacheSize {
		if err := c.ldb.Delete(common.JoinBytes(rollbackByte, common.Uint64ToBytes(identifier.Height-rollbackCacheSize)), nil); err != nil {
			return err
		}
	}
	if err := db.ApplyPatch(db.NewLevelDBWrapperWithFullDelete(c.ldb).Subset(storageByte), patch); err != nil {
		return err
	}
	return nil
}

func (c *cacheDB) Pop() error {
	frontierIdentifier := GetFrontierIdentifier(c.Get())
	rollbackPatch := c.getRollback(frontierIdentifier.Height)

	c.changes.Lock()
	defer c.changes.Unlock()

	if err := db.ApplyPatch(db.NewLevelDBWrapperWithFullDelete(c.ldb).Subset(storageByte), rollbackPatch); err != nil {
		return err
	}
	if err := c.ldb.Delete(common.JoinBytes(rollbackByte, common.Uint64ToBytes(frontierIdentifier.Height)), nil); err != nil {
		return err
	}
	return nil
}

func (c *cacheDB) Stop() error {
	c.changes.Lock()
	defer c.changes.Unlock()
	if err := c.ldb.Close(); err != nil {
		return err
	}
	c.stopped = true
	c.ldb = nil
	return nil
}

func (c *cacheDB) getRollback(height uint64) db.Patch {
	snapshot, _ := c.ldb.GetSnapshot()
	value, err := snapshot.Get(common.JoinBytes(rollbackByte, common.Uint64ToBytes(height)), nil)
	if err == leveldb.ErrNotFound {
		return nil
	}
	common.DealWithErr(err)

	patch, err := db.NewPatchFromDump(value)
	common.DealWithErr(err)
	return patch
}
