package db

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/types"
)

type mockCommit struct {
	hash        types.Hash
	prevHash    types.Hash
	height      uint64
	changesHash types.Hash
}

func (mc *mockCommit) Identifier() types.HashHeight {
	return types.HashHeight{
		Height: mc.height,
		Hash:   mc.hash,
	}
}
func (mc *mockCommit) Previous() types.HashHeight {
	return types.HashHeight{
		Height: mc.height - 1,
		Hash:   mc.prevHash,
	}
}
func (mc *mockCommit) Serialize() ([]byte, error) {
	return common.JoinBytes(mc.hash.Bytes(), mc.prevHash.Bytes(), mc.changesHash.Bytes(), common.Uint64ToBytes(mc.height)), nil
}

type mockTransaction struct {
	patch  Patch
	commit Commit
}

func (m *mockTransaction) GetCommits() []Commit {
	return []Commit{m.commit}
}
func (m *mockTransaction) StealChanges() Patch {
	p := m.patch
	m.patch = nil
	return p
}

func newMockTransaction(seed int64, db DB) *mockTransaction {
	frontier := GetFrontierIdentifier(db)
	ab := &mockCommit{
		prevHash: frontier.Hash,
		height:   frontier.Height + 1,
	}

	r := rand.New(rand.NewSource(seed))
	stressTestConcurrentUse(nil, db, 5, 1, r)

	changes, _ := db.Changes()
	ab.changesHash = PatchHash(changes)
	ab.hash = types.NewHash(ab.changesHash.Bytes())
	return &mockTransaction{
		patch:  changes,
		commit: ab,
	}
}

func applyMockTransactions(m Manager, amount int) map[int]types.HashHeight {
	identifiers := make(map[int]types.HashHeight, amount)
	for i := 1; i <= amount; i++ {
		t := newMockTransaction(int64(i), m.Frontier())
		common.DealWithErr(m.Add(t))
		identifiers[i] = t.commit.Identifier()
	}
	return identifiers
}

func TestVersionedDBConcurrentUse(t *testing.T) {
	m := NewLevelDBManager(t.TempDir(), false)
	defer m.Stop()

	v0 := m.Frontier()
	v01 := m.Frontier()

	t1 := newMockTransaction(1, v0)
	common.ExpectString(t, DebugPatch(t1.patch), `
0c697f48392907a0 - a68447a4189deb99
365a858149c6e2d1 - 57e9d1860d1d68d8
4d65822107fcfd52 - 78629a0f5f3f164f
8866cb397916001e - 9408d2ac22c4d294
d5104dc76695721d - b80704bb7b4d7c03`)
	common.DealWithErr(m.Add(t1))

	common.ExpectString(t, DebugDB(v0), `
0c697f48392907a0 - a68447a4189deb99
365a858149c6e2d1 - 57e9d1860d1d68d8
4d65822107fcfd52 - 78629a0f5f3f164f
8866cb397916001e - 9408d2ac22c4d294
d5104dc76695721d - b80704bb7b4d7c03`)

	v1 := m.Frontier()
	frontier1 := GetFrontierIdentifier(v1)
	common.ExpectString(t, fmt.Sprintf("%v", frontier1), `{5c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d88 1}`)

	frontier0 := GetFrontierIdentifier(v01)
	common.ExpectString(t, fmt.Sprintf("%v", frontier0), `{0000000000000000000000000000000000000000000000000000000000000000 0}`)

	t1p := newMockTransaction(11, v01)
	common.DealWithErr(m.Add(t1p))
	common.ExpectString(t, DebugDB(v01), `
3d31f9c8d42f02c9 - 27397d8e9ad2a99d
566258666a0baae0 - 0e99d01979f82ec3
64743b955ef87a1f - 759b78575bef8f42
8bb5889840140c59 - 6707ee17a8517db0
e871bc355914f2c3 - 48c39064a7c7e355`)
}

func TestVersionedDBVersions(t *testing.T) {
	dir := t.TempDir()
	m := NewLevelDBManager(dir, false)

	db := m.Frontier()
	t1 := newMockTransaction(1, db)
	common.ExpectString(t, DebugPatch(t1.patch), `
0c697f48392907a0 - a68447a4189deb99
365a858149c6e2d1 - 57e9d1860d1d68d8
4d65822107fcfd52 - 78629a0f5f3f164f
8866cb397916001e - 9408d2ac22c4d294
d5104dc76695721d - b80704bb7b4d7c03`)
	common.ExpectString(t, fmt.Sprintf("%+v", t1.commit.Identifier()), `{Hash:5c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d88 Height:1}`)
	common.DealWithErr(m.Add(t1))

	db = m.Frontier()
	common.ExpectString(t, DebugDB(db), `
00 - 0a220a205c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d881001
015c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d88 - 0000000000000001
020000000000000001 - 5c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d8800000000000000000000000000000000000000000000000000000000000000003fc795fb4006a3d73dd511f493010f6d85c7448155ec6c444acaa9238f30c19e0000000000000001
0c697f48392907a0 - a68447a4189deb99
365a858149c6e2d1 - 57e9d1860d1d68d8
4d65822107fcfd52 - 78629a0f5f3f164f
8866cb397916001e - 9408d2ac22c4d294
d5104dc76695721d - b80704bb7b4d7c03`)
	f1 := GetFrontierIdentifier(db)
	t2 := newMockTransaction(2, db)
	common.ExpectString(t, DebugPatch(t2.patch), `
069728dc67d9db56 - 8f3aa6d8bef36a80
1b213e776add09fe - 903e28f8376a23b8
9569f9e2cb82822f - 21ed4caac044316f
b6666b02a03da270 - 1a634384d0ba8f10
cea06b688be116ca - f6bd65cefe8c20dc`)
	common.ExpectString(t, fmt.Sprintf("%+v", t2.commit.Identifier()), `{Hash:e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe739249 Height:2}`)
	common.DealWithErr(m.Add(t2))

	db = m.Frontier()
	f2 := GetFrontierIdentifier(db)
	t3 := newMockTransaction(3, db)
	common.ExpectString(t, DebugPatch(t3.patch), `
299c7bb8757d0bdc - 3bf5be2bb76058f7
36bcb1ac1b221c52 - c0fbdb5d49c1a3a8
789229e09e31f58d - 625250fa2140d8fd
dc2864602be7fb85 - d38967f931a50490
f25f4b21eef64b43 - 9c0a8a2bfc0914df`)
	common.ExpectString(t, fmt.Sprintf("%+v", t3.commit.Identifier()), `{Hash:d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a85 Height:3}`)
	common.DealWithErr(m.Add(t3))

	db = m.Frontier()
	common.ExpectString(t, DebugDB(db), `
00 - 0a220a20d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a851003
015c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d88 - 0000000000000001
01d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a85 - 0000000000000003
01e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe739249 - 0000000000000002
020000000000000001 - 5c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d8800000000000000000000000000000000000000000000000000000000000000003fc795fb4006a3d73dd511f493010f6d85c7448155ec6c444acaa9238f30c19e0000000000000001
020000000000000002 - e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe7392495c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d889cb71dd39b3e4f3991f66538480b1209e2df16863c33729be03430e1b1500b050000000000000002
020000000000000003 - d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a85e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe739249d3fed0cc8f431f60ecd3b4617b2ad8991299350cb8361f2ccf02fd50de4c23620000000000000003
069728dc67d9db56 - 8f3aa6d8bef36a80
0c697f48392907a0 - a68447a4189deb99
1b213e776add09fe - 903e28f8376a23b8
299c7bb8757d0bdc - 3bf5be2bb76058f7
365a858149c6e2d1 - 57e9d1860d1d68d8
36bcb1ac1b221c52 - c0fbdb5d49c1a3a8
4d65822107fcfd52 - 78629a0f5f3f164f
789229e09e31f58d - 625250fa2140d8fd
8866cb397916001e - 9408d2ac22c4d294
9569f9e2cb82822f - 21ed4caac044316f
b6666b02a03da270 - 1a634384d0ba8f10
cea06b688be116ca - f6bd65cefe8c20dc
d5104dc76695721d - b80704bb7b4d7c03
dc2864602be7fb85 - d38967f931a50490
f25f4b21eef64b43 - 9c0a8a2bfc0914df`)
	f3 := GetFrontierIdentifier(db)
	patch, err := db.Changes()
	common.FailIfErr(t, err)
	common.ExpectString(t, DebugPatch(patch), ``)

	patch, err = m.Get(f2).Changes()
	common.FailIfErr(t, err)
	common.ExpectString(t, DebugPatch(patch), ``)

	common.ExpectString(t, DebugDB(m.Get(f1)), `
00 - 0a220a205c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d881001
015c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d88 - 0000000000000001
020000000000000001 - 5c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d8800000000000000000000000000000000000000000000000000000000000000003fc795fb4006a3d73dd511f493010f6d85c7448155ec6c444acaa9238f30c19e0000000000000001
0c697f48392907a0 - a68447a4189deb99
365a858149c6e2d1 - 57e9d1860d1d68d8
4d65822107fcfd52 - 78629a0f5f3f164f
8866cb397916001e - 9408d2ac22c4d294
d5104dc76695721d - b80704bb7b4d7c03`)
	common.ExpectString(t, DebugDB(m.Get(f2)), `
00 - 0a220a20e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe7392491002
015c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d88 - 0000000000000001
01e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe739249 - 0000000000000002
020000000000000001 - 5c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d8800000000000000000000000000000000000000000000000000000000000000003fc795fb4006a3d73dd511f493010f6d85c7448155ec6c444acaa9238f30c19e0000000000000001
020000000000000002 - e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe7392495c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d889cb71dd39b3e4f3991f66538480b1209e2df16863c33729be03430e1b1500b050000000000000002
069728dc67d9db56 - 8f3aa6d8bef36a80
0c697f48392907a0 - a68447a4189deb99
1b213e776add09fe - 903e28f8376a23b8
365a858149c6e2d1 - 57e9d1860d1d68d8
4d65822107fcfd52 - 78629a0f5f3f164f
8866cb397916001e - 9408d2ac22c4d294
9569f9e2cb82822f - 21ed4caac044316f
b6666b02a03da270 - 1a634384d0ba8f10
cea06b688be116ca - f6bd65cefe8c20dc
d5104dc76695721d - b80704bb7b4d7c03`)
	common.ExpectString(t, DebugDB(m.Get(f3)), `
00 - 0a220a20d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a851003
015c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d88 - 0000000000000001
01d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a85 - 0000000000000003
01e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe739249 - 0000000000000002
020000000000000001 - 5c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d8800000000000000000000000000000000000000000000000000000000000000003fc795fb4006a3d73dd511f493010f6d85c7448155ec6c444acaa9238f30c19e0000000000000001
020000000000000002 - e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe7392495c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d889cb71dd39b3e4f3991f66538480b1209e2df16863c33729be03430e1b1500b050000000000000002
020000000000000003 - d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a85e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe739249d3fed0cc8f431f60ecd3b4617b2ad8991299350cb8361f2ccf02fd50de4c23620000000000000003
069728dc67d9db56 - 8f3aa6d8bef36a80
0c697f48392907a0 - a68447a4189deb99
1b213e776add09fe - 903e28f8376a23b8
299c7bb8757d0bdc - 3bf5be2bb76058f7
365a858149c6e2d1 - 57e9d1860d1d68d8
36bcb1ac1b221c52 - c0fbdb5d49c1a3a8
4d65822107fcfd52 - 78629a0f5f3f164f
789229e09e31f58d - 625250fa2140d8fd
8866cb397916001e - 9408d2ac22c4d294
9569f9e2cb82822f - 21ed4caac044316f
b6666b02a03da270 - 1a634384d0ba8f10
cea06b688be116ca - f6bd65cefe8c20dc
d5104dc76695721d - b80704bb7b4d7c03
dc2864602be7fb85 - d38967f931a50490
f25f4b21eef64b43 - 9c0a8a2bfc0914df`)

	common.FailIfErr(t, m.Stop())
	m2 := NewLevelDBManager(dir, false)
	defer m2.Stop()
	db = m2.Frontier()
	common.ExpectString(t, DebugDB(db), `
00 - 0a220a20d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a851003
015c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d88 - 0000000000000001
01d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a85 - 0000000000000003
01e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe739249 - 0000000000000002
020000000000000001 - 5c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d8800000000000000000000000000000000000000000000000000000000000000003fc795fb4006a3d73dd511f493010f6d85c7448155ec6c444acaa9238f30c19e0000000000000001
020000000000000002 - e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe7392495c93068287abae27e59aa5507bab95c2779e6e65c6d6210153e9614ae44f1d889cb71dd39b3e4f3991f66538480b1209e2df16863c33729be03430e1b1500b050000000000000002
020000000000000003 - d8ba48392cd7843812028c9fc3d7c92e232b8a725db741d69c930772e8551a85e5a89ef7fcf0f3c3fe81a782ac68e497bdb0155b3c41eff02113ab67fe739249d3fed0cc8f431f60ecd3b4617b2ad8991299350cb8361f2ccf02fd50de4c23620000000000000003
069728dc67d9db56 - 8f3aa6d8bef36a80
0c697f48392907a0 - a68447a4189deb99
1b213e776add09fe - 903e28f8376a23b8
299c7bb8757d0bdc - 3bf5be2bb76058f7
365a858149c6e2d1 - 57e9d1860d1d68d8
36bcb1ac1b221c52 - c0fbdb5d49c1a3a8
4d65822107fcfd52 - 78629a0f5f3f164f
789229e09e31f58d - 625250fa2140d8fd
8866cb397916001e - 9408d2ac22c4d294
9569f9e2cb82822f - 21ed4caac044316f
b6666b02a03da270 - 1a634384d0ba8f10
cea06b688be116ca - f6bd65cefe8c20dc
d5104dc76695721d - b80704bb7b4d7c03
dc2864602be7fb85 - d38967f931a50490
f25f4b21eef64b43 - 9c0a8a2bfc0914df`)
}

func TestComparePartitionsToNoPartitions(t *testing.T) {
	withPartitions := NewLevelDBManager(t.TempDir(), true)
	defer withPartitions.Stop()
	withoutPartitions := NewLevelDBManager(t.TempDir(), false)
	defer withoutPartitions.Stop()
	partitionConfigs = []partitionConfig{
		{size: uint64(10), prefix: []byte{1}},
		{size: uint64(100), prefix: []byte{2}},
		{size: uint64(1000), prefix: []byte{3}},
	}

	identifiers := applyMockTransactions(withPartitions, 2000)
	applyMockTransactions(withoutPartitions, 2000)

	db1 := withPartitions.Get(identifiers[2000])
	db2 := withoutPartitions.Get(identifiers[2000])
	common.ExpectString(t, DebugDB(db1), DebugDB(db2))

	db1 = withPartitions.Get(identifiers[1999])
	db2 = withoutPartitions.Get(identifiers[1999])
	common.ExpectString(t, DebugDB(db1), DebugDB(db2))

	db1 = withPartitions.Get(identifiers[1500])
	db2 = withoutPartitions.Get(identifiers[1500])
	common.ExpectString(t, DebugDB(db1), DebugDB(db2))

	db1 = withPartitions.Get(identifiers[986])
	db2 = withoutPartitions.Get(identifiers[986])
	common.ExpectString(t, DebugDB(db1), DebugDB(db2))

	db1 = withPartitions.Get(identifiers[599])
	db2 = withoutPartitions.Get(identifiers[599])
	common.ExpectString(t, DebugDB(db1), DebugDB(db2))

	db1 = withPartitions.Get(identifiers[80])
	db2 = withoutPartitions.Get(identifiers[80])
	common.ExpectString(t, DebugDB(db1), DebugDB(db2))

	db1 = withPartitions.Get(identifiers[9])
	db2 = withoutPartitions.Get(identifiers[9])
	common.ExpectString(t, DebugDB(db1), DebugDB(db2))

	db1 = withPartitions.Get(identifiers[1])
	db2 = withoutPartitions.Get(identifiers[1])
	common.ExpectString(t, DebugDB(db1), DebugDB(db2))
}

func TestPopWithPartitions(t *testing.T) {
	withPartitions := NewLevelDBManager(t.TempDir(), true)
	defer withPartitions.Stop()
	partitionConfigs = []partitionConfig{
		{size: uint64(10), prefix: []byte{1}},
		{size: uint64(100), prefix: []byte{2}},
		{size: uint64(1000), prefix: []byte{3}},
	}

	identifiers := applyMockTransactions(withPartitions, 1000)

	for i := 1000; i > 550; i-- {
		err := withPartitions.Pop()
		common.DealWithErr(err)
	}

	frontierIdentifier := GetFrontierIdentifier(withPartitions.Frontier())
	common.ExpectUint64(t, frontierIdentifier.Height, 550)

	db := withPartitions.Get(identifiers[200])
	frontierIdentifier = GetFrontierIdentifier(db)
	common.ExpectUint64(t, frontierIdentifier.Height, 200)
}

func TestRestabilize(t *testing.T) {
	stable := NewLevelDBManager(t.TempDir(), false)
	defer stable.Stop()

	rawDB := stable.Frontier()
	frontierIdentifier := GetFrontierIdentifier(rawDB)
	memdb := &memdbManager{
		stableDB:           rawDB,
		stableIdentifier:   frontierIdentifier,
		frontierIdentifier: frontierIdentifier,
		previous:           map[types.HashHeight]types.HashHeight{},
		versions:           map[types.HashHeight]DB{frontierIdentifier: rawDB},
		patches:            map[types.HashHeight]Patch{},
	}
	defer memdb.Stop()

	applyMockTransactions(stable, 5)
	applyMockTransactions(memdb, 10)

	rawDB = stable.Frontier()
	currentFrontierIdentifier := memdb.frontierIdentifier
	memdb.Restabilize(rawDB)

	common.ExpectString(t, DebugDB(memdb.stableDB), DebugDB(rawDB))
	common.ExpectString(t, memdb.stableIdentifier.Hash.String(), GetFrontierIdentifier(rawDB).Hash.String())
	common.ExpectString(t, memdb.frontierIdentifier.Hash.String(), currentFrontierIdentifier.Hash.String())
	common.Expect(t, len(memdb.previous), 5)
	common.Expect(t, len(memdb.patches), 5)
	common.Expect(t, len(memdb.versions), 6)
}

// Should be run with benchtime=1x to benchmark the performance
// of uncached Get calls.
func BenchmarkGetWithoutPartitions(b *testing.B) {
	manager := NewLevelDBManager(b.TempDir(), false)
	defer manager.Stop()

	identifiers := applyMockTransactions(manager, 100000)

	b.ResetTimer()

	var dbs []DB
	for i := 0; i < b.N; i++ {
		for j := 1; j <= 10; j++ {
			dbs = append(dbs, manager.Get(identifiers[j]))
		}
	}
	runtime.KeepAlive(dbs)
}

// Should be run with benchtime=1x to benchmark the performance
// of uncached Get calls.
func BenchmarkGetWithPartitions(b *testing.B) {
	manager := NewLevelDBManager(b.TempDir(), true)
	defer manager.Stop()

	identifiers := applyMockTransactions(manager, 100000)

	b.ResetTimer()

	var dbs []DB
	for i := 0; i < b.N; i++ {
		for j := 1; j <= 10; j++ {
			dbs = append(dbs, manager.Get(identifiers[j]))
		}
	}
	runtime.KeepAlive(dbs)
}
