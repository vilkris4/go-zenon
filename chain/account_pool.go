package chain

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/inconshreveable/log15"
	"github.com/pkg/errors"

	"github.com/zenon-network/go-zenon/chain/account"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
)

var (
	ErrFailedToAddAccountBlockTransaction = errors.Errorf("failed to insert account-block-transaction")
	ErrPlasmaRatioIsWorse                 = errors.Errorf("plasma ratio is smaller for current block")
	ErrHashTieBreak                       = errors.Errorf("hash tie-break is worse for current block")

	// MaxAccountBlocksInMomentum takes into account batched account-blocks
	MaxAccountBlocksInMomentum = 100
)

type Stable interface {
	GetStableAccountDB(address types.Address) (db.DB, db.Handle)
	ReleaseStableAccountDB(handle db.Handle)
}

type accountPool struct {
	log           log15.Logger
	stable        Stable
	managers      map[types.Address]db.MemDbManager
	changes       sync.Mutex
	stableDbLocks map[db.Handle]bool
}

func (ap *accountPool) getAccountManager(address types.Address) db.MemDbManager {
	manager := ap.managers[address]
	if manager == nil {
		stable, handle := ap.stable.GetStableAccountDB(address)
		manager = db.NewMemDBManager(stable, handle)
		ap.managers[address] = manager
	}
	return manager
}

func (ap *accountPool) canRollback(block *nom.AccountBlock) error {
	log := ap.log.New("header", block.Header())
	address := block.Address
	identifier := block.Identifier()
	previous := block.Previous()

	stable := ap.getStableAccountStore(address)
	defer ap.releaseStableAccountStore(stable)
	stableIdentifier := stable.Identifier()

	// can't insert at all since it's too old
	if stableIdentifier.Height >= identifier.Height {
		log.Info("failed to insert account-block-transaction", "reason", "older than stable identifier", "stable-identifier", stableIdentifier)
		return fmt.Errorf(`%w reason:%v; stable-identifier:%v; identifier:%v`, ErrFailedToAddAccountBlockTransaction, "older than stable identifier", stableIdentifier, identifier)
	}

	frontier := ap.getFrontierAccountStore(address)
	frontierIdentifier := frontier.Identifier()

	// previous doesn't match
	truePrevious, err := frontier.ByHeight(identifier.Height - 1)
	if err != nil {
		log.Info("failed to insert account-block-transaction", "reason", err, "frontier-identifier", frontierIdentifier)
		return fmt.Errorf(`%w reason:%v; frontier-identifier:%v; identifier:%v`, ErrFailedToAddAccountBlockTransaction, err, frontierIdentifier, identifier)
	}
	if truePrevious == nil {
		log.Info("failed to insert account-block-transaction", "reason", "no previous", "frontier-identifier", frontierIdentifier)
		return fmt.Errorf(`%w reason:%v; frontier-identifier:%v; identifier:%v`, ErrFailedToAddAccountBlockTransaction, "missing previous", frontierIdentifier, identifier)
	}
	if truePrevious.Identifier() != previous {
		log.Info("failed to insert account-block-transaction", "reason", "previous mismatch", "frontier-identifier", frontierIdentifier)
		return fmt.Errorf(`%w reason:%v; frontier-identifier:%v; identifier:%v`, ErrFailedToAddAccountBlockTransaction, "missing previous", frontierIdentifier, identifier)
	}

	return nil
}

func higherPriority(a, b *nom.AccountBlock) error {
	if a.TotalPlasma*b.BasePlasma < b.TotalPlasma*a.BasePlasma {
		return ErrPlasmaRatioIsWorse
	} else if a.TotalPlasma*b.BasePlasma == b.TotalPlasma*a.BasePlasma && bytes.Compare(a.Hash.Bytes()[:], b.Hash.Bytes()[:]) > -1 {
		return ErrHashTieBreak
	}

	return nil
}

func (ap *accountPool) AddAccountBlockTransaction(insertLocker sync.Locker, transaction *nom.AccountBlockTransaction) error {
	if insertLocker == nil {
		return errors.Errorf("insertLocker can't be nil")
	}
	ap.changes.Lock()
	defer ap.changes.Unlock()
	return ap.addAccountBlockTransaction(transaction, false)
}
func (ap *accountPool) ForceAddAccountBlockTransaction(insertLocker sync.Locker, transaction *nom.AccountBlockTransaction) error {
	if insertLocker == nil {
		return errors.Errorf("insertLocker can't be nil")
	}
	ap.changes.Lock()
	defer ap.changes.Unlock()
	return ap.addAccountBlockTransaction(transaction, true)
}
func (ap *accountPool) addAccountBlockTransaction(transaction *nom.AccountBlockTransaction, forceAdd bool) error {
	block := transaction.Block
	address := block.Address
	identifier := block.Identifier()
	previous := block.Previous()

	log := ap.log.New("header", block.Header())

	frontier := ap.getFrontierAccountStore(address)
	frontierIdentifier := frontier.Identifier()

	// fast-forward insert on top of chain
	if previous == frontierIdentifier {
		log.Info("fast-forward inserting account-block")
		return ap.getAccountManager(address).Add(transaction)
	}

	// already inserted
	trueBlock, err := frontier.ByHeight(identifier.Height)
	if err != nil {
		log.Info("failed to insert account-block-transaction", "reason", err, "frontier-identifier", frontierIdentifier)
		return fmt.Errorf(`%w reason:%v; frontier-identifier:%v; identifier:%v`, ErrFailedToAddAccountBlockTransaction, err, frontierIdentifier, identifier)
	}
	if trueBlock != nil && trueBlock.Identifier() == identifier {
		log.Info("account-block is already inserted")
		return nil
	}

	if err := ap.canRollback(block); err != nil {
		return err
	}
	if err := higherPriority(block, trueBlock); !forceAdd && err != nil {
		log.Info("failed to insert account-block-transaction", "reason", err, "frontier-identifier", frontierIdentifier)
		return err
	}

	// rollback blocks and insert this one
	manager := ap.getAccountManager(address)
	for {
		currentIdentifier := db.GetFrontierIdentifier(manager.Frontier())
		if currentIdentifier == previous {
			break
		}
		log.Info("rolling back account-block-transaction", "current-identifier", currentIdentifier)
		err = manager.Pop()
		if err != nil {
			log.Info("failed to insert account-block-transaction. can't pop manager", "reason", err, "frontier-identifier", currentIdentifier)
			return fmt.Errorf(`%w can't pop manager; reason:%v; frontier-identifier:%v; identifier:%v`, ErrFailedToAddAccountBlockTransaction, err, currentIdentifier, identifier)
		}
	}

	log.Info("inserting account-block after rollback")
	return ap.getAccountManager(address).Add(transaction)
}

func (ap *accountPool) GetPatch(address types.Address, identifier types.HashHeight) db.Patch {
	ap.changes.Lock()
	defer ap.changes.Unlock()

	return ap.getAccountManager(address).GetPatch(identifier)
}

func (ap *accountPool) Clear() {
	ap.changes.Lock()
	defer ap.changes.Unlock()
	ap.log.Debug("cleaning up account pool, address count: %d", len(ap.managers))

	addresses := make([]types.Address, 0, len(ap.managers))
	for address := range ap.managers {
		addresses = append(addresses, address)
	}

	for _, address := range addresses {
		ap.stable.ReleaseStableAccountDB(ap.managers[address].StableHandle())
		ap.managers[address].Stop()
		delete(ap.managers, address)
	}

	ap.log.Debug("account pool clean up done")
}

func (ap *accountPool) ReleaseAccountStore(store store.Account) {
	ap.changes.Lock()
	defer ap.changes.Unlock()
	delete(ap.stableDbLocks, store.Handle())
	if ap.managers[*store.Address()] == nil {
		ap.stable.ReleaseStableAccountDB(store.Handle())
	}
}

func (ap *accountPool) GetAccountStore(address types.Address, identifier types.HashHeight) store.Account {
	ap.changes.Lock()
	defer ap.changes.Unlock()

	stable := ap.getStableAccountStore(address)
	stableIdentifier := stable.Identifier()
	if stableIdentifier == identifier {
		return stable
	} else if stableIdentifier.Height > identifier.Height {
		ap.log.Info("unable to get account store", "address", address, "stable-identifier", stableIdentifier, "reason", "older than most stable account")
		ap.releaseStableAccountStore(stable)
		return nil
	}
	ap.releaseStableAccountStore(stable)

	manager := ap.getAccountManager(address)
	accountDb := manager.Get(identifier)
	if accountDb == nil {
		frontier := db.GetFrontierIdentifier(manager.Frontier())
		ap.log.Info("unable to get account store", "address", address, "frontier-identifier", frontier, "reason", "missing-db")
		return nil
	}
	store := account.NewAccountStore(address, accountDb, manager.StableHandle())
	ap.stableDbLocks[store.Handle()] = true
	return store
}
func (ap *accountPool) GetFrontierAccountStore(address types.Address) store.Account {
	ap.changes.Lock()
	defer ap.changes.Unlock()
	if ap.hasUncommittedState(address) {
		store := ap.getFrontierAccountStore(address)
		ap.stableDbLocks[store.Handle()] = true
		return store
	} else {
		return ap.getStableAccountStore(address)
	}
}

func (ap *accountPool) hasUncommittedState(address types.Address) bool {
	_, ok := ap.managers[address]
	return ok
}

func (ap *accountPool) getStableAccountStore(address types.Address) store.Account {
	manager := db.NewMemDBManager(ap.stable.GetStableAccountDB(address))
	return account.NewAccountStore(address, manager.Frontier(), manager.StableHandle())
}
func (ap *accountPool) releaseStableAccountStore(store store.Account) {
	ap.stable.ReleaseStableAccountDB(store.Handle())
}
func (ap *accountPool) getFrontierAccountStore(address types.Address) store.Account {
	manager := ap.getAccountManager(address)
	return account.NewAccountStore(address, manager.Frontier(), manager.StableHandle())
}

func (ap *accountPool) InsertMomentum(detailed *nom.DetailedMomentum) {
	ap.changes.Lock()
	defer ap.changes.Unlock()

	if err := ap.rebuild(detailed); err != nil {
		common.ChainLogger.Error("failed to handle InsertMomentum in AccountPool", "reason", err)
	}
}
func (ap *accountPool) DeleteMomentum(*nom.DetailedMomentum) {
	ap.changes.Lock()
	defer ap.changes.Unlock()

	ap.managers = make(map[types.Address]db.MemDbManager)
}
func (ap *accountPool) rebuild(detailed *nom.DetailedMomentum) error {
	addresses := make([]types.Address, 0, len(ap.managers))
	for address := range ap.managers {
		addresses = append(addresses, address)
	}

	ap.log.Debug("started rebuilding account-pool", "momentum-identifier", detailed.Momentum.Identifier())
	for _, address := range addresses {
		log := ap.log.New("address", address)
		log.Debug("start rebuilding")

		uncommitted := make([]*nom.AccountBlock, 0)
		oldManager := ap.managers[address]

		stableDB, stableHandle := ap.stable.GetStableAccountDB(address)
		stable := account.NewAccountStore(address, stableDB, stableHandle)
		uncommittedStore := account.NewAccountStore(address, oldManager.Frontier(), oldManager.StableHandle())
		for i := stable.Identifier().Height + 1; i <= uncommittedStore.Identifier().Height; i += 1 {
			block, err := uncommittedStore.ByHeight(i)
			if err != nil {
				ap.stable.ReleaseStableAccountDB(stableHandle)
				common.DealWithErr(err)
			}
			uncommitted = append(uncommitted, block)
		}
		ap.stable.ReleaseStableAccountDB(stableHandle)

		delete(ap.managers, address)

		_, isOldDbLocked := ap.stableDbLocks[oldManager.StableHandle()]

		if len(uncommitted) == 0 {
			log.Debug("no uncommitted changes")
			if !isOldDbLocked {
				ap.stable.ReleaseStableAccountDB(oldManager.StableHandle())
			}
			oldManager.Stop()
			continue
		}

		log.Debug("staring applying blocks", "num-uncommitted", len(uncommitted))
		stableDB, stableHandle = ap.stable.GetStableAccountDB(address)
		manager := db.NewMemDBManager(stableDB, stableHandle)
		for _, block := range uncommitted {
			patch := oldManager.GetPatch(block.Identifier())
			err := manager.Add(&nom.AccountBlockTransaction{
				Block:   block,
				Changes: patch,
			})
			if err != nil {
				ap.stable.ReleaseStableAccountDB(stableHandle)
				if !isOldDbLocked {
					ap.stable.ReleaseStableAccountDB(oldManager.StableHandle())
				}
				oldManager.Stop()
				return errors.Errorf("account pool rebuild error. Unable to re-apply block %v. Reason %v", block.Header(), err)
			}
		}

		if !isOldDbLocked {
			ap.stable.ReleaseStableAccountDB(oldManager.StableHandle())
		}
		oldManager.Stop()

		ap.managers[address] = manager
		log.Debug("successfully rebuild", "num-uncommitted", len(uncommitted))
	}

	ap.log.Debug("finished rebuilding account-pool")
	return nil
}

func (ap *accountPool) GetNewMomentumContent() []*nom.AccountBlock {
	return ap.filterBlocksToCommit(ap.GetAllUncommittedAccountBlocks())
}
func (ap *accountPool) filterBlocksToCommit(blocks []*nom.AccountBlock) []*nom.AccountBlock {
	toCommit := make([]*nom.AccountBlock, 0, len(blocks))
	batch := make([]*nom.AccountBlock, 0, MaxAccountBlocksInMomentum)
	for index := range blocks {
		batch = append(batch, blocks[index])
		if blocks[index].BlockType != nom.BlockTypeContractSend {
			if len(toCommit)+len(batch) > MaxAccountBlocksInMomentum {
				break
			}
			toCommit = append(toCommit, batch...)
			batch = batch[:0]
		}
	}
	return toCommit
}

func (ap *accountPool) GetAllUncommittedAccountBlocks() []*nom.AccountBlock {
	ap.changes.Lock()
	defer ap.changes.Unlock()

	blocks := make([]*nom.AccountBlock, 0)
	for address := range ap.managers {
		blocks = append(blocks, ap.getUncommittedAccountBlocksByAddress(address)...)
	}

	return blocks
}
func (ap *accountPool) GetUncommittedAccountBlocksByAddress(address types.Address) []*nom.AccountBlock {
	ap.changes.Lock()
	defer ap.changes.Unlock()

	return ap.getUncommittedAccountBlocksByAddress(address)
}
func (ap *accountPool) getUncommittedAccountBlocksByAddress(address types.Address) []*nom.AccountBlock {
	blocks := make([]*nom.AccountBlock, 0)

	stable := ap.getStableAccountStore(address)
	defer ap.releaseStableAccountStore(stable)
	frontier := ap.getFrontierAccountStore(address)
	for i := stable.Identifier().Height + 1; i <= frontier.Identifier().Height; i += 1 {
		block, err := frontier.ByHeight(i)
		common.DealWithErr(err)
		blocks = append(blocks, block)
	}

	return blocks
}

func newAccountPool(stable Stable) *accountPool {
	return &accountPool{
		log:           common.ChainLogger.New("module", "account-pool"),
		stable:        stable,
		managers:      make(map[types.Address]db.MemDbManager),
		stableDbLocks: make(map[db.Handle]bool),
	}
}
func NewAccountPool(stable Stable) AccountPool {
	return newAccountPool(stable)
}
