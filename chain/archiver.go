package chain

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/inconshreveable/log15"
	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/chain/account"
	"github.com/zenon-network/go-zenon/chain/archive"
	"github.com/zenon-network/go-zenon/chain/momentum"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/chain/store"
	"github.com/zenon-network/go-zenon/common"
	"github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

type archiveTransaction struct {
	Identifier *archiveIdentifier
	Changes    db.Patch
}

func (t *archiveTransaction) GetCommits() []db.Commit {
	return []db.Commit{t.Identifier}
}

func (t *archiveTransaction) StealChanges() db.Patch {
	changes := t.Changes
	t.Changes = nil
	return changes
}

type archiveIdentifier struct {
	Height       uint64
	Hash         types.Hash
	PreviousHash types.Hash
}

func (m *archiveIdentifier) Identifier() types.HashHeight {
	return types.HashHeight{
		Height: m.Height,
		Hash:   m.Hash,
	}
}

func (m *archiveIdentifier) Previous() types.HashHeight {
	return types.HashHeight{
		Hash:   m.PreviousHash,
		Height: m.Height - 1,
	}
}

func (d *archiveIdentifier) Serialize() ([]byte, error) {
	return []byte{}, nil
}

type archiveExtractor struct {
	patch db.Patch
}

func (ae *archiveExtractor) Put(key []byte, value []byte) {
	if archiveKey := ae.tryToGetArchiveKey(key); archiveKey != nil {
		ae.patch.Put(archiveKey, value)
	}
}

func (ae *archiveExtractor) Delete(key []byte) {
	if archiveKey := ae.tryToGetArchiveKey(key); archiveKey != nil {
		ae.patch.Delete(archiveKey)
	}
}

func (ae *archiveExtractor) tryToGetArchiveKey(key []byte) []byte {
	if bytes.HasPrefix(key, momentum.AccountStorePrefix) {
		key = bytes.TrimPrefix(key, momentum.AccountStorePrefix)
		address := key[:types.AddressSize]
		keyWithoutAddress := key[types.AddressSize:]

		// Archive fused plasma
		if bytes.Equal(address, types.PlasmaContract.Bytes()) {
			prefix := common.JoinBytes(account.StorageKeyPrefix, definition.FusedAmountKeyPrefix)
			if bytes.HasPrefix(keyWithoutAddress, prefix) {
				return bytes.Replace(keyWithoutAddress, prefix, archive.FusedAmountKeyPrefix, 1)
			}
		}

		// Archive Sporks
		if bytes.Equal(address, types.SporkContract.Bytes()) {
			if bytes.HasPrefix(keyWithoutAddress, account.StorageKeyPrefix) {
				return bytes.Replace(keyWithoutAddress, account.StorageKeyPrefix, archive.SporkStorageKeyPrefix, 1)
			}
		}

		// Archive chain plasma
		if bytes.HasPrefix(keyWithoutAddress, account.ChainPlasmaKey) {
			return common.JoinBytes(archive.ChainPlasmaKeyPrefix, address)
		}
	}
	return nil
}

type archiver struct {
	archiveManager db.Manager
	log            log15.Logger
	changes        sync.Mutex
}

func (a *archiver) getFrontierStore() store.Archive {
	if archiveDB := a.archiveManager.Frontier(); archiveDB == nil {
		return nil
	} else {
		return archive.NewArchiveStore(archiveDB)
	}
}

func (a *archiver) GetFrontierArchiveStore() store.Archive {
	a.changes.Lock()
	defer a.changes.Unlock()
	return a.getFrontierStore()
}

func (a *archiver) GetArchiveStore(identifier types.HashHeight) store.Archive {
	a.changes.Lock()
	defer a.changes.Unlock()
	archiveDB := a.archiveManager.Get(identifier)
	if archiveDB == nil {
		return nil
	}
	return archive.NewArchiveStore(archiveDB)
}

func (a *archiver) AddArchiveTransaction(insertLocker sync.Locker, momentumTransaction *nom.MomentumTransaction) error {
	a.log.Info("inserting new momentum to archive", "identifier", momentumTransaction.Momentum.Identifier())
	if insertLocker == nil {
		return errors.Errorf("insertLocker can't be nil")
	}
	if momentumTransaction.Changes == nil {
		return errors.Errorf("momentumTransaction.Changes can't be nil")
	}
	a.changes.Lock()
	defer a.changes.Unlock()
	m := momentumTransaction.Momentum
	identifier := &archiveIdentifier{Height: m.Height, Hash: m.Hash, PreviousHash: m.PreviousHash}
	extractor := &archiveExtractor{patch: db.NewPatch()}
	if err := momentumTransaction.Changes.Replay(extractor); err != nil {
		return err
	}
	transaction := &archiveTransaction{Identifier: identifier, Changes: extractor.patch}
	if err := a.archiveManager.Add(transaction); err != nil {
		return err
	}
	return nil
}

func (a *archiver) RollbackArchiveTo(insertLocker sync.Locker, identifier types.HashHeight) error {
	a.log.Info("rollbacking archive", "to-identifier", identifier)
	if insertLocker == nil {
		return errors.Errorf("insertLocker can't be nil")
	}
	a.changes.Lock()
	defer a.changes.Unlock()
	a.log.Info("preparing to rollback archive", "identifier", identifier)
	store := a.getFrontierStore()
	archiveIdentifier, err := store.GetIdentifierByHash(identifier.Hash)
	if err != nil {
		return err
	}
	if archiveIdentifier.Hash != identifier.Hash {
		return errors.Errorf("can't rollback archive. Expected identifier %v does not exist", identifier)
	}

	for {
		frontier := store.Identifier()
		if frontier.Height == identifier.Height {
			break
		}
		a.log.Info("rollbacking", "momentum-identifier", frontier)
		if err := a.archiveManager.Pop(); err != nil {
			return err
		}
	}

	return nil
}

func (a *archiver) Init(chainManager db.Manager, momentumStore store.Momentum) error {
	a.changes.Lock()
	defer a.changes.Unlock()
	chainFrontier := db.GetFrontierIdentifier(chainManager.Frontier())
	archiveFrontier := db.GetFrontierIdentifier(a.archiveManager.Frontier())

	if archiveFrontier == chainFrontier {
		return nil
	}

	if archiveFrontier.Height > chainFrontier.Height {
		return errors.Errorf("The archive's state is incorrect. " +
			"You can fix the problem by removing the archive database manually.")
	}

	if chainFrontier.Height-archiveFrontier.Height >= 100000 {
		fmt.Println("Initializing archive: 0%")
	}

	for i := archiveFrontier.Height + 1; i <= chainFrontier.Height; i++ {
		momentum, err := momentumStore.GetMomentumByHeight(i)
		if err != nil {
			return err
		}
		changes := chainManager.GetPatch(momentum.Identifier())
		extractor := &archiveExtractor{patch: db.NewPatch()}
		if err := changes.Replay(extractor); err != nil {
			return err
		}
		identifier := &archiveIdentifier{Height: momentum.Height, Hash: momentum.Hash, PreviousHash: momentum.PreviousHash}
		transaction := &archiveTransaction{Identifier: identifier, Changes: extractor.patch}
		if err := a.archiveManager.Add(transaction); err != nil {
			return err
		}
		if i%100000 == 0 {
			fmt.Printf("Initializing archive: %d%%\n", i*100/chainFrontier.Height)
		}
	}

	return nil
}

func NewArchiver(archiveManager db.Manager) *archiver {
	return &archiver{
		archiveManager: archiveManager,
		log:            common.ChainLogger.New("submodule", "archiver"),
	}
}
