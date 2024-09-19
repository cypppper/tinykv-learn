package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

type MyBadgerTxn struct {
	txn *badger.Txn
}

func (t *MyBadgerTxn) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(t.txn, cf, key)
	if err != nil {
		return nil, nil
	}
	return val, nil
}

func (t *MyBadgerTxn) IterCF(cf string) engine_util.DBIterator {
	ite := engine_util.NewCFIterator(cf, t.txn)
	return ite
}

func (t *MyBadgerTxn) Close() {
	t.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	stand_alone_storage := StandAloneStorage{db: engine_util.CreateDB("/tmp/badger", false)}
	return &stand_alone_storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	my_txn := MyBadgerTxn{txn}

	return &my_txn, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			key := engine_util.KeyWithCF(m.Cf(), m.Key())
			txn.Set(key, m.Value())
		case storage.Delete:
			key := engine_util.KeyWithCF(m.Cf(), m.Key())
			txn.Delete(key)
		default:
			panic("panic: no match type")
		}
	}
	txn.Commit()
	return nil
}
