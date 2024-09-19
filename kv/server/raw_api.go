package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	response := new(kvrpcpb.RawGetResponse)
	response.Value, err = reader.GetCF(req.Cf, req.Key)
	if response.Value == nil {
		response.NotFound = true
		return response, nil
	}
	return response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modifies := make([]storage.Modify, 0)
	modifies = append(modifies, storage.Modify{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}})
	err := server.storage.Write(nil, modifies)
	return nil, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	modifies := make([]storage.Modify, 0)
	modifies = append(modifies, storage.Modify{Data: storage.Delete{Key: req.Key, Cf: req.Cf}})
	err := server.storage.Write(nil, modifies)
	return nil, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	response := new(kvrpcpb.RawScanResponse)
	response.Kvs = make([]*kvrpcpb.KvPair, 0)
	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	for i := 0; i < int(req.Limit) && iter.Valid(); i++ {
		item := iter.Item()
		value, err := item.Value()
		if err != nil {
			return nil, err
		}
		kvpair := kvrpcpb.KvPair{Key: item.Key(), Value: value}
		response.Kvs = append(response.Kvs, &kvpair)
		iter.Next()
	}
	return response, nil
}
