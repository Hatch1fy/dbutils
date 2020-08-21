package dbutils

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/hatchify/errors"
)

const (
	// ErrNotInitialized is returned when actions are performed on a non-initialized instance of db utils
	ErrNotInitialized = errors.Error("dbutils have not been properly initialized")
	// ErrInvalidIndexBytes are returned when an index length is niether 0 nor 8 bytes
	ErrInvalidIndexBytes = errors.Error("invalid index bytes")
)

const (
	indexBktKey = "indexes"
)

var (
	indexBktKeyBytes = []byte(indexBktKey)
)

// New will return a new instance of DBUtils
func New(numDigits int) *DBUtils {
	var dbu DBUtils
	dbu.indexFmt = fmt.Sprintf("%s0%dd", "%", numDigits)
	return &dbu
}

// DBUtils provides a set of common db utilities
type DBUtils struct {
	indexFmt string
}

// Init will initialize DBUtils
func (dbu *DBUtils) Init(txn *bolt.Tx) (err error) {
	_, err = txn.CreateBucketIfNotExists(indexBktKeyBytes)
	return
}

func (dbu *DBUtils) get(txn *bolt.Tx, indexKey []byte) (index uint64, err error) {
	// Get meta bucket
	bkt := txn.Bucket(indexBktKeyBytes)
	if bkt == nil {
		err = ErrNotInitialized
		return
	}

	return bytesToIndex(bkt.Get(indexKey))
}

func (dbu *DBUtils) set(txn *bolt.Tx, indexKey []byte, index uint64) (err error) {
	// Get meta bucket
	bkt := txn.Bucket(indexBktKeyBytes)
	if bkt == nil {
		err = ErrNotInitialized
		return
	}

	// Put new index bytes
	return bkt.Put(indexKey, indexToBytes(index))
}

// GetCurrent will get the current index bytes
// Note: This will NOT increment the value, it is just the "on-deck" value
func (dbu *DBUtils) GetCurrent(txn *bolt.Tx, indexKey []byte) (indexBytes []byte, err error) {
	var index uint64
	// Get current index
	if index, err = dbu.get(txn, indexKey); err != nil {
		return
	}

	// Convert index to index bytes
	indexBytes = []byte(fmt.Sprintf(dbu.indexFmt, index))
	return
}

// Next will get the next available index
// Note: This will increment the index value so the next "on-deck" value will be index + 1
func (dbu *DBUtils) Next(txn *bolt.Tx, indexKey []byte) (indexBytes []byte, err error) {
	var index uint64
	// Get current index
	if index, err = dbu.get(txn, indexKey); err != nil {
		return
	}

	// Update index to be current index plus 1
	if err = dbu.set(txn, indexKey, index+1); err != nil {
		return
	}

	// Convert index to index bytes
	indexBytes = []byte(fmt.Sprintf(dbu.indexFmt, index))
	return
}

// Set will manually set the current index value
// Note: This will increment the index value so the next "on-deck" value will be index + 1
func (dbu *DBUtils) Set(txn *bolt.Tx, indexKey []byte, indexValue uint64) (err error) {
	// Set the current index value as the provided value
	return dbu.set(txn, indexKey, indexValue)
}
