package dbutils

import (
	"fmt"
	"os"
	"testing"

	"github.com/boltdb/bolt"
)

func TestDBUtils(t *testing.T) {
	var (
		dbu *DBUtils
		db  *bolt.DB
		err error
	)

	if err = os.MkdirAll("./test_data", 0744); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./test_data")

	if db, err = bolt.Open("./test_data/dbutils.bdb", 0644, nil); err != nil {
		t.Fatal(err)
	}

	dbu = New(8)

	if err = db.Update(func(txn *bolt.Tx) (err error) {
		return dbu.Init(txn)
	}); err != nil {
		t.Fatal(err)
	}

	if err = db.Update(func(txn *bolt.Tx) (err error) {
		var indexBytes []byte
		if indexBytes, err = dbu.GetCurrent(txn, []byte("test_index")); err != nil {
			return
		}

		var index uint64
		if index, err = ToUint(indexBytes); err != nil {
			return
		}

		if index != 0 {
			return fmt.Errorf("invalid value, expected %d and received %d", 0, index)
		}

		if indexBytes, err = dbu.Next(txn, []byte("test_index")); err != nil {
			return
		}

		if index, err = ToUint(indexBytes); err != nil {
			return
		}

		if index != 0 {
			return fmt.Errorf("invalid value, expected %d and received %d", 0, index)
		}

		if indexBytes, err = dbu.Next(txn, []byte("test_index")); err != nil {
			return
		}

		if index, err = ToUint(indexBytes); err != nil {
			return
		}

		if index != 1 {
			return fmt.Errorf("invalid value, expected %d and received %d", 1, index)
		}

		if indexBytes, err = dbu.Next(txn, []byte("test_index")); err != nil {
			return
		}

		if index, err = ToUint(indexBytes); err != nil {
			return
		}

		if index != 2 {
			return fmt.Errorf("invalid value, expected %d and received %d", 2, index)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

	if err = db.Update(func(txn *bolt.Tx) (err error) {
		var indexBytes []byte
		if indexBytes, err = dbu.Next(txn, []byte("test_index")); err != nil {
			return
		}

		var index uint64
		if index, err = ToUint(indexBytes); err != nil {
			return
		}

		if index != 3 {
			return fmt.Errorf("invalid value, expected %d and received %d", 3, index)
		}

		return
	}); err != nil {
		t.Fatal(err)
	}

}
