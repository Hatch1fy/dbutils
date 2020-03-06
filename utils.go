package dbutils

import (
	"strconv"
	"unsafe"
)

func bytesToIndex(bs []byte) (index uint64, err error) {
	if len(bs) == 0 {
		// Empty bytes are acceptable, we count it as 0
		return
	}

	switch len(bs) {
	case 0:
		// Empty bytes are acceptable, we count it as 0
		return
	case 8:
		// Cast bytes as a uint64
		// Note: This is not safe to switch between little endian and small endian systems.
		// If the data is set as one type, it must be maintained on the same endian type
		index = *(*uint64)(unsafe.Pointer(&bs[0]))

	default:
		err = ErrInvalidIndexBytes
	}

	return
}

func indexToBytes(index uint64) (bs []byte) {
	arr := *(*[8]byte)(unsafe.Pointer(&index))
	bs = arr[:]
	return
}

// ToUint will convert index bytes to a uint64 index
func ToUint(indexBytes []byte) (index uint64, err error) {
	return strconv.ParseUint(string(indexBytes), 10, 64)
}
