package keydb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const keyBlockSize = 4096
const maxKeySize = 1024
const endOfBlock uint16 = 0x8000
const compressedBit uint16 = 0x8000
const maxPrefixLen uint16 = 0xFF ^ 0x80
const maxCompressedLen uint16 = 0xFF
const keyIndexInterval int = 2 // record every 16th block
const removedKeyLen = 0xFFFFFFFF

var errEmptySegment = errors.New("empty segment")

// called to write a memory segment to disk
// 将segment持久化到磁盘
func writeSegmentToDisk(db *Database, table string, seg segment) error {
	defer db.wg.Done() // allows database to close with no writers pending

	var err error

	itr, err := seg.Lookup(nil, nil)
	if err != nil {
		return err
	}

	id := db.nextSegmentID()

	keyFilename := filepath.Join(db.path, fmt.Sprint(table, ".keys.", id))
	dataFilename := filepath.Join(db.path, fmt.Sprint(table, ".data.", id))

	ds, err := writeAndLoadSegment(keyFilename, dataFilename, itr)
	if err != nil && err != errEmptySegment {
		return err
	}

	db.tables[table].Lock()
	defer db.tables[table].Unlock()

	segments := make([]segment, 0)
	for _, v := range db.tables[table].segments {
		if v == seg {
			if ds != nil {
				segments = append(segments, ds)
			}
		} else {
			segments = append(segments, v)
		}
	}

	db.tables[table].segments = segments

	return nil
}

// 将迭代器包含的数据全部写入给定key/data文件，并封装成diskSegment返回
func writeAndLoadSegment(keyFilename, dataFilename string, itr LookupIterator) (segment, error) {

	keyFilenameTmp := keyFilename + ".tmp"
	dataFilenameTmp := dataFilename + ".tmp"

	keyIndex, err := writeSegmentFiles(keyFilenameTmp, dataFilenameTmp, itr)
	if err != nil {
		os.Remove(keyFilenameTmp)
		os.Remove(dataFilenameTmp)
		return nil, err
	}

	os.Rename(keyFilenameTmp, keyFilename)
	os.Rename(dataFilenameTmp, dataFilename)

	return newDiskSegment(keyFilename, dataFilename, keyIndex), nil
}

// 将迭代器包含的数据全部写入给定key/data文件，返回写入记录的key集合
func writeSegmentFiles(keyFName, dataFName string, itr LookupIterator) ([][]byte, error) {

	var keyIndex [][]byte

	keyF, err := os.OpenFile(keyFName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer keyF.Close()

	dataF, err := os.OpenFile(dataFName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer dataF.Close()

	keyW := bufio.NewWriter(keyF)
	dataW := bufio.NewWriter(dataF)

	var dataOffset int64
	var keyBlockLen int
	var keyCount = 0
	var block = 0

	var zeros = make([]byte, keyBlockSize)

	var prevKey []byte

	for {
		key, value, err := itr.Next()
		if err != nil {
			break
		}
		keyCount++

		if _, err := dataW.Write(value); err != nil {
			return nil, err
		}

		// 判断key已经达到写入目标块大小
		if keyBlockLen+2+len(key)+8+4 >= keyBlockSize-2 { // need to leave room for 'end of block marker'
			// key won't fit in block so move to next
			if err := binary.Write(keyW, binary.LittleEndian, endOfBlock); err != nil {
				return nil, err
			}
			keyBlockLen += 2
			if _, err := keyW.Write(zeros[:keyBlockSize-keyBlockLen]); err != nil {
				return nil, err
			}
			keyBlockLen = 0
			prevKey = nil
		}

		// key块长度为0, 第一次进入循环必定满足
		if keyBlockLen == 0 {
			// 稀疏索引策略，每隔{keyIndexInterval}个key持久化一个
			if block%keyIndexInterval == 0 {
				// 将[]key的值append到[][]keyIndex二位数组
				keycopy := make([]byte, len(key))
				copy(keycopy, key)
				keyIndex = append(keyIndex, keycopy)
			}
			block++
		}

		var dataLen uint32
		if value == nil {
			dataLen = removedKeyLen
		} else {
			dataLen = uint32(len(value))
		}

		dk := encodeKey(key, prevKey)
		prevKey = make([]byte, len(key))
		copy(prevKey, key)

		// 组织key块，再通过for循环将key块添加到buf中，最后一次性调用keyWriter写入文件
		var data = []interface{}{
			uint16(dk.keylen),
			dk.compressedKey,
			int64(dataOffset),
			uint32(dataLen)}
		buf := new(bytes.Buffer)
		for _, v := range data {
			err = binary.Write(buf, binary.LittleEndian, v)
			if err != nil {
				goto failed
			}
		}
		if _, err := keyW.Write(buf.Bytes()); err != nil {
			return nil, err
		}

		// 记录key块的长度
		// key块的结构为:
		// key长度[固定2字节]
		// key实体[变长]
		// key指向的data偏移量[固定8字节], 理论上最多可寻址2^64的磁盘地址
		// key指向的data长度[固定4字节], 单个data最多保存2^32b=4GB的数据
		keyBlockLen += 2 + len(dk.compressedKey) + 8 + 4
		// 累加记录value块的偏移
		if value != nil {
			dataOffset += int64(dataLen)
		}
	}

	// pad key file to block size
	if keyBlockLen > 0 && keyBlockLen < keyBlockSize {
		// key won't fit in block so move to next
		if err := binary.Write(keyW, binary.LittleEndian, endOfBlock); err != nil {
			return nil, err
		}
		keyBlockLen += 2
		if _, err := keyW.Write(zeros[:keyBlockSize-keyBlockLen]); err != nil {
			return nil, err
		}
		keyBlockLen = 0
	}

	if err := keyW.Flush(); err != nil {
		return nil, err
	}
	if err := dataW.Flush(); err != nil {
		return nil, err
	}

	if keyCount == 0 {
		return nil, errEmptySegment
	}

	return keyIndex, nil

failed:
	return nil, err
}

type diskkey struct {
	keylen        uint16
	compressedKey []byte
}

func encodeKey(key, prevKey []byte) diskkey {

	prefixLen := calculatePrefixLen(prevKey, key)
	if prefixLen > 0 {
		key = key[prefixLen:]
		return diskkey{keylen: compressedBit | (uint16(prefixLen<<8) | uint16(len(key))), compressedKey: key}
	}
	return diskkey{keylen: uint16(len(key)), compressedKey: key}
}

func decodeKeyLen(keylen uint16) (prefixLen, compressedLen uint16, err error) {
	if (keylen & compressedBit) != 0 {
		prefixLen = (keylen >> 8) & maxPrefixLen
		compressedLen = keylen & maxCompressedLen
		if prefixLen > maxPrefixLen || compressedLen > maxCompressedLen {
			return 0, 0, errors.New(fmt.Sprint("invalid prefix/compressed length,", prefixLen, compressedLen))
		}
	} else {
		if keylen > maxKeySize {
			return 0, 0, errors.New(fmt.Sprint("key > 1024"))
		}
		compressedLen = keylen
	}
	if compressedLen == 0 {
		return 0, 0, errors.New("decoded key length is 0")
	}
	return
}

func decodeKey(key, prevKey []byte, prefixLen uint16) []byte {
	if prefixLen != 0 {
		key = append(prevKey[:prefixLen], key...)
	}
	return key
}

func calculatePrefixLen(prevKey []byte, key []byte) int {
	if prevKey == nil {
		return 0
	}
	var length = 0
	for ; length < len(prevKey) && length < len(key); length++ {
		if prevKey[length] != key[length] {
			break
		}
	}
	if length > int(maxPrefixLen) || len(key)-length > int(maxCompressedLen) {
		length = 0
	}
	return length
}
