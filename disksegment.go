package keydb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// the key file uses 4096 byte blocks, the format is
// keylen uint16
// key []byte
// dataoffset int64
// datalen uint32 (if datalen is 0, the key is "removed"
//
// keylen supports compressed keys. if the high bit is set, then the key is compressed,
// with the 8 lower bits for the key len, and the next 7 bits for the run length. a block
// will never start with a compressed key
//
// the special value of 0x7000 marks the end of a block
//
// the data file can only be read in conjunction with the key
// file since there is no length attribute, it is a raw appended
// byte array with the offset and length in the key file
//
type diskSegment struct {
	keyFile   *os.File
	keyBlocks int64 // 数据块数量
	dataFile  *os.File
	id        uint64
	// nil for segments loaded during initial open
	// otherwise holds the key for every keyIndexInterval block
	keyIndex [][]byte
}

type diskSegmentIterator struct {
	segment      *diskSegment
	lower        []byte
	upper        []byte
	buffer       []byte
	block        int64
	bufferOffset int
	key          []byte
	data         []byte
	isValid      bool
	err          error
	finished     bool
}

var errKeyRemoved = errors.New("key removed")

// 从指定目录读取指定table的key/data文件(以{table}.开头)，并解析为segment数组返回. 如果没有指定的文件，返回空数组
func loadDiskSegments(directory string, table string) []segment {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return []segment{}
	}
	segments := []segment{}
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".tmp") {
			panic("tmp files in " + directory)
		}
		if strings.HasPrefix(file.Name(), table+".") {
			index := strings.Index(file.Name(), ".keys.")
			if index < 0 {
				continue

			}
			base := file.Name()[:index]
			id := getSegmentID(file.Name())
			keyFilename := filepath.Join(directory, base+".keys."+strconv.FormatUint(id, 10))
			dataFilename := filepath.Join(directory, base+".data."+strconv.FormatUint(id, 10))
			segments = append(segments, newDiskSegment(keyFilename, dataFilename, nil)) // don't have keyIndex
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].(*diskSegment).id < segments[j].(*diskSegment).id
	})
	return segments
}

func getSegmentID(filename string) uint64 {
	base := filepath.Base(filename)
	index := strings.LastIndex(base, ".")
	if index >= 0 {
		id, err := strconv.Atoi(base[index+1:])
		if err == nil {
			return uint64(id)
		}
	}
	return 0
}

// 将一个key/data文件对映射为一个diskSegment
// keyIndex: 为nil时(在读取文件生成diskSegment时)会从key文件读取; 不为nil(写入数据到文件时)则直接使用keyIndex作为返回diskSegment的索引
func newDiskSegment(keyFilename, dataFilename string, keyIndex [][]byte) segment {

	segmentID := getSegmentID(keyFilename)

	ds := &diskSegment{}
	kf, err := os.Open(keyFilename)
	if err != nil {
		panic(err)
	}
	df, err := os.Open(dataFilename)
	if err != nil {
		panic(err)
	}
	ds.keyFile = kf
	ds.dataFile = df

	fi, err := kf.Stat()
	if err != nil {
		panic(err)
	}

	ds.keyBlocks = (fi.Size()-1)/keyBlockSize + 1 // key block数量
	ds.id = segmentID

	if keyIndex == nil {
		// TODO maybe load this in the background
		keyIndex = loadKeyIndex(kf, ds.keyBlocks)
	}

	ds.keyIndex = keyIndex

	return ds
}

// 从索引文件kf构建索引
func loadKeyIndex(kf *os.File, keyBlocks int64) [][]byte {
	buffer := make([]byte, keyBlockSize)
	keyIndex := make([][]byte, 0)
	// build key index
	var block int64
	for block = 0; block < keyBlocks; block += int64(keyIndexInterval) {
		_, err := kf.ReadAt(buffer, block*keyBlockSize)
		if err != nil {
			keyIndex = nil
			break
		}
		keylen := binary.LittleEndian.Uint16(buffer)
		if keylen == endOfBlock {
			break
		}
		keycopy := make([]byte, keylen)
		copy(keycopy, buffer[2:2+keylen])
		keyIndex = append(keyIndex, keycopy)
	}
	return keyIndex
}

// 从diskSegment迭代读取数据
func (dsi *diskSegmentIterator) Next() (key []byte, value []byte, err error) {
	if dsi.isValid {
		dsi.isValid = false
		return dsi.key, dsi.data, dsi.err
	}
	dsi.nextKeyValue()
	dsi.isValid = false
	return dsi.key, dsi.data, dsi.err
}

func (dsi *diskSegmentIterator) peekKey() ([]byte, error) {
	if dsi.isValid {
		return dsi.key, dsi.err
	}
	dsi.nextKeyValue()
	return dsi.key, dsi.err
}

func (dsi *diskSegmentIterator) nextKeyValue() error {
	if dsi.finished {
		return EndOfIterator
	}
	var prevKey = dsi.key

	for {
		// 读取16bit的数据
		keylen := binary.LittleEndian.Uint16(dsi.buffer[dsi.bufferOffset:])
		// 读到key结束块
		if keylen == endOfBlock {
			dsi.block++
			// 消费过的块数量 等于 当前diskSegment标记的最大数据块，此segment消费完毕
			if dsi.block == dsi.segment.keyBlocks {
				dsi.finished = true
				dsi.err = EndOfIterator
				dsi.key = nil
				dsi.data = nil
				dsi.isValid = true
				return dsi.err
			}
			// 消费一个新的块，数据缓存在dsi.buffer
			n, err := dsi.segment.keyFile.ReadAt(dsi.buffer, dsi.block*keyBlockSize)
			if err != nil {
				return err
			}
			// 异常! 读取到不完整的数据块
			if n != keyBlockSize {
				return errors.New(fmt.Sprint("did not read block size, read ", n))
			}
			// 从头(0)开始消费数据块
			dsi.bufferOffset = 0
			prevKey = nil
			continue
		}

		/*解析一个key*/
		prefixLen, compressedLen, err := decodeKeyLen(keylen)
		if err != nil {
			return err
		}

		dsi.bufferOffset += 2
		key := dsi.buffer[dsi.bufferOffset : dsi.bufferOffset+int(compressedLen)]
		dsi.bufferOffset += int(compressedLen)

		key = decodeKey(key, prevKey, prefixLen)

		// 解析key对应的data的下标
		dataoffset := binary.LittleEndian.Uint64(dsi.buffer[dsi.bufferOffset:])
		dsi.bufferOffset += 8 // Uint64 = 8byte
		// 解析key对应的data的长度
		datalen := binary.LittleEndian.Uint32(dsi.buffer[dsi.bufferOffset:])
		dsi.bufferOffset += 4 // Uint32 = 4byte

		prevKey = key

		/* 指定了区间，则继续循环，直到找出目标区间的key */
		if dsi.lower != nil {
			if less(key, dsi.lower) {
				continue
			}
			if equal(key, dsi.lower) {
				goto found
			}
		}
		if dsi.upper != nil {
			if equal(key, dsi.upper) {
				goto found
			}
			if !less(key, dsi.upper) {
				dsi.finished = true
				dsi.isValid = true
				dsi.key = nil
				dsi.data = nil
				dsi.err = EndOfIterator
				return EndOfIterator
			}
		}
	found:

		if datalen == removedKeyLen {
			// 被更新移除的键
			dsi.data = nil
		} else {
			// 从dataFile读取从{dataoffset}开始的，{datalen}长度的数据到dsi.data
			dsi.data = make([]byte, datalen)
			_, err = dsi.segment.dataFile.ReadAt(dsi.data, int64(dataoffset))
		}
		// key
		dsi.key = key
		// 标记迭代器完成了一次数据读取
		dsi.isValid = true
		return err
	}
}

func (ds *diskSegment) Put(key []byte, value []byte) error {
	panic("disk segments are not mutable, unable to Put")
}

func (ds *diskSegment) Get(key []byte) ([]byte, error) {
	offset, len, err := binarySearch(ds, key)
	if err == errKeyRemoved {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, len)
	_, err = ds.dataFile.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func binarySearch(ds *diskSegment, key []byte) (offset int64, length uint32, err error) {
	buffer := make([]byte, keyBlockSize)

	var lowblock int64 = 0
	highblock := ds.keyBlocks - 1

	if ds.keyIndex != nil { // we have memory index, so narrow block range down
		index := sort.Search(len(ds.keyIndex), func(i int) bool {
			return less(key, ds.keyIndex[i])
		})

		if index == 0 {
			return 0, 0, KeyNotFound
		}

		index--

		lowblock = int64(index * keyIndexInterval)
		highblock = lowblock + int64(keyIndexInterval)

		if highblock >= ds.keyBlocks {
			highblock = ds.keyBlocks - 1
		}
	}

	block, err := binarySearch0(ds, lowblock, highblock, key, buffer)
	if err != nil {
		return 0, 0, err
	}
	return scanBlock(ds, block, key, buffer)
}

// returns the block that may contain the key, or possible the next block - since we do not have a 'last key' of the block
func binarySearch0(ds *diskSegment, lowBlock int64, highBlock int64, key []byte, buffer []byte) (int64, error) {
	if highBlock-lowBlock <= 1 {
		// the key is either in low block or high block, or does not exist, so check high block
		ds.keyFile.ReadAt(buffer, highBlock*keyBlockSize)
		keylen := binary.LittleEndian.Uint16(buffer)
		skey := buffer[2 : 2+keylen]
		if less(key, skey) {
			return lowBlock, nil
		} else {
			return highBlock, nil
		}
	}

	block := (highBlock-lowBlock)/2 + lowBlock

	ds.keyFile.ReadAt(buffer, block*keyBlockSize)
	keylen := binary.LittleEndian.Uint16(buffer)
	skey := buffer[2 : 2+keylen]

	if less(key, skey) {
		return binarySearch0(ds, lowBlock, block, key, buffer)
	} else {
		return binarySearch0(ds, block, highBlock, key, buffer)
	}
}

func scanBlock(ds *diskSegment, block int64, key []byte, buffer []byte) (offset int64, len uint32, err error) {
	_, err = ds.keyFile.ReadAt(buffer, block*keyBlockSize)
	if err != nil {
		return 0, 0, err
	}

	index := 0
	var prevKey []byte = nil
	for {
		keylen := binary.LittleEndian.Uint16(buffer[index:])
		if keylen == endOfBlock {
			return 0, 0, KeyNotFound
		}

		var compressedLen = keylen
		var prefixLen = 0

		if keylen&compressedBit != 0 {
			prefixLen = int((keylen >> 8) & maxPrefixLen)
			compressedLen = keylen & maxCompressedLen
		}

		endkey := index + 2 + int(compressedLen)
		_key := buffer[index+2 : endkey]

		if prefixLen > 0 {
			_key = append(prevKey[:prefixLen], _key...)
		}

		prevKey = _key

		if bytes.Equal(_key, key) {
			offset = int64(binary.LittleEndian.Uint64(buffer[endkey:]))
			len = binary.LittleEndian.Uint32(buffer[endkey+8:])
			if len == removedKeyLen {
				err = errKeyRemoved
			}
			return
		}
		if !less(_key, key) {
			return 0, 0, KeyNotFound
		}
		index = endkey + 12
	}
}

func (ds *diskSegment) Remove(key []byte) ([]byte, error) {
	panic("disk segments are immutable, unable to Remove")
}

func (ds *diskSegment) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	buffer := make([]byte, keyBlockSize)
	var block int64 = 0
	if lower != nil {
		startBlock, err := binarySearch0(ds, 0, ds.keyBlocks-1, lower, buffer)
		if err != nil {
			return nil, err
		}
		block = startBlock
	}
	n, err := ds.keyFile.ReadAt(buffer, block*keyBlockSize)
	if err != nil {
		return nil, err
	}
	if n != keyBlockSize {
		return nil, errors.New(fmt.Sprint("did not read block size ", n))
	}
	return &diskSegmentIterator{segment: ds, lower: lower, upper: upper, buffer: buffer, block: block}, nil
}

func (ds *diskSegment) Close() error {
	err0 := ds.keyFile.Close()
	err1 := ds.dataFile.Close()
	return errn(err0, err1)
}
