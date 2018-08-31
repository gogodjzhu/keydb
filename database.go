package keydb

import (
	"errors"
	"github.com/nightlyone/lockfile"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// a Database reference is obtained via Open()
type Database struct {
	sync.Mutex
	tables       map[string]*internalTable
	open         bool
	closing      bool
	transactions map[uint64]*Transaction
	path         string
	wg           sync.WaitGroup
	nextSegID    uint64
	lockfile     lockfile.Lockfile
}

// defines a table in the database
type Table struct {
	// the name of the table, should not have any special characters
	Name string
	// the key comparison interface
	Compare KeyCompare
}

type internalTable struct {
	sync.Mutex
	table        Table
	segments     []segment
	transactions int
}

// iterator interface for table scanning. all iterators should be read until completion
type LookupIterator interface {
	// returns EndOfIterator when complete, if err is nil, then key and value are valid
	Next() (key []byte, value []byte, err error)
	// returns the next non-deleted key in the index
	peekKey() ([]byte, error)
}

var dblock sync.RWMutex

// open a database. The database can only be opened by a single process, but the *Database
// reference can be shared across Go routines. The path is a directory name.
// if createIfNeeded is true, them if the db doesn't exist it will be created
// Additional tables can be added on subsequent opens, but there is no current way to delete a table,
// except for deleting the table related files from the directory
func Open(path string, tables []Table, createIfNeeded bool) (*Database, error) {
	dblock.Lock()
	defer dblock.Unlock()

	db, err := open(path, tables)
	if err == NoDatabaseFound && createIfNeeded == true {
		return create(path, tables)
	}
	return db, err
}

func open(path string, tables []Table) (*Database, error) {

	path = filepath.Clean(path)

	fi, err := os.Stat(path)
	if err != nil {
		return nil, NoDatabaseFound
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
	case mode.IsRegular():
		return nil, NoDatabaseFound
	}

	abs, err := filepath.Abs(path + "/lockfile")
	if err != nil {
		return nil, err
	}
	lf, err := lockfile.New(abs)
	if err != nil {
		return nil, err
	}
	err = lf.TryLock()
	if err != nil {
		return nil, DatabaseInUse
	}

	db := &Database{path: path, open: true}
	db.lockfile = lf
	db.transactions = make(map[uint64]*Transaction)
	db.tables = make(map[string]*internalTable)
	for _, v := range tables {
		it := &internalTable{table: v, segments: loadDiskSegments(path, v.Name, v.Compare)}
		db.tables[v.Name] = it
	}

	db.wg.Add(1)
	go mergeDiskSegments(db)

	return db, nil
}

func create(path string, tables []Table) (*Database, error) {
	path = filepath.Clean(path)

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return open(path, tables)
}

// remove the database, deleting all files. the caller must be able to
// gain exclusive access to the database
func Remove(path string) error {
	dblock.Lock()
	defer dblock.Unlock()

	path = filepath.Clean(path)

	fi, err := os.Stat(path)
	if err != nil {
		return err
	}

	abs, err := filepath.Abs(path + "/lockfile")
	if err != nil {
		return err
	}
	lf, err := lockfile.New(abs)
	if err != nil {
		return err
	}
	err = lf.TryLock()
	if err != nil {
		return DatabaseInUse
	}

	switch mode := fi.Mode(); {
	case mode.IsDir():
	case mode.IsRegular():
		return errors.New("path is not a directory")
	}

	return os.RemoveAll(path)
}

// close the database. any memory segments are persisted to disk.
// The resulting segments are merged until the default maxSegments is reached
func (db *Database) Close() error {
	dblock.Lock()
	defer dblock.Unlock()
	if !db.open {
		return DatabaseClosed
	}
	if len(db.transactions) > 0 {
		return DatabaseHasOpenTransactions
	}

	db.Lock()
	db.closing = true
	db.Unlock()

	db.wg.Wait()

	mergeDiskSegments0(db, maxSegments)

	for _, table := range db.tables {
		for _, segment := range table.segments {
			segment.Close()
		}
	}

	db.lockfile.Unlock()
	db.open = false

	return nil
}

// close the database with control of the segment count. if segmentCount is 0, then
// the merge process is skipped
func (db *Database) CloseWithMerge(segmentCount int) error {
	dblock.Lock()
	defer dblock.Unlock()
	if !db.open {
		return DatabaseClosed
	}
	if len(db.transactions) > 0 {
		return DatabaseHasOpenTransactions
	}

	db.Lock()
	db.closing = true
	db.Unlock()

	db.wg.Wait()

	if segmentCount > 0 {
		mergeDiskSegments0(db, segmentCount)
	}

	for _, table := range db.tables {
		for _, segment := range table.segments {
			segment.Close()
		}
	}

	db.lockfile.Unlock()
	db.open = false

	return nil
}

func (db *Database) nextSegmentID() uint64 {
	return atomic.AddUint64(&db.nextSegID, 1)
}
