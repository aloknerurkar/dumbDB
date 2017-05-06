package dumbDB

import (
	"github.com/boltdb/bolt"
	"io"
	"log"
	"os"
	"encoding/json"
)

const MAX_KEY_LEN = 1024 //bytes
const DEFAULT_SUFFIX = ".dumbDB"

type Record interface {
	GetKey() [] byte
	GetVal() [] byte
}

type DumbDB struct {
	DbFullName string
	// Connection to boltDB (more like file descriptor)
	_db *bolt.DB
	// Logger
	err_log *log.Logger
	info_log *log.Logger
}

func (db *DumbDB) initLogger(logger_op io.Writer)  {
	db.err_log = log.New(logger_op, "DumbDB\tERROR\t", log.Ldate|log.Ltime|log.Lshortfile)
	db.info_log = log.New(logger_op, "DumbDB\tINFO\t", log.Ldate|log.Ltime|log.Lshortfile)
}

func NewDumbDB(root_path string, name string, logger_out io.Writer) *DumbDB {
	dumbDB := new(DumbDB)
	dumbDB.initLogger(logger_out)
	if _, e := os.Stat(root_path); e != nil && os.IsNotExist(e) {
		dumbDB.err_log.Printf("Root path invalid %s", root_path)
		return nil
	}
	dumbDB.DbFullName = root_path + "/" + name + DEFAULT_SUFFIX
	db, err := bolt.Open(dumbDB.DbFullName, 0600, nil)
	if err != nil {
		dumbDB.err_log.Printf("Failed to open new database path %s", dumbDB.DbFullName)
		return nil
	}
	dumbDB._db = db
	dumbDB.info_log.Printf("Created new DB %s", dumbDB._db.Path())

	return dumbDB
}

func (db *DumbDB) RemoveBucket(bucket string) (err error) {
	err = db._db.Update(func(tx *bolt.Tx) error {
		e := tx.DeleteBucket([]byte(bucket))
		if e != nil || e != bolt.ErrBucketNotFound {
			db.err_log.Printf("Bucket not found %s.", bucket)
			return e
		}
		return nil
	})
	return
}

func (db * DumbDB) PrintStats() {
	if os.Getenv("test") == "1" {
		st := db._db.Stats()
		json.NewEncoder(os.Stdout).Encode(st)
	}
}

func (db *DumbDB) Get(key Record, bucket string) (ret_val []byte, err error) {

	err = db._db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			db.err_log.Println("Bucket not created yet.")
			return bolt.ErrBucketNotFound
		}

		ret_val = bkt.Get(key.GetKey())
		if ret_val != nil {
			db.info_log.Println("Found key.")
			return nil
		}

		return bolt.ErrKeyRequired
	})
	return
}

func (db *DumbDB) GetAll(bucket string) (ret_val [][]byte, err error) {

	ret_val = make([][]byte, 0)
	err = db._db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			db.err_log.Println("Bucket not created yet.")
			return bolt.ErrBucketNotFound
		}

		c := bkt.Cursor()

		for k, v := c.Last(); k != nil; k, v = c.Prev() {
			ret_val = append(ret_val, v)
			db.info_log.Println("Added value")
		}
		return nil
	})
	return
}

func (db *DumbDB) GetLimited(bucket string, size int, cookie Record) (ret_val [][]byte, next []byte, err error) {
	ret_val = make([][]byte, size)
	itr := 0
	err = db._db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			db.err_log.Println("Bucket not created yet.")
			return bolt.ErrBucketNotFound
		}

		c := bkt.Cursor()

		init_kv := make([][]byte, 2)
		if cookie != nil {
			// This will seek to the last result of the
			// previous search. Initialize the first to the previous val.
			_k, _ := c.Seek(cookie.GetKey())
			if _k == nil {
				db.err_log.Println("Got invalid cookie.")
				return bolt.ErrKeyRequired
			}
			init_kv[0], init_kv[1] = c.Prev()
		} else {
			init_kv[0], init_kv[1] = c.Last()
		}

		for k, v := init_kv[0], init_kv[1]; k != nil && itr < size; k, v = c.Prev() {
			ret_val[itr] = v
			db.info_log.Println("Added value")
			itr++
			next = k
		}
		return nil
	})
	return
}

func (db *DumbDB) Store(record Record, bucket string) error {
	return db._db.Update(func(tx *bolt.Tx) error {

		if len(record.GetKey()) > MAX_KEY_LEN {
			return bolt.ErrKeyTooLarge
		}

		bkt, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		err = bkt.Put(record.GetKey(), record.GetVal())
		return err;
	})
}

func (db *DumbDB) Remove(key Record, bucket string) error {
	return db._db.Update(func(tx *bolt.Tx) error {

		if len(key.GetKey()) > MAX_KEY_LEN {
			return bolt.ErrKeyTooLarge
		}

		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			db.err_log.Println("Failed to open bucket.")
			return bolt.ErrBucketNotFound
		}

		err := bkt.Delete(key.GetKey())
		if err != nil {
			db.err_log.Printf("Failed to delete entry. ERR %v", err)
			return err
		}
		return nil
	})
}