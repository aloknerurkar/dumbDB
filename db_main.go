package dumbDatabase

import (
	"github.com/boltdb/bolt"
	"io"
	"log"
	"os"
	"encoding/json"
)

const MAX_KEY_LEN = 1024 //bytes
const DEFAULT_SUFFIX = ".dumbDB"

type DumbDB struct {
	DbFullName string
	// Connection to boltDB (more like file descriptor)
	dbP *bolt.DB
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
	dumbDB.dbP = db
	dumbDB.info_log.Printf("Created new DB %s", dumbDB.dbP.Path())

	return dumbDB
}

/*
 * This is a no-op. We use this in the testing package.
 */
func (db * DumbDB) PrintStats() {
	if os.Getenv("test") == "1" {
		st := db.dbP.Stats()
		json.NewEncoder(os.Stdout).Encode(st)
	}
}

/*
 * RemoveBucket
 * Used to remove a bucket from the DB.
 * @param 	bucket		name of bucket
 */
func (db *DumbDB) RemoveBucket(bucket string) (err error) {
	err = db.dbP.Update(func(tx *bolt.Tx) error {
		e := tx.DeleteBucket([]byte(bucket))
		if e != nil {
			db.err_log.Printf("Error removing Bucket %s.", bucket)
			return e
		}
		return nil
	})
	return
}

/*
 * Get
 * Pointed Get query. Using key.
 * @param 	key		byte slice containing key
 * @param 	bucket		name of bucket
 * @returns 	ret_val		return value as byte slice
 */
func (db *DumbDB) Get(key []byte, bucket string) (ret_val []byte, err error) {

	err = db.dbP.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			db.err_log.Println("Bucket not created yet.")
			return bolt.ErrBucketNotFound
		}

		ret_val = bkt.Get(key)
		if ret_val != nil {
			db.info_log.Println("Found key.")
			return nil
		}

		return bolt.ErrKeyRequired
	})
	return
}

/*
 * GetMultiple
 * GetMultiple values given multiple keys
 * @param 	keys		[][]byte slice containing keys
 * @param 	bucket		name of bucket
 * @returns 	values		return values as slices of byte slice
 */
func (db *DumbDB) GetMultiple(keys [][]byte, bucket string) (values [][]byte, err error) {

	err = db.dbP.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			db.err_log.Println("Bucket not created yet.")
			return bolt.ErrBucketNotFound
		}

		for _, key := range keys {
			value := bkt.Get(key)
			if value != nil {
				values = append(values, value)
			} else {
				db.err_log.Println("Could not find value for key:%s", key)
				values = nil
				return bolt.ErrInvalid
			}
		}
		// Will return empty array if no error occurred
		return nil
	})
	return
}

/*
 * GetAll
 * Get all records from a bucket.
 * @param 	bucket		name of bucket
 * @returns 	ret_val[]	returns slice of records. Each record is a byte slice.
 */
func (db *DumbDB) GetAll(bucket string) (ret_val [][]byte, err error) {

	ret_val = make([][]byte, 0)
	err = db.dbP.View(func(tx *bolt.Tx) error {
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

/*
 * GetLimited
 * Get limited no of records from bucket. Optional cookie can be used.
 * The cookie can be for ex the last record of the previous search. We will seek
 * and continue getting records. This method is used to walk the bucket in ranges.
 * @param 		bucket		name of bucket
 * @param 		size		no of results to return
 * @optional param 	cookie		key of the record to resume search from. Can be nil for last.
 * @returns 		ret_val[]	returns slice of records. Each record is a byte slice.
 */
func (db *DumbDB) GetLimited(bucket string, size int, cookie []byte) (ret_val [][]byte, err error) {
	ret_val = make([][]byte, size)
	itr := 0
	err = db.dbP.View(func(tx *bolt.Tx) error {
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
			_k, _ := c.Seek(cookie)
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
		}
		return nil
	})
	return
}

/*
 * Store
 * Store value into bucket. The bucket will be created if it is a first insert.
 * @param 		bucket		name of bucket
 * @param 		record		key value pair record[0] => key, record[1] => value
 * @returns 		error
 */
func (db *DumbDB) Store(record [][]byte, bucket string) error {
	return db.dbP.Update(func(tx *bolt.Tx) error {

		if len(record[0]) > MAX_KEY_LEN {
			return bolt.ErrKeyTooLarge
		}

		bkt, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}

		err = bkt.Put(record[0], record[1])
		return err;
	})
}

/*
 * Remove
 * Remove value from bucket.
 * @param 		bucket		name of bucket
 * @param 		key		byte slice containing key
 * @returns 		error
 */
func (db *DumbDB) Remove(key []byte, bucket string) error {
	return db.dbP.Update(func(tx *bolt.Tx) error {

		if len(key) > MAX_KEY_LEN {
			return bolt.ErrKeyTooLarge
		}

		bkt := tx.Bucket([]byte(bucket))
		if bkt == nil {
			db.err_log.Println("Failed to open bucket.")
			return bolt.ErrBucketNotFound
		}

		err := bkt.Delete(key)
		if err != nil {
			db.err_log.Printf("Failed to delete entry. ERR %v", err)
			return err
		}
		return nil
	})
}
