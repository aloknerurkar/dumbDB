package tests

import (
	"os"
	"log"
	"testing"
	"dumbDB"
	"encoding/binary"
	"encoding/json"
	"github.com/boltdb/bolt"
)

type UserRecord struct {
	ID int
	Name string
	Position string
}

func (ur UserRecord) GetKey() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(ur.ID))
	return b
}

func (ur UserRecord) GetVal() []byte {
	b, err := json.Marshal(ur)
	if err != nil {
		log.Fatal("Failed marshalling")
	}
	return b
}

type LargeKeyRecord struct {
	Dummy string
}

func (lr LargeKeyRecord) GetKey() []byte {
	b := make([]byte, 2048)
	for i := 0; i < 2048; i++ {
		b[i] = byte(0xff)
	}
	return b
}

func (lr LargeKeyRecord) GetVal() []byte {
	b, err := json.Marshal(lr)
	if err != nil {
		log.Fatal("Failed marshalling")
	}
	return b
}

var removeDbFile = func(name string) {
	_ = os.Remove(name)
}

func TestMain(t *testing.M) {
	os.Setenv("test", "1")
	os.Exit(t.Run())
}

var user1 = UserRecord {
	ID: 1,
	Name: "Alok",
	Position: "Engineer",
}

var user2 = UserRecord {
	ID: 2,
	Name: "Ameya",
	Position: "Engineer",
}

// 1. Create 2 test records and store
// 2. Do a GetAll and make sure we return 2 records.
// 3. Try to store duplicate key => This should error.
// 4. Try to store key more than 1024 bytes => This should fail.
func TestDumbDB_Store(t *testing.T) {

	dbName := "TestDumbDB_Store"
	dbP := dumbDB.NewDumbDB(".", dbName, os.Stdout)

	if dbP == nil {
		t.Errorf("Error creating DB %s", dbName)
	}

	err := dbP.Store(user1, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user1, err.Error())
	}

	err = dbP.Store(user2, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user2, err.Error())
	}

	records, err := dbP.GetAll(dbName)
	if err != nil || len(records) != 2 {
		t.Errorf("Returned incorrect no of records Expected: %d Got: %d", 2, len(records))
	}

	dummy := LargeKeyRecord{
		Dummy: "This will simulate large key record.",
	}

	err = dbP.Store(dummy, dbName)
	if err != bolt.ErrKeyTooLarge {
		t.Errorf("Expected error while creating record. Key size: %d", len(dummy.GetKey()))
	}

	dbP.PrintStats()
	removeDbFile(dbP.DbFullName)
}

// 1. Create 2 test records and store
// 2. Remove the 2 records stored. Should not return error.
// 3. Try to remove by sending huge key. => This should fail
func TestDumbDB_Remove(t *testing.T) {

	dbName := "TestDumbDB_Remove"
	dbP := dumbDB.NewDumbDB(".", dbName, os.Stdout)

	if dbP == nil {
		t.Errorf("Error creating DB %s", dbName)
	}

	err := dbP.Store(user1, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user1, err.Error())
	}

	err = dbP.Store(user2, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user2, err.Error())
	}

	err = dbP.Remove(user1, dbName)
	if err != nil {
		t.Errorf("Error deleting Record Record: %v Error: %s", user1, err.Error())
	}

	err = dbP.Remove(user2, dbName)
	if err != nil {
		t.Errorf("Error deleting Record Record: %v Error: %s", user2, err.Error())
	}

	dummy := LargeKeyRecord{
		Dummy: "This will simulate large key record.",
	}

	err = dbP.Remove(dummy, dbName)
	if err != bolt.ErrKeyTooLarge {
		t.Errorf("Expected error while creating record. Key size: %d", len(dummy.GetKey()))
	}

	dbP.PrintStats()
	removeDbFile(dbP.DbFullName)
}

// 1. Create 2 test records and store
// 2. Remove the 2 records stored. Should not return error.
// 3. Try to remove by sending huge key. => This should fail
func TestDumbDB_Get(t *testing.T) {

	dbName := "TestDumbDB_Get"
	dbP := dumbDB.NewDumbDB(".", dbName, os.Stdout)

	if dbP == nil {
		t.Errorf("Error creating DB %s", dbName)
	}

	err := dbP.Store(user1, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user1, err.Error())
	}

	err = dbP.Store(user2, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user2, err.Error())
	}

	rec1, err := dbP.Get(user1, dbName)
	if err != nil {
		t.Errorf("Error getting Record Record: %v Error: %s", user1, err.Error())
	}

	ret_user1 := UserRecord{}
	err = json.Unmarshal(rec1, &ret_user1)
	if err != nil {
		t.Errorf("Error unmarshaling Record Error: %s", err.Error())
	}

	if ret_user1.Name != user1.Name || ret_user1.ID != user1.ID || ret_user1.Position != user1.Position {
		t.Errorf("Got incorrect value. Expected: %v Got: %v", user1, ret_user1)
	}

	rec2, err := dbP.Get(user2, dbName)
	if err != nil {
		t.Errorf("Error getting Record Record: %v Error: %s", user1, err.Error())
	}

	ret_user2 := UserRecord{}
	err = json.Unmarshal(rec2, &ret_user2)
	if err != nil {
		t.Errorf("Error unmarshaling Record Error: %s", err.Error())
	}

	if ret_user2.Name != user2.Name || ret_user2.ID != user2.ID || ret_user2.Position != user2.Position {
		t.Errorf("Got incorrect value. Expected: %v Got: %v", user2, ret_user2)
	}

	err = dbP.Remove(user1, dbName)
	if err != nil {
		t.Errorf("Error deleting Record Record: %v Error: %s", user1, err.Error())
	}

	err = dbP.Remove(user2, dbName)
	if err != nil {
		t.Errorf("Error deleting Record Record: %v Error: %s", user2, err.Error())
	}

	_, err = dbP.Get(user1, dbName)
	if err != bolt.ErrKeyRequired {
		t.Errorf("Expected Error: %v Got: %v", bolt.ErrKeyRequired, err.Error())
	}

	_, err = dbP.Get(user1, "RANDOM_BUCKET")
	if err != bolt.ErrBucketNotFound {
		t.Errorf("Expected Error: %v Got: %v", bolt.ErrBucketNotFound, err.Error())
	}

	dbP.PrintStats()
	removeDbFile(dbP.DbFullName)
}
