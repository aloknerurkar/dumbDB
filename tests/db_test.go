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
	Name: "Alan",
	Position: "Engineer",
}

var user2 = UserRecord {
	ID: 2,
	Name: "Olof",
	Position: "Doctor",
}

var user3 = UserRecord {
	ID: 3,
	Name: "May",
	Position: "Architect",
}

var user4 = UserRecord {
	ID: 4,
	Name: "Travis",
	Position: "Chef",
}

var user5 = UserRecord {
	ID: 5,
	Name: "April",
	Position: "Engineer",
}

// 1. Create 2 test records and store
// 2. Do a GetAll and make sure we return 2 records.
// 3. Try to store key more than 1024 bytes => This should fail.
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

// 1. Create 2 test records and store
// 2. Remove the 2 records stored. Should not return error.
// 3. Try to remove by sending huge key. => This should fail
func TestDumbDB_GetAllRange(t *testing.T) {

	dbName := "TestDumbDB_GetAll"
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

	err = dbP.Store(user3, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user2, err.Error())
	}

	err = dbP.Store(user4, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user2, err.Error())
	}

	err = dbP.Store(user5, dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", user2, err.Error())
	}

	all_recs, err := dbP.GetAll(dbName)
	if err != nil {
		t.Errorf("Error getting all Records Error: %s", err.Error())
	}

	if len(all_recs) != 5 {
		t.Errorf("GetAll size incorrect Expected: %d Got: %d", 5, len(all_recs))
	}

	doAllRecordsCheck(t, all_recs)

	recs1, cookie, err := dbP.GetLimited(dbName, 2, nil)
	if err != nil {
		t.Errorf("Error getting Records Error: %s", err.Error())
	}

	if len(recs1) != 2 {
		t.Errorf("GetAll size incorrect Expected: %d Got: %d", 2, len(recs1))
	}

	id := int64(binary.LittleEndian.Uint64(cookie))
	cookie_user := UserRecord{
		ID: int(id),
	}

	recs2, cookie, err := dbP.GetLimited(dbName, 1, cookie_user)
	if err != nil {
		t.Errorf("Error getting Records Error: %s", err.Error())
	}

	id = int64(binary.LittleEndian.Uint64(cookie))
	cookie_user2 := UserRecord{
		ID: int(id),
	}

	if len(recs2) != 1 {
		t.Errorf("GetAll size incorrect Expected: %d Got: %d", 1, len(recs2))
	}

	recs3, _, err := dbP.GetLimited(dbName, 2, cookie_user2)
	if err != nil {
		t.Errorf("Error getting Records Error: %s", err.Error())
	}

	if len(recs3) != 2 {
		t.Errorf("GetAll size incorrect Expected: %d Got: %d", 2, len(recs3))
	}

	all_recs2 := make([][]byte, 0)
	appendRecords(all_recs2, recs1)
	appendRecords(all_recs2, recs2)
	appendRecords(all_recs2, recs3)

	doAllRecordsCheck(t, all_recs2)

	_, err = dbP.GetAll("RANDOM_BUCKET")
	if err != bolt.ErrBucketNotFound {
		t.Errorf("Expected Error: %v Got: %v", bolt.ErrBucketNotFound, err.Error())
	}

	_, _, err = dbP.GetLimited("RANDOM_BUCKET", 5, nil)
	if err != bolt.ErrBucketNotFound {
		t.Errorf("Expected Error: %v Got: %v", bolt.ErrBucketNotFound, err.Error())
	}

	// Send cookie out of range
	id = 20
	cookie_user = UserRecord{
		ID: int(id),
	}

	_, _, err = dbP.GetLimited(dbName, 2, cookie_user)
	if err != bolt.ErrKeyRequired {
		t.Error("Expected error while getting out of range cookie. Instead got success.")
	}

	dbP.PrintStats()
	removeDbFile(dbP.DbFullName)
}

func appendRecords(dst, src [][]byte) [][]byte {
	for x := range src  {
		dst = append(dst, src[x])
	}
	return dst
}

func doAllRecordsCheck(t *testing.T, all_recs [][] byte) {
	for x := range all_recs {
		user := UserRecord{}
		err := json.Unmarshal(all_recs[x], &user)
		if err != nil {
			t.Errorf("Error unmarshalling User Record Error: %s", err.Error())
		}
		switch {
		case x == 4:
			if user.Name != user1.Name || user.ID != user1.ID || user.Position != user1.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", user1, user)
			}
			break
		case x == 3:
			if user.Name != user2.Name || user.ID != user2.ID || user.Position != user2.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", user2, user)
			}
			break
		case x == 2:
			if user.Name != user3.Name || user.ID != user3.ID || user.Position != user3.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", user3, user)
			}
			break
		case x == 1:
			if user.Name != user4.Name || user.ID != user4.ID || user.Position != user4.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", user4, user)
			}
			break
		case x == 0:
			if user.Name != user5.Name || user.ID != user5.ID || user.Position != user5.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", user5, user)
			}
			break
		}
	}
}
