package tests

import (
	"os"
	"testing"
	dDB "dumbDB"
	"github.com/boltdb/bolt"
)

var removeDbFile = func(name string) {
	_ = os.Remove(name)
}

func TestMain(t *testing.M) {
	os.Setenv("test", "1")
	os.Exit(t.Run())
}

// 1. Create 2 test records and store
// 2. Do a GetAll and make sure we return 2 records.
// 3. Try to store key more than 1024 bytes => This should fail.
func TestDumbDB_Store(t *testing.T) {

	dbName := "TestDumbDB_Store"
	dbP := dDB.NewDumbDB(".", dbName, os.Stdout)

	if dbP == nil {
		t.Errorf("Error creating DB %s", dbName)
	}

	err := dbP.Store(User1.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User1, err.Error())
	}

	err = dbP.Store(User2.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User2, err.Error())
	}

	records, err := dbP.GetAll(dbName)
	if err != nil || len(records) != 2 {
		t.Errorf("Returned incorrect no of records Expected: %d Got: %d", 2, len(records))
	}

	dummy := LargeKeyRecord{
		Dummy: "This will simulate large key record.",
	}

	err = dbP.Store(dummy.GetRecord(), dbName)
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
	dbP := dDB.NewDumbDB(".", dbName, os.Stdout)

	if dbP == nil {
		t.Errorf("Error creating DB %s", dbName)
	}

	err := dbP.Store(User1.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User1, err.Error())
	}

	err = dbP.Store(User2.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User2, err.Error())
	}

	err = dbP.Remove(User1.GetKey(), dbName)
	if err != nil {
		t.Errorf("Error deleting Record Record: %v Error: %s", User1, err.Error())
	}

	err = dbP.Remove(User2.GetKey(), dbName)
	if err != nil {
		t.Errorf("Error deleting Record Record: %v Error: %s", User2, err.Error())
	}

	dummy := LargeKeyRecord{
		Dummy: "This will simulate large key record.",
	}

	err = dbP.Remove(dummy.GetKey(), dbName)
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
	dbP := dDB.NewDumbDB(".", dbName, os.Stdout)

	if dbP == nil {
		t.Errorf("Error creating DB %s", dbName)
	}

	err := dbP.Store(User1.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User1, err.Error())
	}

	err = dbP.Store(User2.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User2, err.Error())
	}

	rec1, err := dbP.Get(User1.GetKey(), dbName)
	if err != nil {
		t.Errorf("Error getting Record Record: %v Error: %s", User1, err.Error())
	}

	ret_user1 := UserRecord{}
	ret_user1 = ret_user1.PutVal(rec1)

	if ret_user1.Name != User1.Name || ret_user1.ID != User1.ID || ret_user1.Position != User1.Position {
		t.Errorf("Got incorrect value. Expected: %v Got: %v", User1, ret_user1)
	}

	rec2, err := dbP.Get(User2.GetKey(), dbName)
	if err != nil {
		t.Errorf("Error getting Record Record: %v Error: %s", User2, err.Error())
	}

	ret_user2 := UserRecord{}
	ret_user2 = ret_user2.PutVal(rec2)

	if ret_user2.Name != User2.Name || ret_user2.ID != User2.ID || ret_user2.Position != User2.Position {
		t.Errorf("Got incorrect value. Expected: %v Got: %v", User2, ret_user2)
	}

	err = dbP.Remove(User1.GetKey(), dbName)
	if err != nil {
		t.Errorf("Error deleting Record Record: %v Error: %s", User1, err.Error())
	}

	err = dbP.Remove(User2.GetKey(), dbName)
	if err != nil {
		t.Errorf("Error deleting Record Record: %v Error: %s", User2, err.Error())
	}

	_, err = dbP.Get(User1.GetKey(), dbName)
	if err != bolt.ErrKeyRequired {
		t.Errorf("Expected Error: %v Got: %v", bolt.ErrKeyRequired, err.Error())
	}

	_, err = dbP.Get(User2.GetKey(), "RANDOM_BUCKET")
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
	dbP := dDB.NewDumbDB(".", dbName, os.Stdout)

	if dbP == nil {
		t.Errorf("Error creating DB %s", dbName)
	}

	err := dbP.Store(User1.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User1, err.Error())
	}

	err = dbP.Store(User2.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User2, err.Error())
	}

	err = dbP.Store(User3.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User3, err.Error())
	}

	err = dbP.Store(User4.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User4, err.Error())
	}

	err = dbP.Store(User5.GetRecord(), dbName)
	if err != nil {
		t.Errorf("Error creating Record Record: %v Error: %s", User5, err.Error())
	}

	all_recs, err := dbP.GetAll(dbName)
	if err != nil {
		t.Errorf("Error getting all Records Error: %s", err.Error())
	}

	if len(all_recs) != 5 {
		t.Errorf("GetAll size incorrect Expected: %d Got: %d", 5, len(all_recs))
	}

	doAllRecordsCheck(t, all_recs)

	recs1, err := dbP.GetLimited(dbName, 2, nil)
	if err != nil {
		t.Errorf("Error getting Records Error: %s", err.Error())
	}

	if len(recs1) != 2 {
		t.Errorf("GetAll size incorrect Expected: %d Got: %d", 2, len(recs1))
	}

	cookie := recs1[len(recs1) - 1]

	cookie_user := UserRecord{}
	cookie_user = cookie_user.PutVal(cookie)

	recs2, err := dbP.GetLimited(dbName, 1, cookie_user.GetKey())
	if err != nil {
		t.Errorf("Error getting Records Error: %s", err.Error())
	}

	cookie = recs1[len(recs2) - 1]

	cookie_user2 := UserRecord{}
	cookie_user2 = cookie_user2.PutVal(cookie)

	if len(recs2) != 1 {
		t.Errorf("GetAll size incorrect Expected: %d Got: %d", 1, len(recs2))
	}

	recs3, err := dbP.GetLimited(dbName, 2, cookie_user2.GetKey())
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

	_, err = dbP.GetLimited("RANDOM_BUCKET", 5, nil)
	if err != bolt.ErrBucketNotFound {
		t.Errorf("Expected Error: %v Got: %v", bolt.ErrBucketNotFound, err.Error())
	}

	// Send cookie out of range
	id := 20
	cookie_user = UserRecord{
		ID: int(id),
	}

	_, err = dbP.GetLimited(dbName, 2, cookie_user.GetKey())
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
		user = user.PutVal(all_recs[x])
		switch {
		case x == 4:
			if user.Name != User1.Name || user.ID != User1.ID || user.Position != User1.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", User1, user)
			}
			break
		case x == 3:
			if user.Name != User2.Name || user.ID != User2.ID || user.Position != User2.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", User2, user)
			}
			break
		case x == 2:
			if user.Name != User3.Name || user.ID != User3.ID || user.Position != User3.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", User3, user)
			}
			break
		case x == 1:
			if user.Name != User4.Name || user.ID != User4.ID || user.Position != User4.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", User4, user)
			}
			break
		case x == 0:
			if user.Name != User5.Name || user.ID != User5.ID || user.Position != User5.Position {
				t.Errorf("Found order of results incorrect Expected: %v Found: %v", User5, user)
			}
			break
		}
	}
}
