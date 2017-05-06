package main

import (
	"dumbDB"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
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


func main() {
	db := dumbDB.NewDumbDB(".", "db1", os.Stdout)

	record1 := UserRecord{
		ID: 1,
		Name: "Alan",
		Position:"Engineer",
	}

	record2 := UserRecord{
		ID: 2,
		Name: "April",
		Position:"Doctor",
	}

	record3 := UserRecord{
		ID: 3,
		Name: "Olof",
		Position:"Engineer",
	}

	record4 := UserRecord{
		ID: 4,
		Name: "Summer",
		Position:"HR",
	}

	e := db.Store(record1, "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Store(record2, "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Store(record3, "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Store(record4, "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}

	ret_vals, err := db.GetAll("Users")
	if err != nil {
		log.Fatal("Failed getting users")
	} else {
		for u := range ret_vals {
			user := UserRecord{}
			err = json.Unmarshal(ret_vals[u], &user)
			if err != nil {
				log.Fatal("Failed unmarshalling " + err.Error())
			}
			log.Printf("Found record %v", user)
		}
	}

	ret_vals_part1, key, err := db.GetLimited("Users", 1, nil)
	log.Println("Results query 1")
	for u := range ret_vals_part1 {
		user := UserRecord{}
		err = json.Unmarshal(ret_vals_part1[u], &user)
		if err != nil {
			log.Fatal("Failed unmarshalling " + err.Error())
		}
		log.Printf("Found record %v", user)
	}

	id := int64(binary.LittleEndian.Uint64(key))
	cookie := UserRecord{
		ID: int(id),
	}

	ret_vals_part2, _, err := db.GetLimited("Users", 3, cookie)
	log.Println("Results query 2")
	for u := range ret_vals_part2 {
		user := UserRecord{}
		err = json.Unmarshal(ret_vals_part2[u], &user)
		if err != nil {
			log.Fatal("Failed unmarshalling " + err.Error())
		}
		log.Printf("Found record %v", user)
	}

	e = db.Remove(record1, "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Remove(record2, "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Remove(record3, "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Remove(record4, "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
}

