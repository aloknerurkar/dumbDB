package main

import (
	dDB "dumbDB"
	"log"
	"os"
	"dumbDB/tests"
)

func main() {
	db := dDB.NewDumbDB(".", "db1", os.Stdout)

	e := db.Store(tests.User1.GetRecord(), "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Store(tests.User2.GetRecord(), "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Store(tests.User3.GetRecord(), "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Store(tests.User4.GetRecord(), "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}

	ret_vals, err := db.GetAll("Users")
	if err != nil {
		log.Fatal("Failed getting users")
	} else {
		for u := range ret_vals {
			user := tests.UserRecord{}
			user = user.PutVal(ret_vals[u])
			log.Printf("Found record %v", user)
		}
	}

	ret_vals_part1, err := db.GetLimited("Users", 1, nil)
	log.Println("Results query 1")
	for u := range ret_vals_part1 {
		user := tests.UserRecord{}
		user = user.PutVal(ret_vals_part1[u])
		log.Printf("Found record %v", user)
	}

	cookie := tests.UserRecord{}

	cookie = cookie.PutVal(ret_vals_part1[len(ret_vals_part1) - 1])

	ret_vals_part2, err := db.GetLimited("Users", 3, cookie.GetKey())
	log.Println("Results query 2")
	for u := range ret_vals_part2 {
		user := tests.UserRecord{}
		user = user.PutVal(ret_vals_part2[u])
		log.Printf("Found record %v", user)
	}

	e = db.Remove(tests.User1.GetKey(), "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Remove(tests.User2.GetKey(), "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Remove(tests.User3.GetKey(), "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
	e = db.Remove(tests.User4.GetKey(), "Users")
	if e != nil {
		log.Fatal("Failed storing record")
	}
}

