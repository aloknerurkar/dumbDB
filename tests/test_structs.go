package tests

import (
	"encoding/json"
	"log"
	"encoding/binary"
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

func (ur UserRecord) GetRecord() [][]byte {
	b := make([][]byte, 2)
	b[0] = ur.GetKey()
	b[1] = ur.GetVal()
	return b
}

func (ur UserRecord) PutVal(val []byte) UserRecord {
	err := json.Unmarshal(val, &ur)
	if err != nil {
		log.Fatal("Failed unmarshalling")
	}
	return ur
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

func (lr LargeKeyRecord) GetRecord() [][]byte {
	b := make([][]byte, 2)
	b[0] = lr.GetKey()
	b[1] = lr.GetVal()
	return b
}

func (lr LargeKeyRecord) PutVal(val []byte) {
	lr.Dummy = "Dummy Val"
}

var User1 = UserRecord {
	ID: 1,
	Name: "Alan",
	Position: "Engineer",
}

var User2 = UserRecord {
	ID: 2,
	Name: "Olof",
	Position: "Doctor",
}

var User3 = UserRecord {
	ID: 3,
	Name: "May",
	Position: "Architect",
}

var User4 = UserRecord {
	ID: 4,
	Name: "Travis",
	Position: "Chef",
}

var User5 = UserRecord {
	ID: 5,
	Name: "April",
	Position: "Engineer",
}
