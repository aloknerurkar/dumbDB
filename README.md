# dumbDB

This library serves as a simple cross-platform key-value store. It is built using boltDB underneath.

It provides a simple Get/Store/Remove type of interface over boltDB.

## Use cases

I have recently tried to find a way to reduce common code between the 
Android and iOS apps. gomobile is one such tool that I have found very useful.

For more information about gomobile:
https://github.com/golang/go/wiki/Mobile

The dumbDB can be compiled directly into a library using gomobile bind
or we can build an interface on top of it for dealing with specific structs.

`
db := dumbDB.NewDumbDB(".", "db1", os.Stdout)
`

This will create a new DumbDB pointer. It will use the file 'db1.dumbDB' in the
current directory. The os.Stdout directs the logs to Stdout.

### Using an interface
 
 For example look at the tests/test_structs.go file.
 
 The following interface is defined.
 
 `type UserRecord struct {
 	ID int
 	Name string
 	Position string
 }`
 
 `func (ur UserRecord) GetKey() []byte {
 	b := make([]byte, 8)
 	binary.LittleEndian.PutUint64(b, uint64(ur.ID))
 	return b
 }`
 
 `func (ur UserRecord) GetVal() []byte {
 	b, err := json.Marshal(ur)
 	if err != nil {
 		log.Fatal("Failed marshalling")
 	}
 	return b
 }`
 
 `func (ur UserRecord) GetRecord() [][]byte {
 	b := make([][]byte, 2)
 	b[0] = ur.GetKey()
 	b[1] = ur.GetVal()
 	return b
 }`
 
 `func (ur UserRecord) PutVal(val []byte) UserRecord {
 	err := json.Unmarshal(val, &ur)
 	if err != nil {
 		log.Fatal("Failed unmarshalling")
 	}
 	return ur
 }`
 
 Now you can write wrappers, which call the DumbDB APIs underneath.
 
 For example:
 
 ` GetUser() UserRecord { 
    b,e := Get(USER_BUCKET) 
    user := UserRecord{} 
    user = user.PutVal(b) 
    return user
 }`
 
 