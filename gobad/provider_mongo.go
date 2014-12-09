package gobad

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"regexp"
)

type ProviderMongo struct {
	ses   *mgo.Session
	db_bw *mgo.Database
	db_mq *mgo.Database
}

//== SHARED
func (p *ProviderMongo) Initialize(params map[string]interface{}) {
	ses, err := mgo.Dial(params["mongodb"].(string))
	if err != nil {
		Report.Fatal("could not connect to mongo: %v", err)
	}
	p.ses = ses
	p.db_bw = ses.DB("bosswavequery")
	p.db_mq = ses.DB("metadataquery")

	//BosswaveQuery initialization
	p.db_bw.C("records").EnsureIndex(mgo.Index{Key: []string{"key"}, Unique: true})
	p.db_bw.C("records").EnsureIndex(mgo.Index{Key: []string{"allocset"}, Unique: true})

	//MetadataQuery initialization
	p.db_mq.C("records").EnsureIndex(mgo.Index{Key: []string{"uuid"}, Unique: true})
}

//== BosswaveQuery

//Get a specific value
func (p *ProviderMongo) GetRecord(key string) BosswaveRecord {
	q := p.db_bw.C("records").Find(bson.M{"key": key})
	rv := BosswaveRecord{}
	qerr := q.One(&rv)
	if qerr != nil {
		Report.Fatal("could not query bosswave record: %v", qerr)
	}
	return rv
}

//Insert a record
func (p *ProviderMongo) InsertRecord(r BosswaveRecord) {
	err := p.db_bw.C("records").Insert(r)
	if err != nil {
		Report.Fatal("could not insert bosswave record: %v", err)
	}
}

//Get a list of keys up to a slash
//so GetKeysUpToSlash(/foo/bar/) would return /foo/bar/baz
//but not /foo/bar/baz/box
func (p *ProviderMongo) GetKeysUpToSlash(keyprefix string) []string {

	regex := "^" + regexp.QuoteMeta(keyprefix) + "[^/]*"
	rv := make([]string, 1024)
	q := p.db_bw.C("records").Find(bson.M{"key": bson.M{"$regex": bson.RegEx{Pattern: regex}}})
	it := q.Iter()
	val := struct{ Key string }{}
	for it.Next(&val) {
		rv = append(rv, val.Key)
	}
	return rv
}

//Get sum(size) for all records with the given allocation set
func (p *ProviderMongo) SumSize(AllocSet int64) int64 {
	return 0
}

//Create an allocation set
func (p *ProviderMongo) CreateAllocSet(r AllocationSet) {

}

//Get the allocation set ID
func (p *ProviderMongo) GetAllocSetID(vk VK) int64 {
	return 0
}

//== MetadataQuery
// Get Operations

// converts k/v pairs in bson.M to a list of key/value pairs
func Bson2KVList(doc bson.M) KVList {
	ret := KVList{}
	for k, v := range doc {
		ret = append(ret, [2]string{k, v.(string)})
	}
	return ret
}

// converts list of key/value pairs into a bson.M document
func KVList2Bson(list KVList) bson.M {
	ret := bson.M{}
	for _, kv := range list {
		ret[kv[0]] = kv[1]
	}
	return ret
}

// get a single document by using a unique identifier
func (p *ProviderMongo) GetDocumentUnique(uuid string) (KVList, error) {
	var res bson.M
	err := p.db_mq.C("records").FindId(uuid).One(&res)
	return Bson2KVList(res), err
}

// get a set of documents using a where clause
func (p *ProviderMongo) GetDocumentSetWhere(where KVList) ([]KVList, error) {
	q := p.db_mq.C("records").Find(KVList2Bson(where))
	it := q.Iter()
	ret := []KVList{}
	doc := bson.M{}
	for it.Next(&doc) {
		ret = append(ret, Bson2KVList(doc))
	}
	return ret, nil
}

// get list of unique values for a given key
func (p *ProviderMongo) GetUniqueValues(key string) ([]interface{}, error) {
	var res []interface{}
	err := p.db_mq.C("records").Find(bson.M{}).Distinct(key, &res)
	return res, err
}

// get a set of documents with a key/value matching a glob (anchored regex)
func (p *ProviderMongo) GetDocumentSetValueGlob(key, value_glob string) ([]KVList, error) {
	q := p.db_mq.C("records").Find(bson.M{key: bson.M{"$regex": value_glob}})
	it := q.Iter()
	ret := []KVList{}
	doc := bson.M{}
	for it.Next(&doc) {
		ret = append(ret, Bson2KVList(doc))
	}
	return ret, nil
}

// get a set of keys that match a glob
// MongoDB doesn't provide this functionality, so we actually fetch all keys
// for all documents and check them individually
func (p *ProviderMongo) GetKeyGlob(key_glob string) ([]string, error) {
	re := regexp.MustCompile(key_glob)
	q := p.db_mq.C("records").Find(bson.M{})
	it := q.Iter()
	ret := []string{}
	doc := bson.M{}
	for it.Next(&doc) {
		for key, _ := range doc {
			if re.MatchString(key) {
				ret = append(ret, key)
			}
		}
	}
	return ret, nil
}

// Set Operations

// insert list of documents
func (p *ProviderMongo) InsertDocument(docs []KVList) error {
	return nil
}

// set k/v pairs in unique document
func (p *ProviderMongo) SetKVDocumentUnique(kv KVList, uuid string) error {
	return nil
}

// set k/v pairs in set of documents using where clause
func (p *ProviderMongo) SetKVDocumentWhere(kv, where KVList) error {
	return nil
}

// set k/v pairs for set of documents with k/v matching glob
func (p *ProviderMongo) SetKVDocumentValueGlob(kv KVList, key, value_glob string) error {
	return nil
}

// Delete Operations

// delete list of keys in unique document
func (p *ProviderMongo) DeleteKeyDocumentUnique(keys []string, uuid string) error {
	return nil
}

// delete list of keys in set of documents using where clause
func (p *ProviderMongo) DeleteKeyDocumentWhere(keys []string, where KVList) error {
	return nil
}

// delete keys that match glob in unique document
func (p *ProviderMongo) DeleteKeyGlobDocumentUnique(key_glob, uuid string) error {
	return nil
}

// delete keys that match glob in set of documents using where clause
func (p *ProviderMongo) DeleteKeyGlobDocumentWhere(key_glob string, where KVList) error {
	return nil
}
