package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"regexp"
)

type ProviderMongo struct {
	ses   *mgo.Session
	db_bw *mgo.Database

	// NOTE: this particular Mongo provider implements a document
	// as naive {key: value} and only indexes on the unique identifier
	// "uuid". Another implementation would use {"key": realkey, "value": realvalue}
	db_mq *mgo.Database
}

//== SHARED
func (p *ProviderMongo) Initialize() {
	ses, err := mgo.Dial(os.Getenv("MONGODB_SERVER"))
	if err != nil {
		Report.Fatal("could not connect to mongo: %v", err)
	}
	p.ses = ses
	p.db_bw = ses.DB("bosswavequery")
	p.db_mq = ses.DB("metadataquery")
	p.db_bw.DropDatabase()
	p.db_mq.DropDatabase()

	//BosswaveQuery initialization
	p.db_bw.C("records").EnsureIndex(mgo.Index{Key: []string{"key"}, Unique: true})
	p.db_bw.C("records").EnsureIndex(mgo.Index{Key: []string{"allocset"}, Unique: false})

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
	rv := []string{}
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
	pipe := []bson.M{
		bson.M{"$match": bson.M{"allocset": AllocSet}},
		bson.M{"$group": bson.M{"_id": "", "sum": bson.M{"$sum": "$size"}}},
	}
	pr := p.db_bw.C("records").Pipe(pipe)
	val := struct{ Sum int64 }{}
	err := pr.One(&val)
	if err != nil {
		Report.Fatal("Could not sum size: %v", err)
	}
	return val.Sum
}

//Create an allocation set
func (p *ProviderMongo) CreateAllocSet(r AllocationSet) {
	if err := p.db_bw.C("allocset").Insert(r); err != nil {
		Report.Fatal("Could not insert allocation set: %v", err)
	}
}

//Get the allocation set ID
func (p *ProviderMongo) GetAllocSetID(vk VK) int64 {
	q := p.db_bw.C("allocset").Find(bson.M{"vk": bson.Binary{Kind: 0, Data: []byte(vk)}})
	rv := struct{ Id int64 }{}
	qerr := q.One(&rv)
	if qerr != nil {
		Report.Fatal("could not query allocset record: %v", qerr)
	}
	return rv.Id
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
func (p *ProviderMongo) GetDocumentUnique(uuid string) KVList {
	var res bson.M
	err := p.db_mq.C("records").Find(bson.M{"uuid": uuid}).One(&res)
	if err != nil {
		Report.Fatal("Error finding unique document: %v", err)
	}
	return Bson2KVList(res)
}

// get a set of documents using a where clause
func (p *ProviderMongo) GetDocumentSetWhere(where KVList) []KVList {
	q := p.db_mq.C("records").Find(KVList2Bson(where))
	it := q.Iter()
	ret := []KVList{}
	doc := bson.M{}
	for it.Next(&doc) {
		ret = append(ret, Bson2KVList(doc))
	}
	return ret
}

// get list of unique values for a given key
func (p *ProviderMongo) GetUniqueValues(key string) []interface{} {
	var res []interface{}
	err := p.db_mq.C("records").Find(bson.M{}).Distinct(key, &res)
	if err != nil {
		Report.Fatal("Error retreiving unique values: %v", err)
	}
	return res
}

// get a set of documents with a key/value matching a glob (anchored regex)
func (p *ProviderMongo) GetDocumentSetValueGlob(key, value_glob string) []KVList {
	q := p.db_mq.C("records").Find(bson.M{key: bson.M{"$regex": value_glob}})
	it := q.Iter()
	ret := []KVList{}
	doc := bson.M{}
	for it.Next(&doc) {
		ret = append(ret, Bson2KVList(doc))
	}
	return ret
}

// get a set of keys that match a glob
// MongoDB doesn't provide this functionality, so we actually fetch all keys
// for all documents and check them individually
func (p *ProviderMongo) GetKeyGlob(key_glob string) []string {
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
	return ret
}

// Set Operations

// insert list of documents
func (p *ProviderMongo) InsertDocument(docs []KVList) {
	bson_docs := []bson.M{}
	for _, doc := range docs {
		bson_docs = append(bson_docs, KVList2Bson(doc))
	}
	err := p.db_mq.C("records").Insert(bson_docs)
	if err != nil {
		Report.Fatal("Error inserting documents: %v")
	}
}

// set k/v pairs in unique document
func (p *ProviderMongo) SetKVDocumentUnique(kv KVList, uuid string) {
	err := p.db_mq.C("records").Update(bson.M{"uuid": uuid}, KVList2Bson(kv))
	if err != nil {
		Report.Fatal("Error setting k/v pairs: %v", err)
	}
}

// set k/v pairs in set of documents using where clause
func (p *ProviderMongo) SetKVDocumentWhere(kv, where KVList) {
	// discarding mgo.CollectionInfo
	_, err := p.db_mq.C("records").UpdateAll(KVList2Bson(where), KVList2Bson(kv))
	if err != nil {
		Report.Fatal("Error setting k/v pairs: %v", err)
	}
}

// set k/v pairs for set of documents with k/v matching glob
func (p *ProviderMongo) SetKVDocumentValueGlob(kv KVList, key, value_glob string) {
	// discarding mgo.CollectionInfo
	_, err := p.db_mq.C("records").UpdateAll(bson.M{key: bson.M{"$regex": value_glob}}, KVList2Bson(kv))
	if err != nil {
		Report.Fatal("Error setting k/v pairs: %v", err)
	}
}

// Delete Operations

// delete list of keys in unique document
func (p *ProviderMongo) DeleteKeyDocumentUnique(keys []string, uuid string) {
	removekeys := bson.M{}
	for _, key := range keys {
		removekeys[key] = ""
	}
	update := bson.M{"$unset": removekeys}
	err := p.db_mq.C("records").Update(bson.M{"uuid": uuid}, update)
	if err != nil {
		Report.Fatal("Error deleting key from document: %v", err)
	}
}

// delete list of keys in set of documents using where clause
func (p *ProviderMongo) DeleteKeyDocumentWhere(keys []string, where KVList) {
	removekeys := bson.M{}
	for _, key := range keys {
		removekeys[key] = ""
	}
	update := bson.M{"$unset": removekeys}
	_, err := p.db_mq.C("records").UpdateAll(KVList2Bson(where), update)
	if err != nil {
		Report.Fatal("Error deleting key from documents: %v", err)
	}
}

// delete keys that match glob in unique document
func (p *ProviderMongo) DeleteKeyGlobDocumentUnique(key_glob, uuid string) {
	var doc bson.M
	re := regexp.MustCompile(key_glob)
	removekeys := bson.M{}
	err := p.db_mq.C("records").Find(bson.M{"uuid": uuid}).One(&doc)
	if err != nil {
		Report.Fatal("Error finding doc with uuid %v", err)
	}
	for k, _ := range doc {
		if re.MatchString(k) {
			removekeys[k] = ""
		}
	}
	update := bson.M{"$unset": removekeys}
	err = p.db_mq.C("records").Update(bson.M{"uuid": uuid}, update)
	if err != nil {
		Report.Fatal("Error deleting keys from document: %v", err)
	}
}

// delete keys that match glob in set of documents using where clause
func (p *ProviderMongo) DeleteKeyGlobDocumentWhere(key_glob string, where KVList) {
	q := p.db_mq.C("records").Find(KVList2Bson(where))
	it := q.Iter()
	doc := bson.M{}
	for it.Next(&doc) {
		p.DeleteKeyGlobDocumentUnique(key_glob, doc["uuid"].(string))
	}
}
