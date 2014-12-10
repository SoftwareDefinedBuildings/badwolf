package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
)

// The "Exploded" Mongo structures each document as having a linking docid field,
// and all the key/value pairs are stored as separate documents of the form
// {"key": key, "value": value, "docid": docid}.
// This allows us to index on keys as well as values
type ProviderMongoExploded struct {
	ses *mgo.Session

	// We only re-implement db_mq here because we are comparing
	// a different structure of database
	db_mq *mgo.Database
}

func (p *ProviderMongoExploded) Initialize() {
	ses, err := mgo.Dial(os.Getenv("MONGODB_SERVER"))
	if err != nil {
		Report.Fatal("could not connect to mongo: %v", err)
	}
	p.ses = ses
	p.db_mq = ses.DB("metadataquery")
	p.db_mq.DropDatabase()

	//MetadataQuery initialization
	p.db_mq.C("records").EnsureIndex(mgo.Index{Key: []string{"key"}, Unique: false})
	p.db_mq.C("records").EnsureIndex(mgo.Index{Key: []string{"docid"}, Unique: false})
}

//== MetadataQuery

// Get Operations

// Converts documents of {"key": key, "value": value} to
func ExplodedBson2KVList(docs []bson.M) KVList {
	ret := KVList{}
	for _, doc := range docs {
		ret = append(ret, [2]string{doc["key"].(string), doc["value"].(string)})
	}
	return ret
}

// Converts a KVList into a list of {"key": key, "value": value} documents
// but without a document ID (use this for where clauses)
func KVList2ExplodedBsonMany(list KVList) []bson.M {
	ret := []bson.M{}
	for _, kv := range list {
		ret = append(ret, bson.M{"key": kv[0], "value": kv[1]})
	}
	return ret
}

// Converts a KVList into a list of {"key": key, "value": value, "docid": docid} documents
// with an included docid (use for compiling into a single document)
func KVList2ExplodedBsonOne(list KVList, docid string) []bson.M {
	ret := []bson.M{}
	for _, kv := range list {
		ret = append(ret, bson.M{"key": kv[0], "value": kv[1], "docid": docid})
	}
	return ret
}

// get a single document by using a unique identifier
// get the document that has the given uuid, then extract all documents that
// share the resulting docid
func (p *ProviderMongoExploded) GetDocumentUnique(uuid string) KVList {
	var first bson.M
	var res []bson.M
	var err error
	err = p.db_mq.C("records").Find(bson.M{"key": "uuid", "value": uuid}).One(&first)
	if err != nil {
		Report.Fatal("Error fetching record uuid: %v", err)
	}

	err = p.db_mq.C("records").Find(bson.M{"docid": first["docid"].(string)}).All(&res)
	if err != nil {
		Report.Fatal("Error fetching all docs with same docid: %v", err)
	}
	return ExplodedBson2KVList(res)
}

// get a set of documents using a where clause
func (p *ProviderMongoExploded) GetDocumentSetWhere(where KVList) []KVList {
	pipe := []bson.M{
		bson.M{"$match": KVList2ExplodedBsonMany(where)},
		bson.M{"$group": bson.M{"_id": "$docid", "kvpairs": bson.M{"$push": "$$ROOT"}}},
	}
	q := p.db_mq.C("records").Pipe(pipe)
	it := q.Iter()
	ret := []KVList{}
	doc := bson.M{}
	for it.Next(&doc) {
		ret = append(ret, ExplodedBson2KVList(doc["metadata"].([]bson.M)))
	}
	return ret
}

// get list of unique values for a given key
// Find all documents with a "key" of [key], and then find distinct "value"
func (p *ProviderMongoExploded) GetUniqueValues(key string) []interface{} {
	var res []interface{}
	err := p.db_mq.C("records").Find(bson.M{"key": key}).Distinct("value", &res)
	if err != nil {
		Report.Fatal("Error fetching unique values for doc: %v", err)
	}
	return res
}

// get a set of documents with a key/value matching a glob (anchored regex)
func (p *ProviderMongoExploded) GetDocumentSetValueGlob(key, value_glob string) []KVList {
	pipe := []bson.M{
		bson.M{"$match": bson.M{"key": key, "value": bson.M{"$regex": value_glob}}},
		bson.M{"$group": bson.M{"_id": "$docid", "kvpairs": bson.M{"$push": "$$ROOT"}}},
	}
	q := p.db_mq.C("records").Pipe(pipe)
	it := q.Iter()
	ret := []KVList{}
	doc := bson.M{}
	for it.Next(&doc) {
		ret = append(ret, ExplodedBson2KVList(doc["metadata"].([]bson.M)))
	}
	return ret
}

// get a set of keys that match a glob
func (p *ProviderMongoExploded) GetKeyGlob(key_glob string) []string {
	var res []string
	q := p.db_mq.C("records").Find(bson.M{"key": bson.M{"$regex": key_glob}}).Select(bson.M{"key": 1})
	err := q.All(&res)
	if err != nil {
		Report.Fatal("Error retreiving keys that match blob: %v", err)
	}
	return res
}

// Set Operations

// insert list of documents
func (p *ProviderMongoExploded) InsertDocument(docs []KVList) {
	for i, doc := range docs {
		for _, rec := range KVList2ExplodedBsonOne(doc, string(i)) {
			err := p.db_mq.C("records").Insert(rec)
			if err != nil {
				Report.Fatal("Error inserting documents: %v", err)
			}
		}
	}
}

// set k/v pairs in unique document
func (p *ProviderMongoExploded) SetKVDocumentUnique(kv KVList, uuid string) {
	var first bson.M
	var err error
	err = p.db_mq.C("records").Find(bson.M{"key": "uuid", "value": uuid}).One(&first)
	if err != nil {
		Report.Fatal("Error fetching doc with uuid: %v", err)
	}

	bson_docs := []bson.M{}
	for _, kv := range kv {
		bson_docs = append(bson_docs, bson.M{"key": kv[0], "value": kv[1], "docid": first["docid"].(string)})
	}
	err = p.db_mq.C("records").Insert(bson_docs)
	if err != nil {
		Report.Fatal("Error inserting documents: %v", err)
	}
}

// set k/v pairs in set of documents using where clause
func (p *ProviderMongoExploded) SetKVDocumentWhere(kv, where KVList) {
	var docids []string
	err := p.db_mq.C("records").Find(KVList2ExplodedBsonMany(where)).Distinct("docid", &docids)
	if err != nil {
		Report.Fatal("Error selecting documents: %v", err)
	}

	bson_docs := []bson.M{}
	for _, kv := range kv {
		for _, docid := range docids {
			bson_docs = append(bson_docs, bson.M{"key": kv[0], "value": kv[1], "docid": docid})
		}
	}
	err = p.db_mq.C("records").Insert(bson_docs)
	if err != nil {
		Report.Fatal("Error inserting documents: %v", err)
	}
}

// set k/v pairs for set of documents with k/v matching glob
func (p *ProviderMongoExploded) SetKVDocumentValueGlob(kv KVList, key, value_glob string) {
	var docids []string
	err := p.db_mq.C("records").Find(bson.M{"key": key, "value": bson.M{"$regex": value_glob}}).Distinct("docid", &docids)
	if err != nil {
		Report.Fatal("Error selecting documents: %v", err)
	}

	bson_docs := []bson.M{}
	for _, kv := range kv {
		for _, docid := range docids {
			bson_docs = append(bson_docs, bson.M{"key": kv[0], "value": kv[1], "docid": docid})
		}
	}
	err = p.db_mq.C("records").Insert(bson_docs)
	if err != nil {
		Report.Fatal("Error inserting documents: %v", err)
	}
}

// Delete Operations

// delete list of keys in unique document
func (p *ProviderMongoExploded) DeleteKeyDocumentUnique(keys []string, uuid string) {
	var first bson.M
	err := p.db_mq.C("records").Find(bson.M{"key": "uuid", "value": uuid}).One(&first)
	if err != nil {
		Report.Fatal("Error finding document with uuid: %v", err)
	}

	for _, key := range keys {
		_, err = p.db_mq.C("records").RemoveAll(bson.M{"docid": first["docid"].(string), "key": key})
		if err != nil {
			Report.Fatal("Error deleting key from document: %v", err)
		}
	}
}

// delete list of keys in set of documents using where clause
func (p *ProviderMongoExploded) DeleteKeyDocumentWhere(keys []string, where KVList) {
	var docids []string
	err := p.db_mq.C("records").Find(KVList2ExplodedBsonMany(where)).Distinct("docid", &docids)
	if err != nil {
		Report.Fatal("Error selecting documents: %v", err)
	}
	for _, key := range keys {
		for _, docid := range docids {
			_, err := p.db_mq.C("records").RemoveAll(bson.M{"key": key, "docid": docid})
			if err != nil {
				Report.Fatal("Error deleting key from document: %v", err)
			}
		}
	}
}

// delete keys that match glob in unique document
func (p *ProviderMongoExploded) DeleteKeyGlobDocumentUnique(key_glob, uuid string) {
	var first bson.M
	err := p.db_mq.C("records").Find(bson.M{"key": "uuid", "value": uuid}).One(&first)
	if err != nil {
		Report.Fatal("Error finding document with uuid: %v", err)
	}
	_, err = p.db_mq.C("records").RemoveAll(bson.M{"docid": first["docid"].(string), "key": bson.M{"$regex": key_glob}})
	if err != nil {
		Report.Fatal("Error deleting key from document: %v", err)
	}
}

// delete keys that match glob in set of documents using where clause
func (p *ProviderMongoExploded) DeleteKeyGlobDocumentWhere(key_glob string, where KVList) {
	var docids []string
	err := p.db_mq.C("records").Find(KVList2ExplodedBsonMany(where)).Distinct("docid", &docids)
	if err != nil {
		Report.Fatal("Error selecting documents: %v", err)
	}
	for _, docid := range docids {
		_, err := p.db_mq.C("records").RemoveAll(bson.M{"key": bson.M{"$regex": key_glob}, "docid": docid})
		if err != nil {
			Report.Fatal("Error deleting key from document: %v", err)
		}
	}
}
