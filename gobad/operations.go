package gobad

// currently using bson.M as a placeholder for a "universal" return type
import (
	"gopkg.in/mgo.v2/bson"
)

type Operation interface {
	// get a single document by using a unique identifier
	GetDocumentUnique(uuid string) bson.M

	// get a set of documents using a where clause
	GetDocumentSetWhere(where bson.M) []bson.M

	// get list of unique values for a given key
	GetUniqueValues(key string) []interface{}

	// get a set of documents with a key/value matching a glob (anchored regex)
	GetDocumentSetValueGlob(key, value_glob string) []bson.M

	// get a set of keys that match a glob
	GetKeyGlob(key_glob string) []string

	// insert list of documents
	InsertDocument(docs []bson.M) bool

	// set k/v pairs in unique document
	SetKVDocumentUnique(kv bson.M, uuid string) bool

	// set k/v pairs in set of documents using where clause
	SetKVDocumentWhere(kv, where bson.M) bool

	// delete list of keys in unique document
	DeleteKeyDocumentUnique(keys []string, uuid string) bool

	// delete list of keys in set of documents using where clause
	DeleteKeyDocumentWhere(keys []string, where bson.M) bool
}
