package gobad

// currently using bson.M as a placeholder for a "universal" return type
import (
	"gopkg.in/mgo.v2/bson"
)

type Operation interface {
	// Get Operations

	// get a single document by using a unique identifier
	GetDocumentUnique(uuid string) (bson.M, error)

	// get a set of documents using a where clause
	GetDocumentSetWhere(where bson.M) ([]bson.M, error)

	// get list of unique values for a given key
	GetUniqueValues(key string) ([]interface{}, error)

	// get a set of documents with a key/value matching a glob (anchored regex)
	GetDocumentSetValueGlob(key, value_glob string) ([]bson.M, error)

	// get a set of keys that match a glob
	GetKeyGlob(key_glob string) ([]string, error)

	// Set Operations

	// insert list of documents
	InsertDocument(docs []bson.M) error

	// set k/v pairs in unique document
	SetKVDocumentUnique(kv bson.M, uuid string) error

	// set k/v pairs in set of documents using where clause
	SetKVDocumentWhere(kv, where bson.M) error

	// set k/v pairs for set of documents with k/v matching glob
	SetKVDocumentValueGlob(kv bson.M, key, value_glob string) error

	// Delete Operations

	// delete list of keys in unique document
	DeleteKeyDocumentUnique(keys []string, uuid string) error

	// delete list of keys in set of documents using where clause
	DeleteKeyDocumentWhere(keys []string, where bson.M) error

	// delete keys that match glob in unique document
	DeleteKeyGlobDocumentUnique(key_glob, uuid string) error

	// delete keys that match glob in set of documents using where clause
	DeleteKeyGlobDocumentWhere(key_glob string, where bson.M) error
}
