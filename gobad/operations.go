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
}
