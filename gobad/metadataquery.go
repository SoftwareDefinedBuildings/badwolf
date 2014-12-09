package main

// a document is a list of key/value pairs
type KVList [][2]string

type MetadataQuery interface {

	//Do any initial config
	Initialize()

	// Get Operations

	// get a single document by using a unique identifier
	GetDocumentUnique(uuid string) KVList

	// get a set of documents using a where clause
	GetDocumentSetWhere(where KVList) []KVList

	// get list of unique values for a given key
	GetUniqueValues(key string) []interface{}

	// get a set of documents with a key/value matching a glob (anchored regex)
	GetDocumentSetValueGlob(key, value_glob string) []KVList

	// get a set of keys that match a glob
	GetKeyGlob(key_glob string) []string

	// Set Operations

	// insert list of documents
	InsertDocument(docs []KVList)

	// set k/v pairs in unique document
	SetKVDocumentUnique(kv KVList, uuid string)

	// set k/v pairs in set of documents using where clause
	SetKVDocumentWhere(kv, where KVList)

	// set k/v pairs for set of documents with k/v matching glob
	SetKVDocumentValueGlob(kv KVList, key, value_glob string)

	// Delete Operations

	// delete list of keys in unique document
	DeleteKeyDocumentUnique(keys []string, uuid string)

	// delete list of keys in set of documents using where clause
	DeleteKeyDocumentWhere(keys []string, where KVList)

	// delete keys that match glob in unique document
	DeleteKeyGlobDocumentUnique(key_glob, uuid string)

	// delete keys that match glob in set of documents using where clause
	DeleteKeyGlobDocumentWhere(key_glob string, where KVList)
}
