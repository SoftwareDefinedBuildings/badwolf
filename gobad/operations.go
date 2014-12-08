package gobad

import (
	"gopkg.in/mgo.v2/bson"
)

type Operation interface {
	GetDocumentUnique(uuid string) bson.M
	GetDocumentSetWhere(where bson.M) []bson.M
}
