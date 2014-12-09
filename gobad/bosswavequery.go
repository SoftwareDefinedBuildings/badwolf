package main

type BosswaveRecord struct {
	Key      string
	Allocset int64
	Owner    int64
	Size     int64
	Value    []byte
}

type VK []byte //32

type AllocationSet struct {
	Owner VK
	Id    int64
}

type BosswaveQuery interface {

	//Do any initial config
	Initialize()

	//Get a specific value
	GetRecord(key string) BosswaveRecord

	//Insert a record
	InsertRecord(r BosswaveRecord)

	//Get a list of keys up to a slash
	//so GetKeysUpToSlash(/foo/bar/) would return /foo/bar/baz
	//but not /foo/bar/baz/box
	GetKeysUpToSlash(keyprefix string) []string

	//Get sum(size) for all records with the given allocation set
	SumSize(AllocSet int64) int64

	//Create an allocation set
	CreateAllocSet(r AllocationSet)

	//Get the allocation set ID
	GetAllocSetID(vk VK) int64
}
