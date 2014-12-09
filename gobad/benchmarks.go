package main

import (
	"code.google.com/p/go-uuid/uuid"
	"fmt"
	"math/rand"
	"time"
)

//If all benchmark constants are relative to this, accuracy can be
//scaled arbitrarily and reproducably by editing this constant
const FACTOR = 1024

func benchmarks_entry() {
	sd := time.Now().Unix()
	rand.Seed(sd)

	//TODO some reflection BS here, instead of hard coding
	//foreach database type
	for run := 0; run < FACTOR/10; run++ {
		provider := new(ProviderMongo)
		provider.Initialize()
		//Benchmarks
		BENCH_BWQ_A(provider, "mongo", run)
		BENCH_MetadataQuery(provider, "mongo", run)
	}

	Report.WriteOut()

}

func BWUtil_GenVk() []byte {
	rv := make([]byte, 32)
	for i := 0; i < 32; i++ {
		rv[i] = byte(rand.Int())
	}
	return rv
}

//BosswaveQuery
func BENCH_BWQ_A(p BosswaveQuery, pfx string, run int) {

	recs := make([]BosswaveRecord, FACTOR)
	for i := 0; i < FACTOR; i++ {
		recs[i] = BosswaveRecord{
			Key:      fmt.Sprintf("/foo/bar/%d/%d/%d/%d", run, i%100, i%10, i),
			Allocset: int64(i % 100),
			Owner:    rand.Int63(),
			Value:    []byte{},
		}
	}

	st := Report.StartTimer()
	for _, d := range recs {
		p.InsertRecord(d)
	}
	Report.DeltaMetric(pfx+".A", run, st)
}

func BENCH_MetadataQuery(mq MetadataQuery, prefix string, run int) {
	fmt.Printf("Doing MetadataQuery Run %d\n", run)
	// generate documents
	sg := NewStringGenerator("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
	toplevelkeys := sg.GenerateNRandomStrings(10, 10) // 10 random strings with length 10
	toplevelvalues := sg.GenerateNRandomStrings(10, 10)
	// InsertDocument
	recs := make([]KVList, FACTOR)
	for i := 0; i < FACTOR; i++ {
		record := [][2]string{[2]string{"uuid", uuid.New()}}
		for _, tlk := range toplevelkeys {
			record = append(record, [2]string{tlk, toplevelvalues[rand.Intn(10)]})
		}
		recs[i] = record
	}

	st := Report.StartTimer()
	for _, rec := range recs {
		mq.InsertDocument([]KVList{rec})
	}
	Report.DeltaMetric(prefix+".InsertDocument", run, st)

	// GetDocumentUnique
	st = Report.StartTimer()
	for _, rec := range recs {
		mq.GetDocumentUnique(rec[0][1]) // fetch uuid
	}
	Report.DeltaMetric(prefix+".GetDocumentUnique", run, st)

	// GetDocumentSetWhere -- 1 doc
	st = Report.StartTimer()
	for _, rec := range recs {
		mq.GetDocumentSetWhere(rec) // fetch 1 doc
	}
	Report.DeltaMetric(prefix+".GetDocumentSetWhere1Doc", run, st)

	// GetDocumentSetWhere -- many doc
	st = Report.StartTimer()
	for _, tlv := range toplevelvalues {
		mq.GetDocumentSetWhere(KVList{[2]string{toplevelkeys[rand.Intn(10)], tlv}}) // fetch 1 doc
	}
	Report.DeltaMetric(prefix+".GetDocumentSetWhereManyDoc", run, st)

}
