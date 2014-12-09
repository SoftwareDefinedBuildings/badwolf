package main

import (
	"fmt"
	"math/rand"
	"time"
)

func benchmarks_entry() {
	sd := time.Now().Unix()
	rand.Seed(sd)

	//TODO some reflection BS here, instead of hard coding
	//foreach database type
	{
		provider := new (ProviderMongo)
		provider.Initialize()
		//Benchmarks
		BENCH_BWQ_A(provider, "mongo")
	}

	Report.WriteOut()


}

//If all benchmark constants are relative to this, accuracy can be
//scaled arbitrarily and reproducably by editing this constant
const FACTOR = 1024

func BWUtil_GenVk() []byte {
	rv := make([]byte,32)
	for i:=0; i<32; i++ {
		rv[i] = byte(rand.Int())
	}
	return rv
}
//BosswaveQuery
func BENCH_BWQ_A(p BosswaveQuery, pfx string) {


	for run := 0; run < FACTOR/10 ; run ++ {
		recs := make([]BosswaveRecord, FACTOR)
		for i := 0; i < FACTOR; i++ {
			recs[i] = BosswaveRecord{
					Key:fmt.Sprintf("/foo/bar/%d/%d/%d/%d",run,i%100, i%10,i),
					Allocset: int64(i%100),
					Owner: rand.Int63(),
					Value: []byte{},
					}
		}

		st := Report.StartTimer()
		for _, d := range recs {
			p.InsertRecord(d)
		}
		Report.DeltaMetric(pfx+".A",run, st)
	}
}
