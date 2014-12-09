package gobad

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type BPoint struct {
	Id        string  `json:"id"`
	Iteration int     `json:"iteration"`
	Value     float64 `json:"value"`
}
type Reporter struct {
	VAL_Ok       bool     `json:"ok"`
	VAL_FatalMsg string   `json:"fatalmsg"`
	VAL_Metrics  []BPoint `json:"metrics"`
}

type Measurement time.Time

var Report Reporter

func init() {
	Report = Reporter{}
	Report.VAL_Ok = true
	Report.VAL_Metrics = make([]BPoint, 1024)
}

func (r *Reporter) Fatal(format string, args ...interface{}) {
	log.Printf(format, args...)
	r.VAL_Ok = false
	r.VAL_FatalMsg = fmt.Sprintf(format, args...)
	r.WriteOut()
	os.Exit(1)
}

func (r *Reporter) Metric(id string, iteration int, value float64) {
	r.VAL_Metrics = append(r.VAL_Metrics, BPoint{Id: id, Iteration: iteration, Value: value})
}

func (r *Reporter) DeltaMetric(id string, iteration int, start time.Time) {
	r.Metric(id, iteration, r.FinishTimer(start))
}

func (r *Reporter) WriteOut() {
	f, err := os.Create("benchmarkresult.json")
	if err != nil {
		log.Panicf("Could not create result file")
	}
	enc := json.NewEncoder(f)
	enc.Encode(r)
	f.Close()
}

func (r *Reporter) StartTimer() time.Time {
	return time.Now()
}

func (r *Reporter) FinishTimer(t time.Time) float64 {
	return float64(time.Now().Sub(t)) / float64(time.Microsecond)
}
