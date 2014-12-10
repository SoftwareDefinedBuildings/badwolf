package main

import (
	"math/rand"
)

type StringGenerator struct {
	used     map[string]bool
	alphabet string
}

func NewStringGenerator(alphabet string) *StringGenerator {
	return &StringGenerator{alphabet: alphabet, used: map[string]bool{}}
}

func (sg *StringGenerator) RandomString(length int) string {
	if length < 1 {
		return ""
	}
tryagain:
	b := make([]byte, length)
	for i := 0; i < length; i++ {
		b[i] = byte(sg.alphabet[rand.Intn(len(sg.alphabet))])
	}
	if sg.used[string(b)] {
		goto tryagain
	}
	sg.used[string(b)] = true
	return string(b)
}

func (sg *StringGenerator) GenerateNRandomStrings(number, length int) []string {
	ret := make([]string, number)
	for i := 0; i < number; i++ {
		ret[i] = sg.RandomString(length)
	}
	return ret
}
