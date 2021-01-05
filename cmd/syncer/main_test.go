package main

import (
	"fmt"
	"testing"

	"golang.org/x/text/unicode/norm"
)

func TestGenerateWordsForIndexASCII(t *testing.T) {
	words := make(map[string]struct{})
	text := "abcdehdiaba"
	generateWordsForIndex(text, &words)
    patterns := [...]string {
		"abcd",
		"bcde",
		"cdeh",
		"dehd",
		"ehdi",
		"hdia",
		"diab",
		"iaba",
	}

	for _, s := range patterns {
		if _, ok := words[s]; !ok {
			fmt.Println(s)
			t.Fail()
		}
	}
	if _, ok := words["abc"]; ok {
		t.Fail()
	}
	if _, ok := words["abcde"]; ok {
		t.Fail()
	}
	if _, ok := words["aba"]; ok {
		t.Fail()
	}
	if _, ok := words["ba"]; ok {
		t.Fail()
	}
	if _, ok := words["a"]; ok {
		t.Fail()
	}
}

func TestGenerateWordsForIndexNonASCII(t *testing.T) {
	words := make(map[string]struct{})
	text := "日本語"
	generateWordsForIndex(text, &words)
	for i := 0; i <= 3; i++ {
		if _, ok := words[text[i:i+6]]; !ok {
			t.Fail()
		}
	}
}

func TestGenerateWordsForIndexWithNormalization(t *testing.T) {
	words := make(map[string]struct{})
	text := norm.NFC.String("ÁÅÇÈñöûÆĲŒß")
	patterns := [...]string {
		"aace",
		"acen",
		"ceno",
		"enou",
		"noua",
		"ouae",
		"uaei",
		"aeij",
		"eijo",
		"ijoe",
		"joes",
		"oess",
	}
	generateWordsForIndex(text, &words)

	for _, s := range patterns {
		if _, ok := words[s]; !ok {
			fmt.Println(s)
			t.Fail()
		}
	}
}

