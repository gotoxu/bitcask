package main

import (
	"log"

	"github.com/prologic/bitcask"
)

func main() {
	db, err := bitcask.Open("test.db")
	if err != nil {
		log.Fatal(err)
	}
	l := db.Hash([]byte("foo"))
	err = l.Set([]byte("1"), []byte("one"))
	if err != nil {
		log.Fatal(err)
	}
	err = l.Set([]byte("2"), []byte("two"))
	if err != nil {
		log.Fatal(err)
	}
	err = l.Set([]byte("3"), []byte("three"))
	if err != nil {
		log.Fatal(err)
	}
}
