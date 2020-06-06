package main

import (
	"fmt"
	"log"

	"github.com/prologic/bitcask"
)

func main() {
	db, err := bitcask.Open("test.db")
	if err != nil {
		log.Fatal(err)
	}
	l := db.List([]byte("foo"))
	err = l.Append([]byte("one"))
	if err != nil {
		log.Fatal(err)
	}
	err = l.Append([]byte("two"))
	if err != nil {
		log.Fatal(err)
	}
	err = l.Append([]byte("three"))
	if err != nil {
		log.Fatal(err)
	}
	len, err := l.Len()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("len: %d\n", len)
}
