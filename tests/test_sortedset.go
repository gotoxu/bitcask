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
	z := db.SortedSet([]byte("foo"))
	added, err := z.Add(
		bitcask.Int64ToScore(1), []byte("a"),
		bitcask.Int64ToScore(2), []byte("b"),
		bitcask.Int64ToScore(3), []byte("c"),
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("added %d\n", added)

	score, err := z.Score([]byte("b"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("score: %d\n", bitcask.ScoreToInt64(score))
}
