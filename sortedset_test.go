package bitcask

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortedSet(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	var (
		db *Bitcask
		z  *SortedSet
	)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
			z = db.SortedSet([]byte("foo"))
		})
	})

	t.Run("Add", func(t *testing.T) {
		added, err := z.Add(
			Int64ToScore(1), []byte("a"),
			Int64ToScore(2), []byte("b"),
			Int64ToScore(3), []byte("c"),
		)
		assert.NoError(err)
		assert.Equal(3, added)
	})

	t.Run("Score", func(t *testing.T) {
		score, err := z.Score([]byte("b"))
		assert.NoError(err)
		assert.Equal(int64(2), ScoreToInt64(score))
	})
}
