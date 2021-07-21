package bitcask

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	var (
		db *Bitcask
		l  *List
	)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
			l = db.List([]byte("foo"))
		})
	})

	t.Run("Append", func(t *testing.T) {
		err := l.Append([]byte("one"))
		assert.NoError(err)

		err = l.Append([]byte("two"))
		assert.NoError(err)

		err = l.Append([]byte("three"))
		assert.NoError(err)
	})

	t.Run("Len", func(t *testing.T) {
		len, err := l.Len()
		assert.NoError(err)
		assert.Equal(int64(3), len)
	})
}
