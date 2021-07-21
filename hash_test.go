package bitcask

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHash(t *testing.T) {
	assert := assert.New(t)

	testdir, err := ioutil.TempDir("", "bitcask")
	assert.NoError(err)

	var (
		db *Bitcask
		h  *Hash
	)

	t.Run("Setup", func(t *testing.T) {
		t.Run("Open", func(t *testing.T) {
			db, err = Open(testdir)
			assert.NoError(err)
			h = db.Hash([]byte("foo"))
		})
	})

	t.Run("Set", func(t *testing.T) {
		err := h.Set([]byte("1"), []byte("one"))
		assert.NoError(err)

		err = h.Set([]byte("2"), []byte("two"))
		assert.NoError(err)

		err = h.Set([]byte("3"), []byte("three"))
		assert.NoError(err)
	})

	t.Run("Get", func(t *testing.T) {
		val, err := h.Get([]byte("1"))
		assert.NoError(err)
		assert.Equal([]byte("one"), val)

		val, err = h.Get([]byte("2"))
		assert.NoError(err)
		assert.Equal([]byte("two"), val)

		val, err = h.Get([]byte("3"))
		assert.NoError(err)
		assert.Equal([]byte("three"), val)
	})
}
