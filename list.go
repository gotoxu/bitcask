package bitcask

import (
	"bytes"
	"errors"
	"log"
)

func (b *Bitcask) List(key []byte) *List {
	return &List{db: b, key: key}
}

// List ...
// +key,l = ""
// l[key]0 = "a"
// l[key]1 = "b"
// l[key]2 = "c"
type List struct {
	db  *Bitcask
	key []byte
}

func (l *List) Index(i int64) ([]byte, error) {
	x, err := l.leftIndex()
	if err != nil {
		return nil, err
	}
	return l.db.Get(l.indexKey(x + i))
}

// Range enumerate value by index
// <start> must >= 0
// <stop> should equal to -1 or lager than <start>
func (l *List) Range(start, stop int64, fn func(i int64, value []byte, quit *bool)) error {
	if start < 0 || (stop != -1 && start > stop) {
		return errors.New("bad start/stop index")
	}
	x, y, err := l.rangeIndex()
	if err != nil {
		return err
	}
	if stop == -1 {
		stop = (y - x + 1) - 1 // (size) - 1
	}
	min := l.indexKey(x + int64(start))
	max := l.indexKey(x + int64(stop))
	var i int64 // 0
	ErrStopIteration := errors.New("err: stop iteration")
	err = l.db.Scan(min, func(key []byte) error {
		if key != nil && bytes.Compare(key, max) <= 0 {
			val, err := l.db.Get(key)
			if err != nil {
				return err
			}
			quit := false
			if fn(start+i, val, &quit); quit {
				return ErrStopIteration
			}
			i++
			return nil
		}
		return ErrStopIteration
	})
	if err == ErrStopIteration {
		return nil
	}
	return err
}

// Append ...
func (l *List) Append(vals ...[]byte) error {
	x, y, err := l.rangeIndex()
	if err != nil {
		return err
	}
	if x == 0 && y == -1 {
		if err := l.db.Put(l.rawKey(), nil); err != nil {
			return err
		}
	}
	for i, val := range vals {
		if err := l.db.Put(l.indexKey(y+int64(i)+1), val); err != nil {
			return err
		}
	}
	return nil
}

// Pop ...
func (l *List) Pop() ([]byte, error) {
	x, y, err := l.rangeIndex()
	if err != nil {
		return nil, err
	}

	size := y - x + 1
	if size == 0 {
		return nil, nil
	} else if size < 0 { // double check
		return nil, errors.New("bad list struct")
	}

	idxkey := l.indexKey(y)

	val, err := l.db.Get(idxkey)
	if err != nil {
		return nil, err
	}
	if err := l.db.Delete(idxkey); err != nil {
		return nil, err
	}
	if size == 1 { // clean up
		return nil, l.db.Delete(l.rawKey())
	}

	return val, nil
}

// Len ...
func (l *List) Len() (int64, error) {
	x, y, err := l.rangeIndex()
	return y - x + 1, err
}

func (l *List) rangeIndex() (int64, int64, error) {
	left, err := l.leftIndex()
	if err != nil {
		return 0, -1, err
	}
	right, err := l.rightIndex()
	if err != nil {
		return 0, -1, err
	}
	log.Printf("left: %d\n", left)
	log.Printf("right: %d\n", right)
	return left, right, nil
}

func (l *List) leftIndex() (int64, error) {
	log.Println("leftIndex:")
	idx := int64(0) // default 0
	prefix := l.keyPrefix()
	log.Printf(" prefix: %s\n", prefix)
	ErrStopIteration := errors.New("err: stop iteration")
	err := l.db.Scan(prefix, func(key []byte) error {
		log.Printf(" key: %v\n", key)
		if bytes.HasPrefix(key, prefix) {
			idx = l.indexInKey(key)
			log.Printf("  idx: %d\n", idx)
		}
		return ErrStopIteration
	})
	if err == ErrStopIteration {
		return idx, nil
	}
	return idx, err
}

func (l *List) rightIndex() (int64, error) {
	log.Println("rightIndex:")
	idx := int64(-1) // default -1
	prefix := l.keyPrefix()
	log.Printf(" prefix: %s\n", prefix)
	err := l.db.Scan(prefix, func(key []byte) error {
		log.Printf(" key: %v\n", key)
		if bytes.HasPrefix(key, prefix) {
			idx = l.indexInKey(key)
			log.Printf(" idx: %d\n", idx)
		}
		return nil
	})
	return idx, err
}

// +key,l = ""
func (l *List) rawKey() []byte {
	return rawKey(l.key, ElemType(LIST))
}

// l[key]
func (l *List) keyPrefix() []byte {
	return bytes.Join([][]byte{[]byte{byte(LIST)}, SOK, l.key, EOK}, nil)
}

// l[key]0 = "a"
func (l *List) indexKey(i int64) []byte {
	sign := []byte{0}
	if i >= 0 {
		sign = []byte{1}
	}
	b := bytes.Join([][]byte{l.keyPrefix(), sign, itob(i)}, nil)
	log.Printf("indexKeu: %x\n", b)
	return b
}

// split l[key]index into index
func (l *List) indexInKey(key []byte) int64 {
	idxbuf := bytes.TrimPrefix(key, l.keyPrefix())
	return btoi(idxbuf[1:]) // skip sign "0/1"
}
