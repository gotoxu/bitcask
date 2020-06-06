package bitcask

import (
	"bytes"
	"errors"
)

func (b *Bitcask) Hash(key []byte) *Hash {
	return &Hash{db: b, key: key}
}

// Hash ...
// 	+key,h = ""
// 	h[key]name = "latermoon"
// 	h[key]age = "27"
// 	h[key]sex = "Male"
type Hash struct {
	db  *Bitcask
	key []byte
}

func (h *Hash) Get(field []byte) ([]byte, error) {
	return h.db.Get(h.fieldKey(field))
}

func (h *Hash) MGet(fields ...[]byte) ([][]byte, error) {
	vals := make([][]byte, 0, len(fields))
	for _, field := range fields {
		val, err := h.db.Get(h.fieldKey(field))
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}
	return vals, nil
}

// GetAll ...
func (h *Hash) GetAll() (map[string][]byte, error) {
	keyVals := map[string][]byte{}
	prefix := h.fieldPrefix()
	err := h.db.Scan(prefix, func(key []byte) error {
		val, err := h.db.Get(key)
		if err != nil {
			return err
		}
		keyVals[string(h.fieldInKey(key))] = val
		return nil
	})
	return keyVals, err
}

func (h *Hash) Set(field, value []byte) error {
	return h.MSet(field, value)
}

func (h *Hash) MSet(fieldVals ...[]byte) error {
	if len(fieldVals) == 0 || len(fieldVals)%2 != 0 {
		return errors.New("invalid field value pairs")
	}

	for i := 0; i < len(fieldVals); i += 2 {
		field, val := fieldVals[i], fieldVals[i+1]
		if err := h.db.Put(h.fieldKey(field), val); err != nil {
			return err
		}
	}
	return h.db.Put(h.rawKey(), nil)
}

func (h *Hash) Remove(fields ...[]byte) error {
	for _, field := range fields {
		if err := h.db.Delete(h.fieldKey(field)); err != nil {
			return err
		}
	}
	// clean up
	prefix := h.fieldPrefix()
	return h.db.Scan(prefix, func(key []byte) error {
		return h.db.Delete(key)
	})
}

func (h *Hash) Drop() error {
	prefix := h.fieldPrefix()
	err := h.db.Scan(prefix, func(key []byte) error {
		return h.db.Delete(key)
	})
	if err != nil {
		return err
	}
	return h.db.Delete(h.rawKey())
}

// +key,h
func (h *Hash) rawKey() []byte {
	return rawKey(h.key, HASH)
}

// h[key]field
func (h *Hash) fieldKey(field []byte) []byte {
	return bytes.Join([][]byte{h.fieldPrefix(), field}, nil)
}

// h[key]
func (h *Hash) fieldPrefix() []byte {
	return bytes.Join([][]byte{[]byte{byte(HASH)}, SOK, h.key, EOK}, nil)
}

// split h[key]field into field
func (h *Hash) fieldInKey(fieldKey []byte) []byte {
	right := bytes.Index(fieldKey, EOK)
	return fieldKey[right+1:]
}
