package bitcask

import (
	"bytes"
	"errors"
)

func (b *Bitcask) SortedSet(key []byte) *SortedSet {
	return &SortedSet{db: b, key: key}
}

// SortedSet ...
// +key,z = ""
// z[key]m member = score
// z[key]s score member = ""
type SortedSet struct {
	db  *Bitcask
	key []byte
}

// Add add score & member pairs
// SortedSet.Add(Score, []byte, Score, []byte ...)
func (s *SortedSet) Add(scoreMembers ...[]byte) (int, error) {
	count := len(scoreMembers)
	if count < 2 || count%2 != 0 {
		return 0, errors.New("invalid score/member pairs")
	}
	added := 0
	for i := 0; i < count; i += 2 {
		score, member := scoreMembers[i], scoreMembers[i+1]
		skey, mkey := s.scoreKey(score, member), s.memberKey(member)
		oldscore, err := s.db.Get(mkey)
		if err != nil && err != ErrKeyNotFound {
			return added, err
		}
		// remove old score key
		if oldscore != nil {
			oldskey := s.scoreKey(oldscore, member)
			if err := s.db.Delete(oldskey); err != nil {
				return added, err
			}
		} else {
			added++
		}
		if err := s.db.Put(mkey, score); err != nil {
			return added, err
		}
		if err := s.db.Put(skey, nil); err != nil {
			return added, err
		}
	}
	if err := s.db.Put(s.rawKey(), nil); err != nil {
		return added, err
	}
	return added, nil
}

func (s SortedSet) Score(member []byte) (Score, error) {
	return s.db.Get(s.memberKey(member))
}

func (s *SortedSet) Remove(members ...[]byte) (int, error) {
	removed := 0 // not including non existing members
	for _, member := range members {
		score, err := s.db.Get(s.memberKey(member))
		if err != nil {
			return removed, err
		}
		if score == nil {
			continue
		}
		if err := s.db.Delete(s.scoreKey(score, member)); err != nil {
			return removed, err
		}
		if err := s.db.Delete(s.memberKey(member)); err != nil {
			return removed, err
		}
		removed++
	}
	// clean up
	prefix := s.keyPrefix()
	ErrStopIteration := errors.New("err: stop iteration")
	err := s.db.Scan(prefix, func(key []byte) error {
		if !bytes.HasPrefix(key, prefix) {
			if err := s.db.Delete(s.rawKey()); err != nil {
				return err
			}
		}
		return ErrStopIteration
	})
	if err != ErrStopIteration {
		return removed, err
	}
	return removed, nil
}

// Range ...
// <fr> is less than <to>
func (s *SortedSet) Range(fr, to Score, fn func(i int64, score Score, member []byte, quit *bool)) error {
	min := s.scorePrefix(fr)
	max := append(s.scorePrefix(to), MAXBYTE)
	var i int64 // 0
	ErrStopIteration := errors.New("err: stop iteration")
	err := s.db.Scan(min, func(key []byte) error {
		if bytes.Compare(key, max) <= 0 {
			quit := false
			score, member, err := s.splitScoreKey(key)
			if err != nil {
				return err
			}
			if fn(i, score, member, &quit); quit {
				return ErrStopIteration
			}
			i++
		}
		return nil
	})
	if err != ErrStopIteration {
		return err
	}
	return nil
}

// +key,z = ""
func (s *SortedSet) rawKey() []byte {
	return rawKey(s.key, ElemType(SORTEDSET))
}

// z[key]
func (s *SortedSet) keyPrefix() []byte {
	return bytes.Join([][]byte{[]byte{byte(SORTEDSET)}, SOK, s.key, EOK}, nil)
}

// z[key]m
func (s *SortedSet) memberKey(member []byte) []byte {
	return bytes.Join([][]byte{s.keyPrefix(), []byte{'m'}, member}, nil)
}

// z[key]s score
func (s *SortedSet) scorePrefix(score []byte) []byte {
	return bytes.Join([][]byte{s.keyPrefix(), []byte{'s'}, score, []byte{' '}}, nil)
}

// z[key]s score member
func (s *SortedSet) scoreKey(score, member []byte) []byte {
	return bytes.Join([][]byte{s.keyPrefix(), []byte{'s'}, score, []byte{' '}, member}, nil)
}

// split (z[key]s score member) into (score, member)
func (s *SortedSet) splitScoreKey(skey []byte) ([]byte, []byte, error) {
	buf := bytes.TrimPrefix(skey, s.keyPrefix())
	pairs := bytes.Split(buf[1:], []byte{' '}) // skip score mark 's'
	if len(pairs) != 2 {
		return nil, nil, errors.New("invalid score/member key: " + string(skey))
	}
	return pairs[0], pairs[1], nil
}

// split (z[key]m member) into (member)
func (s *SortedSet) splitMemberKey(mkey []byte) ([]byte, error) {
	buf := bytes.TrimPrefix(mkey, s.keyPrefix())
	return buf[1:], nil // skip member mark 'm'
}
