package pontoon

import (
	"errors"
	"log"
	"sync"
)

type Entry struct {
	Index int64
	Term  int64
	Data  []byte
}

type Log struct {
	sync.RWMutex

	Index   int64
	Term    int64
	Entries []*Entry
}

func (l *Log) Get(index int64) *Entry {
	if index < 0 {
		return nil
	}
	return l.Entries[index]
}

func (l *Log) FresherThan(index int64, term int64) bool {
	if l.Term > term {
		return true
	}

	if l.Term < term {
		return false
	}

	return l.Index > index
}

func (l *Log) Check(prevLogIndex int64, prevLogTerm int64, index int64, term int64) error {
	if len(l.Entries) > 0 && index > 0 {
		if index > int64(len(l.Entries)) {
			return errors.New("behind")
		}
		lastGoodEntry := l.Entries[index-1]
		if lastGoodEntry.Term != prevLogTerm && lastGoodEntry.Index != prevLogIndex {
			return errors.New("inconsistent")
		}
	} else if index != 0 {
		return errors.New("missing")
	}
	return nil
}

func (l *Log) Append(index int64, term int64, data []byte) error {
	if term != l.Term {
		l.Term = term
	}

	e := &Entry{
		Index: index,
		Term:  l.Term,
		Data:  data,
	}

	log.Printf("... appending %+v", e)

	if index < l.Index {
		log.Printf("... truncating to %d", index-1)
		l.Entries = l.Entries[:index]
	}

	l.Entries = append(l.Entries, e)

	l.Index = index + 1
	return nil
}
