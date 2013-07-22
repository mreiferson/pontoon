package pontoon

import (
	"errors"
	"log"
	"sync"
)

type Entry struct {
	Term  int64
	Index int64
	Data  []byte
}

type Log struct {
	sync.RWMutex

	Term    int64
	Index   int64
	Entries []*Entry
}

func (l *Log) FresherThan(index int64, term int64) bool {
	l.RLock()
	defer l.RUnlock()

	if l.Term > term {
		return true
	}

	if l.Term < term {
		return false
	}

	return l.Index > index
}

func (l *Log) checkFields(prevLogTerm int64, prevLogIndex int64, term int64, index int64) error {
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

func (l *Log) Nop(prevLogTerm int64, prevLogIndex int64, term int64, index int64) error {
	l.Lock()
	defer l.Unlock()

	return l.checkFields(prevLogTerm, prevLogIndex, term, index)
}

func (l *Log) Append(prevLogTerm int64, prevLogIndex int64, term int64, index int64, data []byte) error {
	l.Lock()
	defer l.Unlock()

	err := l.checkFields(prevLogTerm, prevLogIndex, term, index)
	if err != nil {
		return err
	}

	if term != l.Term {
		l.Term = term
	}

	e := &Entry{
		Term:  l.Term,
		Index: l.Index,
		Data:  data,
	}

	log.Printf("... appending %+v", e)

	l.Entries = append(l.Entries, e)

	l.Index++
	return nil
}
