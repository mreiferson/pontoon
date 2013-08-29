package pontoon

import (
	"errors"
	"log"
	"sync"
)

type Entry struct {
	CmdID int64
	Index int64
	Term  int64
	Data  []byte
}

type Log struct {
	sync.RWMutex

	index int64
	term  int64

	Entries []*Entry
}

func (l *Log) Index() int64 {
	return l.index
}

func (l *Log) LastIndex() int64 {
	return l.index - 1
}

func (l *Log) Term() int64 {
	return l.term
}

func (l *Log) Get(index int64) *Entry {
	if index < 0 {
		return nil
	}
	return l.Entries[index]
}

func (l *Log) GetEntryForRequest(index int64) (*Entry, int64, int64) {
	if index < 0 {
		return nil, -1, -1
	}
	if index < 1 {
		return l.Entries[index], -1, -1
	}
	return l.Entries[index], l.Entries[index-1].Index, l.Entries[index-1].Term
}

func (l *Log) FresherThan(index int64, term int64) bool {
	if l.term > term {
		return true
	}

	if l.term < term {
		return false
	}

	return l.index > index
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

func (l *Log) Append(e *Entry) error {
	if e.Term != l.term {
		l.term = e.Term
	}

	if e.Index < l.index {
		log.Printf("... truncating to %d", e.Index-1)
		l.Entries = l.Entries[:e.Index]
	}

	l.Entries = append(l.Entries, e)
	l.index = e.Index + 1
	return nil
}
