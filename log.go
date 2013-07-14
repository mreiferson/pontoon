package pontoon

type Entry struct {
	Term  int64
	Index int64
	Data  []byte
}

type Log struct {
	Term    int64
	Index   int64
	Entries []Entry
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
