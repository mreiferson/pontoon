package pontoon

type Entry struct {
	Term  int64
	Index int64
	Data  []byte
}

type Log struct {
	Term int64
	Index int64
	Entries []Entry
}
