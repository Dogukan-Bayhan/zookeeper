package server

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Storage is the durable state of a server: the write-ahead transaction log,
// the latest snapshot and the Zab epochs (Section 4.3).
type Storage interface {
	// Append durably appends transactions to the log. It must not return
	// before the data is on stable storage (Section 4).
	Append(txns []*Txn) error
	// Entries returns the whole log.
	Entries() ([]*Txn, error)
	// Compact drops log entries with zxid <= upTo.
	Compact(upTo int64) error
	// Reset replaces the whole log.
	Reset(entries []*Txn) error
	SaveSnapshot(*Snapshot) error
	// LoadSnapshot returns nil, nil when there is no snapshot.
	LoadSnapshot() (*Snapshot, error)
	SaveEpochs(accepted, current int64) error
	LoadEpochs() (accepted, current int64, err error)
	Close() error
}

// MemStorage keeps everything in memory. Reusing the same MemStorage when a
// server is restarted simulates a crash with intact disks.
type MemStorage struct {
	mu                sync.Mutex
	log               []*Txn
	snap              *Snapshot
	accepted, current int64
}

// NewMemStorage returns an empty in-memory storage.
func NewMemStorage() *MemStorage { return &MemStorage{} }

func (m *MemStorage) Append(txns []*Txn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append(m.log, txns...)
	return nil
}

func (m *MemStorage) Entries() ([]*Txn, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*Txn(nil), m.log...), nil
}

func (m *MemStorage) Compact(upTo int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	i := 0
	for i < len(m.log) && m.log[i].Zxid <= upTo {
		i++
	}
	m.log = append([]*Txn(nil), m.log[i:]...)
	return nil
}

func (m *MemStorage) Reset(entries []*Txn) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.log = append([]*Txn(nil), entries...)
	return nil
}

func (m *MemStorage) SaveSnapshot(s *Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = s
	return nil
}

func (m *MemStorage) LoadSnapshot() (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snap, nil
}

func (m *MemStorage) SaveEpochs(a, c int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accepted, m.current = a, c
	return nil
}

func (m *MemStorage) LoadEpochs() (int64, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.accepted, m.current, nil
}

func (m *MemStorage) Close() error { return nil }

// FileStorage persists the log, snapshots and epochs in a directory. Log
// records are length-prefixed and checksummed; a torn record at the tail
// (crash during a write) is discarded on recovery.
type FileStorage struct {
	mu     sync.Mutex
	dir    string
	sync   bool
	logF   *os.File
	logBuf *bufio.Writer
}

// NewFileStorage opens (or creates) a storage directory. When fsync is true
// every append is forced to the disk media before it is acknowledged.
func NewFileStorage(dir string, fsync bool) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	fs := &FileStorage{dir: dir, sync: fsync}
	// Validate the log and drop a torn tail before appending to it.
	entries, err := fs.readLog()
	if err != nil {
		return nil, err
	}
	if err := fs.rewriteLog(entries); err != nil {
		return nil, err
	}
	return fs, nil
}

func (fs *FileStorage) logPath() string { return filepath.Join(fs.dir, "txnlog") }

func encodeRecord(v any) ([]byte, error) {
	var body bytes.Buffer
	if err := gob.NewEncoder(&body).Encode(v); err != nil {
		return nil, err
	}
	rec := make([]byte, 8+body.Len())
	binary.BigEndian.PutUint32(rec[0:4], uint32(body.Len()))
	binary.BigEndian.PutUint32(rec[4:8], crc32.ChecksumIEEE(body.Bytes()))
	copy(rec[8:], body.Bytes())
	return rec, nil
}

// decodeRecord reads one record; io.EOF means a clean end, errTorn a corrupt
// or incomplete record.
var errTorn = errors.New("torn record")

func decodeRecord(r io.Reader, v any) error {
	var hdr [8]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF {
			return io.EOF
		}
		return errTorn
	}
	n := binary.BigEndian.Uint32(hdr[0:4])
	if n > 64<<20 {
		return errTorn
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return errTorn
	}
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(hdr[4:8]) {
		return errTorn
	}
	if err := gob.NewDecoder(bytes.NewReader(body)).Decode(v); err != nil {
		return errTorn
	}
	return nil
}

func (fs *FileStorage) readLog() ([]*Txn, error) {
	f, err := os.Open(fs.logPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var out []*Txn
	for {
		var t Txn
		err := decodeRecord(r, &t)
		if err == io.EOF || err == errTorn {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, &t)
	}
}

// rewriteLog atomically replaces the log file and reopens it for appending.
func (fs *FileStorage) rewriteLog(entries []*Txn) error {
	if fs.logF != nil {
		fs.logBuf.Flush()
		fs.logF.Close()
		fs.logF = nil
	}
	tmp := fs.logPath() + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, t := range entries {
		rec, err := encodeRecord(t)
		if err != nil {
			f.Close()
			return err
		}
		w.Write(rec)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmp, fs.logPath()); err != nil {
		return err
	}
	fs.logF, err = os.OpenFile(fs.logPath(), os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	fs.logBuf = bufio.NewWriter(fs.logF)
	return nil
}

func (fs *FileStorage) Append(txns []*Txn) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.logF == nil {
		return errors.New("storage closed")
	}
	for _, t := range txns {
		rec, err := encodeRecord(t)
		if err != nil {
			return err
		}
		if _, err := fs.logBuf.Write(rec); err != nil {
			return err
		}
	}
	if err := fs.logBuf.Flush(); err != nil {
		return err
	}
	if fs.sync {
		return fs.logF.Sync()
	}
	return nil
}

func (fs *FileStorage) Entries() ([]*Txn, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.logBuf != nil {
		fs.logBuf.Flush()
	}
	return fs.readLog()
}

func (fs *FileStorage) Compact(upTo int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.logBuf.Flush()
	entries, err := fs.readLog()
	if err != nil {
		return err
	}
	i := 0
	for i < len(entries) && entries[i].Zxid <= upTo {
		i++
	}
	return fs.rewriteLog(entries[i:])
}

func (fs *FileStorage) Reset(entries []*Txn) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.rewriteLog(entries)
}

func (fs *FileStorage) writeFileAtomic(name string, v any) error {
	rec, err := encodeRecord(v)
	if err != nil {
		return err
	}
	tmp := filepath.Join(fs.dir, name+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(rec); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return os.Rename(tmp, filepath.Join(fs.dir, name))
}

func (fs *FileStorage) readFile(name string, v any) (bool, error) {
	f, err := os.Open(filepath.Join(fs.dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	if err := decodeRecord(bufio.NewReader(f), v); err != nil {
		return false, fmt.Errorf("storage: corrupt %s: %v", name, err)
	}
	return true, nil
}

func (fs *FileStorage) SaveSnapshot(s *Snapshot) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.writeFileAtomic("snapshot", s)
}

func (fs *FileStorage) LoadSnapshot() (*Snapshot, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var s Snapshot
	ok, err := fs.readFile("snapshot", &s)
	if !ok || err != nil {
		return nil, err
	}
	return &s, nil
}

type epochs struct{ Accepted, Current int64 }

func (fs *FileStorage) SaveEpochs(a, c int64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.writeFileAtomic("epochs", epochs{a, c})
}

func (fs *FileStorage) LoadEpochs() (int64, int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var e epochs
	_, err := fs.readFile("epochs", &e)
	return e.Accepted, e.Current, err
}

func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if fs.logF == nil {
		return nil
	}
	fs.logBuf.Flush()
	err := fs.logF.Close()
	fs.logF = nil
	return err
}
