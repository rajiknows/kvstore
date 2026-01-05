package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

type Op byte

const (
	_ Op = iota
	OpGet
	OpPut
	OpDelete
)

// so lets design a on-disk logs format
type Log struct {
	// 1->  get
	// 2-> put
	// 3-> delete
	Op    Op
	Key   []byte
	Value []byte
}

func (e *Log) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)

	if err := buf.WriteByte(byte(e.Op)); err != nil {
		return nil, err
	}

	if err := binary.Write(buf, binary.BigEndian, int32(len(e.Key))); err != nil {
		return nil, err
	}
	buf.Write(e.Key)

	if err := binary.Write(buf, binary.BigEndian, int32(len(e.Value))); err != nil {
		return nil, err
	}
	buf.Write(e.Value)

	checksum := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.BigEndian, checksum); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func Decode(r io.Reader) (*Log, error) {
	var e Log
	var op byte
	if err := binary.Read(r, binary.BigEndian, &op); err != nil {
		return nil, err
	}
	e.Op = Op(op)

	var klen int32
	if err := binary.Read(r, binary.BigEndian, &klen); err != nil {
		return nil, err
	}
	e.Key = make([]byte, klen)
	if _, err := io.ReadFull(r, e.Key); err != nil {
		return nil, err
	}
	var vlen int32
	if err := binary.Read(r, binary.BigEndian, &vlen); err != nil {
		return nil, err
	}
	e.Value = make([]byte, vlen)
	if _, err := io.ReadFull(r, e.Value); err != nil {
		return nil, err
	}

	var checksum uint32
	if err := binary.Read(r, binary.BigEndian, &checksum); err != nil {
		return nil, err
	}

	// verify checksum
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(e.Op))
	binary.Write(buf, binary.BigEndian, klen)
	buf.Write(e.Key)
	binary.Write(buf, binary.BigEndian, vlen)
	buf.Write(e.Value)

	if crc32.ChecksumIEEE(buf.Bytes()) != checksum {
		return nil, fmt.Errorf("checksum mismatch")
	}

	return &e, nil
}

func WriteLog(mu *sync.Mutex, f *os.File, entry *Log) error {
	mu.Lock()
	defer mu.Unlock()

	data, err := entry.Encode()
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		return err
	}

	return f.Sync()
}

func ReplayWAL(f *os.File, store *Kvstore) error {
	if _, e := f.Seek(0, 0); e != nil {
		return e
	}

	for {
		entry, err := Decode(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		switch entry.Op {
		case OpPut:
			store.Put(string(entry.Key), string(entry.Value))

		case OpDelete:
			store.Delete(string(entry.Key))
		}

	}
	return nil
}
