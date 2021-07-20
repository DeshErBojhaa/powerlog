package log

import (
	"github.com/DeshErBojhaa/powerlog/internal/mmap"
	"io"
	"os"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap mmap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = mmap.Map(
		idx.file.Fd(),
		mmap.PROT_READ|mmap.PROT_WRITE,
		mmap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read takes in an offset and returns the associated record’s
// position in the store. The given offset is relative to the
// segment’s base offset; 0 is always the offset of the index’s
// first entry, 1 is the second entry, and so on. We use relative
// offsets to reduce the size of the indexes by storing offsets as uint32s.
func (i *index) Read(in int64) (uint32, uint64, error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	var out uint32
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos := uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = encoding.Uint32(i.mmap[pos : pos+offWidth])
	pos = encoding.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index.
// First, we validate that we have space to write the entry.
// If there’s space, we then encode the offset and position
// and write them to the memory-mapped file. Then we increment
// the position where the next write will go.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	encoding.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	encoding.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

// Close makes sure the memory-mapped file has synced its data
// to the persisted file and that the persisted file has flushed
// its contents to stable storage. Then it truncates the persisted
// file to the amount of data that’s actually in it and closes the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(mmap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Name() string {
	return i.file.Name()
}
