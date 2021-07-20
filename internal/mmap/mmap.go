package mmap

import (
	"reflect"
	"syscall"
	"unsafe"
)

// MMap type represents a memory mapped file or device. The slice offers
// direct access to the memory mapped content.
type MMap []byte

// Map creates a new mapping in the virtual address space
// by using the fstat system call with the provided file
// descriptor to discover its length.
func Map(fd uintptr, prot ProtFlags, flags MapFlags) (MMap, error) {
	mm, err := MapAt(0, fd, 0, -1, prot, flags)
	return mm, err
}

// MapAt creates a new mapping in the virtual address space of the calling
// process, using the specified region of the provided file or device. The
// provided addr parameter will be used as a hint of the address where the
// kernel should position the memory mapped region. If -1 is provided as
// length, this function will attempt to map until the end of the provided
// file descriptor by using the fstat system call to discover its length.
func MapAt(addr uintptr, fd uintptr, offset, length int64, prot ProtFlags, flags MapFlags) (MMap, error) {
	if length == -1 {
		var stat syscall.Stat_t
		if err := syscall.Fstat(int(fd), &stat); err != nil {
			return nil, err
		}
		length = stat.Size
	}
	addr, err := mmapSyscall(addr, uintptr(length), uintptr(prot), uintptr(flags), fd, offset)
	if err != syscall.Errno(0) {
		return nil, err
	}
	mm := MMap{}

	dh := (*reflect.SliceHeader)(unsafe.Pointer(&mm))
	dh.Data = addr
	dh.Len = int(length)
	dh.Cap = dh.Len
	return mm, nil
}

// Sync flushes changes made to the region determined by the mmap slice
// back to the device. Without calling this method, there are no guarantees
// that changes will be flushed back before the region is unmapped. The
// flags parameter specifies whether flushing should be done synchronously
// (before the method returns) with MS_SYNC, or asynchronously (flushing is just
// scheduled) with MS_ASYNC.
func (mm MMap) Sync(flags SyncFlags) error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mm))
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, rh.Data, uintptr(rh.Len), uintptr(flags))
	if err != 0 {
		return err
	}
	return nil
}

func mmapSyscall(addr, length, prot, flags, fd uintptr, offset int64) (uintptr, error) {
	addr, _, err := syscall.Syscall6(syscall.SYS_MMAP, addr, length, prot, flags, fd, uintptr(offset))
	return addr, err
}
