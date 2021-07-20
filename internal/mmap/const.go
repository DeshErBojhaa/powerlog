package mmap

type ProtFlags uint

const (
	PROT_NONE  ProtFlags = 0x0
	PROT_READ  ProtFlags = 0x1
	PROT_WRITE ProtFlags = 0x2
)

type MapFlags uint

const (
	MAP_SHARED  MapFlags = 0x1
	MAP_PRIVATE MapFlags = 0x2
)

type SyncFlags uint

const (
	MS_SYNC  SyncFlags = 0x4
	MS_ASYNC SyncFlags = 0x1
)
