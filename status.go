package remote_channel

type ChStatus int

const (
	NORMAL = iota
	CLOSED

	NET_IN
	NET_OUT
	NET_CREATE

	BLOCK_IN
	BLOCK_OUT
)
