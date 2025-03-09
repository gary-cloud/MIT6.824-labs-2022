package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongMessage = "ErrWrongMessage"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.

	// Uniquely identify client operations to ensure that the key/value service executes
	Id		int64

	SeenId 	int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	// Uniquely identify client operations to ensure that the key/value service executes
	Id	int64

	SeenId 	int64
}

type GetReply struct {
	Err   Err
	Value string
}
