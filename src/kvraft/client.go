package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "fmt"
import "sync"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// Remember which server turned out to be the leader for the last RPC, and send the next RPC to that server first.
	// This will avoid wasting time searching for the leader on every RPC, which may help you pass some of the tests quickly enough.
	lastLeader int

	// store the seenIds to avoid duplicate operations.
	seenIds map[int64]bool

	// protect the seenIds.
	seenIdsMu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.seenIds = make(map[int64]bool)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	id := nrand()

	// get the first seenId.
	var seenId int64
	ck.seenIdsMu.Lock()
	for seenId = range ck.seenIds {
		break
	}
	ck.seenIdsMu.Unlock()

	// Same id, same seenId.
	args := GetArgs{Key: key, Id: id, SeenId: seenId}

	for i := ck.lastLeader; i < len(ck.servers); i = (i + 1) % len(ck.servers) {
		// construct a new reply to avoid labgob warning.
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok {
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		} else if reply.Err == OK {
			ck.lastLeader = i

			ck.seenIdsMu.Lock()
			// add a new seenId.
			ck.seenIds[id] = true
			// free the memory of seenId.
			_, ok = ck.seenIds[seenId]
			if ok {
				delete(ck.seenIds, seenId)
			}
			ck.seenIdsMu.Unlock()

			return reply.Value
		}
	}

	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	id := nrand()

	// get the first seenId.
	var seenId int64
	ck.seenIdsMu.Lock()
	for seenId = range ck.seenIds {
		break
	}
	ck.seenIdsMu.Unlock()

	// Same id, same seenId.
	args := PutAppendArgs{Key: key, Value: value, Op: op, Id: id, SeenId: seenId}

	for i := ck.lastLeader; i < len(ck.servers); i = (i + 1) % len(ck.servers) {
		// construct a new reply to avoid labgob warning.
		reply := PutAppendReply{}
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err != OK {
			fmt.Printf("PutAppend failed: %v\n", reply.Err)
			continue
		}
		ck.lastLeader = i

		ck.seenIdsMu.Lock()
		// add a new seenId.
		ck.seenIds[id] = true
		// free the memory of seenId.
		_, ok = ck.seenIds[seenId]
		if ok {
			delete(ck.seenIds, seenId)
		}
		ck.seenIdsMu.Unlock()

		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
