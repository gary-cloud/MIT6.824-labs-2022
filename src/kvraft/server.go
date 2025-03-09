package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Type 	string
	Key 	string
	Value 	string
	Id 		int64
	SeenId 	int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// key-value stored data.
	data 	map[string]string
	
	// record identifiers of the operations completed to avoid duplicate operations.
	completedIds	map[int64]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var ok bool
	var completedValue string

	key :=  args.Key
	id := args.Id
	seenId := args.SeenId

	_, ok = kv.completedIds[seenId]
	if ok {
		delete(kv.completedIds, seenId)
	}

	completedValue, ok = kv.completedIds[id]
	if ok {
		reply.Err = OK
		fmt.Println("Get(): OK")
		reply.Value = completedValue
		return
	}

	// Start() will return immediately.
	_, _, isLeader := kv.rf.Start(Op{Type: "Get", Key: key, Value: "", Id: id, SeenId: seenId})

	if !isLeader {
		reply.Err = ErrWrongLeader
		fmt.Println("Get(): ErrWrongLeader")
		reply.Value = completedValue
		return
	}

	rep := <- kv.applyCh

	if rep.CommandValid == true && rep.Command.(Op).Type == "Get" && rep.Command.(Op).Key == key {
		value, ok := kv.data[key]
		if ok {
			reply.Err = OK
			fmt.Println("Get(): OK")
			reply.Value = value
			// record the identifier of the last operation.
			kv.completedIds[id] = value
		} else {
			reply.Err = ErrNoKey
			fmt.Println("Get(): ErrNoKey")
			reply.Value = ""
		}
	} else {
		reply.Err = ErrWrongMessage
		fmt.Println("Get(): WrongMessage")
		reply.Value = ""
	}
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	defer kv.mu.Unlock()

	var ok bool

	key := args.Key
	value := args.Value
	op := args.Op
	id := args.Id
	seenId := args.SeenId

	_, ok = kv.completedIds[seenId]
	if ok {
		delete(kv.completedIds, seenId)
	}

	_, ok = kv.completedIds[id]
	if ok {
		reply.Err = OK
		fmt.Println("PutAppend(): OK")
		return
	}

	// Start() will return immediately.
	_, _, isLeader := kv.rf.Start(Op{Type: op, Key: key, Value: value, Id: id, SeenId: seenId})

	if !isLeader {
		reply.Err = ErrWrongLeader
		fmt.Println("PutAppend(): ErrWrongLeader")
		return
	}

	rep := <- kv.applyCh

	if rep.CommandValid == true && rep.Command.(Op).Type == op && rep.Command.(Op).Key == key && rep.Command.(Op).Value == value {
		if op == "Put" {
			kv.data[key] = value
		} else if op == "Append" {
			kv.data[key] += value
		}
		reply.Err = OK
		fmt.Println("PutAppend(): OK")

		// record the identifier of the last operation.
		kv.completedIds[id] = value
	} else {
		reply.Err = ErrWrongMessage
		fmt.Println("PutAppend(): WrongMessage")
	}
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// Followers receive apply messages and update the data of kv server in the background.
func (kv *KVServer) appliedToStateMachine() {
	for !kv.killed() {
		
		_, isLeader := kv.rf.GetState()

		// Skip the leader
		if isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		// For the followers
		for m := range kv.applyCh {
			if !m.CommandValid {
				panic("m.CommandValid can't be false")
			}

			op := m.Command.(Op).Type
			key := m.Command.(Op).Key
			value := m.Command.(Op).Value
			id := m.Command.(Op).Id
			seenId := m.Command.(Op).SeenId

			if op == "Put" {
				kv.data[key] = value
			} else if op == "Append" {
				kv.data[key] += value
			}

			// record the identifier of the last operation.
			kv.completedIds[id] = value

			// If the seenId is in the completedIds, delete it.
			_, ok := kv.completedIds[seenId]
			if ok {
				delete(kv.completedIds, seenId)
			}

			_, isLeader := kv.rf.GetState()
			if isLeader {
				break
			}
		}

		kv.mu.Unlock()
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	kv.completedIds = make(map[int64]string)

	return kv
}
