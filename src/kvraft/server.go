package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
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
	Index    int
	Term     int
	Name     string
	Key      string
	Value    string
	ClientId int64
	SeqId    int64
}

type OpContext struct {
	op          Op
	wrongLeader bool
	keyExist    bool
	ignored     bool
	value       string
	notify      chan bool
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dbMap  map[string]string
	opMap  map[int]*OpContext
	seqMap map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server: KVServer[%d] Get(%s)", kv.me, args.Key)
	// Enter Get() in the Raft Log
	newOp := Op{
		Name:     "Get",
		Key:      args.Key,
		Value:    "",
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}
	var isLeader bool
	newOp.Index, newOp.Term, isLeader = kv.rf.Start(newOp)
	DPrintf("Server: KVServer[%d] Get(%s) index = %d, term = %d, leader = %v", kv.me, args.Key, newOp.Index, newOp.Term, isLeader)
	// Judge whether the current kvserver is leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Server: KVServer[%d] Get(%s) Leader = %d", kv.me, args.Key, kv.rf.GetLeader())
	// Update Op Context
	newOpCtx := OpContext{
		op:          newOp,
		wrongLeader: false,
		keyExist:    false,
		value:       "",
		notify:      make(chan bool),
	}

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.opMap[newOp.Index] = &newOpCtx
	}()

	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.opMap, newOp.Index)
	}()

	// Wait for the Raft Log to be committed
	// or the Raft Leader to change
	timer := time.NewTimer(1000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-newOpCtx.notify:
		if newOpCtx.wrongLeader {
			reply.Err = ErrWrongLeader
			return
		} else if newOpCtx.keyExist {
			reply.Err = OK
			reply.Value = newOpCtx.value
			DPrintf("Server: KVServer[%d] Get(%s) = %s", kv.me, args.Key, reply.Value)
			return
		} else {
			reply.Err = ErrNoKey
			DPrintf("Server: KVServer[%d] Get(%s) = None", kv.me, args.Key)
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	DPrintf("Server: KVServer[%d] PutAppend(%s, %s)", kv.me, args.Key, args.Value)
	// Enter PutAppend() in the Raft Log
	newOp := Op{
		Name:     args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}
	var isLeader bool
	newOp.Index, newOp.Term, isLeader = kv.rf.Start(newOp)
	DPrintf("Server: KVServer[%d] PutAppend(%s, %s) index = %d, term = %d leader = %v", kv.me, args.Key, args.Value, newOp.Term, newOp.Index, isLeader)
	// Judge whether the current kvserver is leader
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Server: KVServer[%d] PutAppend(%s, %s) Leader = %d", kv.me, args.Key, args.Value, kv.rf.GetLeader())
	// Wait for the Raft Log to be committed
	// or the Raft Leader to change
	newOpCtx := OpContext{
		op:          newOp,
		wrongLeader: false,
		notify:      make(chan bool),
		keyExist:    false,
		value:       "",
	}

	func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		kv.opMap[newOp.Index] = &newOpCtx
	}()

	defer func() {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.opMap, newOp.Index)
	}()

	timer := time.NewTimer(1000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-newOpCtx.notify:
		if newOpCtx.wrongLeader {
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			func() {
				kv.mu.Lock()
				defer kv.mu.Unlock()
				DPrintf("Server: KVServer[%d] PutAppend(%s, %s) = %v", kv.me, args.Key, args.Value, kv.dbMap[args.Key])
			}()
			return
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		return
	}
}

// Read Applied Msg from Raft
func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		msg := <-kv.applyCh
		cmd := msg.Command
		index := msg.CommandIndex
		term := msg.CommandTerm
		op := cmd.(Op)

		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()

			opCtx, existOp := kv.opMap[index]
			prevSeq, existSeq := kv.seqMap[op.ClientId]
			kv.seqMap[op.ClientId] = op.SeqId
			DPrintf("Server: KVServer[%d] Op: %v ExistOp: %v Cmd: %v, Index: %v, Term: %v ExistSeq: %v, prevSeq: %v", kv.me, opCtx, existOp, op, index, term, existSeq, prevSeq)

			if existOp {
				if opCtx.op.Term != term || opCtx.op.Index != index {
					opCtx.wrongLeader = true
					return
				}
			}

			DPrintf("Server: KVServer[%d] Leadership not change", kv.me)

			switch op.Name {
			case "Put":
				if !existSeq || prevSeq < op.SeqId {
					kv.dbMap[op.Key] = op.Value
				} else if existOp {
					opCtx.ignored = true
				}
			case "Append":
				if !existSeq || prevSeq < op.SeqId {
					if _, ok := kv.dbMap[op.Key]; !ok {
						kv.dbMap[op.Key] = op.Value
					} else {
						kv.dbMap[op.Key] += op.Value
					}
				} else if existOp {
					opCtx.ignored = true
				}
			case "Get":
				if existOp {
					opCtx.value, opCtx.keyExist = kv.dbMap[op.Key]
				}
			}

			DPrintf("Server: KVServer[%d] Apply Command: %v", kv.me, op)

			// notify the waiting RPC handler
			if existOp {
				close(opCtx.notify)
			}
		}()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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

	kv.dbMap = make(map[string]string)
	kv.opMap = make(map[int]*OpContext)
	kv.seqMap = make(map[int64]int64)

	go kv.applyLoop()

	return kv
}
