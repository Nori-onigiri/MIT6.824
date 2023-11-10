package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int   // The leaderId of the cluster
	clientId int64 // The clientId of the client
	seqId    int64 // The sequence number of the request
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
	ck.clientId = nrand()
	return ck
}

func (ck *Clerk) changeLeaderId() {
	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	for {
		DPrintf("Client: KVServer[%d] Get(%s)", ck.leaderId, key)
		reply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			ck.changeLeaderId()
			time.Sleep(1 * time.Millisecond)
			continue
		}
		// Send RPC to server successfully, need to wait for response
		switch reply.Err {
		case OK:
			DPrintf("Client: KVServer[%d] Get(%s) = %s", ck.leaderId, key, reply.Value)
			return reply.Value
		case ErrNoKey:
			DPrintf("Client: KVServer[%d] Get(%s) = None", ck.leaderId, key)
			return ""
		case ErrWrongLeader:
			DPrintf("Client: KVServer[%d] Not Leader", ck.leaderId)
			ck.changeLeaderId()
			time.Sleep(1 * time.Millisecond)
			continue
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    atomic.AddInt64(&ck.seqId, 1),
	}
	for {
		DPrintf("Client: KVServer[%d] PutAppend(%s, %s)", ck.leaderId, key, value)
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			ck.changeLeaderId()
			time.Sleep(1 * time.Millisecond)
			continue
		}
		switch reply.Err {
		case OK:
			DPrintf("Client: KVServer[%d] PutAppend(%s, %s) = OK", ck.leaderId, key, value)
			return
		case ErrWrongLeader:
			DPrintf("Client: KVServer[%d] Not Leader", ck.leaderId)
			ck.changeLeaderId()
			time.Sleep(1 * time.Millisecond)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
