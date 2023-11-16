package lib

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	// "sort"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
)

// Subset returns true if as subset of bs, false otherwise
func Subset(a, b []int) bool {
	return decomp.Subset(a, b)
	// if len(a) == 0 {
	// 	return true
	// }
	// if len(a) > len(b) {
	// 	return false
	// }

	// var A sortutil.IntSlice
	// var B sortutil.IntSlice

	// A = a
	// B = b
	// sort.Stable(A)
	// sort.Stable(B)
	// a = A
	// b = B

	// for len(a) > 0 {

	// 	switch {
	// 	case len(b) == 0:
	// 		return false
	// 	case a[0] == b[0]:
	// 		a = a[1:]
	// 		b = b[1:]
	// 	case a[0] < b[0]:
	// 		return false
	// 	case a[0] > b[0]:
	// 		b = b[1:]
	// 	}
	// }
	// return true
}

// Init is used to initialise the current search and is sent by the coordinator to other processes
type Init struct {
	Input decomp.Graph
	Width int
}

// the MessageType type used to indicate which kinds of messages are being sent
type MessageType int

// var encodeBuffer1 bytes.Buffer
// var decodeBuffer1 bytes.Buffer
// var encodeBuffer2 bytes.Buffer
// var decodeBuffer2 bytes.Buffer
// var encodeBuffer3 bytes.Buffer
// var decodeBuffer3 bytes.Buffer

// these consts are used to indicate the type of message sent between various processes
const (
	Register MessageType = iota
	SendComponent
	SendBlock
	SearchGraph
	Initialise
	Result
	Exhausted
	Idle
)

// MessageToCoordinator is used by the worker and CTDCheck to sent various messages back to the coordinator
type MessageToCoordinator struct {
	Mtype       MessageType // the type of message sent
	Id          string      // the id of the worker
	Search      SearchType
	Count       int            // sent by worker, indicates current state
	Comp        []decomp.Graph // components found by the worker
	Upper       decomp.Graph   // used by logk style search
	CTD         bool           // if true, then this message was sent by a CTDCheck unit
	AllowedFull decomp.Edges   // for logk search, this maps each entry of comp to one of allowed
	AllowedRed  decomp.Edges   // for logk search, this maps each entry of comp to one of allowed
	Conns       [][]int        // for logk search, this maps each entry of comp to one of Conns
	OldConn     []int          // for logk
	Decomp      decomp.Decomp  // for returning the result
}

func (m MessageToCoordinator) ToBytes() []byte {
	var encodeBuffer bytes.Buffer
	enc := gob.NewEncoder(&encodeBuffer)
	err := enc.Encode(&m)
	if err != nil {
		log.Fatal("MessageToCoordinator encoding error: ", err)
	}
	return encodeBuffer.Bytes()
}

func (m *MessageToCoordinator) FromBytes(data []byte) {
	var decodeBuffer1 bytes.Buffer
	decodeBuffer1.Write(data)
	dec := gob.NewDecoder(&decodeBuffer1)
	err := dec.Decode(m)
	if err != nil {
		log.Fatal("MessageToCoordinator Decode error: ", err)
	}
}

type SearchType int

const (
	BalancedGoSearch SearchType = iota
	LogKSearch
)

// CoordinatorToWorker is used by the coordinator to “speak” to the worker
type CoordinatorToWorker struct {
	Mtype   MessageType
	Init    Init
	Workers []string
	Search  SearchType
	Comp    Comp // use the local comp definition
	Gen     []decomp.Generator
}

func (m CoordinatorToWorker) ToBytes() []byte {
	// gob.Register(decomp.CombinationIterator{})
	// var gen decomp.Generator
	// gen =
	gob.Register(decomp.Generator(&decomp.CombinationIterator{}))
	var encodeBuffer bytes.Buffer
	enc := gob.NewEncoder(&encodeBuffer)
	err := enc.Encode(&m)
	if err != nil {
		log.Fatal("CoordinatorToWorker encoding error: ", err)
	}
	return encodeBuffer.Bytes()
}

func (m *CoordinatorToWorker) FromBytes(data []byte) {
	// gob.Register(decomp.CombinationIterator{})
	// var gen decomp.Generator
	// gen = &decomp.CombinationIterator{}
	gob.Register(decomp.Generator(&decomp.CombinationIterator{}))
	var encodeBuffer bytes.Buffer
	encodeBuffer.Write(data)
	dec := gob.NewDecoder(&encodeBuffer)
	err := dec.Decode(m)
	if err != nil {
		log.Fatal("CoordinatorToWorker Decode error: ", err)
	}
}

// CoordinatorToCTD is used by the coordinator and worker to “speak” to the CTDCheck process
type MessageToCTD struct {
	Mtype  MessageType
	Init   Init
	Blocks []Block
}

func (m MessageToCTD) ToBytes() []byte {
	gob.Register(decomp.Generator(&decomp.CombinationIterator{}))
	var encodeBuffer bytes.Buffer
	enc := gob.NewEncoder(&encodeBuffer)
	err := enc.Encode(&m)
	if err != nil {
		log.Fatal("MessageToCTD encoding error: ", err)
	}
	return encodeBuffer.Bytes()
}

func (m *MessageToCTD) FromBytes(data []byte) {
	gob.Register(decomp.Generator(&decomp.CombinationIterator{}))
	var decodeBuffer bytes.Buffer
	decodeBuffer.Write(data)
	dec := gob.NewDecoder(&decodeBuffer)
	err := dec.Decode(m)
	if err != nil {
		log.Fatal("MessageToCTD Decode error: ", err)
	}
}

// // WorkerToCTD is used by worker to send blocks to CTD
// type WorkerToCTD struct {
//  Mtype  MessageType
//  Blocks []Block
// }

// func (m WorkerToCTD) ToBytes() []byte {
//  encodeBuffer.Reset()
//  enc := gob.NewEncoder(&encodeBuffer)
//  err := enc.Encode(&m)
//  if err != nil {
//      log.Fatal("encoding error: ", err)
//  }
//  return encodeBuffer.Bytes()
// }

// func (m *WorkerToCTD) FromBytes(data []byte) {
//  decodeBuffer.Reset()
//  decodeBuffer.Write(data)
//  dec := gob.NewDecoder(&decodeBuffer)
//  err := dec.Decode(m)
//  if err != nil {
//      log.Fatal("Decode error: ", err)
//  }
// }

// data structures for blocks and the CTD Check

// A Block encodes the basic building blocks of a hypergraph decomposition
type Block struct {
	Sep       decomp.Edges
	Comp      decomp.Edges
	TreeComp  decomp.Edges
	HeadIndex []int   // Head intersected with oldSep (Conn, essentially)
	Head      []int   // vertices of the separator
	Tail      []int   // the vertex-component view on the component
	hash      *uint64 // pointer to already computed hash
	Context   decomp.Edges
}

func (b Block) String() string {
	out := "( " + decomp.PrintVertices(b.Head) + ", " + b.Comp.String() + " ) Conn:" + decomp.PrintVertices(b.HeadIndex)

	out = out + fmt.Sprintln("\n", "sep: ", b.Sep, "Tail: ", decomp.PrintVertices(b.Tail), "Context: ", b.Context)

	// out = out + fmt.Sprintln("treeComp", b.treeComp)

	out = out + fmt.Sprintln("headHash", decomp.IntHash(b.Head))
	out = out + fmt.Sprintln("Edges Hashes: ")
	for i := range b.Comp.Slice() {
		out = out + fmt.Sprint("Edge ", decomp.PrintVertices([]int{b.Comp.Slice()[i].Name}), " ", b.Comp.Slice()[i].Hash())
	}

	return out
}

// CreateBlock sets  up a new block based on a separator and corresponding component, both of type Edges
func CreateBlock(sep decomp.Edges, comp decomp.Edges, treeComp decomp.Edges, context decomp.Edges, conn []int) Block {
	var output Block

	output.Sep, output.Comp, output.TreeComp = sep, comp, treeComp
	output.Head = decomp.Inter(sep.Vertices(), context.Vertices())
	output.HeadIndex = decomp.Inter(output.Head, conn)
	output.Tail = decomp.Diff(comp.Vertices(), output.Head)
	output.Context = context

	return output
}

func (b *Block) Hash() uint64 {
	if b.hash != nil {
		return *b.hash
	}
	var output uint64

	hashIndex := decomp.IntHash(b.HeadIndex)
	hashHead := decomp.IntHash(b.Head)
	hashTail := decomp.IntHash(b.Tail)

	output = output ^ uint64(hashIndex)*7
	output = output ^ uint64(hashHead)*13
	output = output ^ uint64(hashTail)*23

	b.hash = &output // cash the computed hash

	return output
}

// IsSubsetFast skips the check for the tail (to be used in conjunction with a form of CTDCheck)
func (b Block) IsSubsetFast(c Block) bool {
	// if !Subset(b.Tail, c.Tail) {
	//  return false
	// }

	if !Subset(b.Head, append(c.Head, c.Tail...)) {
		return false
	}

	// if !decomp.Subset(b.Tail, c.Tail) {
	//  return false
	// }

	// if !decomp.Subset(b.Head, append(c.Head, c.Tail...)) {
	//  return false
	// }

	// if !decomp.Subset(b.comp.Vertices(), c.comp.Vertices()) {
	//  return false
	// }
	return true
}

func (b Block) IsSubset(c Block) bool {
	if !Subset(b.Tail, c.Tail) {
		return false
	}

	if !Subset(b.Head, append(c.Head, c.Tail...)) {
		return false
	}

	// if !decomp.Subset(b.Tail, c.Tail) {
	//  return false
	// }

	// if !decomp.Subset(b.Head, append(c.Head, c.Tail...)) {
	//  return false
	// }

	// if !decomp.Subset(b.comp.Vertices(), c.comp.Vertices()) {
	//  return false
	// }
	return true
}

// A Node maps to a Block and saves info about the node in the overall search structure
type Node struct {
	Block          Block // the block this nodes corresponds to
	Satisfied      bool  // mark if this node is satisfied in the search structure
	CachedChildren uint32
	EdgesToCover   map[uint64]struct{}        // to easily see which edges are covered
	BasisCache     map[uint32]map[uint64]bool // a cache indicating which edges (by their hash) have been covered
}

func CreateNode(block Block) Node {
	var output Node

	output.Block = block

	// already set trivial blocks to be satisfied
	if len(output.Block.Tail) == 0 {
		output.Satisfied = true
	}

	// set up the map used for the cache
	output.BasisCache = make(map[uint32]map[uint64]bool)

	// set up the map used to check which edges are in comp
	output.EdgesToCover = make(map[uint64]struct{})

	for i := range block.Comp.Slice() {
		output.EdgesToCover[block.Comp.Slice()[i].Hash()] = decomp.Empty
	}

	return output
}
