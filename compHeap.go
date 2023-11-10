package lib

import (
	"container/heap"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
)

// TODO: introduce extended comp data structure with allowedEdges,comp, and Conn, and make that the input object of this heap

type Comp struct {
	Edges   decomp.Graph
	Conn    []int
	Allowed decomp.Edges
}

func (c Comp) Len() int {
	return c.Edges.Len()
}

func (c Comp) Hash() uint64 {
	return c.Edges.Hash()
}

// A CompHeap (short for component heap) is a priority queue which allows
// to quickly find the smallest component among a given input set
type CompHeap struct {
	heap   []Comp
	hashes map[uint64]struct{}
}

var Empty struct{}

// Sort interfaces
func (c CompHeap) Len() int { return len(c.heap) }

func (c CompHeap) Less(i, j int) bool {
	// return len(c.heap[i].Vertices()) < len(c.heap[j].Vertices())
	return c.heap[i].Len() < c.heap[j].Len()
	// return false
}
func (c CompHeap) Swap(i, j int) { c.heap[i], c.heap[j] = c.heap[j], c.heap[i] }

// heap interfaces
func (c *CompHeap) Push(x interface{}) {
	c.heap = append(c.heap, x.(Comp))
}

func (c *CompHeap) Pop() interface{} {
	old := c.heap
	n := len(old)
	x := old[n-1]
	c.heap = old[0 : n-1]
	return x
}

func (c *CompHeap) Add(comp Comp) {
	if c.hashes == nil {
		c.hashes = make(map[uint64]struct{})
	}

	_, ok := c.hashes[comp.Hash()]

	if ok {
		// fmt.Println("not adding", comp, " to heap as it already in the heap")
		return // don't add duplicate component
	}
	c.hashes[comp.Hash()] = Empty // index the hash of this component

	heap.Init(c)
	heap.Push(c, comp)
}

func (c *CompHeap) GetNext() Comp {
	out := heap.Pop(c)

	return out.(Comp)
}

func (c CompHeap) HasNext() bool {
	return len(c.heap) > 0
}
