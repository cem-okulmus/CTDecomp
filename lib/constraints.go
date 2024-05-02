package lib

import (
	"log"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
	"github.com/cem-okulmus/disjoint"
)

type Constraint interface {
	Check(graph decomp.Graph, node *decomp.Node, decomp *decomp.Decomp) bool
}

// A BagConstraint defines a function that takes as input a hypergraph and a vertex set
// and either rejects or accepts it
type BagConstraint interface {
	Check(graph decomp.Graph, node *decomp.Node, decomp *decomp.Decomp) bool
	CheckBag(graph decomp.Graph, bag []int) bool
}

// Check whether the bag is a connected set in the given graph
type ConnectedBagConstraint struct{}

func (c ConnectedBagConstraint) Check(graph decomp.Graph, node *decomp.Node, decomp *decomp.Decomp) bool {
	if decomp != nil || node == nil {
		log.Panicln("Calling a ConnectedBagConstraint in illegal way (no node or with decomp)")
	}
	return c.CheckBag(graph, node.Bag)
}

func (c ConnectedBagConstraint) CheckBag(graph decomp.Graph, bag []int) bool {
	tempMap := make(map[int]*disjoint.Element)

	// construct the induced subhypergraph
	var newEdges []decomp.Edge

	for _, oldEdge := range graph.Edges.Slice() {
		newBag := decomp.Inter(oldEdge.Vertices, bag)
		newEdges = append(newEdges, decomp.Edge{Vertices: newBag})
	}
	induc := decomp.Graph{Edges: decomp.NewEdges(newEdges)}

	comps, _, _ := induc.GetComponents(graph.Edges, tempMap)

	return len(comps) == 1
}

type TopConstraint struct{}

func (t TopConstraint) Check(graph decomp.Graph, node *decomp.Node, decomp *decomp.Decomp) bool {
	return true
}

// A CoverConstraint limits the allowed edge covers in a decomposition
type CoverConstraint interface {
	Check(graph decomp.Graph, node *decomp.Node, decomp *decomp.Decomp) bool
	CheckCover(graph decomp.Graph, cover decomp.Edges) bool
}

type ConnectedCoverConstraint struct{}

func (c ConnectedCoverConstraint) Check(graph decomp.Graph, node *decomp.Node, decomp *decomp.Decomp) bool {
	if decomp != nil || node == nil {
		log.Panicln("Calling a ConnectedBagConstraint in illegal way (no node or with decomp)")
	}
	return c.CheckCover(graph, node.Cover)
}

// Check whether the cover is a connected
func (c ConnectedCoverConstraint) CheckCover(graph decomp.Graph, cover decomp.Edges) bool {
	tempMap := make(map[int]*disjoint.Element)
	tmpGraph := decomp.Graph{Edges: cover}

	comps, _, _ := tmpGraph.GetComponents(cover, tempMap)

	return len(comps) == 1
}
