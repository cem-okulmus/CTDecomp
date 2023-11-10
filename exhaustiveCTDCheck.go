package lib

import (
	"fmt"
	"log"
	"reflect"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
)

type ExhaustiveCTDSearch struct {
	Nodes []Node // the slice of all nodes in the search structure
	// Arcs   map[int][]int // maps indices in Nodes to other indices
	// CoArcs map[int][]int // reverse mapping of Arcs
	// nodeCheck lib.IntersectCheck //used to quickly find relevant nodes when adding new node
	Hashes       map[uint64]struct{}
	MaxSatisfied int
}

func (o ExhaustiveCTDSearch) String() string {
	var output string

	for i, v := range o.Nodes {
		output = output + fmt.Sprintln("[", i, "] ", v.Block, " Satisfied: ", v.Satisfied)
	}

	output = output + fmt.Sprintln("cachedChildren: ")
	for i := range o.Nodes {
		output = output + fmt.Sprintln("[", i, "] {")
		for _, c := range o.GetBlocksByHead(o.Nodes[i].CachedChildren, o.Nodes[i].Block) {
			output = output + fmt.Sprint("*", c.Block)
		}
		output = output + fmt.Sprintln("}")
	}

	return output
}

func CreateCTDSearch(input decomp.Graph) ExhaustiveCTDSearch {
	var output ExhaustiveCTDSearch

	// output.Arcs = make(map[int][]int)
	// output.CoArcs = map[int][]int{}
	// output.nodeCheck = lib.SetupIntersectCheck()

	// Add (\emptyset, H) as the root block
	emptySep := decomp.NewEdges([]decomp.Edge{})
	rootBlock := CreateBlock(emptySep, input.Edges, input.Edges, input.Edges, input.Vertices())
	rootNode := CreateNode(rootBlock)
	output.AddNode(rootNode)

	return output
}

func (o *ExhaustiveCTDSearch) IsSatisfied() bool {
	return o.Nodes[0].Satisfied // search structure is satisfied, if the trivial node is satisfied
}

func (o *ExhaustiveCTDSearch) GetBlocksByHead(head uint32, other Block) []*Node {
	var output []*Node

	for i := range o.Nodes {
		if decomp.IntHash(o.Nodes[i].Block.Head) == head && o.Nodes[i].Satisfied && o.Nodes[i].Block.IsSubset(other) {
			output = append(output, &o.Nodes[i])
		}
	}

	return output
}

func (o *ExhaustiveCTDSearch) GetDecomp(graph decomp.Graph) (bool, decomp.Decomp) {
	if !o.IsSatisfied() { // nothing to return if root not yet satisfied
		return false, decomp.Decomp{}
	}

	output := decomp.Decomp{
		Graph: graph,
		Root:  o.ExtractDecomp(&o.Nodes[0]),
	}

	if !output.Correct(graph) {
		fmt.Println("Decomp: ", output)
		log.Panicln("search satisfied, but produced no correct decomp")
	}

	return true, output
}

// CheckBasis implements the definition of basis given under [Gottlob et al., JACM 2021]. This check also uses
// the data in the basisCache field to speed repeat applications on the same node position
func (o *ExhaustiveCTDSearch) CheckBasis(node int) bool {
	if o.Nodes[node].Satisfied {
		return false // nothing to do if node already satisfied
	}

	var output bool

	thisNode := &o.Nodes[node]
	// children := o.Arcs[node]

	for i := range o.Nodes {
		childNode := o.Nodes[i]

		if !childNode.Block.IsSubset(thisNode.Block) || !childNode.Satisfied {
			continue // only consider satisfied nodes   TODO: maybe implement an eager check of basis?
		}

		sep := childNode.Block.Head
		sepHash := decomp.IntHash(sep)
		for i := range childNode.Block.TreeComp.Slice() {
			edgeHash := childNode.Block.TreeComp.Slice()[i].Hash()
			_, ok := thisNode.EdgesToCover[edgeHash]

			if ok {
				_, ok2 := thisNode.BasisCache[sepHash]

				if !ok2 {
					thisNode.BasisCache[sepHash] = make(map[uint64]bool)
				}

				thisNode.BasisCache[sepHash][edgeHash] = true
			}
		}

		if node == 0 && len(thisNode.BasisCache[sepHash]) > o.MaxSatisfied {
			// fmt.Println("Root check! Exhaustive")
			// fmt.Println("satisfied: ", len(thisNode.BasisCache[sepHash]), "visited: ", len(o.Nodes))
			o.MaxSatisfied = len(thisNode.BasisCache[sepHash])
		}
	}

	// check if there exists some head, s.t. all edges of this node are covered by its blocks
	for k, v := range thisNode.BasisCache {

		lenEdges := len(thisNode.EdgesToCover)

		numberEdgesContained := len(v)
		if numberEdgesContained == lenEdges {
			thisNode.Satisfied = true
			thisNode.CachedChildren = k

			output = true
			// if node == 0 {
			// 	fmt.Println("Root satisfied!")
			// }
			break
		}

	}

	// // if this node is satisfied
	// if thisNode.Satisfied {
	// 	for _, index := range o.CoArcs[node] {
	// 		if o.Nodes[0].Satisfied {
	// 			break // stop satisfaction propagation if root satisfied
	// 		}
	// 		o.CheckBasis(index)
	// 	}
	// }

	return output
}

// AddNode computes the correct position where this node should be placed in the search structur, based on the intersect relation of its head.
func (o *ExhaustiveCTDSearch) AddNode(node Node) {
	if o.Hashes == nil {
		o.Hashes = make(map[uint64]struct{})
	}

	_, ok := o.Hashes[node.Block.Hash()]

	if ok {
		// fmt.Println("Already seen block", node.Block)
		return // don't add duplicate component
	}
	o.Hashes[node.Block.Hash()] = decomp.Empty // index the hash of this component

	// add the node to Nodes, determining its index
	pos := len(o.Nodes)
	o.Nodes = append(o.Nodes, node)

	// use the intersect check to determine which arcs to add

	// potentialNodeIndices := o.nodeCheck.GetIntersections(node.Block.Head)

	// var actualNodeIndices []int

	changed := true

	// outer:
	for len(o.Nodes) > 1 && changed {
	inner:
		for index := range o.Nodes {
			if index == pos {
				continue inner
			}

			nodeToCheck := o.Nodes[index]

			if node.Block.IsSubset(nodeToCheck.Block) {
				changed = o.CheckBasis(index)
				// if changed {
				// 	continue outer
				// }
				// actualNodeIndices = append(actualNodeIndices, index)

				// val, ok := o.Arcs[index]

				// if !ok {
				// 	o.Arcs[index] = []int{pos}
				// } else {
				// 	val = append(val, pos)
				// 	o.Arcs[index] = val
				// }
			} else if nodeToCheck.Block.IsSubset(node.Block) {
				changed = o.CheckBasis(pos)
				// if changed {
				// 	continue outer
				// }
				// val, ok := o.Arcs[pos]

				// if !ok {
				// 	o.Arcs[pos] = []int{index}
				// } else {
				// 	val = append(val, index)
				// 	o.Arcs[pos] = val
				// }
			}
		}
	}

	// o.CoArcs[pos] = actualNodeIndices

	// arc to root
	// if pos != 0 && len(o.CoArcs[pos]) == 0 {
	// 	val, ok := o.Arcs[0]

	// 	if !ok {
	// 		o.Arcs[0] = []int{pos}
	// 	} else {
	// 		val = append(val, pos)
	// 		o.Arcs[0] = val
	// 	}

	// 	o.CoArcs[pos] = []int{0}
	// }

	// finally, we add the Conn of the block of the new node to the intersect check
	// o.nodeCheck.AddSet(pos, node.Block.HeadIndex)

	// if trivial node was added, then peform the basis check on its ancestors
	// if node.Satisfied {
	// 	for _, index := range actualNodeIndices {
	// 		if o.Nodes[0].Satisfied {
	// 			break // stop satisfaction propagation if root satisfied
	// 		}
	// 		o.CheckBasis(index)
	// 	}
	// }
}

func (o *ExhaustiveCTDSearch) ExtractDecomp(n *Node) decomp.Node {
	var output decomp.Node

	children := o.GetBlocksByHead(n.CachedChildren, n.Block)

	if !n.Satisfied {
		return decomp.Node{}
	}

	if len(children) == 0 && len(n.Block.Tail) != 0 {
		log.Panicln("non-trivial Node satisfied, by no children")
	}

	if len(children) > 0 {
		output.Bag = children[0].Block.Head
		output.Cover = children[0].Block.Sep

	} else {
		output.Bag = n.Block.Head
		output.Cover = n.Block.Sep
	}

	for i := range children {
		if len(children[i].Block.Tail) == 0 {
			continue // no need to add nodes for trivial blocks
		}

		temp := o.ExtractDecomp(children[i])
		if reflect.DeepEqual(decomp.Node{}, temp) {
			fmt.Println("Node in question:", children[i])
			log.Panicln("node satisfied, but one of the children has no decomp")
		}
		output.Children = append(output.Children, temp)
	}

	return output
}
