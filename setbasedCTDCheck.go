package lib

import (
	"fmt"
	"log"
	"reflect"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
)

/* TODOs

* think about bringing back the caching concept from the Arcs and CoaArcs fields
	- 	this is kinda hard since the new setbased checks are dynamic, they may change, thus the caching
		is only of limited use and needs a mechanism to rerun the set-based checks if needed
	-	realistically, you'd probably want to integrate this into the check struct, as it's the only
		place where the info for what has changed is located

*/

// SetBasedCTDCheck uses set inclusion checks to speed up the CTDCheck compared to the naive exhaustive approach
type SetBasedCTDCheck struct {
	Nodes []Node // the slice of all nodes in the search structure
	// Arcs   map[int][]int // maps indices in Nodes to other indices
	// CoArcs map[int][]int // reverse mapping of Arcs
	// nodeCheck lib.IntersectCheck //used to quickly find relevant nodes when adding new node
	Hashes       map[uint64]struct{}
	MaxSatisfied int

	subsetCheck   SubsetCheck   // used to check which blocks are subsets of other blocks
	supersetCheck SupersetCheck // used to chek which blocks are supsersets of other blocks
}

func (s SetBasedCTDCheck) String() string {
	var output string

	for i, v := range s.Nodes {
		output = output + fmt.Sprintln("[", i, "] ", v.Block, " Satisfied: ", v.Satisfied)
	}

	output = output + fmt.Sprintln("cachedChildren: ")
	for i := range s.Nodes {
		output = output + fmt.Sprintln("[", i, "] {")
		for _, c := range s.GetBlocksByHead(s.Nodes[i].CachedChildren, s.Nodes[i].Block) {
			output = output + fmt.Sprint("*", c.Block)
		}
		output = output + fmt.Sprintln("}")
	}

	return output
}

func CreateSetBasedCheck(input decomp.Graph) SetBasedCTDCheck {
	var output SetBasedCTDCheck

	// output.Arcs = make(map[int][]int)
	// output.CoArcs = map[int][]int{}
	// output.nodeCheck = lib.SetupIntersectCheck()
	output.subsetCheck = SetupSubsetCheck(input.Vertices()) // the vertices of input graph act as the universe
	output.supersetCheck = SetupSupersetCheck()

	// Add (\emptyset, H) as the root block
	emptySep := decomp.NewEdges([]decomp.Edge{})
	rootBlock := CreateBlock(emptySep, input.Edges, input.Edges, input.Edges, input.Vertices())
	rootNode := CreateNode(rootBlock)
	output.AddNode(rootNode)

	return output
}

func (s *SetBasedCTDCheck) GetDecomp(graph decomp.Graph) (bool, decomp.Decomp) {
	if !s.IsSatisfied() { // nothing to return if root not yet satisfied
		return false, decomp.Decomp{}
	}

	output := decomp.Decomp{
		Graph: graph,
		Root:  s.ExtractDecomp(&s.Nodes[0]),
	}

	if !output.Correct(graph) {
		fmt.Println("Decomp: ", output)
		log.Panicln("search satisfied, but produced no correct decomp")
	}

	return true, output
}

func (s *SetBasedCTDCheck) IsSatisfied() bool {
	return s.Nodes[0].Satisfied // search structure is satisfied, if the trivial node is satisfied
}

func (s *SetBasedCTDCheck) GetBlocksByHead(head uint32, other Block) []*Node {
	var output []*Node

	for i := range s.Nodes {
		if decomp.IntHash(s.Nodes[i].Block.Head) == head && s.Nodes[i].Satisfied && s.Nodes[i].Block.IsSubset(other) {
			output = append(output, &s.Nodes[i])
		}
	}

	return output
}

// CheckBasis implements the definition of basis given under [Gottlob et al., JACM 2021]. This check also uses
// the data in the basisCache field to speed repeat applications on the same node position
func (s *SetBasedCTDCheck) CheckBasis(node int) bool {
	if s.Nodes[node].Satisfied {
		return false // nothing to do if node already satisfied
	}

	var output bool

	thisNode := &s.Nodes[node]
	subsetNodes := s.subsetCheck.GetSubSets(thisNode.Block.Tail)
	// subsetNodes2 := s.subsetCheck.spCheck.GetSuperSets([]int{})

	// children := o.Arcs[node]

	// fmt.Println("Node (", decomp.PrintVertices(thisNode.Block.Head), ",", decomp.PrintVertices(thisNode.Block.Tail), ") has  following potential subset", subsetNodes, "in Basis Check")
	// fmt.Println(decomp.PrintVertices(s.subsetCheck.Universe), " , ", decomp.PrintVertices(thisNode.Block.Tail), ", ", subsetNodes2)
	// fmt.Println("All Sets ", s.subsetCheck.spCheck.allSets)
	for _, index := range subsetNodes {
		if index == node {
			// fmt.Println("Set based skipping since index", index, " same as current ndoe ", node)
			continue
		}

		nodeToCheck := s.Nodes[index]

		if !nodeToCheck.Block.IsSubset(thisNode.Block) || !nodeToCheck.Satisfied {
			// fmt.Println("Set based skipping not subset or not satisfied")
			continue // only consider satisfied nodes
		}

		sep := nodeToCheck.Block.Head
		sepHash := decomp.IntHash(sep)
		for i := range nodeToCheck.Block.TreeComp.Slice() {
			edgeHash := nodeToCheck.Block.TreeComp.Slice()[i].Hash()
			_, ok := thisNode.EdgesToCover[edgeHash]

			if ok {
				_, ok2 := thisNode.BasisCache[sepHash]

				if !ok2 {
					thisNode.BasisCache[sepHash] = make(map[uint64]bool)
				}

				thisNode.BasisCache[sepHash][edgeHash] = true
			}
		}

		if node == 0 && len(thisNode.BasisCache[sepHash]) > s.MaxSatisfied {
			// fmt.Println("Root check! SetBase")
			// fmt.Println("satisfied: ", len(thisNode.BasisCache[sepHash]), "visited: ", len(s.Nodes))
			s.MaxSatisfied = len(thisNode.BasisCache[sepHash])
		}

	}

	// check if there exists some head, s.t. all edges of this node are covered by its blocks
	for k, v := range thisNode.BasisCache {

		numberEdgesContained := len(v)
		if numberEdgesContained == len(thisNode.EdgesToCover) {
			thisNode.Satisfied = true
			thisNode.CachedChildren = k

			output = true
			// if node == 0 {
			// 	fmt.Println("Root satisfied!")
			// }
			break
		}

	}

	// Satisfaction Propagation
	if thisNode.Satisfied {
		supersetNodes := s.supersetCheck.GetSuperSets(thisNode.Block.Tail)

		for _, index := range supersetNodes {
			if s.Nodes[0].Satisfied {
				break // stop if root is satisfied
			}

			nodeToCheck := s.Nodes[index]

			if thisNode.Block.IsSubset(nodeToCheck.Block) {
				s.CheckBasis(index)
			}

		}
	}

	// TODO: test if this actually provides a speed up
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
func (s *SetBasedCTDCheck) AddNode(node Node) {
	if s.Hashes == nil {
		s.Hashes = make(map[uint64]struct{})
	}

	_, ok := s.Hashes[node.Block.Hash()]

	if ok {
		// fmt.Println("Already seen block", node.Block)
		return // don't add duplicate component
	}

	s.Hashes[node.Block.Hash()] = decomp.Empty // index the hash of this component

	// add the node to Nodes, determining its index
	pos := len(s.Nodes)
	s.Nodes = append(s.Nodes, node)

	// use the tail vertices to approximate potential blocks for basis check down the line
	supersetNodes := s.supersetCheck.GetSuperSets(node.Block.Tail)
	subsetNodes := s.subsetCheck.GetSubSets(node.Block.Tail)

	// var actualNodeIndices []int

	// add the newly added node to the two indices
	// fmt.Println("Adding node ", node.Block.Tail, " to pos", pos)
	s.supersetCheck.AddSet(pos, node.Block.Tail)
	s.subsetCheck.AddSet(pos, node.Block.Tail)

	// check potential nodes which are supersets of new node

	// fmt.Println("Node (", decomp.PrintVertices(node.Block.Head), ",", decomp.PrintVertices(node.Block.Tail), ") has  following potential supersets", supersetNodes)
	for _, index := range supersetNodes {
		if pos == index {
			continue
		}

		nodeToCheck := s.Nodes[index]

		// res := node.Block.IsSubset(nodeToCheck.Block)
		// fmt.Println("Node (", decomp.PrintVertices(node.Block.Head), ",", decomp.PrintVertices(node.Block.Tail), ") subset of node index ", index, ": ", res)
		if node.Block.IsSubset(nodeToCheck.Block) {
			s.CheckBasis(index)
			// fmt.Println("result of basis check: ", res2)
		}
	}

	// fmt.Println("Node (", decomp.PrintVertices(node.Block.Head), ",", decomp.PrintVertices(node.Block.Tail), ") has  following potential subset", subsetNodes)
	for _, index := range subsetNodes {
		if pos == index {
			continue
		}
		nodeToCheck := s.Nodes[index]

		if nodeToCheck.Block.IsSubset(node.Block) {
			s.CheckBasis(pos)
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

func (s *SetBasedCTDCheck) ExtractDecomp(n *Node) decomp.Node {
	var output decomp.Node

	children := s.GetBlocksByHead(n.CachedChildren, n.Block)

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

		temp := s.ExtractDecomp(children[i])
		if reflect.DeepEqual(decomp.Node{}, temp) {
			fmt.Println("Node in question:", children[i])
			log.Panicln("node satisfied, but one of the children has no decomp")
		}
		output.Children = append(output.Children, temp)
	}

	return output
}
