package lib

import (
	"fmt"
	"log"
	"reflect"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
)

// BitSetCTDCheck uses bitset based set comparisons to speed up the CTDCheck, compared to both the other set-based CTD Check and the naive approach
type BitSetCTDCheck struct {
	Nodes        []Node // the slice of all nodes in the search structure
	Hashes       map[uint64]struct{}
	MaxSatisfied int

	setCompCheck SetCompBitSet
}

func (s BitSetCTDCheck) String() string {
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

func CreateBitSetCheck(input decomp.Graph) BitSetCTDCheck {
	var output BitSetCTDCheck

	output.setCompCheck = SetupSetCompBitSet(input.Vertices()) // the vertices of input graph act as the universe

	// Add (\emptyset, H) as the root block
	emptySep := decomp.NewEdges([]decomp.Edge{})
	rootBlock := CreateBlock(emptySep, input.Edges, input.Edges, input.Edges, input.Vertices())
	rootNode := CreateNode(rootBlock)
	output.AddNode(rootNode)

	return output
}

func (s *BitSetCTDCheck) GetDecomp(graph decomp.Graph) (bool, decomp.Decomp) {
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

func (s *BitSetCTDCheck) IsSatisfied() bool {
	return s.Nodes[0].Satisfied // search structure is satisfied, if the trivial node is satisfied
}

func (s *BitSetCTDCheck) GetBlocksByHead(head uint32, other Block) []*Node {
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
func (s *BitSetCTDCheck) CheckBasis(node int) bool {
	if s.Nodes[node].Satisfied {
		return false // nothing to do if node already satisfied
	}

	var output bool

	thisNode := &s.Nodes[node]
	subsetNodes := s.setCompCheck.GetSubSets(thisNode.Block.Tail)
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

		if !nodeToCheck.Satisfied || !nodeToCheck.Block.IsSubsetFast(thisNode.Block) {
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
		supersetNodes := s.setCompCheck.GetSuperSets(thisNode.Block.Tail)

		for _, index := range supersetNodes {
			if s.Nodes[0].Satisfied {
				break // stop if root is satisfied
			}

			nodeToCheck := s.Nodes[index]

			if thisNode.Block.IsSubsetFast(nodeToCheck.Block) {
				s.CheckBasis(index)
			}

		}
	}

	return output
}

// AddNode computes the correct position where this node should be placed in the search structur, based on the intersect relation of its head.
func (s *BitSetCTDCheck) AddNode(node Node) {
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
	supersetNodes := s.setCompCheck.GetSuperSets(node.Block.Tail)
	subsetNodes := s.setCompCheck.GetSubSets(node.Block.Tail)

	// var actualNodeIndices []int

	// add the newly added node to the two indices
	// fmt.Println("Adding node ", node.Block.Tail, " to pos", pos)
	s.setCompCheck.AddSet(pos, node.Block.Tail)
	// s.setCompCheck.AddSet(pos, node.Block.Tail)

	// check potential nodes which are supersets of new node

	// fmt.Println("Node (", decomp.PrintVertices(node.Block.Head), ",", decomp.PrintVertices(node.Block.Tail), ") has  following potential supersets", supersetNodes)
	for _, index := range supersetNodes {
		if pos == index {
			continue
		}

		nodeToCheck := s.Nodes[index]

		// res := node.Block.IsSubset(nodeToCheck.Block)
		// fmt.Println("Node (", decomp.PrintVertices(node.Block.Head), ",", decomp.PrintVertices(node.Block.Tail), ") subset of node index ", index, ": ", res)
		if node.Block.IsSubsetFast(nodeToCheck.Block) {
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

		if nodeToCheck.Block.IsSubsetFast(node.Block) {
			s.CheckBasis(pos)
		}
	}
}

func (s *BitSetCTDCheck) ExtractDecomp(n *Node) decomp.Node {
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
