package lib

import (
	"fmt"
	"testing"

	algo "github.com/cem-okulmus/BalancedGo/algorithms"
	"github.com/cem-okulmus/BalancedGo/lib"
	"github.com/cem-okulmus/disjoint"
)

func TestBitSetCTDCheck(t *testing.T) {
	// 1. Generate a random hypergraph

	// s := rand.NewSource(time.Now().UnixNano())
	// r := rand.New(s)

	graphInitial, _ := getRandomGraph(10)

	// fmt.Println("The random graph, \n", graphInitial.Edges.FullString())

	// 2. Get a GHD of this hypergraph

	width := 1

	det := &algo.DetKDecomp{
		K:         width,
		Graph:     graphInitial,
		BalFactor: 2,
		SubEdge:   true,
	}

	det.SetGenerator(lib.ParallelSearchGen{})

	var decomp lib.Decomp

	solved := false
	k := 1
	for ; !solved; k++ {
		det.SetWidth(k)
		decomp = det.FindDecomp()
		solved = decomp.Correct(graphInitial)
	}

	// fmt.Println("\nThe found decomp \n", decomp)

	// 3. Generate blocks from the nodes of this GHD

	var currentNodes []lib.Node
	var blockList []Block

	// initialise BDF traversal of decomp nodes
	currentNodes = append(currentNodes, decomp.Root)

	for k := 0; len(currentNodes) > k; k++ {
		tempNode := currentNodes[k]

		head := tempNode.Bag
		sep := lib.CutEdges(tempNode.Cover, head)
		Vertices := make(map[int]*disjoint.Element)

		globalComps, _, isolated := graphInitial.GetComponents(sep, Vertices)

		// fmt.Println("Generated components ", globalComps)

		if len(isolated) != 0 && len(isolated) != graphInitial.Edges.Len() {
			conn := sep.Vertices()

			isolatedEdges := lib.NewEdges(isolated)

			out := CreateBlock(sep, isolatedEdges, isolatedEdges, isolatedEdges, conn)
			// fmt.Println("Generated block", out)
			blockList = append(blockList, out)
		}

		if len(globalComps) == 0 {
			conn := sep.Vertices()

			out := CreateBlock(sep, graphInitial.Edges, graphInitial.Edges, graphInitial.Edges, conn)
			// fmt.Println("Generated block", out)
			blockList = append(blockList, out)
		} else {
			for i := range globalComps {
				conn := lib.Inter(globalComps[i].Vertices(), sep.Vertices())
				treeComp := lib.NewEdges(append(globalComps[i].Edges.Slice(), isolated...))

				out := CreateBlock(sep, globalComps[i].Edges, treeComp, graphInitial.Edges, conn)
				// fmt.Println("Generated block", out)
				blockList = append(blockList, out)

			}
		}

		currentNodes = append(currentNodes, tempNode.Children...) // append the children of current node to end of list

	}

	// fmt.Println("The blocks generated, ", len(blockList))

	// for i := range blockList {
	// 	block := blockList[i]
	// 	fmt.Println(block)
	// }

	// 4. Try to find another (or the same) GHD from blocks using SetBasedCheck

	check := CreateBitSetCheck(graphInitial)

	checkExhaustive := CreateCTDSearch(graphInitial)

	bitSet := false
	exhaustive := false

	for i := range blockList {
		// fmt.Println("Adding block ", i)
		tempNode := CreateNode(blockList[i])
		check.AddNode(tempNode)
		checkExhaustive.AddNode(tempNode)

		if !bitSet && check.IsSatisfied() {
			// fmt.Println("BitSet Satsisfied!")

			bitSet = true
		}

		if !exhaustive && checkExhaustive.IsSatisfied() {
			// fmt.Println("Exhaustive Satsisfied!")
			// fmt.Println("\n\nContent of CheckExhaustive at end: \n", checkExhaustive)
			// out, decomp := checkExhaustive.GetDecomp(graphInitial)
			// fmt.Println("Decomp", decomp, "\n\n bool: ", out)
			exhaustive = true
		}

	}

	if !check.IsSatisfied() && checkExhaustive.IsSatisfied() {

		fmt.Println("\n\nContent of Check at end: \n", check)
		fmt.Println("\n\nContent of CheckExhaustive at end: \n", checkExhaustive)

		t.Errorf("Couldn't find any decomp")
		return
	}
	if check.IsSatisfied() && !checkExhaustive.IsSatisfied() {

		fmt.Println("\n\nContent of Check at end: \n", check)
		fmt.Println("\n\nContent of CheckExhaustive at end: \n", checkExhaustive)

		t.Errorf("Couldn't find any decomp")
		return
	}
}
