package lib

// TestSetBasedCTDChek is meant to test the basic functionlity of the eponymous method
// in finding decompositions by first producing a GHD and then seeing if it can be reconstructed from its blocks.
// func TestExhaustiveBasedCTDChek(t *testing.T) {

// 	// 1. Generate a random hypergraph

// 	// s := rand.NewSource(time.Now().UnixNano())grch
// 	// r := rand.New(s)

// 	graphInitial, _ := getRandomGraph(30)

// 	fmt.Println("The random graph, \n", graphInitial.Edges.FullString())

// 	// 2. Get a GHD of this hypergraph

// 	width := 1

// 	det := &algo.DetKDecomp{
// 		K:         width,
// 		Graph:     graphInitial,
// 		BalFactor: 2,
// 		SubEdge:   true,
// 	}

// 	det.SetGenerator(lib.ParallelSearchGen{})

// 	var decomp lib.Decomp

// 	solved := false
// 	k := 1
// 	for ; !solved; k++ {
// 		det.SetWidth(k)
// 		decomp = det.FindDecomp()
// 		solved = decomp.Correct(graphInitial)
// 	}

// 	fmt.Println("\nThe found decomp \n", decomp)

// 	// 3. Generate blocks from the nodes of this GHD

// 	var currentNodes []lib.Node
// 	var blockList []Block

// 	// initialise BDF traversal of decomp nodes
// 	currentNodes = append(currentNodes, decomp.Root)

// 	for k := 0; len(currentNodes) > k; k++ {
// 		tempNode := currentNodes[k]

// 		// fmt.Println("working with node ", tempNode)

// 		head := tempNode.Bag
// 		sep := lib.CutEdges(tempNode.Cover, head)

// 		globalComps, _, isolated := graphInitial.GetComponents(sep)

// 		// fmt.Println("Generated components ", globalComps)

// 		if len(isolated) != 0 {
// 			conn := sep.Vertices()

// 			isolatedEdges := lib.NewEdges(isolated)

// 			out := CreateBlock(sep, isolatedEdges, isolatedEdges, isolatedEdges, conn)
// 			// fmt.Println("Generated block", out)
// 			blockList = append(blockList, out)
// 		}

// 		if len(globalComps) == 0 {
// 			conn := sep.Vertices()

// 			out := CreateBlock(sep, graphInitial.Edges, graphInitial.Edges, graphInitial.Edges, conn)
// 			// fmt.Println("Generated block", out)
// 			blockList = append(blockList, out)
// 		} else {
// 			for i := range globalComps {
// 				conn := lib.Inter(globalComps[i].Vertices(), sep.Vertices())
// 				treeComp := lib.NewEdges(append(globalComps[i].Edges.Slice(), isolated...))

// 				out := CreateBlock(sep, globalComps[i].Edges, treeComp, treeComp, conn)
// 				// fmt.Println("Generated block", out)
// 				blockList = append(blockList, out)

// 			}
// 		}

// 		currentNodes = append(currentNodes, tempNode.Children...) // append the children of current node to end of list

// 	}

// 	fmt.Println("The blocks generated, ", len(blockList))

// 	for i := range blockList {
// 		block := blockList[i]
// 		fmt.Println(block)
// 	}

// 	// 4. Try to find another (or the same) GHD from blocks using SetBasedCheck

// 	check := CreateCTDSearch(graphInitial)

// 	for i := range blockList {
// 		tempNode := CreateNode(blockList[i])
// 		check.AddNode(tempNode)
// 	}

// 	if !check.IsSatisfied() {
// 		t.Errorf("Couldn't find any decomp")
// 	}

// }
