package lib

import (
	"fmt"
	"log"

	"golang.org/x/exp/maps"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
	"github.com/cem-okulmus/disjoint"
	logk "github.com/cem-okulmus/log-k-decomp/lib"
)

// TODO
// DONE: * handling of allowed edges must match logk exactly
// * introduce tuple of (comp,conn,allowedFull) <- this needs to be handled at Coord level
// * decide how to deal with inter-node level parallelism, should be supported for both types of search

// attach the two subtrees to form one -- copied from log-k to avoid otherwise needed refactoring
func attachingSubtrees(subtreeAbove decomp.Node, subtreeBelow decomp.Node, connecting decomp.Edges) decomp.Node {
	// log.Println("Two Nodes enter: ", subtreeAbove, subtreeBelow)
	// log.Println("Connecting: ", PrintVertices(connecting.Vertices))

	// finding connecting leaf in parent
	leaf := subtreeAbove.CombineNodes(subtreeBelow, connecting)

	if leaf == nil {
		fmt.Println("\n \n Connection ", decomp.PrintVertices(connecting.Vertices()))
		fmt.Println("subtreeAbove ", subtreeAbove)

		log.Panicln("subtreeAbove doesn't contain connecting node!")
	}

	return *leaf
}

func SearchBagsLogK(gen decomp.Generator, input, comp decomp.Graph, toCoord chan MessageToCoordinator, found chan MessageToCTD, workerID string, countComps int, Conn []int, allowedFull decomp.Edges, width int) {
	VerticesH := append(comp.Vertices())

	allowed := decomp.FilterVertices(allowedFull, VerticesH)

	// Set up iterator for child

	// genChild := decomp.SplitCombin(allowed.Len(), width, runtime.GOMAXPROCS(-1), false)
	// parallelSearch := gen
	// parallelSearch := Search{H: &H, Edges: &allowed, BalFactor: l.BalFactor, Generators: genChild}
	pred := decomp.BalancedCheck{}
	// parallelSearch.FindNext(pred) // initial Search
	Vertices := make(map[int]*disjoint.Element)

CHILD:
	for gen.HasNext() {

		j := gen.GetNext()

		childλ := decomp.GetSubset(allowed, j)

		check, compsε, _ := pred.CheckOut(&comp, &childλ, 2, Vertices)
		gen.Confirm()

		if !check {
			continue CHILD
		}

		// Check if child is possible root
		if Subset(Conn, childλ.Vertices()) {
			// log.Printf("Child-Root cover chosen: %v of %v \n", childλ, H)
			// log.Printf("Comps of Child-Root: %v\n", comps_c)

			childχ := decomp.Inter(childλ.Vertices(), VerticesH)

			maps.Clear(Vertices)
			globalComps, _, isolated := input.GetComponents(childλ, Vertices)

			var Conns [][]int

			for y := range compsε {
				VCompε := compsε[y].Vertices()
				Connγ := decomp.Inter(VCompε, childχ)
				Conns = append(Conns, Connγ)
			}

			sendComponents(compsε, globalComps, Conns, allowedFull, allowedFull, toCoord, found, workerID, countComps, isolated, childλ, comp, decomp.Graph{}, []int{})
		}

		// Set up iterator for parent
		allowedParent := decomp.FilterVertices(allowed, append(Conn, childλ.Vertices()...))
		genParent := decomp.SplitCombin(allowedParent.Len(), width, 1, false)
		parallelSearch := decomp.ParallelSearchGen{}
		parentalSearch := parallelSearch.GetSearch(&comp, &allowedParent, 2, genParent)
		// parentalSearch := Search{H: &H, Edges: &allowedParent, BalFactor: l.BalFactor, Generators: genParent}
		predPar := logk.ParentCheck{Conn: Conn, Child: childλ.Vertices()}
		parentalSearch.FindNext(predPar)
		// parentFound := false

		// ----- Parent Loop -------
		for ; !parentalSearch.SearchEnded(); parentalSearch.FindNext(predPar) {

			parentλ := decomp.GetSubset(allowedParent, parentalSearch.GetResult())
			// log.Println("Looking at parent ", parentλ)
			compsπ, _, isolatedEdges := comp.GetComponents(parentλ, Vertices)
			// log.Println("Parent components ", comps_p)

			foundLow := false
			var compLowIndex int
			var compLow decomp.Graph

			balancednessLimit := (((comp.Len()) * (2 - 1)) / 2)

			// Check if parent is un-balanced
			for i := range compsπ {
				if compsπ[i].Len() > balancednessLimit {
					foundLow = true
					compLowIndex = i // keep track of the index for composing comp_up later
					compLow = compsπ[i]
				}
			}

			if !foundLow {
				fmt.Println("Current SubGraph, ", comp)
				fmt.Println("Conn ", decomp.PrintVertices(Conn))

				fmt.Printf("Current Allowed Edges: %v\n", allowed)
				// fmt.Printf("Current Allowed Edges in Parent Search: %v\n", parentalSearch.Edges)

				fmt.Println("Child ", childλ)
				fmt.Println("Comps of child ", compsε)
				fmt.Println("parent ", parentλ, " ( ", parentalSearch.GetResult(), ")")

				fmt.Println("Comps of p: ")
				for i := range compsπ {
					fmt.Println("Component: ", compsπ[i], " Len: ", compsπ[i].Len())
				}

				log.Panicln("the parallel search didn't actually find a valid parent")
			}

			vertCompLow := compLow.Vertices()
			childχ := decomp.Inter(childλ.Vertices(), vertCompLow)

			// determine which componenents of child are inside comp_low
			compsε, _, _ = compLow.GetComponents(childλ, Vertices)

			maps.Clear(Vertices)
			globalComps, _, isolated := input.GetComponents(childλ, Vertices)

			var Conns [][]int

			for x := range compsε {
				Connχ := decomp.Inter(compsε[x].Vertices(), childχ)
				Conns = append(Conns, Connχ)
			}

			var compUp decomp.Graph
			var specialChild decomp.Edges
			tempEdgeSlice := []decomp.Edge{}
			tempSpecialSlice := []decomp.Edges{}

			tempEdgeSlice = append(tempEdgeSlice, isolatedEdges...)
			for i := range compsπ {
				if i != compLowIndex {
					tempEdgeSlice = append(tempEdgeSlice, compsπ[i].Edges.Slice()...)
					tempSpecialSlice = append(tempSpecialSlice, compsπ[i].Special...)
				}
			}

			// specialChild = NewEdges([]Edge{Edge{Vertices: Inter(childχ, comp_up.Vertices())}})
			specialChild = decomp.NewEdges([]decomp.Edge{{Vertices: childχ}})

			// Reducing the allowed edges
			allowedReduced := allowedFull.Diff(compLow.Edges)

			// if no comps_p, other than comp_low, just use parent as is
			if len(compsπ) == 1 {
				compUp.Edges = parentλ
				// adding new Special Edge to connect Child to comp_up
				compUp.Special = append(compUp.Special, specialChild)

			} else if len(tempEdgeSlice) > 0 { // otherwise compute decomp for comp_up
				compUp.Edges = decomp.NewEdges(tempEdgeSlice)
				compUp.Special = tempSpecialSlice
				// adding new Special Edge to connect Child to comp_up
				compUp.Special = append(compUp.Special, specialChild)
			}

			sendComponents(compsε, globalComps, Conns, allowedFull, allowedReduced, toCoord, found, workerID, countComps, isolated, childλ, comp, compUp, Conn)

		}

	}
}

func sendComponents(localComps, globalComps []decomp.Graph, Conns [][]int, allowedFull, allowedRed decomp.Edges, toCoord chan MessageToCoordinator, found chan MessageToCTD, workerID string, countComps int, isolated []decomp.Edge, sep decomp.Edges, H, Upper decomp.Graph, OldConn []int) {
	// fmt.Println("sending ", sep, " comps to coord")
	// send the newly found components to Coordinator
	toCoord <- MessageToCoordinator{
		Mtype:       SendComponent,
		Id:          workerID,
		Search:      LogKSearch,
		Count:       countComps,
		Comp:        localComps,
		Upper:       Upper,
		CTD:         false,
		Conns:       Conns,
		OldConn:     OldConn,
		AllowedFull: allowedFull,
		AllowedRed:  allowedRed,
	}

	// send the final blocks to CTDCheck
	var blocks []Block
	for i := range globalComps {
		conn := decomp.Inter(globalComps[i].Vertices(), sep.Vertices())

		treeComp := decomp.NewEdges(append(globalComps[i].Edges.Slice(), isolated...))

		out := CreateBlock(sep, globalComps[i].Edges, treeComp, H.Edges, conn)
		blocks = append(blocks, out)
		// fmt.Println("Sending block: \n", out)
		// countComps++
		// found <- out
	}
	found <- MessageToCTD{
		Mtype:  SendBlock,
		Blocks: blocks,
	}
}

func SearchBagsBalgo(gen decomp.Generator, input, comp decomp.Graph, toCoord chan MessageToCoordinator, found chan MessageToCTD, workerID string, countComps int) {
	for gen.HasNext() {

		j := gen.GetNext()

		sep := decomp.GetSubset(input.Edges, j)
		// var sepSub *decomp.SepSub
		// SubEdgeSearchEnded := false

		// for !SubEdgeSearchEnded {
		// 	if sepSub != nil {
		// 		if sepSub.HasNext() {
		// 			sep = sepSub.GetCurrent()
		// 			fmt.Println("looking at subsep", sep)
		// 		} else {
		// 			fmt.Println("done")
		// 			SubEdgeSearchEnded = true
		// 			continue
		// 		}
		// 	}

		pred := decomp.BalancedCheck{}

		Vertices := make(map[int]*disjoint.Element)
		check, comps, _ := pred.CheckOut(&comp, &sep, 2, Vertices)

		if check {
			gen.Found() // cache result
			// fmt.Println("Found sep ", sep)
			// if sepSub == nil {
			// 	sepSub = decomp.GetSepSub(globalGraph.Edges, sep, globalWidth)
			// }

			// currentCompVert := currentComp.Vertices()

			// recompute components based on the input hypergraph
			sepReduced := decomp.CutEdges(sep, comp.Vertices())

			Vertices2 := make(map[int]*disjoint.Element)
			globalComps, _, isolated := input.GetComponents(sepReduced, Vertices2)

			// fmt.Println("sending ", sep, " comps to coord")
			// send the newly found components to Coordinator
			toCoord <- MessageToCoordinator{
				Mtype:  SendComponent,
				Id:     workerID,
				Search: BalancedGoSearch,
				Count:  countComps,
				Comp:   comps,
				CTD:    false,
			}

			// send the final blocks to CTDCheck
			var blocks []Block
			for i := range globalComps {
				conn := decomp.Inter(globalComps[i].Vertices(), sep.Vertices())

				treeComp := decomp.NewEdges(append(globalComps[i].Edges.Slice(), isolated...))

				out := CreateBlock(sep, globalComps[i].Edges, treeComp, comp.Edges, conn)
				blocks = append(blocks, out)
				// fmt.Println("Sending block: \n", out)
				// countComps++
				// found <- out
			}
			found <- MessageToCTD{
				Mtype:  SendBlock,
				Blocks: blocks,
			}

			// log.Println("Worker", index, "won, found: ", j)
			// fmt.Println("Worker ", workerID, " found sep ", sep, " for H ", comp)

			// *finished = true
			// return
		}
		// 	else {
		// 		if sepSub == nil {
		// 			SubEdgeSearchEnded = true // skip subedge search if not balanced
		// 		}

		// 	}
		// }

		gen.Confirm()
	}
}
