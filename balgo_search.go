package main

import (
	decomp "github.com/cem-okulmus/BalancedGo/lib"
	"github.com/cem-okulmus/CTDecomp/lib"
	"github.com/cem-okulmus/disjoint"
)

func SearchBagsBalgo(gen decomp.Generator, input, comp decomp.Graph, toCoord chan lib.MessageToCoordinator, found chan lib.MessageToCTD, workerID string, countComps int) {
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
			toCoord <- lib.MessageToCoordinator{
				Mtype:  lib.SendComponent,
				Id:     workerID,
				Search: lib.BalancedGoSearch,
				Count:  countComps,
				Comp:   comps,
				CTD:    false,
			}

			// send the final blocks to CTDCheck
			var blocks []lib.Block
			for i := range globalComps {
				conn := decomp.Inter(globalComps[i].Vertices(), sep.Vertices())

				treeComp := decomp.NewEdges(append(globalComps[i].Edges.Slice(), isolated...))

				out := lib.CreateBlock(sep, globalComps[i].Edges, treeComp, comp.Edges, conn)
				blocks = append(blocks, out)
				// fmt.Println("Sending block: \n", out)
				// countComps++
				// found <- out
			}
			found <- lib.MessageToCTD{
				Mtype:  lib.SendBlock,
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
