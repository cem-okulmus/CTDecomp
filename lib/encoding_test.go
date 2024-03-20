package lib

import (
	"bytes"
	"encoding/gob"
	"log"
	"testing"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
	"github.com/cem-okulmus/disjoint"
)

func TestGOB(t *testing.T) {
	input := "E1 (V1, V2, V9)," +
		"E2 (V2, V3, V10)," +
		"E3 (V3, V4)," +
		"E4 (V4, V5, V9)," +
		"E5 (V5, V6, V10)," +
		"E6 (V6, V7, V9)," +
		"E7 (V7, V8, V10)," +
		"E8 (V8, V1). "

	graph, _ := decomp.GetGraph(input)

	// workerID := "sadfasdf1"
	// coordID := "asdfasdf12"
	// ctdID := "Iejwoeioewd098"

	init := Init{
		Input: graph,
		Width: 2,
	}

	// Generators := decomp.SplitCombin(graph.Edges.Len(), 2, 1, false)

	// generate some blocks

	sep := decomp.NewEdges([]decomp.Edge{graph.Edges.Slice()[0], graph.Edges.Slice()[1]})
	Vertices := make(map[int]*disjoint.Element)

	comps, _, _ := graph.GetComponents(sep, Vertices)

	var blocks []Block
	for i := range comps {
		block1 := CreateBlock(sep, comps[i].Edges, comps[i].Edges, graph.Edges, decomp.Inter(sep.Vertices(), comps[i].Vertices()))
		blocks = append(blocks, block1)
	}

	// mtd := lib.MessageToCoordinator{
	// 	Mtype: lib.Register,
	// 	Id:    workerID,
	// 	Count: 12,
	// 	Comp:  []decomp.Graph{graph},
	// 	CTD:   false,
	// }
	// var mtd2 lib.MessageToCoordinator

	// ctw := lib.CoordinatorToWorker{
	// 	Mtype:   lib.Initialise,
	// 	Init:    init,
	// 	Workers: []string{workerID},
	// 	Comp:    graph,
	// 	Gen:     Generators,
	// }
	// var ctw2 lib.CoordinatorToWorker

	mtc := MessageToCTD{
		Mtype:  Exhausted,
		Init:   init,
		Blocks: blocks,
	}
	var mtc2 MessageToCTD

	// fmt.Println("MessageMTD: ", mtd)
	// out := mtd.ToBytes()
	// fmt.Println("MessageMTD as bytes: ", out)
	// mtd2.FromBytes(out)
	// fmt.Println("MessageMTD from bytes: ", mtd2)

	// fmt.Println("Sep: ", blocks[0].Sep)

	var encodeBuffer bytes.Buffer

	enc := gob.NewEncoder(&encodeBuffer)
	err := enc.Encode(&blocks[0])
	if err != nil {
		log.Fatal("sep encoding errors: ", err)
	}
	out := encodeBuffer.Bytes()

	var sep2 Block
	var decodeBuffer1 bytes.Buffer
	decodeBuffer1.Write(out)
	dec := gob.NewDecoder(&decodeBuffer1)
	err = dec.Decode(&sep2)
	if err != nil {
		log.Fatal("sep Decode error: ", err)
	}
	// fmt.Println("Sep Decoded: ", sep2.Sep)

	// fmt.Println("MessageMTC: ", mtc)
	out = mtc.ToBytes()
	// fmt.Println("Message as bytes: ", out)
	mtc2.FromBytes(out)
	// fmt.Println("Message from bytes: ", mtc2)

	// fmt.Println("MessageCTW: ", ctw)
	// out = ctw.ToBytes()
	// fmt.Println("Message as bytes: ", out)
	// ctw2.FromBytes(out)
	// fmt.Println("Message from bytes: ", ctw2)
}
