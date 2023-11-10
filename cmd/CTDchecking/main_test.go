package main

import (
	"fmt"
	"log"
	"testing"
	"time"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
	"github.com/cem-okulmus/CTDecomp/lib"

	"github.com/cem-okulmus/disjoint"
)

// write a test scenario that
// 1) creates blocks for a random hypergraph
// 2) adds them to an ODAG until the root is satisfied

var (
	ctdCheckOutput    bool
	workerOutput      bool
	coordinatorOutput bool
)

var counterSent int

func conPrint(con bool, args ...interface{}) {
	if !con {
		return
	}
	fmt.Println(args...)
}

func ctdCheckBitSet(toCDT chan []byte, toCoordOut chan []byte) {
	CTDID := fmt.Sprint("ctdCheckingID", 12)
	var input decomp.Graph

	fmt.Println("Current id: ", CTDID)

	messages := make(chan lib.MessageToCTD)
	// found := make(chan lib.WorkerToCTD)
	toCoord := make(chan lib.MessageToCoordinator)

	// background goroutine for receiving messages
	go func(messages chan lib.MessageToCTD) {
		for {
			data := <-toCDT

			var message lib.MessageToCTD
			message.FromBytes(data)
			messages <- message
		}
	}(messages)

	// background goroutine for sending messages
	go func(toCoord chan lib.MessageToCoordinator) {
		for {
			m := <-toCoord
			toCoordOut <- m.ToBytes()
		}
	}(toCoord)

	fmt.Println("Set up background goroutines for receiving and sending messages.")

	// register to CTD

	toCoord <- lib.MessageToCoordinator{
		Mtype: lib.Register,
		Id:    CTDID,
		CTD:   true,
	}

	message := <-messages
	input = message.Init.Input

	fmt.Println("Registered with coordinator.")

	// start receiving blocks and look for GHDs

	var odag lib.BitSetCTDCheck
	odag = lib.CreateBitSetCheck(input)
	fmt.Println("ctdCheck has begun")
	searchEnded := false

outer:
	for {

		// fmt.Println("Reading new MTC")
		m := <-messages
		// fmt.Println("Successly read new MTC")

		switch m.Mtype {
		case lib.SendBlock:
			for i := range m.Blocks {
				// fmt.Println("ctdCheck got new block ", out)
				node := lib.CreateNode(m.Blocks[i])
				if node.Satisfied {
					// fmt.Println("New node is triv. satisfied!")
				}
				odag.AddNode(node)
			}

			if odag.IsSatisfied() {

				toCoord <- lib.MessageToCoordinator{
					Mtype: lib.Result,
					Id:    CTDID,
					CTD:   true,
				}

				// fmt.Println("Sent Done Message")
				fmt.Println("Ctd Check done")

				break outer
			}

		case lib.Exhausted:
			fmt.Println("Got exhausted message from Coordinator!")
			searchEnded = true

		}

		if searchEnded && len(messages) == 0 {
			fmt.Println("Ending it")
			break outer
		}
		// fmt.Println("\n\nContent of ODAG at end: \n", odag)
	}

	// fmt.Println("Nodes at end:", odag)
	_, decomp := odag.GetDecomp(input)
	fmt.Println("Is Correct:", decomp.Correct(input))
	if decomp.Correct(input) {
		fmt.Println("Produced decomp:", decomp)
	}

	fmt.Println("Found a decomposition in the OADG, somwhere", len(odag.Nodes), odag.IsSatisfied())
}

func ctdCheckSetBased(toCDT chan []byte, toCoordOut chan []byte) {
	CTDID := fmt.Sprint("ctdCheckingID", 12)
	var input decomp.Graph

	fmt.Println("Current id: ", CTDID)

	messages := make(chan lib.MessageToCTD)
	// found := make(chan lib.WorkerToCTD)
	toCoord := make(chan lib.MessageToCoordinator)

	// background goroutine for receiving messages
	go func(messages chan lib.MessageToCTD) {
		for {
			data := <-toCDT

			var message lib.MessageToCTD
			message.FromBytes(data)
			messages <- message
		}
	}(messages)

	// background goroutine for sending messages
	go func(toCoord chan lib.MessageToCoordinator) {
		for {
			m := <-toCoord
			toCoordOut <- m.ToBytes()
		}
	}(toCoord)

	fmt.Println("Set up background goroutines for receiving and sending messages.")

	// register to CTD

	toCoord <- lib.MessageToCoordinator{
		Mtype: lib.Register,
		Id:    CTDID,
		CTD:   true,
	}

	message := <-messages
	input = message.Init.Input

	fmt.Println("Registered with coordinator.")

	// start receiving blocks and look for GHDs

	var odag lib.SetBasedCTDCheck
	odag = lib.CreateSetBasedCheck(input)
	fmt.Println("ctdCheck has begun")
	searchEnded := false

outer:
	for {

		// fmt.Println("Reading new MTC")
		m := <-messages
		// fmt.Println("Successly read new MTC")

		switch m.Mtype {
		case lib.SendBlock:
			for i := range m.Blocks {
				// fmt.Println("ctdCheck got new block ", out)
				node := lib.CreateNode(m.Blocks[i])
				if node.Satisfied {
					// fmt.Println("New node is triv. satisfied!")
				}
				odag.AddNode(node)
			}

			if odag.IsSatisfied() {

				toCoord <- lib.MessageToCoordinator{
					Mtype: lib.Result,
					Id:    CTDID,
					CTD:   true,
				}

				// fmt.Println("Sent Done Message")
				fmt.Println("Ctd Check done")

				break outer
			}

		case lib.Exhausted:
			fmt.Println("Got exhausted message from Coordinator!")
			searchEnded = true

		}

		if searchEnded && len(messages) == 0 {
			fmt.Println("Ending it")
			break outer
		}
		// fmt.Println("\n\nContent of ODAG at end: \n", odag)
	}

	// fmt.Println("Nodes at end:", odag)
	_, decomp := odag.GetDecomp(input)
	fmt.Println("Is Correct:", decomp.Correct(input))
	if decomp.Correct(input) {
		fmt.Println("Produced decomp:", decomp)
	}

	fmt.Println("Found a decomposition in the OADG, somwhere", len(odag.Nodes), odag.IsSatisfied())
}

func ctdCheck(toCDT chan []byte, toCoordOut chan []byte) {
	CTDID := fmt.Sprint("ctdCheckingID", 12)
	var input decomp.Graph

	fmt.Println("Current id: ", CTDID)

	messages := make(chan lib.MessageToCTD)
	// found := make(chan lib.WorkerToCTD)
	toCoord := make(chan lib.MessageToCoordinator)

	// background goroutine for receiving messages
	go func(messages chan lib.MessageToCTD) {
		for {
			data := <-toCDT

			var message lib.MessageToCTD
			message.FromBytes(data)

			messages <- message
		}
	}(messages)

	// go func(components chan lib.WorkerToCTD) {

	// 	for true {
	// 		data := subB.ReceivingTimed(start)
	// 		var message lib.WorkerToCTD
	// 		message.FromBytes(data)
	// 		components <- message
	// 	}

	// }(found)

	// background goroutine for sending messages
	go func(toCoord chan lib.MessageToCoordinator) {
		for {
			m := <-toCoord
			toCoordOut <- m.ToBytes()
		}
	}(toCoord)

	fmt.Println("Set up background goroutines for receiving and sending messages.")

	// register to CTD

	toCoord <- lib.MessageToCoordinator{
		Mtype: lib.Register,
		Id:    CTDID,
		CTD:   true,
	}

	message := <-messages
	input = message.Init.Input

	fmt.Println("Registered with coordinator.")

	// start receiving blocks and look for GHDs

	var odag lib.ExhaustiveCTDSearch

	odag = lib.CreateCTDSearch(input)

	fmt.Println("ctdCheck has begun")

	searchEnded := false

outer:
	for {

		fmt.Println("Reading new MTC")
		m := <-messages
		fmt.Println("Successly read new MTC")

		switch m.Mtype {
		case lib.SendBlock:
			for i := range m.Blocks {
				// fmt.Println("ctdCheck got new block ", out)
				node := lib.CreateNode(m.Blocks[i])
				if node.Satisfied {
					fmt.Println("New node is triv. satisfied!")
				}
				odag.AddNode(node)
			}

			// if odag.Nodes[0].Satisfied {
			// 	fmt.Println("ROOT DONE")
			// }

			if odag.IsSatisfied() {

				// if odag.Nodes[0].Satisfied {
				// 	fmt.Println("ROOT DONE2")
				// }
				// fmt.Println("Sending Done Message")
				toCoord <- lib.MessageToCoordinator{
					Mtype: lib.Result,
					Id:    CTDID,
					CTD:   true,
				}

				// fmt.Println("Sent Done Message")
				fmt.Println("Ctd Check done")

				// if odag.Nodes[0].Satisfied {
				// 	fmt.Println("ROOT DONE3")
				// }
				break outer
			}

		case lib.Exhausted:
			fmt.Println("Got exhausted message from Coordinator!")
			searchEnded = true

		}

		if searchEnded && len(messages) == 0 {
			fmt.Println("Ending it")
			break outer
		}
		fmt.Println("\n\nContent of ODAG at end: \n", odag)
	}

	// fmt.Println("Nodes at end:", odag)

	_, decomp := odag.GetDecomp(input)
	fmt.Println("Produced decomp:", decomp)
	fmt.Println("Is Correct:", decomp.Correct(input))
	if decomp.Correct(input) {
		fmt.Println("Produced decomp:", decomp)
	}

	fmt.Println("Found a decomposition in the OADG, somwhere", len(odag.Nodes), odag.IsSatisfied())
}

// used to determine the next component to decompose
var compHeap lib.CompHeap

func coordinationWorker(input decomp.Graph, width int, toCTDOut chan []byte, toWorkerOut chan []byte, toCoord chan []byte, searchFlag bool) {
	var chosenSearch lib.SearchType

	if searchFlag {
		chosenSearch = lib.BalancedGoSearch
	} else {
		chosenSearch = lib.LogKSearch
	}
	fmt.Println("Parsed parameter")

	graph := input

	fmt.Println("Parsed input")

	var initStruct lib.Init
	initStruct.Input, initStruct.Width = graph, width

	messages := make(chan lib.MessageToCoordinator, 100)
	toWorker := make(chan lib.CoordinatorToWorker, 100)
	toCTD := make(chan lib.MessageToCTD)

	CTDDone := false

	// background goroutine for receiving messages
	go func(messages chan lib.MessageToCoordinator) {
		for {
			data := <-toCoord

			var message lib.MessageToCoordinator
			message.FromBytes(data)

			if message.Mtype == lib.Result {
				CTDDone = true
				fmt.Println("CTD is doneso, telling workers to stop")
				toWorker <- lib.CoordinatorToWorker{
					Mtype: lib.Result,
				}
			}

			messages <- message
		}
	}(messages)

	// background goroutine for sending messages
	go func(toWorker chan lib.CoordinatorToWorker) {
		for {
			m := <-toWorker
			toWorkerOut <- m.ToBytes()
			// topicW.Write(m)
		}
	}(toWorker)

	go func(toCTD chan lib.MessageToCTD) {
		for {
			m := <-toCTD
			toCTDOut <- m.ToBytes()
		}
	}(toCTD)

	fmt.Println("Set up background goroutines for receiving and sending messages.")

	// Initial Phase, registering workers and CTD function
	haveWorkers, haveCTD := false, false
	var activeWorkers []string

	// keep track of how many comps each worker has finished when sending idle message
	workerIdleMap := make(map[string]int)

	// continue this loop until at leas one worker and one CTD function have registered

	// TOOD: allow adding workers after starting coordinator later
	//			right now coord. will wait for CTD to start, and no workers can be added later
	for !haveWorkers || !haveCTD {

		m := <-messages
		fmt.Println("Received message: ", m)

		if m.Mtype == lib.Register { // ignore any messages that aren't registering workers
			fmt.Println("Got registration attempt")
			if m.CTD {
				haveCTD = true

				toCTD <- lib.MessageToCTD{
					Mtype: lib.Initialise,
					Init:  initStruct,
				}

				fmt.Println("Gotten registration from CTD")
			} else {
				haveWorkers = true

				activeWorkers = append(activeWorkers, m.Id)

				toWorker <- lib.CoordinatorToWorker{
					Mtype:  lib.Initialise,
					Init:   initStruct,
					Search: chosenSearch,
				}

				fmt.Println("Gotten registration from Worker")
			}

		}

	}

	time.Sleep(time.Second * 1)

	fmt.Println("Registered all needed units, starting the overall search process.")

	// message2 := lib.CoordinatorToWorker{
	// 	Mtype: lib.Initialise,
	// 	Init:  initStruct,
	// 	Comp:  decomp.Graph{},
	// }
	// topicW.Write(message2.ToBytes())
	// to_worker.Write([]byte("Message"))

	// var compsFound []decomp.Edges

	initalComp := lib.Comp{
		Edges:   graph,
		Allowed: graph.Edges,
	}

	compHeap.Add(initalComp)

	counter := 0
	counterSent := 0

	// CompHead Phase, sending new components to workers until no more new comps generated

	// continue decomposing new components until none are left or told to terminate
compSearch:
	for compHeap.HasNext() {

		if CTDDone {
			// conPrint(coordinatorOutput, "Coordination worker finished")
			fmt.Println("Coordinatior finished")
			break compSearch
		}

		current := compHeap.GetNext()
		// compsFound = append(compsFound, current)

		var blocks []lib.Block
		if current.Len() <= width {
			curEdges := current.Edges.Edges
			out := lib.CreateBlock(curEdges, curEdges, curEdges, curEdges, curEdges.Vertices())
			blocks = append(blocks, out)
			counterSent++
			// create components

			Vertices := make(map[int]*disjoint.Element)
			globalComps, _, isolated := graph.GetComponents(curEdges, Vertices)

			for i := range globalComps {
				conn := decomp.Inter(globalComps[i].Vertices(), curEdges.Vertices())

				treeComp := decomp.NewEdges(append(globalComps[i].Edges.Slice(), isolated...))

				out := lib.CreateBlock(curEdges, globalComps[i].Edges, treeComp, curEdges, conn)
				// fmt.Println("Sending block: \n", out)
				counterSent++
				blocks = append(blocks, out)
			}

			toCTD <- lib.MessageToCTD{
				Mtype:  lib.SendBlock,
				Blocks: blocks,
			}

			// conPrint(coordinatorOutput, "Found trivial block for ", current)
			// fmt.Println("Found trivial block for ", current)
			// if len(messages) == 0 {
			// 	// test if workers done
			// 	finishedWorkers := 0
			// 	for i := range workerIdleMap {
			// 		if workerIdleMap[i] == counter {
			// 			finishedWorkers++
			// 		}
			// 	}
			// 	// fmt.Println("Found ", finishedWorkers, " idle workers")
			// 	if finishedWorkers == len(activeWorkers) {
			// 		continue compSearch
			// 	}

			// }

		} else {
			Generators := decomp.SplitCombin(graph.Edges.Len(), width, len(activeWorkers), false)

			// fmt.Println("Starting new search for balanced sep in comp ", current)
			toWorker <- lib.CoordinatorToWorker{
				Mtype:   lib.SearchGraph,
				Workers: activeWorkers,
				Comp:    current,
				Gen:     Generators,
			}
			counter++ // increase the count of components sent to workers
		}

		// outer:
		for {
			if CTDDone {
				break compSearch
			}
			// check if workers are all idle, continue the search if so
			finishedWorkers := 0
			for i := range workerIdleMap {
				if workerIdleMap[i] == counter {
					finishedWorkers++
				}
			}
			// fmt.Println("Found ", finishedWorkers, " idle workers")
			if finishedWorkers == len(activeWorkers) {
				continue compSearch
			}

			m := <-messages

			switch m.Mtype {
			case lib.Result:
				CTDDone = true
				// fmt.Println("CTD is doneso, telling workers to stop")
				toWorker <- lib.CoordinatorToWorker{
					Mtype: lib.Result,
				}

			// 	break compSearch
			case lib.SendComponent:

				if m.Search == lib.BalancedGoSearch {
					for i := range m.Comp {
						compHeap.Add(lib.Comp{Edges: m.Comp[i]})
					}
				} else {
					for i := range m.Comp {
						comp := lib.Comp{
							Edges:   m.Comp[i],
							Conn:    m.Conns[i],
							Allowed: m.AllowedFull,
						}
						compHeap.Add(comp)
					}

					// check if we need to add upper (check if empty graph)
					if m.Upper.Len() > 0 {
						comp := lib.Comp{
							Edges:   m.Upper,
							Conn:    m.OldConn,
							Allowed: m.AllowedRed,
						}
						compHeap.Add(comp)
					}

				}

			case lib.Idle:
				// fmt.Println("Gotten Idle message")
				worker := m.Id
				count := m.Count
				workerIdleMap[worker] = count

				// test if workers done

			}

		}

	}

	fmt.Println("----\nCOORDINATOR ENDING MAIN LOOP\n------", compHeap.HasNext(), workerIdleMap, CTDDone, counter)

	if !CTDDone {
		if compHeap.HasNext() {
			log.Panicln("exited loop while still comps to be found")
		}

		// tell CTD to wind things down
		toCTD <- lib.MessageToCTD{
			Mtype: lib.Exhausted,
		}
	}

	toWorker <- lib.CoordinatorToWorker{
		Mtype: lib.Result,
	}

	time.Sleep(time.Second * 1)

	fmt.Println("Components found: ", counter, compHeap.HasNext(), counterSent)
	fmt.Println("All done with EVERYTHING!\n Comps found")
}

// func worker(currentComp *decomp.Graph, index int, found chan lib.Block, component chan decomp.Graph, wg *sync.WaitGroup) {

func worker(toCoordOut chan []byte, toCTDOut chan []byte, toWorker chan []byte) {
	workerID := fmt.Sprint("workerMach", 42)
	var input decomp.Graph
	var width int
	var countComps int

	fmt.Println("Current id: ", workerID)

	fmt.Println("Created topics and subscription handlers.")

	messages := make(chan lib.CoordinatorToWorker, 100)
	toCoord := make(chan lib.MessageToCoordinator, 100)
	found := make(chan lib.MessageToCTD, 100)

	// background goroutine for receiving messages
	go func(messages chan lib.CoordinatorToWorker) {
		for {
			data := <-toWorker

			var message lib.CoordinatorToWorker
			message.FromBytes(data)

			messages <- message
		}
	}(messages)

	// background goroutine for sending messages
	go func(components chan lib.MessageToCoordinator) {
		for {
			m := <-components
			toCoordOut <- m.ToBytes()
			// ToCoord.Write(m)
		}
	}(toCoord)

	go func(found chan lib.MessageToCTD) {
		for {
			m := <-found
			toCTDOut <- m.ToBytes()
			// ToCTD.Write(m)
		}
	}(found)

	fmt.Println("Set up background goroutines for receiving and sending messages.")

	// register worker and get split factor

	toCoord <- lib.MessageToCoordinator{
		Mtype: lib.Register,
		Id:    workerID,
	}

	message2 := <-messages

	input = message2.Init.Input
	width = message2.Init.Width

	fmt.Println("registered at coordinator")

	// start working until receiving to stop from coordinator

	searchActive := true

	for searchActive {

		message := <-messages

		if message.Mtype == lib.Result {
			searchActive = false
			fmt.Println("Worker is stopping the search!")
			continue
		}

		var comp lib.Comp
		var gen decomp.Generator
		var search lib.SearchType

		comp = message.Comp
		search = message.Search

		foundID := false
		for i := range message.Workers {
			if message.Workers[i] == workerID {
				foundID = true
				gen = message.Gen[i]
			}
		}
		if !foundID {
			log.Panicln("couldn't find workerID in pool", workerID, " ", message.Workers)
		}

		// fmt.Println("Started work on new comp: ", comp)
		countComps++ // keep track of number of comps received from coordinator

		switch search {
		case lib.BalancedGoSearch:
			SearchBagsBalgo(gen, input, comp.Edges, toCoord, found, workerID, countComps)
		case lib.LogKSearch:
			SearchBagsLogK(gen, input, comp.Edges, toCoord, found, workerID, countComps, comp.Conn, comp.Allowed, width)
		}

		// sent Idle message to coordinator

		// fmt.Println("Sent idle notice to coordinator", comp)

		toCoord <- lib.MessageToCoordinator{
			Mtype: lib.Idle,
			Id:    workerID,
			Count: countComps,
			CTD:   false,
		}

	}
}

func TestCTD(t *testing.T) {
	coordinatorOutput = false
	workerOutput = false
	ctdCheckOutput = false

	// input graph

	// input := "E1 (V1, V2, V9)," +
	// 	"E2 (V2, V3, V10)," +
	// 	"E3 (V3, V4)," +
	// 	"E4 (V4, V5, V9)," +
	// 	"E5 (V5, V6, V10)," +
	// 	"E6 (V6, V7, V9)," +
	// 	"E7 (V7, V8, V10)," +
	// 	"E8 (V8, V1). "

	input := "E12(xL62J,xL10J,xL11J)," +
		"E23(xL73J,xL21J,xL22J)," +
		"E17(xL16J,xL15J,xL67J)," +
		"E5(xL3J,xL4J,xL55J)," +
		"E43(xL60J,xL41J,xL40J)," +
		"E52(xL49J,xL50J,xL51J)," +
		"E46(xL57J,xL43J,xL44J)," +
		"E51(xL49J,xL48J,xL52J)," +
		"E36(xL34J,xL67J,xL33J)," +
		"E18(xL16J,xL17J,xL68J)," +
		"E4(xL2J,xL3J,xL54J)," +
		"E33(xL70J,xL30J,xL31J)," +
		"E42(xL39J,xL61J,xL40J)," +
		"E49(xL47J,xL46J,xL54J)," +
		"E45(xL58J,xL42J,xL43J)," +
		"E39(xL36J,xL37J,xL64J)," +
		"E13(xL63J,xL12J,xL11J)," +
		"E24(xL74J,xL23J,xL22J)," +
		"E9(xL59J,xL7J,xL8J)," +
		"E14(xL64J,xL12J,xL13J)," +
		"E25(xL75J,xL23J,xL24J)," +
		"E8(xL58J,xL6J,xL7J)," +
		"E32(xL71J,xL29J,xL30J)," +
		"E19(xL17J,xL69J,xL18J)," +
		"E41(xL38J,xL39J,xL62J)," +
		"E3(xL1J,xL53J,xL2J)," +
		"E48(xL45J,xL46J,xL55J)," +
		"E20(xL70J,xL18J,xL19J)," +
		"E27(xL25J,xL76J,xL77J)," +
		"E2(xL0J,xL52J,xL1J)," +
		"E44(xL59J,xL41J,xL42J)," +
		"E10(xL9J,xL60J,xL8J)," +
		"E29(xL27J,xL26J,xL74J)," +
		"E1(xL0J,xL50J,xL51J)," +
		"E50(xL47J,xL48J,xL53J)," +
		"E35(xL68J,xL32J,xL33J)," +
		"E40(xL38J,xL37J,xL63J)," +
		"E47(xL45J,xL56J,xL44J)," +
		"E26(xL24J,xL76J,xL77J)," +
		"E15(xL14J,xL13J,xL65J)," +
		"E7(xL5J,xL57J,xL6J)," +
		"E31(xL28J,xL29J,xL72J)," +
		"E38(xL36J,xL35J,xL65J)," +
		"E11(xL9J,xL61J,xL10J)," +
		"E21(xL71J,xL19J,xL20J)," +
		"E16(xL14J,xL15J,xL66J)," +
		"E6(xL4J,xL56J,xL5J)," +
		"E34(xL69J,xL31J,xL32J)," +
		"E28(xL25J,xL26J,xL75J)," +
		"E22(xL20J,xL72J,xL21J)," +
		"E30(xL27J,xL28J,xL73J)," +
		"E37(xL34J,xL35J,xL66J)."

	toCoord := make(chan []byte)
	toCTD := make(chan []byte)
	toWorker := make(chan []byte)

	graph, _ := decomp.GetGraph(input)

	graph.Edges = decomp.GetMaxSepOrder(graph.Edges)
	go coordinationWorker(graph, 2, toCTD, toWorker, toCoord, true)

	// time.Sleep(2 * time.Second)

	// go ctdCheck(toCTD, toCoord)
	// go ctdCheckSetBased(toCTD, toCoord)
	go ctdCheckBitSet(toCTD, toCoord)

	// time.Sleep(5 * time.Second)

	worker(toCoord, toCTD, toWorker)

	// time.Sleep(3 * time.Second)
}
