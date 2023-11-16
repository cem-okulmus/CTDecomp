// The coordinator is responsible for managing the overall computation,
// from start to finish. It keeps a list of all components so far considered,
// and it also keeps a list of all active workers. The final output is delived back
// to the coordinator and output by it.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	decomp "github.com/cem-okulmus/BalancedGo/lib"
	"github.com/cem-okulmus/CTDecomp/lib"
	"github.com/cem-okulmus/disjoint"
)

func CTDCheckRoutine(id int, input decomp.Graph, width int, messages chan lib.MessageToCTD,
	toCoord chan lib.MessageToCoordinator,
) {
	CTDID := fmt.Sprint("coordinator", id)
	// register to CTD

	toCoord <- lib.MessageToCoordinator{
		Mtype: lib.Register,
		Id:    CTDID,
		CTD:   true,
	}

	fmt.Println("CTDChecking: Registered with coordinator.")

	// start receiving blocks and look for GHDs

	// var odag lib.ExhaustiveCTDSearch
	// var odag lib.BitSetCTDCheck
	var odag lib.SetBasedCTDCheck

	// odag = lib.CreateCTDSearch(input)
	// odag = lib.CreateBitSetCheck(input)
	odag = lib.CreateSetBasedCheck(input)

	fmt.Println("CTDChecking: ctdCheck has begun")

	searchEnded := false

outer:
	for true {

		fmt.Println("CTDChecking: waiting on next input")
		m := <-messages

		switch m.Mtype {
		case lib.SendBlock:
			for i := range m.Blocks {
				// fmt.Println("ctdCheck got new block ", out)
				// var blockOut BlockOut
				// blockOut.FromBlock(m.Blocks[i])
				// receivedBlocks = append(receivedBlocks, blockOut) // logging the blocks as they come in
				node := lib.CreateNode(m.Blocks[i])
				if node.Satisfied {
					fmt.Println("CTDChecking: New node is triv. satisfied!")
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

				_, decomp := odag.GetDecomp(input)

				fmt.Println("CTDChecking: Odag Is Satisfied. Sending Done Message")
				toCoord <- lib.MessageToCoordinator{
					Mtype:  lib.Result,
					Id:     CTDID,
					CTD:    true,
					Decomp: decomp,
				}

				// time.Sleep(time.Second * 1)

				// fmt.Println("Sent Done Message")
				fmt.Println("CTDChecking: Ctd Check done")

				// if odag.Nodes[0].Satisfied {
				// 	fmt.Println("ROOT DONE3")
				// }
				break outer
			}

		case lib.Exhausted:
			fmt.Println("CTDChecking: Got exhausted message from Coordinator!")
			searchEnded = true

		}

		if searchEnded && len(messages) == 0 {
			fmt.Println("CTDChecking: Ending it")
			break outer
		}
		// fmt.Println("\n\nContent of ODAG at end: \n", odag)
	}

	// // fmt.Println("Nodes at end:", odag)

	// if odag.IsSatisfied() && decomp.Correct(input) {

	// 	// fmt.Println("Produced decomp:", decomp)
	// 	fmt.Println("CTDChecking: Is Correct:", decomp.Correct(input))
	// 	fmt.Println("CTDChecking: Produced decomp:", decomp)
	// } else {
	// 	fmt.Println("CTDChecking: Failed to find a decomp!")
	// }

	// fmt.Println("CTDChecking: Found a decomposition in the OADG?: ", odag.IsSatisfied(), "num indexed nodes", len(odag.Nodes))
}

func workerRoutine(id int, input decomp.Graph, width int, messages chan lib.CoordinatorToWorker,
	toCoord chan lib.MessageToCoordinator, toCTD chan lib.MessageToCTD,
) {
	workerID := fmt.Sprint("worker", id)

	fmt.Println(workerID, ": Wanting to send to COORD")
	toCoord <- lib.MessageToCoordinator{
		Mtype: lib.Register,
		Id:    workerID,
	}

	message2 := <-messages

	input = message2.Init.Input
	width = message2.Init.Width

	fmt.Println(workerID, ": registered at coordinator")
	// start working until receiving to stop from coordinator

	found := make(chan lib.MessageToCTD)

	go func(found chan lib.MessageToCTD) {
		for true {
			m := <-found
			toCTD <- m
		}
	}(found)

	searchActive := true
	var countComps int

	for searchActive {
		fmt.Println(workerID, ": curent count ", countComps)

		gottenNextInput := false
		var message lib.CoordinatorToWorker

		for !gottenNextInput {
			message = <-messages
			if message.Mtype != lib.Initialise {
				gottenNextInput = true
			}
		}

		// Check if either search exhausted or decomp found
		if message.Mtype == lib.Exhausted || message.Mtype == lib.Result {
			searchActive = false
			fmt.Println(workerID, ": Worker is stopping the search!")
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
			log.Println(workerID, ": couldn't find workerID in pool", workerID, " ", message.Workers)
		}

		// fmt.Println("Started work on new comp: ", comp)
		countComps++ // keep track of number of comps received from coordinator

		fmt.Println(workerID, ": Worker started working on received gen")

		// inputs for the search:
		// * generator decomp.genarator
		// * input decomp.graph
		// * comp decomp.graph
		// * toCoord lib.MessageToCoordinator
		// * countComps int
		// * found lib.MessageToCTD

		switch search {
		case lib.BalancedGoSearch:
			lib.SearchBagsBalgo(gen, input, comp.Edges, toCoord, found, workerID, countComps)
		case lib.LogKSearch:
			lib.SearchBagsLogK(gen, input, comp.Edges, toCoord, found, workerID, countComps,
				comp.Conn, comp.Allowed, width)
		}

		// sent Idle message to coordinator

		fmt.Println(workerID, ": Sent idle notice to coordinator")

		toCoord <- lib.MessageToCoordinator{
			Mtype: lib.Idle,
			Id:    workerID,
			Count: countComps,
			CTD:   false,
		}

	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

// TODO:
//   * first create a simple test to see if the PSCHannel type can handle both send and receive
//   * draft how the coordinator assigns numbers to workers, find a UID for workers, and to which
//   * topics and subscriptions to send things

func main() {
	// start := time.Now()
	// name, err := metadata.Hostname()
	// if err != nil {
	// 	log.Fatal("couldn't get host name ", err)
	// }

	// fmt.Println("Coordinator: Current host: ", name)

	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)

	// input flags
	input := flagSet.String("input", "",
		"Input hypergraph")
	width := flagSet.Int("width", 0,
		"the width to look for")
	numWorkers := flagSet.Int("numWorkers", 0,
		"the number of workers to concurrently employ.")

	searchFlag := flagSet.Bool("useLogK", false,
		"If provided, the distributed search will use LogK bags")

	parseError := flagSet.Parse(os.Args[1:])
	if parseError != nil {
		fmt.Print("Parse Error:\n", parseError.Error(), "\n\n")
	}

	// Output usage message if graph and width not specified
	if *width == 0 || *input == "" {
		out := "Usage of CTDecomp: "
		fmt.Fprintln(os.Stderr, out)

		flagSet.VisitAll(func(f *flag.Flag) {
			s := fmt.Sprintf("%T", f.Value) // used to get type of flag
			if s[6:len(s)-5] != "bool" {
				fmt.Printf("  -%-10s \t<%s>\n", f.Name, s[6:len(s)-5])
			} else {
				fmt.Printf("  -%-10s \n", f.Name)
			}
			fmt.Println("\t" + f.Usage)
		})

		return
	}

	var chosenSearch lib.SearchType

	if *searchFlag {
		chosenSearch = lib.BalancedGoSearch
	} else {
		chosenSearch = lib.LogKSearch
	}

	fmt.Println("Coordinator: Parsed parameter")

	dat, err := os.ReadFile(*input)
	check(err)
	graph, _ := decomp.GetGraph(string(dat))

	fmt.Println("Coordinator: Parsed input: ", graph, "Len: ", graph.Edges.Len())

	var initStruct lib.Init
	initStruct.Input, initStruct.Width = graph, *width

	// Generate the needed topics
	// lib.SetupAndCreateChannel(lib.Sending, "to_coordinator"+*suffix, "")
	// topicW := lib.SetupAndCreateChannel(lib.Sending, "to_worker"+*suffix, "")
	// topicC := lib.SetupAndCreateChannel(lib.Sending, "to_CTD"+*suffix, "")

	// channels for workers

	// subA := lib.SetupAndCreateChannel(lib.Receiving, "to_coordinator"+*suffix, *id)

	fmt.Println("Coordinator: Created topics and subscription handlers.")

	// data := subA.Receiving()

	messagesFromWorkers := make(chan lib.MessageToCoordinator)
	messagesFromCTD := make(chan lib.MessageToCoordinator)
	toWorker := make(chan lib.CoordinatorToWorker)
	toCTD := make(chan lib.MessageToCTD)
	var workerChans []chan lib.CoordinatorToWorker
	// var mainChan chan lib.CoordinatorToWorker

	defer close(toCTD)
	defer close(toWorker)
	defer close(messagesFromWorkers)
	defer close(messagesFromCTD)

	defer func() {
		for i := range workerChans {
			close(workerChans[i])
		}
	}()

	go func() {
		for {
			mess := <-toWorker
			for _, wc := range workerChans {
				go func(wc chan lib.CoordinatorToWorker) {
					wc <- mess
				}(wc)
			}
		}
	}()

	CTDDone := false

	// spawn worker routines

	theCount := 0

	fmt.Println("Creating ", *numWorkers, " workers")
	for i := 0; i < *numWorkers; i++ {
		tmpChan := make(chan lib.CoordinatorToWorker)
		workerChans = append(workerChans, tmpChan)

		go workerRoutine(theCount, graph, *width, tmpChan, messagesFromWorkers, toCTD)
		// activeWorkers = append(activeWorkers, fmt.Sprint("worker", theCount))

		theCount++
	}

	// spawn CTD routine

	go CTDCheckRoutine(theCount, graph, *width, toCTD, messagesFromCTD)
	theCount++

	fmt.Println("Coordinator: Set up background goroutines for receiving and sending messages.")

	// Initial Phase, registering workers and CTD function
	var activeWorkers []string
	haveCTD := false

	// keep track of how many comps each worker has finished when sending idle message
	workerIdleMap := make(map[string]int)

	var result decomp.Decomp // the var for the result

	// continue this loop until at leas one worker and one CTD function have registered

	// TOOD: allow adding workers after starting coordinator later
	// 			right now coord. will wait for CTD to start, and no workers can be added later
	for len(activeWorkers) < *numWorkers || !haveCTD {
		var m lib.MessageToCoordinator
		select {
		case m = <-messagesFromWorkers:
		case m = <-messagesFromCTD:
		}

		// m := <-messages
		// fmt.Println(" Coordinator: Received message: ", m)

		if m.Mtype == lib.Register { // ignore any messages that aren't registering workers
			fmt.Println("Coordinator: Got registration attempt")
			if m.CTD {
				haveCTD = true

				toCTD <- lib.MessageToCTD{
					Mtype: lib.Initialise,
					Init:  initStruct,
				}
				// fmt.Println("Sending input: ", initStruct.Input, "Len: ",
				// initStruct.Input.Edges.Len())

				fmt.Println("Coordinator: Gotten registration from CTD")
			} else {

				activeWorkers = append(activeWorkers, m.Id)

				// time.Sleep(time.Second * 1)
				toWorker <- lib.CoordinatorToWorker{
					Mtype:  lib.Initialise,
					Init:   initStruct,
					Search: chosenSearch,
				}

				// time.Sleep(time.Second * 1)

				// fmt.Println("Sending input: ", initStruct.Input, "Len: ",
				// initStruct.Input.Edges.Len())

				fmt.Println("Coordinator: Gotten registration from Worker, total Workers:",
					len(activeWorkers))
			}

		}

	}

	// fmt.Println("Coordinator: Registered all needed units, starting the overall search process.")

	// message2 := lib.CoordinatorToWorker{
	// 	Mtype: lib.Initialise,
	// 	Init:  initStruct,
	// 	Comp:  decomp.Graph{},
	// }
	// topicW.Write(message2.ToBytes())
	// to_worker.Write([]byte("Message"))

	// var compsFound []decomp.Edges

	// used to determine the next component to decompose
	var compHeap lib.CompHeap

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
		fmt.Println("Len Heap:", compHeap.Len())

		if CTDDone {
			// conPrint(coordinatorOutput, "Coordination worker finished")
			fmt.Println("Coordinator: Coordinator finished")
			break compSearch
		}

		current := compHeap.GetNext()
		// compsFound = append(compsFound, current)

		var blocks []lib.Block
		if current.Len() <= *width {
			curEdges := current.Edges.Edges
			out := lib.CreateBlock(curEdges, curEdges, curEdges,
				curEdges, curEdges.Vertices())
			blocks = append(blocks, out)
			counterSent++
			// create components

			Vertices := make(map[int]*disjoint.Element)
			globalComps, _,
				isolated := graph.GetComponents(curEdges, Vertices)

			for i := range globalComps {
				conn := decomp.Inter(globalComps[i].Vertices(), curEdges.Vertices())

				tmpEdges := append(globalComps[i].Edges.Slice(), isolated...)
				treeComp := decomp.NewEdges(tmpEdges)

				out := lib.CreateBlock(curEdges,
					globalComps[i].Edges,
					treeComp,
					curEdges,
					conn)
				// fmt.Println("Sending block: \n", out)
				counterSent++
				blocks = append(blocks, out)
			}
			toCTD <- lib.MessageToCTD{
				Mtype:  lib.SendBlock,
				Blocks: blocks,
			}

			// conPrint(coordinatorOutput, "Found trivial block for ", current)
			fmt.Println("Found trivial block for ", current)
			// continue compSearch
		} else {
			// Generators :=

			fmt.Println("Coordinator: Starting new search for balanced sep in comp ", current.Edges)

			toWorker <- lib.CoordinatorToWorker{
				Mtype:   lib.SearchGraph,
				Workers: activeWorkers,
				Comp:    current,
				Gen:     decomp.SplitCombin(graph.Edges.Len(), *width, len(activeWorkers), false),
			}

			counter++

		}

		// outer:
		for {

			fmt.Println("Coordinator: Current Counter: ", counter)
			select {

			case m := <-messagesFromCTD:

				if m.Mtype == lib.Result {
					CTDDone = true
					fmt.Println("Coordinator: CTD is doneso, telling workers to stop")
					toWorker <- lib.CoordinatorToWorker{
						Mtype: lib.Result,
					}
					result = m.Decomp
				}

			case m := <-messagesFromWorkers:

				switch m.Mtype {
				case lib.Result:
					CTDDone = true
					fmt.Println("Coordinator: CTD is doneso, telling workers to stop")
					toWorker <- lib.CoordinatorToWorker{
						Mtype: lib.Result,
					}

				// 	break compSearch
				case lib.SendComponent:
					fmt.Println("Coordinator: Adding new stuff to compHeap, old len:", compHeap.Len())

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

					// fmt.Println("New CompHeap Len", compHeap.Len())
				case lib.Idle:
					fmt.Println("Gotten Idle message")
					worker := m.Id
					count := m.Count
					workerIdleMap[worker] = count

					// test if workers done

				}
			default:
			}

			if CTDDone {
				break compSearch
			}
			// check if workers are all idle, continue the search if so
			finishedWorkers := 0
			for i := range workerIdleMap {
				fmt.Println("At worker ", i, " have counter ", workerIdleMap[i])
				if workerIdleMap[i] == counter {
					finishedWorkers++
				}
			}

			fmt.Println("Coordinator: Found ", finishedWorkers, " idle workers")
			if finishedWorkers == len(activeWorkers) {
				fmt.Println("Coordinator: All workers idle")
				continue compSearch
			}

		}

	}
	if !CTDDone {
		if compHeap.HasNext() {
			log.Panicln("Coordinator: exited loop while still comps to be found")
		}

		// tell CTD to wind things down
		fmt.Println("Coordinator: Telling CTD to wind down")
		toCTD <- lib.MessageToCTD{
			Mtype: lib.Exhausted,
		}

		// tell Workers to wind things down
		fmt.Println("Coordinator: Telling Worker to wind down")
		toWorker <- lib.CoordinatorToWorker{
			Mtype: lib.Exhausted,
		}

	} else {
		// tell Workers to wind things down
		fmt.Println("Coordinator: Telling Worker to wind down")
		toWorker <- lib.CoordinatorToWorker{
			Mtype: lib.Result,
		}

	}

	// time.Sleep(time.Second * 1)

	if result.Correct(graph) {

		// fmt.Println("Produced decomp:", decomp)
		fmt.Println("Coordinator: Is Correct:", result.Correct(graph))
		fmt.Println("Coordinator: Produced decomp:", result)
	} else {
		fmt.Println("Coordinator: Failed to find a decomp!")
	}

	fmt.Println("Coordinator: Components found: ", counter, compHeap.HasNext(), counterSent)
	fmt.Println("Coordinator: All done with EVERYTHING!") //" \n Comps found", compsFound)
}
