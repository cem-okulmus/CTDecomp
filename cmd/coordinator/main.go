// The coordinator is responsible for managing the overall computation,
// from start to finish. It keeps a list of all components so far considered,
// and it also keeps a list of all active workers. The final output is delived back
// to the coordinator and output by it.
package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"cloud.google.com/go/compute/metadata"
	decomp "github.com/cem-okulmus/BalancedGo/lib"
	"github.com/cem-okulmus/CTDecomp/lib"
	"github.com/cem-okulmus/disjoint"
)

// TODO:
//   * first create a simple test to see if the PSCHannel type can handle both send and receive
//   * draft how the coordinator assigns numbers to workers, find a UID for workers, and to which
//   * topics and subscriptions to send things

func main() {
	start := time.Now()
	name, err := metadata.Hostname()
	if err != nil {
		log.Fatal("couldn't get host name ", err)
	}

	fmt.Println("Coordinator: Current host: ", name)

	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	flagSet.SetOutput(ioutil.Discard)

	// input flags
	input := flagSet.String("input", "",
		"Input hypergraph")
	width := flagSet.Int("width", 0,
		"the width to look for")
	id := flagSet.String("id", "subA",
		"name of subscription used by coordinator (new one recommended for each run")
	suffix := flagSet.String("suffix", "",
		"needed to distinguish the PubSub topics used for communication")
	searchFlag := flagSet.Bool("useLogK", false,
		"If provided, the distributed search will use LogK bags")

	parseError := flagSet.Parse(os.Args[1:])
	if parseError != nil {
		fmt.Print("Parse Error:\n", parseError.Error(), "\n\n")
	}

	// Output usage message if graph and width not specified
	if *width == 0 || *input == "" {
		out := fmt.Sprint("Usage of worker: ")
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

	dat, err := ioutil.ReadFile(*input)
	graph, _ := decomp.GetGraph(string(dat))

	fmt.Println("Coordinator: Parsed input: ", graph, "Len: ", graph.Edges.Len())

	var initStruct lib.Init
	initStruct.Input, initStruct.Width = graph, *width

	// Generate the needed topics
	lib.SetupAndCreateChannel(lib.Sending, "to_coordinator"+*suffix, "")
	topicW := lib.SetupAndCreateChannel(lib.Sending, "to_worker"+*suffix, "")
	topicC := lib.SetupAndCreateChannel(lib.Sending, "to_CTD"+*suffix, "")
	subA := lib.SetupAndCreateChannel(lib.Receiving, "to_coordinator"+*suffix, *id)

	fmt.Println("Coordinator: Created topics and subscription handlers.")

	// data := subA.Receiving()

	messages := make(chan lib.MessageToCoordinator, 100)
	toWorker := make(chan lib.CoordinatorToWorker)
	toCTD := make(chan lib.MessageToCTD)

	CTDDone := false

	// background goroutine for receiving messages
	go func(messages chan lib.MessageToCoordinator) {
		fmt.Println("Coordinator: starting messages goroutine")
		subChan := subA.ReceivingTimed(start)
		for true {
			// fmt.Println("Waiting on next data")
			data := <-subChan

			// fmt.Println("Gotten message in main, ", data)

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
		for true {
			m := <-toWorker
			topicW.Write(m)
		}
	}(toWorker)

	go func(toCTD chan lib.MessageToCTD) {
		for {
			m := <-toCTD
			topicC.Write(m)
		}
	}(toCTD)

	fmt.Println("Coordinator: Set up background goroutines for receiving and sending messages.")

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
				haveWorkers = true

				activeWorkers = append(activeWorkers, m.Id)

				toWorker <- lib.CoordinatorToWorker{
					Mtype:  lib.Initialise,
					Init:   initStruct,
					Search: chosenSearch,
				}

				// fmt.Println("Sending input: ", initStruct.Input, "Len: ",
				// initStruct.Input.Edges.Len())

				fmt.Println("Coordinator: Gotten registration from Worker, total Workers:",
					len(activeWorkers))
			}

		}

	}

	time.Sleep(time.Second * 1)

	fmt.Println("Coordinator: Registered all needed units, starting the overall search process.")

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
			// fmt.Println("Found trivial block for ", current)
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
				fmt.Println("Coordinator: All workers idle")
				continue compSearch
			}

			m := <-messages

			switch m.Mtype {
			// case lib.Result:
			// 	CTDDone = true
			// 	fmt.Println("CTD is doneso, telling workers to stop")
			// 	toWorker <- lib.CoordinatorToWorker{
			// 		Mtype: lib.Result,
			// 	}

			// 	break compSearch
			case lib.SendComponent:
				// fmt.Println("Adding new stuff to compHeap, old len:", compHeap.Len())

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
				// fmt.Println("Gotten Idle message")
				worker := m.Id
				count := m.Count
				workerIdleMap[worker] = count

				// test if workers done

			}

		}

	}
	if !CTDDone {
		if compHeap.HasNext() {
			log.Panicln("exited loop while still comps to be found")
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

	time.Sleep(time.Second * 1)

	fmt.Println("Coordinator: Components found: ", counter, compHeap.HasNext(), counterSent)
	fmt.Println("Coordinator: All done with EVERYTHING!") //" \n Comps found", compsFound)
}
