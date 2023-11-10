// The workers are performing the actual search, as instructed by
// the coordinator. If they find balanced separators, they forward both
// the separator and its components (called _blocks_) to CTDchecking.
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
)

// type WorkerState struct {
// 	subgraph
// }

func main() {
	start := time.Now()
	name, err := metadata.Hostname()
	if err != nil {
		log.Fatal("couldn't get host name ", err)
	}

	fmt.Println("worker: Current host: ", name)

	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	flagSet.SetOutput(ioutil.Discard)

	// input flags
	id := flagSet.Int("id", 0, "a positive, non-zero integer indicating the id of this worker")
	inputFlag := flagSet.String("input", "", "Input hypergraph")
	suffix := flagSet.String("suffix", "", "needed to distinguish the PubSub topics used for communication")

	parseError := flagSet.Parse(os.Args[1:])
	if parseError != nil {
		fmt.Print("Parse Error:\n", parseError.Error(), "\n\n")
	}

	// Output usage message if graph and width not specified
	if *id == 0 {
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

	if *inputFlag != "" {
		dat, _ := ioutil.ReadFile(*inputFlag)
		graphParsed, _ := decomp.GetGraph(string(dat))

		fmt.Println("worker:  Parsed Graph", graphParsed)

	}

	workerID := fmt.Sprint(name, *id)
	var input decomp.Graph
	var width int
	var countComps int

	fmt.Println("worker: Current id: ", workerID)

	ToCoord := lib.SetupChannel(lib.Sending, "to_coordinator"+*suffix, "")
	ToCTD := lib.SetupChannel(lib.Sending, "to_CTD"+*suffix, "")
	subB := lib.SetupAndCreateChannel(lib.Receiving, "to_worker"+*suffix, "worker"+workerID)

	fmt.Println("worker: Created topics and subscription handlers.")

	messages := make(chan lib.CoordinatorToWorker, 100)
	toCoord := make(chan lib.MessageToCoordinator, 100)
	found := make(chan lib.MessageToCTD)

	// background goroutine for receiving messages
	go func(messages chan lib.CoordinatorToWorker) {
		subChan := subB.ReceivingTimed(start)
		for true {
			data := <-subChan

			var message lib.CoordinatorToWorker
			message.FromBytes(data)

			messages <- message
		}
	}(messages)

	// background goroutine for sending messages
	go func(toCoord chan lib.MessageToCoordinator) {
		for true {
			m := <-toCoord
			ToCoord.Write(m)
		}
	}(toCoord)

	go func(found chan lib.MessageToCTD) {
		for true {
			m := <-found
			ToCTD.Write(m)
		}
	}(found)

	fmt.Println("worker: Set up background goroutines for receiving and sending messages.")

	// register worker and get split factor

	toCoord <- lib.MessageToCoordinator{
		Mtype: lib.Register,
		Id:    workerID,
	}

	message2 := <-messages

	input = message2.Init.Input
	width = message2.Init.Width

	fmt.Println("worker: registered at coordinator")
	fmt.Println("worker: Current input: ", input, "Len: ", input.Edges.Len())

	// start working until receiving to stop from coordinator

	searchActive := true

	for searchActive {

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
			fmt.Println("worker: Worker is stopping the search!")
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
			log.Println("worker: couldn't find workerID in pool", workerID, " ", message.Workers)
		}

		// fmt.Println("Started work on new comp: ", comp)
		countComps++ // keep track of number of comps received from coordinator

		fmt.Println("worker: Worker started working on received gen")

		// inputs for the search:
		// * generator decomp.genarator
		// * input decomp.graph
		// * comp decomp.graph
		// * toCoord lib.MessageToCoordinator
		// * countComps int
		// * found lib.MessageToCTD

		switch search {
		case lib.BalancedGoSearch:
			SearchBagsBalgo(gen, input, comp.Edges, toCoord, found, workerID, countComps)
		case lib.LogKSearch:
			SearchBagsLogK(gen, input, comp.Edges, toCoord, found, workerID, countComps, comp.Conn, comp.Allowed, width)
		}

		// sent Idle message to coordinator

		fmt.Println("worker: Sent idle notice to coordinator")

		toCoord <- lib.MessageToCoordinator{
			Mtype: lib.Idle,
			Id:    workerID,
			Count: countComps,
			CTD:   false,
		}

	}
}
