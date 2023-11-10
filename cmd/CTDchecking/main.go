// CTDcecking is responsible for actually finding a decomposition.
// It receives _blocks_ from the workers, and continously checks if it has enough
// blocks to build a complete CTD for the given input hypergraph.
// It returns its output to the coordinator.
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

/* TODOs + Work log
* maybe have the vertex and edge view combined into one datastructure?
	[STATUS] : Done, 28.11, Block now has both sep,comp and Head, Tail ``views''

* Implement creation of the HeadIndex, indicating how to quickly find related
  blocks in the ODAG.
	[STATUS] : Not relevant right now, see below

* Make coordinator detect duplicate components (using hash)
	[Status]: Done, 30.11, compHeap now checks for duplicates

* Make the ODAG detect duplicate components
|	[Status]: Done, 1.12, use a map to detect for possible duplicate entries
└-> * Write a hash function for blocks
		[Status]: Done, 30.11, summing up the hashes of the three int slices

* Get the ODAG to be more reliable: seems like the arcs vary strangely (might need some investigation)
|	[Status]: Not Fixed, Instead decided to get rid of the ODAG for now to focus on correctness
└-> * ODAG must allow for blocks to check for ancestors as well as predeessor relationship
	|
	└-> * Add subset and intersect tests

* Basis check is still broken
	[Status]: Fixed, 1.12, use the concept of treecomp to detect which edges are covered by a block


*/

type BlockOut struct {
	Sep       string
	Comp      string
	TreeComp  string
	HeadIndex string
	Head      string
	Tail      string
	Context   string
}

func (b *BlockOut) FromBlock(block lib.Block) {
	b.Sep = fmt.Sprint(block.Sep)
	b.Comp = fmt.Sprint(block.Comp)
	b.TreeComp = fmt.Sprint(block.TreeComp)
	b.HeadIndex = fmt.Sprint(block.HeadIndex)
	b.Head = fmt.Sprint(block.Head)
	b.Tail = fmt.Sprint(block.Tail)
	b.Context = fmt.Sprint(block.Context)
}

// var receivedBlocks []BlockOut

func main() {
	// defer func() {
	// 	outSring, _ := json.Marshal(receivedBlocks)
	// 	err := os.WriteFile("receivedBlocks.json", outSring, 0644)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()

	start := time.Now()
	name, err := metadata.Hostname()
	if err != nil {
		log.Fatal("couldn't get host name ", err)
	}

	fmt.Println("CTDChecking: Current host: ", name)

	flagSet := flag.NewFlagSet("", flag.ContinueOnError)
	flagSet.SetOutput(ioutil.Discard)

	// input flags
	id := flagSet.String("id", "", "a positive, non-zero integer indicating the id of this worker")
	inputFlag := flagSet.String("input", "", "Input hypergraph")
	suffix := flagSet.String("suffix", "", "needed to distinguish the PubSub topics used for communication")

	parseError := flagSet.Parse(os.Args[1:])
	if parseError != nil {
		fmt.Print("Parse Error:\n", parseError.Error(), "\n\n")
	}

	// Output usage message if graph and width not specified
	if *id == "" || *inputFlag == "" {
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

		fmt.Println("CTDChecking:  Parsed Graph", graphParsed)

	}

	CTDID := fmt.Sprint(name, *id)
	var input decomp.Graph

	fmt.Println("CTDChecking: Current id: ", CTDID)

	ToCoord := lib.SetupChannel(lib.Sending, "to_coordinator"+*suffix, "")
	subB := lib.SetupAndCreateChannel(lib.Receiving, "to_CTD"+*suffix, CTDID)

	fmt.Println("CTDChecking: Created topics and subscription handlers.")

	messages := make(chan lib.MessageToCTD, 100)
	gottenInput := make(chan lib.Init)
	toCoord := make(chan lib.MessageToCoordinator)

	// background goroutine for receiving messages
	go func(messages chan lib.MessageToCTD) {
		subChan := subB.ReceivingTimed(start)
		for true {
			data := <-subChan

			var message lib.MessageToCTD
			message.FromBytes(data)

			if message.Mtype == lib.Initialise {
				gottenInput <- message.Init
				continue
			}

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
		for true {
			m := <-toCoord
			ToCoord.Write(m)
		}
	}(toCoord)

	fmt.Println("CTDChecking: Set up background goroutines for receiving and sending messages.")

	// register to CTD

	toCoord <- lib.MessageToCoordinator{
		Mtype: lib.Register,
		Id:    CTDID,
		CTD:   true,
	}

	// message := <-messages
	init := <-gottenInput
	input = init.Input

	fmt.Println("CTDChecking: Registered with coordinator.")
	fmt.Println("CTDChecking: Current input: ", input, "Len: ", input.Edges.Len())

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
				// fmt.Println("Sending Done Message")
				toCoord <- lib.MessageToCoordinator{
					Mtype: lib.Result,
					Id:    CTDID,
					CTD:   true,
				}

				time.Sleep(time.Second * 1)

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

	// fmt.Println("Nodes at end:", odag)

	_, decomp := odag.GetDecomp(input)
	if odag.IsSatisfied() && decomp.Correct(input) {

		// fmt.Println("Produced decomp:", decomp)
		fmt.Println("CTDChecking: Is Correct:", decomp.Correct(input))
		fmt.Println("CTDChecking: Produced decomp:", decomp)
	} else {
		fmt.Println("CTDChecking: Failed to find a decomp!")
	}

	fmt.Println("CTDChecking: Found a decomposition in the OADG?: ", odag.IsSatisfied(), "num indexed nodes", len(odag.Nodes))
}
