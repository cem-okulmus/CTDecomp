package lib

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cem-okulmus/BalancedGo/lib"
)

func TestSuperset(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// generate some random sets
	var baseSets [][]int
	num := rand.Intn(100) + 100 // generate between 10 to 20 sets

	for i := 0; i <= num; i++ {
		var temp []int
		card := rand.Intn(100) + 5 // generate sets of size 5 to 15

		for j := 0; j <= card; j++ {
			temp = append(temp, rand.Intn(100))
		}

		baseSets = append(baseSets, temp)
	}

	// create index of all things
	spCheck := SetupSupersetCheck()

	var allIndices []int

	for i := range baseSets {
		allIndices = append(allIndices, i)
		spCheck.AddSet(i, baseSets[i])
	}

	emptySet := []int{} // adding empty for test purposes
	baseSets = append(baseSets, emptySet)

	// test if all supersets can be found

	for i := range baseSets {
		target := baseSets[i] // currently selecetd set, the "target"
		out := spCheck.GetSuperSets(target)

		for _, i := range out {
			other := baseSets[i]

			if !lib.Subset(target, other) {
				fmt.Println("Final sets")
				for i := range baseSets {
					fmt.Println(i, " ", baseSets[i])
				}

				fmt.Println("out ", out)

				t.Error("Found a set that's not a superset target: ", target, " other:", other, " index ", i)
			}
		}

		notInOut := lib.Diff(allIndices, out) // all sets that must not be subsets

		for _, i := range notInOut {
			other := baseSets[i]

			if lib.Subset(target, other) {
				fmt.Println("Final sets")
				for i := range baseSets {
					fmt.Println(i, " ", baseSets[i])
				}

				fmt.Println("out ", out)

				t.Error("Found a set that's a superset but not in out: ", target, " other:", other, " index ", i)
			}
		}
	}
}

func BenchmarkSetBasedSuperSet(b *testing.B) {
	baseSets, universe := GetFixedSets()

	b.ResetTimer()
	// create index of all things
	spCheck := SetupSupersetCheck()

	var allIndices []int

	for i := range baseSets {
		allIndices = append(allIndices, i)
		spCheck.AddSet(i, baseSets[i])
	}

	fullSet := universe // adding full for test purposes
	baseSets = append(baseSets, fullSet)

	emptySet := []int{} // adding empty for test purposes
	baseSets = append(baseSets, emptySet)

	// test if all supersets can be found

	for i := range baseSets {
		target := baseSets[i] // currently selecetd set, the "target"
		spCheck.GetSuperSets(target)

	}
}
