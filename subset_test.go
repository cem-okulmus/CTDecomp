package lib

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cem-okulmus/BalancedGo/lib"
)

func TestSubset(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// create universe
	var universe []int
	cardU := rand.Intn(50) + 100

	for i := 0; i <= cardU; i++ {
		universe = append(universe, rand.Intn(100))
	}

	// generate some random sets
	var baseSets [][]int
	num := rand.Intn(100) + 100 // generate between 10 to 20 sets

	for i := 0; i <= num; i++ {
		var temp []int
		card := rand.Intn(100) + 5 // generate sets of size 5 to 15

		for j := 0; j <= card; j++ {
			temp = append(temp, universe[rand.Intn(cardU)])
		}

		baseSets = append(baseSets, temp)
	}

	// create index of all things
	spCheck := SetupSubsetCheck(universe)

	var allIndices []int

	for i := range baseSets {
		allIndices = append(allIndices, i)
		spCheck.AddSet(i, baseSets[i])
	}

	fullSet := universe // adding full for test purposes
	baseSets = append(baseSets, fullSet)

	emptySet := []int{} // adding empty for test purposes
	baseSets = append(baseSets, emptySet)

	// test out subsets
	for i := range baseSets {
		target := baseSets[i] // currently selecetd set, the "target"
		out := spCheck.GetSubSets(target)

		for _, i := range out {
			other := baseSets[i]

			if !lib.Subset(other, target) {
				fmt.Println("Final sets")
				for i := range baseSets {
					fmt.Println(i, " ", baseSets[i])
				}

				fmt.Println("out ", out)

				t.Error("Found a set that's not a subset target: ", target, " other:", other, " index ", i)
			}
		}

		notInOut := lib.Diff(allIndices, out) // all sets that must not be subsets

		for _, i := range notInOut {
			other := baseSets[i]

			if lib.Subset(other, target) {
				fmt.Println("Final sets")
				for i := range baseSets {
					fmt.Println(i, " ", baseSets[i])
				}

				fmt.Println("out ", out)

				t.Error("Found a set that's subset, but not in out: ", target, " other:", other, " index ", i)
			}
		}
	}
}

func GetFixedSets() ([][]int, []int) {
	year, month, day := time.Now().Date()
	rand.Seed(time.Date(year, month, day, 0, 0, 0, 0, time.UTC).UnixNano())

	// create universe
	var universe []int
	cardU := rand.Intn(500) + 100

	for i := 0; i <= cardU; i++ {
		universe = append(universe, rand.Intn(100))
	}

	// generate some random sets
	var baseSets [][]int
	num := rand.Intn(1000) + 100 // generate between 10 to 20 sets

	for i := 0; i <= num; i++ {
		var temp []int
		card := rand.Intn(1000) + 5 // generate sets of size 5 to 15

		for j := 0; j <= card; j++ {
			temp = append(temp, universe[rand.Intn(cardU)])
		}

		baseSets = append(baseSets, temp)
	}

	return baseSets, universe
}

func BenchmarkSetBasedSubset(b *testing.B) {
	baseSets, universe := GetFixedSets()

	b.ResetTimer()
	// create index of all things
	spCheck := SetupSubsetCheck(universe)

	var allIndices []int

	for i := range baseSets {
		allIndices = append(allIndices, i)
		spCheck.AddSet(i, baseSets[i])
	}

	fullSet := universe // adding full for test purposes
	baseSets = append(baseSets, fullSet)

	emptySet := []int{} // adding empty for test purposes
	baseSets = append(baseSets, emptySet)

	// test out subsets
	for i := range baseSets {
		target := baseSets[i] // currently selecetd set, the "target"
		spCheck.GetSubSets(target)

	}
}
