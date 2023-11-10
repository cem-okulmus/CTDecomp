package lib

import (
	"github.com/cem-okulmus/BalancedGo/lib"
)

// SubsetCheck implements a way to quickly determine all intersecting
// subsets of an input set amonng a previously indexed list of sets
type SubsetCheck struct {
	Universe []int
	spCheck  SupersetCheck
}

func SetupSubsetCheck(universe []int) SubsetCheck {
	var output SubsetCheck

	output.spCheck = SetupSupersetCheck()
	output.Universe = universe

	return output
}

func (i *SubsetCheck) AddSet(name int, vertices []int) {
	// if len(vertices) == 0 {
	// 	return
	// }

	// fmt.Println("Adding ", name, "to SubsetCheck")
	i.spCheck.AddSet(name, lib.Diff(i.Universe, vertices))
	// fmt.Println("Current allSets ", i.spCheck.allSets)
}

func (i SubsetCheck) GetSubSets(vertices []int) []int {
	if len(vertices) == 0 {
		return []int{}
	}
	result := lib.Diff(i.Universe, vertices)

	if len(result) == 0 {
		return i.spCheck.allSets
	}

	return i.spCheck.GetSuperSets(result)
}
