package lib

import (
	"github.com/cem-okulmus/BalancedGo/lib"
)

// SupersetCheck implements a way to quickly determine all intersecting
// supersets of an input set amonng a previously indexed list of sets
type SupersetCheck struct {
	List    map[int][]int
	Subsets map[int][]int
	allSets []int
}

func SetupSupersetCheck() SupersetCheck {
	var output SupersetCheck

	output.List = make(map[int][]int)
	output.Subsets = make(map[int][]int)

	return output
}

func (i *SupersetCheck) AddSet(name int, vertices []int) {
	_, ok := i.Subsets[name]

	if ok {
		// fmt.Println("Not adding empty or dublicate")
		return
	}

	for _, e := range vertices {
		i.List[e] = append(i.List[e], name)
	}

	i.Subsets[name] = vertices
	i.allSets = append(i.allSets, name)
}

func (i *SupersetCheck) GetSuperSets(vertices []int) []int {
	var output []int

	if len(vertices) == 0 {
		return i.allSets
	}
	first := true

	for _, e := range vertices {
		temp := i.List[e]
		if first { // copy the contents for initial list
			output = temp
			first = false
		} else {
			output = lib.Inter(output, temp)
		}
	}

	return output
}

func (i *SupersetCheck) GetVertices(pos int) []int {
	return i.Subsets[pos]
}

func (i *SupersetCheck) GetVerticesAll(positions []int) [][]int {
	var output [][]int

	for _, p := range positions {
		output = append(output, i.GetVertices(p))
	}

	return output
}
