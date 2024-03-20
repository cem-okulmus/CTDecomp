// A set comparison check based on representing sets as bitstrings
package lib

import "math/big"

// SubsetBitSet will determine linear time the set of subsets of another set in a given family of sets. All sets in question must have a shared universe of elements.
type SetCompBitSet struct {
	Universe    []int // the underlying (overlying?) universe of the sets
	universeMap map[int]int
	sets        map[int]*big.Int
}

func SetupSetCompBitSet(universe []int) SetCompBitSet {
	var output SetCompBitSet

	output.Universe = universe
	output.universeMap = make(map[int]int)

	for i, v := range output.Universe {
		output.universeMap[v] = i
	}

	output.sets = make(map[int]*big.Int)

	return output
}

// func (i *SetCompBitSet) BigtoSlice(big *big.Int) []int {
// 	var output []int

// 	for x := 0; x < len(i.Universe); x++ {
// 		if big.Bit(x) == 1 {
// 			output = append(output, i.Universe[x])
// 		}
// 	}

// 	return output
// }

func (i *SetCompBitSet) SliceToBig(slice []int) *big.Int {
	var output *big.Int
	output = big.NewInt(0)

	for _, v := range slice {
		output.SetBit(output, i.universeMap[v], 1)
	}

	return output
}

var output = big.NewInt(0)

func BigSubset(a *big.Int, b *big.Int) bool {
	output = output.And(a, b)

	return 0 == output.Cmp(a)
}

func (i *SetCompBitSet) AddSet(name int, vertices []int) {
	i.sets[name] = i.SliceToBig(vertices)
}

func (i SetCompBitSet) GetSubSets(vertices []int) []int {
	var output []int
	bitRep := i.SliceToBig(vertices)

	for k, v := range i.sets {
		if BigSubset(v, bitRep) {
			output = append(output, k)
		}
	}

	return output
}

func (i SetCompBitSet) GetSuperSets(vertices []int) []int {
	var output []int
	bitRep := i.SliceToBig(vertices)

	for k, v := range i.sets {
		if BigSubset(bitRep, v) {
			output = append(output, k)
		}
	}

	return output
}
