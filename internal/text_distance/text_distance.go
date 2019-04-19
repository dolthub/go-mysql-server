package text_distance

import (
	"github.com/texttheater/golang-levenshtein/levenshtein"
	"reflect"
)

// FindSimilarName returns the best match by Levenshtein distance
// for the src string among the specified names.
func FindSimilarName(names []string, src string) string {
	minDistance := -1
	var bestMatch string
	s := []rune(src)
	for _, name := range names {
		r := []rune(name)
		dist := levenshtein.DistanceForStrings(r, s, levenshtein.DefaultOptions)
		if dist == 0 {
			// Perfect match, shouldn't happen if this is used for errors
			return name
		}

		if minDistance == -1 || dist < minDistance {
			minDistance = dist
			bestMatch = name
		}
	}

	return bestMatch
}

// FindSimilarNameFromMap does the same as FindSimilarName but taking a map instead
// of a string array as first argument.
func FindSimilarNameFromMap(names interface{}, src string) string {
	rnames := reflect.ValueOf(names)
	if rnames.Kind() != reflect.Map {
		panic("Implementation error: non map used as first argument " +
			"to FindSimilarNameFromMap")
	}

	t := rnames.Type()
	if t.Key().Kind() != reflect.String {
		panic("Implementation error: non string key for map used as " +
			  "first argument to FindSimilarNameForMap")
	}

	var namesList []string
	for _, kv := range rnames.MapKeys() {
		namesList = append(namesList, kv.String())
	}

	return FindSimilarName(namesList, src)
}
