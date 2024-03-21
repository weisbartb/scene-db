package mysql

import "strings"

var defaultStopWords = map[string]struct{}{
	"about": {},
	"are":   {},
	"com":   {},
	"for":   {},
	"from":  {},
	"how":   {},
	"that":  {},
	"the":   {},
	"this":  {},
	"was":   {},
	"what":  {},
	"when":  {},
	"where": {},
	"who":   {},
	"will":  {},
	"with":  {},
	"und":   {},
	"www":   {},
}

// FTSCleanup cleans up strings for full text
func FTSCleanup(value string) string {
	value = strings.Trim(value, "+-<>~*,$")
	var allowedWords []string
	for _, w := range strings.Fields(value) {
		// Need at least 3 characters for partial FT match (trigrams)
		if len(w) <= 2 {
			continue
		}
		if _, ok := defaultStopWords[strings.ToLower(w)]; ok {
			continue
		}
		allowedWords = append(allowedWords, w)
	}
	return strings.Join(allowedWords, " ")
}

func FTSWordBreak(value string) string {
	return "+" + strings.Join(strings.Split(FTSCleanup(value), " "), " +")
}
