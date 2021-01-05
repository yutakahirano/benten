package benten

import (
	"strings"

	"golang.org/x/text/unicode/norm"
)

// Normalize normalizes given the string and returns it.
func Normalize(s string) string {
	r := strings.NewReplacer(
		"\u0301", "", // Combining Acute Accent
		"\u0307", "", // Combining Dot Above
		"\u0309", "", // Combining Hook Above
		"\u0300", "", // Combining Grave Accent
		"\u0302", "", // Combining Circumflex Accent
		"\u0303", "", // Combining Tilde
		"\u0304", "", // Combining Macron
		"\u0306", "", // Combining Breve
		"\u0308", "", // Combining Diaeresis
		"\u030a", "", // Combining Ring Above
		"\u030b", "", // Combining Double Acute Accent
		"\u030c", "", // Combining Caron
		"\u031b", "", // Combining Horn
		"\u0323", "", // Combining Dot Below
		"\u0326", "", // Combining Comma Below
		"\u0327", "", // Combining Cedilla
		"\u0328", "", // Combining Ogonek

		"\u00e6", "ae",
		"\u0133", "ij",
		"\u0153", "oe",
		"\u00df", "ss",
	)
	return r.Replace(strings.ToLower(norm.NFKD.String(s)))
}
