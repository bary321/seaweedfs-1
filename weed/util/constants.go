package util

import (
	"fmt"
)

var (
	VERSION = fmt.Sprintf("%s %d.%d", sizeLimit, 2, 00)
	COMMIT  = ""
)

func Version() string {
	return VERSION + " " + COMMIT
}
