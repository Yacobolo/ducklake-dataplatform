package sitegen

import "regexp"

var directiveAttrRE = regexp.MustCompile(`([A-Za-z0-9_-]+)=(".*?"|'.*?'|[^\s]+)`)
