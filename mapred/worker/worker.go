package worker

import (
	"github.com/ujjwalt/hagoop/mapred"
)

var (
	started bool // The staretd?
)

type Mapper struct{}

type MapArgs struct {
	mapred.Map
}

func (m *Mapper) DoMapWork(args MapArgs, nilPointer *int) error {

}

func Start() {

}
