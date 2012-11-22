// Represents a worker on a host machine that provides services for doing map and reduce work
package mapred

import (
	"net/rpc"
)

var (
	started bool // The staretd?
)

// The object that will be exposed as a service for map work
type Mapper struct{}

// The object that will be exposed as a service for reduce work
type Reducer struct{}

// Arguments for a map work
type MapArgs struct {
	in      <-chan MapInput
	emit    chan<- Intermediate
	mapFunc MapFunc
}

// Arguments for a reduce work
type ReduceArgs struct {
	in      <-chan reduceInput
	out     <-chan Id
	redFunc ReduceFunc
}

func (m *Mapper) DoMapWork(args MapArgs, err *error) error {

}

func Start() {
	mapper := new(Mapper)
	reducer := new(Reducer)
	rpc.Register(mapper)
}
