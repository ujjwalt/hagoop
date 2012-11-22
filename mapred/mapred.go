// Data types and functions for the core mapreduce package
package mapred

import (
	"errors"
	"fmt"
	"github.com/ujjwalt/hagoop/mapred/worker"
	"io"
	"net"
	"os"
)

// Defines wether a worker is a mapworker, reduce worker or the master
const (
	mapWorker = iota
	reduceWorker
	master
)

// State of a host
const (
	idle = iota
	inProgress
	completed
	failed
)

// Renaming the generic type - Id from objective-c
type Id interface{}

// Give input as a map instead of indivIdual key-value pairs to reduce the overhead of function calls
// and accomodate as many key-value pairs as possible in a single function call. How many? That's left
// to the user to decIde
type MapInput map[Id]Id

// Result of the mapreduce computations which includes the result
type MapReduceResult struct {
	Result map[Id]Id
}

// Intermediate key-value pairs that are emitted
type Intermediate struct {
	Key, Value Id
}

// We use a channel to be able to iterate over the values as and when they are available. This allows
// very large lists to be handled since only as many items are sent through the channel as can fit in memory.
type ReduceInput map[Id]chan Id

// The map function to be provIded by the user
type MapFunc func(in <-chan MapInput, emit chan<- Intermediate) error

// The Reader should read in bytes of chunksize and returns a channel of MapInput
type Reader interface {
	Read(chunk []byte) (chan MapInput, error)
}

// The Writer should read in bytes of chunksize and returns a channel of ReduceInput
type Writer interface {
	Write(chunk []byte) error
}

// the reduce function - provIded by the user
type ReduceFunc func(in <-chan reduceInput, out <-chan Id) error

// the mapreduce specification object
type Specs struct {
	// A slice of input and output file paths
	InputFiles, OutputFiles []string
	// Total number of mappers and reducers to use
	M, R uint
	// Chunk of data to be sent to each host.
	// This should determined on the basis of the type of the underlying filesystem and is unexported
	ChunkSize uint64
	/* The workers can either be supplied as a slice of Worker structs or as a network address on which a Worker equest
		is broadcasted. Workers which reply in the affirmative are assigned. Setting both a network address as well as a host
	    slice containing one or more Workers is an error!
	*/
	Network string
	// The length of this slice is >= M+R
	Workers []Worker

	// Functions
	inputReader Reader
	mapFunc     MapFunc
	reduceFunc  ReduceFunc
}

// Represents a machine in the cluster
type Worker struct {
	// The IP address of the host
	IPAddr net.Addr
	// Port on which the host is listening
	Port uint16
	// Tasks assigned to the host
	tasks []Task
}

type Mapper interface {
	LocationsAndSizes() (locations []string, sizes []uint64)
}

type Task struct {
	// State of the host
	state int
}

// default chunk size is 64MB
const defaultChunkSize = 1 << 16

// The MapReduce() call that triggers off the magic
func MapReduce(specs Specs) (result MapReduceResult, err error) {
	// ValIdate the specs
	if err = valIdateSpecsObject(specs); err != nil {
		return
	}
	if specs.Network {
		// Populate the hosts slice of specs based on the network
		hostChan := make(chan Worker, specs.M+specs.R)
		if err = populateHosts(specs, hostChan); err != nil {
			return
		}
		for w := range hostChan {
			// loop until hostChan is closed and we have enough workers
			append(specs.Workers, w)
		}
	}

	// Assign the work to all the hosts
}

func valIdateSpecsObject(specs Specs) error {
	// Only one of them should be set
	if !(specs.Network ^ specs.Workers) {
		return fmt.Errorf("Specs object should have either a network address: %s or a slice of hosts: %v, not both!", specs.Network, specs.Workers)
	}
	// If []Workers is set then M+R >= num of Workers
	if specs.Workers && specs.M+specs.R < len(specs.Workers) {
		return fmt.Errorf("M: %d & R: %d are less than the num of hosts: %d", specs.M, specs.R, len(specs.Workers))
	}
	return nil
}

func populateHosts(specs Specs, hostChan chan Worker) error {
	// Populate Workers from specs.Network until the num of workers is >= M+R
}
