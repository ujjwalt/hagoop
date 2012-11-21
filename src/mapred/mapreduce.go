// Data types and functions for the core mapreduce package
package mapred

import (
	"/worker"
	"errors"
	"os"
)

// Renaming the generic type - id from objective-c
type id interface{}

// Give input as a map instead of individual key-value pairs to reduce the overhead of function calls
// and accomodate as many key-value pairs as possible in a single function call. How many? That's left
// to the user to decide
type MapInput map[id]id

// Result of the mapreduce computations which includes the result
type MapReduceResult struct {
	Result map[id]id
	// TODO add stats
}

// Intermediate key-value pairs that are emitted
type Intermediate struct {
	Key, Value id
}

// We use a channel to be able to iterate over the values as and when they are available. This allows
// very large lists to be handled since only as many items are sent through the channel as can fit in memory.
type ReduceInput map[id]chan id

// The map function to be provided by the user
type MapFunc func(in <-chan MapInput, emit chan<- Intermediate) error

// the reduce function - provided by the user
type ReduceFunc func(in <-chan reduceInput, out <-chan id) error

// the mapreduce specification object
type Specs struct {
	// A slice of input and output file names
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
	Workers []Worker
}

// Represents a machine in the cluster
type Worker struct {
	// The IP address of the host
	IPAddr string
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

// The MapReduce() call that triggers off the magic
func MapReduce(specObj Specs, mapFunc MapFunc, reduceFunc ReduceFunc) (result MapReduceResult, err error) {
	// TODO Implement the library
}
