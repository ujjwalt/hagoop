// Data types and functions for the core mapreduce package
package mapreduce

import (
    "errors"
    "os"
)

// Renaming the generic type - id from objective-c
type id interface{}

// Give input as a map instead of individual key-value pairs to reduce the overhead of function calls
// and accomodate as many key-value pairs as possible in a single function call. How many? That's left
// to the user to decide
type MapInput map[id]id
type MapReduceResult MapInput

// Intermediate key-value pairs that are emitted
type Intermediate struct {
    Key, Value id
}


// We use a channel to be able to iterate over the values as and when they are available. This allows
// very large lists to be handled since only as many items are sent through the channel as can fit in memory.
type ReduceInput map[id]chan id

// The map function to be provided by the user
type MapFunc func (input chan MapInput, emitter chan Intermediate) error

// the reduce function - provided by the user
type ReduceFunc func(reduceInput)

// the mapreduce specification object
type Specs struct {
    // A slice of input and output files
    InputFiles, OutputFiles []os.File
    // Total number of mappers and reducers to use
    M, R uint
    // Chunk of data to be sent to each host.
    // This is determined on the basis of the type of the underlying filesystem and is unexported
    chunkSize uint64
}

// Represents a machine in the cluster
type Host struct {
    // The IP address and the port that the host is listening on
    IPAddr string
    Port uint16
}

// The MapReduce() call that triggers off the magic
func MapReduce(specObj Specs, map MapFunc, reduce ReduceFunc) (result MapReduceResult, err error) {
    // TODO Implement the library
}
