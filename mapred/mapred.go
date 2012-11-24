// Data types and functions for the core mapreduce package
package mapred

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
)

// State of a worker
type state int

// Type of this host
type wType int

// State of a host
const (
	idle state = iota
	inProgress
	completed
	failed
)

const (
	master wType = iota
	mapper
	reducer
)

const (
	port          = ":2607" // All communication happens over this port
	idleService   = "Service.Idle"
	MapService    = "Service.Map"
	ReduceService = "Service.Reduce"
)

var (
	started    bool         // Indicates wether the service has staretd
	l          net.Listener // The service listener
	masterAddr net.TCPAddr  // Address of the master
	task       id           // Current task
	w          *Worker      //The object to use as worker
)

// Default chunk size is 16MB
const defaultChunkSize uint64 = 1 << 14

// Renaming the generic type - id from objective-c
type id interface{}

// Give input as a map instead of individual key-value pairs to reduce the overhead of function calls
// and accomodate as many key-value pairs as possible in a single function call. How many? That's left
// to the user to decide
type MapInput map[id]id

// Result of the mapreduce result - same as MapInput
type MapReduceResult MapInput

// Intermediate key-value pairs that are emitted
type Intermediate struct {
	Key, Value id
}

// We use a channel to be able to iterate over the values as and when they are available. This allows
// very large lists to be handled since only as many items are sent through the channel as can fit in memory.
type ReduceInput map[id]chan id

// the mapreduce specification object
type Specs struct {
	// A slice of input and output file paths
	InputFiles, OutputFiles []string
	// Total number of mappers and reducers to use
	M, R uint
	// Size of each split.
	// This should determined on the basis of the type of the underlying filesystem.
	ChunkSize uint64
	// The length of this slice should be >= M+R
	Workers []worker
}

// Represents a machine in the cluster
type worker struct {
	// The IP address of the host
	Addr net.TCPAddr
	// Task assigned to the worker
	task id
	// RPC cient for communication
	client *rpc.Client
	// Answers to queries
	ans map[id]id // TODO type needs to be changed later
}

// Task consists of the current state of the task, the split assigned to it if it is a map task
type MapTask struct {
	// State of the task
	state state
	// Split assigned to this task - valid only if it is a map task.
	split os.File
	// Same as R, used by map workers
	r uint
}

type ReduceTask struct {
	// State of the task
	state state
}

// Worker is an inteface that defines the behaviour required by the application.
// It is important that these methods are defined on a pointer for the rpc mechanism to work.
// A Worker object is passed to Start() to start providing the service
type Worker interface {
	// Read takes as input a byte slice, converts it into a MapInput and returns it back
	Read([]byte) (MapInput, error)
	// Map is the map function which takes a channel of MapInput and emits
	// the Intermediate key-value pairs for every key-value pair of MapInput through the channel
	// returned by it.
	Map(<-chan MapInput) (<-chan Intermediate, error)
	//Reduce is the reduce function which takes a channel of ReduceInput and an output channel to transfer
	Reduce(<-chan ReduceInput) (<-chan id, error)
}

type Service struct{}
