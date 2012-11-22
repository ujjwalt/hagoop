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

// State of a host
const (
	idle state = iota
	inProgress
	completed
	failed
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
	// The workers can either be supplied as a slice of worker structs or as a network address on which a "Worker.Idle" request
	// is broadcasted. Sorkers which reply in the affirmative are assigned. Setting both a network address as well as a host
	// slice containing one or more workers is an error.
	Network string
	// The length of this slice should be >= M+R
	Workers []worker
}

// Represents a machine in the cluster
type worker struct {
	// The IP address of the host
	Addr net.TCPAddr
	// Task assigned to the worker
	task id
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

// The MapReduce() call that triggers off the magic
func MapReduce(specs Specs) (result MapReduceResult, err error) {
	// Validate the specs
	if err = validateSpecs(specs); err != nil {
		return
	}
	if specs.Network != "" {
		// Populate the hosts slice of specs based on the network
		var hostChan chan worker
		if hostChan, err = populateHosts(specs); err != nil {
			return
		}
		for w := range hostChan {
			// loop until hostChan is closed and we have enough workers
			specs.Workers = append(specs.Workers, w)
		}
	}

	// Determine the total size of the all input files and start splitting it. Assign each split to a worker as a map task
	// and assign a reduce task to the rest of the workers in a ratio of M:R
	totalWorkers := uint(len(specs.Workers))     // total workers
	clients := make([]*rpc.Client, totalWorkers) // clients returned by rpc calls
	calls := make([]*rpc.Call, totalWorkers)     // calls returned by rpc calls
	unit := totalWorkers / (specs.M + specs.R)   // unit worker for ratios
	ans := make([]bool, totalWorkers)
	m := int(unit * specs.M) // number of map workers
	r := int(unit * specs.R) // number of reduce workers
	dialFailures := 0        // number of dial failure - max limit is
	myAddr := myTCPAddr()
	for i := 0; i < m+r; i++ {
		clients[i], err = rpc.DialHTTP("tcp", specs.Workers[i].Addr.String())
		if err != nil {
			dialFailures++
			if dialFailures > m/2 {
				err = fmt.Errorf("Number of dial failures is more than %d", m/2)
				return // Return if number of dial failures are too many
			}
		}
		calls[i] = clients[i].Go(idleService, myAddr, &ans[i], nil) // Call the service method to ask if the host is idle
	}

	err = nil // Reset err
	return
}

func validateSpecs(specs Specs) error {
	// Only one of them should be set
	validNetwork := bToi(specs.Network != "")
	validWorkers := bToi(specs.Workers != nil)
	if validNetwork^validWorkers != 0 {
		return fmt.Errorf("Specs object should have either a network address: %s or a slice of hosts: %v, not both!", specs.Network, specs.Workers)
	}
	// If []workers is set then M+R >= num of workers
	if validWorkers != 0 && specs.M+specs.R < uint(len(specs.Workers)) {
		return fmt.Errorf("M: %d & R: %d are less than the num of hosts: %d", specs.M, specs.R, len(specs.Workers))
	}
	// If the chunk size specified is less than 16Mb then set it to 16Mb
	if specs.ChunkSize < defaultChunkSize {
		specs.ChunkSize = defaultChunkSize
	}
	return nil
}

func populateHosts(specs Specs) (hostChan chan worker, err error) {
	// Populate workers from specs.Network by broadcasting an "Worker.Idle" message until the num of workers is >= M+R
	// TODO implement later
	// hostChan := make(chan worker, specs.M+specs.R)
	err = fmt.Errorf("Providing Network is not supported by the package right now")
	return
}

// Converts a bool to int
func bToi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func myTCPAddr() net.TCPAddr {
	// Return own ip address used for the connections
	return net.TCPAddr{}
}
