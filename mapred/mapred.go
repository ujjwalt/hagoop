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

// The MapReduce() call that triggers off the magic
func MapReduce(specs Specs) (result MapReduceResult, err error) {
	// Validate the specs
	if err = validateSpecs(specs); err != nil {
		return
	}

	// Determine the total size of the all input files and start splitting it. Assign each split to a worker as a map task
	// and assign a reduce task to the rest of the workers in a ratio of M:R
	totalWorkers := uint(len(specs.Workers))     // total workers
	clients := make([]*rpc.Client, totalWorkers) // clients returned by rpc calls
	splits := make([]os.File, specs.M)           // Splits of files
	calls := make([]*rpc.Call, totalWorkers)     // calls returned by rpc calls
	unit := totalWorkers / (specs.M + specs.R)   // unit worker for ratios
	ans := make([]bool, totalWorkers)            // answers received from machines wether they are idle or not?
	m := int(unit * specs.M)                     // number of map workers
	r := totalWorkers - m                        // number of reduce workers
	dialFailures := 0                            // number of dial failure - max limit is
	myAddr := myTCPAddr()

	for i := 0; i < m+r; i++ {
		clients[i], err = rpc.DialHTTP("tcp", specs.Workers[i].Addr.String())
		if err != nil {
			dialFailures++
			if dialFailures > totalWorkers/2 {
				err = fmt.Errorf("Number of dial failures is more than %d", totalWorkers/2)
				return // Return if number of dial failures are too many
			}
		}
		calls[i] = clients[i].Go(idleService, myAddr, &ans[i], nil) // Call the service method to ask if the host is idle
	}

	// Accept the first m map workers which reply yes
	var done [m]bool
	signalCompletion := make(chan bool, 2) // to signal completion of accept for m & r
	accept := func(n int) {
		var i, acceptedWorkers = 0, 0
		// loop unitl we have covered all clients or got m accepted workers
		for ; acceptedWorkers < n; i = (i + 1) % totalWorkers {
			select {
			case <-calls[i].Done && !done[i]:
				// Assign the task
				switch n {
				case m:
					specs.Workers[acceptedWorkers].task = MapTask{idle, split[acceptedWorkers], specs.R}
				case r:
					specs.Workers[acceptedWorkers].task = ReduceTask{idle}
				}
				specs.Workers[acceptedWorkers].client = clients[i] // Assign the client
				done[i] = true                                     // mark caller as accepted
				acceptedWorkers++
			default:
			}
		}
		if acceptedWorkers == n {
			signalCompletion <- true
		} else {
			signalCompletion <- false
		}
	}
	go accept(m)
	go accept(r)
	if !<-signalCompletion || !<-signalCompletion {
		err = fmt.Errorf("Could not gather enough hosts to work. M: %d, R: %d", m, r)
		return
	}

	// Now that we have

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
