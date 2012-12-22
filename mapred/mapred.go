// mapred is a mapreduce implmentation for fast, scalable and efficient distributed systems in go
package mapred

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path"
	"strconv"
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
//const defaultChunkSize int64 = 1 << 14

// Renaming the generic type - id from Objective-C.
// This is only for less typing - lazy me!
type Id interface{}

// Give input as a map instead of individual key-value pairs to reduce the overhead of function calls
// and accomodate as many key-value pairs as possible in a single function call. How many? That's left
// to the user to decide.
type MapInput map[Id]Id

// Result of the mapreduce result - same as MapInput
type MapReduceResult MapInput

// Intermediate key-value pairs that are emitted
type Intermediate struct {
	Key, Value Id
}

// We use a channel to be able to iterate over the values as and when they are available. This allows
// very large lists to be handled since only as many items are sent through the channel as can fit in memory.
type ReduceInput map[Id]chan Id

// The mapreduce specification object
type Specs struct {
	// A slice of input and output file paths
	InputFiles, OutputFiles []string
	// Total number of mappers and reducers to use
	M, R int
	// The length of this slice should be >= M+R
	Workers []worker
}

// Represents a machine in the cluster
type worker struct {
	// ID of the worker
	Id wID
	// The IP address of the host
	Addr *net.TCPAddr
	// Task assigned to the worker
	task *task
	// State of the task
	state state
	// RPC cient for communication
	// client *rpc.Client
	// Answers to queries
	ans map[Id]Id // TODO type needs to be changed later
}

// Task consists of the current state of the task, the split assigned to it if it is a map task
type task struct {
	// Split assigned to this task - valid only if it is a map task
	split string
	// Same as R, used by map workers
	r int
}

// Type used for providin services over rpc
type Service struct{}

// The MapReduce() call that triggers off the magic
func MapReduce(specs Specs) (result MapReduceResult, err error) {
	// Validate the specs
	if err = validateSpecs(specs); err != nil {
		return
	}

	// Setup rpc services of the master
	if err = setUpMasterServices(); err != nil {
		return
	}

	// Determine the total size of the all input files and start splitting it. Assign each split to a worker as a map task
	// and assign a reduce task to the rest of the workers in a ratio of M:R
	totalWorkers := len(specs.Workers)                           // total workers
	clients := make([]*rpc.Client, totalWorkers)                 // clients returned by rpc calls
	splitsIndex, err := split(specs.ChunkSize, specs.InputFiles) // Splits of files
	if err != nil {
		return
	}
	calls := make([]*rpc.Call, totalWorkers)   // calls returned by rpc calls
	unit := totalWorkers / (specs.M + specs.R) // unit worker for ratios
	ans := make([]bool, totalWorkers)          // answers received from machines wether they are idle or not?
	m := int(unit * specs.M)                   // number of map workers
	r := totalWorkers - m                      // number of reduce workers
	dialFailures := 0                          // number of dial failures - max limit is
	myAddr, err := myTCPAddr()
	if err != nil {
		return
	}

	//Ask all hosts wether they are idle or not
	for i := 0; i < m+r; i++ {
		clients[i], err = rpc.DialHTTP("tcp", specs.Workers[i].Addr.String())
		if err != nil {
			dialFailures++
			if dialFailures > totalWorkers/2 {
				err = fmt.Errorf("Number of dial failures is more than %d", totalWorkers/2)
				return // Return if number of dial failures are too many
			}
		}
		args := idleArgs{*myAddr, wID(i)}
		calls[i] = clients[i].Go(idleService, args, &ans[i], nil) // Call the service method to ask if the host is idle
	}

	// Accept the first m map workers which reply yes
	done := make([]bool, m)
	signalCompletion := make(chan bool, 2) // to signal completion of accept for m & r
	// Pointers to map and reduce workers to get faster access to them
	mapWorkers := make([]*worker, m)
	reduceWorkers := make([]*worker, r)
	// Open all splits as files
	splits := make([]*os.File, len(splitsIndex))
	for i, index := range splitsIndex {
		splits[i], err = os.Open(path.Join(tmp, "split"+strconv.Itoa(index)))
		if err != nil {
			return
		}
		defer splits[i].Close()
	}

	// A function accept te first n workers. Value of n is used to check if its for mappers or reducers
	accept := func(n int) {
		i, aw := 0, 0 // aw => Accepted Workers
		// loop unitl we have covered all clients or got n accepted workers
		for ; aw < n; i = (i + 1) % n {
			select {
			case <-calls[i].Done:
				if done[i] {
					break
				}
				// Assign the task
				switch n {
				case m:
					mapWorkers[aw] = &specs.Workers[i] // Assign reference to mapWorkers
				case r:
					reduceWorkers[aw] = &specs.Workers[i]
				}
				// specs.Workers[i].client = clients[i] // Assign the client
				done[i] = true // mark caller as accepted
				aw++
			default:
				continue
			}
		}
		signalCompletion <- (aw == n) // did we get enough number of workers?
	}
	go accept(m)
	go accept(r)
	if !<-signalCompletion || !<-signalCompletion {
		err = fmt.Errorf("Could not gather enough hosts to work. M: %d, R: %d", m, r)
		return
	}

	// Assign task to each mapworker and ask it to perform it
	for i, mw := range mapWorkers {
		mw.task.split = path.Join(tmp, "split"+strconv.Itoa(si))
		calls[i] = mw.client.Go(mapService, mw.task, nil, nil) // Ask to do the map work
	}

	// Wait for all tasks to complete and keep assigning the next job to the next idle worker
	for i := 0; true; i = (i + 1) % m {
		select {
		case <-calls[i].Done:
			b := make([]byte, splits[aw].Stat().Size()) // The contents of file
			_, err = splits[aw].Read(b)
			if err != nil {
				return
			}
			specs.Workers[i].task = &task{b, specs.R} // Setup the worker
			mapWorkers[aw] = &specs.Workers[i]        // Assign reference to mapWorkers
			aw++
		default: // keep polling
		}
	}

	err = nil // Reset err
	return
}

func validateSpecs(specs Specs) error {
	// If []workers is set then M+R >= num of workers
	if specs.Workers != nil && specs.M+specs.R < len(specs.Workers) {
		return fmt.Errorf("M: %d & R: %d are less than the num of hosts: %d", specs.M, specs.R, len(specs.Workers))
	}
	// // If the chunk size specified is less than 16Mb then set it to 16Mb
	// if specs.ChunkSize < defaultChunkSize {
	// 	specs.ChunkSize = defaultChunkSize
	// }
	return nil
}

// Singals a task completed
func (s *Service) TaskCompleted() {

}

func (s *Service) IAmAlive(id wID, null *int) {

}

// Utilities
func setUpMasterServices() error {
	rpc.Register(&Service{})
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	go http.Serve(l, nil)
	return nil
}

// Split files into chunks of size chunkS and return the file handles
func split(m int64, files []string) ([]int, error) {
	// name each intermediate file based on the hash of the contents of the input file
	var splits []string
	tmp := path.Join(os.TempDir(), "github.com", "ujjwalt", "hagoop")
	// determine chunkS
	var chunkS int64 = 0
	fileHandles := make([]*os.File, len(files))
	for i, fn := range files {
		fileHadles[i], err = os.Open(fn)
		if err != nil {
			return
		}
		defer fileHandles[i].Close()
		chunkS += fileHandles[i].Stat().Size()
	}
	chunkS /= m // divide total fil size by number of map workers
	b := make([]byte, chunkS)
	var off, read int64 = 0, 0                     // offset and how many bytes read
	for i, si, n := 0, 0, len(files); i < n; i++ { // si is the split index
		fHadle := fileHandles[i]
		// Read chunkS bytes into splits[i]
	tryAgain:
		r, err := fHandle.ReadAt(b[read:chunkS], off) // Read chunkS bytes into b
		off += int64(r)
		read += int64(r)
		switch {
		case read < chunkS:
			if err == io.EOF {
				off = 0 // if the file is over, get ready for the next file
				continue
			} else {
				goto tryAgain
			}
		case read > chunkS:
			return splits, err // something really shitty happened here!
		}
		read = 0 // reset for reading the next chunk
		// Create a temporary file for the split
		splits = append(splits, si)
		splitN, err := os.Create(path.Join(tmp, "split"+strconv.Itoa(si))) // Create the file
		if err != nil {
			return splits, err
		} else {
			defer splitN.Close()
		}
		splitN.WriteAt(b, off)
		err = splitN.Close()
		if err != nil {
			return splits, err
		}
		si++ // move onto the next split
	}
	return splits, nil
}

// Converts a bool to int
func bToi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func myTCPAddr() (a *net.TCPAddr, err error) {
	// Return own ip address used for the connections by dialing to http://www.google.com
	c, err := net.Dial("tcp", "google.com:80")
	if err != nil {
		return
	}
	defer c.Close() // close the connection at exit
	host, p, err := net.SplitHostPort(c.LocalAddr().String())
	if err != nil {
		return
	}
	ip := net.ParseIP(host)
	if ip == nil {
		err = fmt.Errorf("Invalid ip: %v", ip)
		return
	}
	port, err := strconv.Atoi(p)
	if err != nil {
		return
	}
	a = &net.TCPAddr{IP: ip, Port: port}
	return
}
