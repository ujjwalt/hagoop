package mapred

import (
	"io"
	"net"
	"os"
	"path"
)

// The MapReduce() call that triggers off the magic
func MapReduce(specs Specs) (result MapReduceResult, err error) {
	// Validate the specs
	if err = validateSpecs(specs); err != nil {
		return
	}

	// Setup rpc services of the master
	setUpMasterServices()

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

	//Ask all hosts wether they are or idle or not
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
	// Pointers to map and reduce workers to get faster access to them
	mapWorkers := make([]*worker, m)
	reduceWorkers := make([]*worker, r)
	// A function accept te first n workers. Value of n is used to check if its for mappers or reducers
	accept := func(n int) {
		var i, aw = 0, 0 // aw => Accepted Workers
		// loop unitl we have covered all clients or got m accepted workers
		for ; aw < n; i = (i + 1) % totalWorkers {
			select {
			case <-calls[i].Done && !done[i]:
				// Assign the task
				switch n {
				case m:
					specs.Workers[i].task = MapTask{idle, split[aw], specs.R} // Setup the worker
					mapWorkers[aw] = &specs.Workers[i]                        // Assign reference to mapWorkers
				case r:
					specs.Workers[i].task = ReduceTask{idle}
					reduceWorkers[aw] = &specs.Workers[i]
				}
				specs.Workers[i].client = clients[i] // Assign the client
				done[i] = true                       // mark caller as accepted
				aw++
			default:
			}
		}
		if aw == n {
			signalCompletion <- true
		} else {
			signalCompletion <- false
		}
	}
	go accept(m)
	go accept(r)
	if <-signalCompletion && <-signalCompletion {
		err = fmt.Errorf("Could not gather enough hosts to work. M: %d, R: %d", m, r)
		return
	}

	// keep looping until all are done
	for {
		// Ask each of them to perform their duty
		for _, mw := range mapWorkers {
			if mw.task.(MapTask).state == idle {
				mw.client.Go(MapService, mw.task, nil, nil) // Ask to do the map work
			}

		}
		for _, rw := range reduceWorkers {
			if rw.task.(ReduceTask).state == idle {
				rw.client.Go(MapService, rw.task, nil, nil) // Ask to do the map work
			}
		}

		// wait for a reply from map workers
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

func (s *Service) TaskCompleted() {

}

func (s *Service) IAmAlive() {

}

func setUpMasterServices() error {
	s := &Service{}
	rpc.Register(s)
	rpc.HandleHTTP()
	var e error
	l, e = net.Listen("tcp", port)
	if e != nil {
		return e
	}
	go http.Serve(l, nil)
	return nil
}

// Split files into chunks of size chunkS and return the file handles
func split(chunkS uint64, files []string) (splits []string, err error) {
	// name each intermediate file based on the hash of the contents of the input file
	var fHandle, splitN os.File
	var tmp = os.TempDir()
	var b [chunkS]byte
	var off, read, r = 0, 0, 0                    // offset to read
	for i, si, n = 0, 0, len(files); i < n; i++ { // si is the split index
		fHandle, err = os.Open(files[i])
		if err != nil {
			return
		} else {
			defer fHandle.Close()
		}
		// Read chunkS bytes into splits[i]
	tryAgain:
		r, err = fHandle.ReadAt(b[read:chunkS], off) // Read chunkS bytes into b
		off += r
		read += r
		switch {
		case read < chunkS:
			if err == io.EOF {
				off = 0 // if the file is over, get ready for the next file
				continue
			} else {
				goto tryAgain
			}
		case read > chunkS:
			return // something really shitty happened here!
		}
		read = 0 // reset for reading the next chunk
		// Create a temporary file for the split
		splits = append(splits, path.Join(tmp, "split"+si))
		splitN, err = &os.Create(splits[si])
		if err != nil {
			return
		} else {
			defer splitN.Close()
		}
		splitN.WriteAt(b, off)
		err = splitN.Close()
		if err != nil {
			return
		}
		si++ // move onto the next split
	}
	err = nil
	return
}
