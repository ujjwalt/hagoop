package mapred

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
					clients[aw].Go(MapService, mapWorkers[aw].task, nil, nil) // Ask to do the map work
				case r:
					specs.Workers[i].task = ReduceTask{idle}
					reduceWorkers[aw] = &specs.Workers[i]
					clients[aw].Go(ReduceService, reduceWorkers[aw].task, nil, nil)
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
