package mapred

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
)

// Type of this host
type wType int

// Type of this worker
const (
	master wType = iota
	mapper
	reducer
)

// Services provided by a worker
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
	task       Id           // Current task
	w          *Worker      //The object to use as worker
	myID       wID          // ID of this worker
)

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
	// Partioning function to partition the intermediate key-value pairs into R space
	Parition(Intermediate)
	//Reduce is the reduce function which takes a channel of ReduceInput and an output channel to transfer
	Reduce(<-chan ReduceInput) (<-chan Id, error)
}

// Id of a worker. Used for idenitfying each host
type wID int

// Arguemnts to the idle service
type idleArgs struct {
	master net.TCPAddr // Address of master
	yourID wID         // ID assigned to the worker if it agrees to work
}

// Asks wether the host is willing to work on a mapreduce task. It replies yes only if it is idle.
// The first argument is the tcp address of the master. The second arguement contains the reply.
func (s *Service) Idle(arg idleArgs, reply *bool) error {
	masterAddr = arg.master
	myID = arg.yourID
	*reply = task == nil
	return nil
}

func (s *Service) Map(t MapTask, reply *bool) error {
	return nil
}

// Star a worker to accept work from some master
func Start(worker *Worker) error {
	// Check if the service is already running or not
	if started {
		return errors.New("Service has already been started. Use Stop() to stop the service.")
	}
	rpc.Register(&Service{})
	rpc.HandleHTTP()
	var e error
	l, e = net.Listen("tcp", port)
	if e != nil {
		return e
	}
	go http.Serve(l, nil)
	started = true
	w = worker
	return nil
}

// Stop the services of a worker
func Stop() error {
	started = false
	return l.Close()
}
