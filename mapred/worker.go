// Represents a worker on a host machine that provides services for doing map and reduce work
package mapred

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
)

const (
	port        = ":2607" // All communication happens over this port
	idleService = "service.Idle"
)

var (
	started bool         // Indicates wether the service has staretd
	l       net.Listener // The service listener
	master  net.TCPAddr  // Address of the master
	task    id           // Current task
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
	//Reduce is the reduce function which takes a channel of ReduceInput and an output channel to transfer
	Reduce(<-chan ReduceInput) (<-chan id, error)
}

type service struct{}

// Asks wether the host is willing to work on a mapreduce task. It replies yes only if it is idle.
// The first argument is the tcp address of the master. The second arguement contains the reply.
func (w *service) Idle(ip net.TCPAddr, reply *bool) error {
	*reply = task == nil
	return nil
}

func Start(worker *Worker) error {
	// Check if the service is already running or not
	if started {
		return errors.New("Service has already been started. Use Stop() to stop the service.")
	}
	rpc.Register(worker)
	rpc.Register(&service{})
	rpc.HandleHTTP()
	var e error
	l, e = net.Listen("tcp", port)
	if e != nil {
		return e
	}
	go http.Serve(l, nil)
	started = true
	return nil
}

func Stop() error {
	started = false
	return l.Close()
}
