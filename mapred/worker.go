// Represents a worker on a host machine that provides services for doing map and reduce work
package mapred

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
)

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
	task       id           // Current task
	w          *Worker      //The object to use as worker
	id         wID          // ID of this worker
)

// Arguemnts to the idle service
type idleArgs struct {
	master net.TCPAddr // Address of master
	id     wID         // ID assigned to the worker if it agrees to work
}

// Asks wether the host is willing to work on a mapreduce task. It replies yes only if it is idle.
// The first argument is the tcp address of the master. The second arguement contains the reply.
func (s *Service) Idle(arg idleArgs, reply *bool) error {
	masterAddr = arg.master
	id = arg.id
	*reply = task == nil
	return nil
}

func (s *Service) Map(t MapTask, reply *bool) error {

}

func Start(worker *Worker) error {
	// Check if the service is already running or not
	if started {
		return errors.New("Service has already been started. Use Stop() to stop the service.")
	}
	rpc.Register(&service{})
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

func Stop() error {
	started = false
	return l.Close()
}
