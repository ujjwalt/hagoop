// Represents a worker on a host machine that provides services for doing map and reduce work
package mapred

import (
	"errors"
	"net"
	"net/http"
	"net/rpc"
)

// Asks wether the host is willing to work on a mapreduce task. It replies yes only if it is idle.
// The first argument is the tcp address of the master. The second arguement contains the reply.
func (w *Service) Idle(ip net.TCPAddr, reply *bool) error {
	*reply = task == nil
	return nil
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
