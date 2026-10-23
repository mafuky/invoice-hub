// Package dial checks that a backing service is reachable before a service starts.
package dial

import (
	"fmt"
	"net"
	"time"
)

// Check probes addr over TCP. The error carries addr and hint, never a DSN.
func Check(addr string, timeout time.Duration, hint string) error {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return fmt.Errorf("cannot reach %s (%s)", addr, hint)
	}
	return c.Close()
}
