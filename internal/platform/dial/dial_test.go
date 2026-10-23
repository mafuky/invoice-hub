package dial

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestDialCheck_PortClosed_FailsFastWithHint(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	start := time.Now()
	err = Check(addr, 5*time.Second, "make tunnel")
	if err == nil {
		t.Fatal("expected an error for a closed port")
	}
	if time.Since(start) > 6*time.Second {
		t.Fatalf("took too long: %v", time.Since(start))
	}
	if !strings.Contains(err.Error(), "make tunnel") || !strings.Contains(err.Error(), addr) {
		t.Fatalf("error lacks hint or addr: %v", err)
	}
}

func TestDialCheck_PortOpen(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := Check(l.Addr().String(), time.Second, "make tunnel"); err != nil {
		t.Fatal(err)
	}
}
