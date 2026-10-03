package core

import (
	"syscall"
	"time"
)

// CPUTime returns this process's user+system CPU time so far (getrusage).
func CPUTime() time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
