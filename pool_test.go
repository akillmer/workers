package workers

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolSend(t *testing.T) {
	pool := NewPool(runtime.NumCPU(), func(data interface{}) error {
		<-time.NewTimer(data.(time.Duration)).C
		return nil
	})
	if err := pool.Open(); err != nil {
		t.Fatal(err)
	}

	var scale = time.Duration(10)
	start := time.Now()

	for i := 0; i < pool.Size()*int(scale); i++ {
		if err := pool.Send(time.Second / time.Duration(scale)); err != nil {
			t.Fatal(err)
		}
	}

	pool.Wait()
	delta := int(time.Since(start).Seconds())

	if delta != 1 {
		t.Errorf("pool should've taken 1 second, took %d", delta)
	}
}

func TestPoolClose(t *testing.T) {
	pool := NewPool(runtime.NumCPU(), func(data interface{}) error {
		// update the sentJobs var
		atomic.AddUint32(data.(*uint32), 1)
		return nil
	})
	if err := pool.Open(); err != nil {
		t.Fatal(err)
	}

	var counter uint32
	sentJobs := pool.Size() * 10

	for i := 0; i < sentJobs; i++ {
		pool.Send(&counter)
	}

	canceled := pool.Close()
	total := int(counter) + canceled

	if total != sentJobs {
		t.Errorf("expected completed + canceled jobs to equal %d, got %d",
			sentJobs, total)
	}
}

func BenchmarkPool(b *testing.B) {
	pool := NewPool(runtime.NumCPU(), func(data interface{}) error {
		return nil
	})
	if err := pool.Open(); err != nil {
		b.Fatal(err)
	}

	for i := 0; i < b.N; i++ {
		if err := pool.Send(nil); err != nil {
			b.Fatal(err)
		}
	}

	pool.Wait()
	pool.Close()
}
