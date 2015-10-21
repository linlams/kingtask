package timer

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var sum int32 = 0
var N int32 = 300
var tt *Timer

func now(arg interface{}) {
	fmt.Println(time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(arg)
	atomic.AddInt32(&sum, 1)
	v := atomic.LoadInt32(&sum)
	if v == N {
		tt.Stop()
	}

}

func TestTimer(t *testing.T) {
	timer := New(time.Millisecond * 10)
	tt = timer
	fmt.Println(timer)
	var i int32
	go timer.Start()
	for i = 0; i < N; i++ {
		timer.NewTimer(time.Millisecond*time.Duration(5*i), now, 12)
	}
	time.Sleep(time.Second * 10)
	if sum != N {
		t.Errorf("sum=%d,fail", sum)
	}
}
