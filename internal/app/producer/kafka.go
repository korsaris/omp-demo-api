package producer

import (
	"context"
	"sync"
	"time"

	"github.com/ozonmp/omp-demo-api/internal/app/sender"
	"github.com/ozonmp/omp-demo-api/internal/model"

	"github.com/gammazero/workerpool"
)

type Producer interface {
	Start(ctx context.Context)
	Close()
}

type producer struct {
	n       uint64
	timeout time.Duration

	sender sender.EventSender
	events <-chan model.RetentionEvent

	workerPool *workerpool.WorkerPool

	wg         *sync.WaitGroup
	cancelFunc func()
}

// todo for students: add repo
func NewKafkaProducer(
	n uint64,
	sender sender.EventSender,
	events <-chan model.RetentionEvent,
	workerPool *workerpool.WorkerPool,
) Producer {

	wg := &sync.WaitGroup{}

	return &producer{
		n:          n,
		sender:     sender,
		events:     events,
		workerPool: workerPool,
		wg:         wg,
	}
}

func (p *producer) Start(ctx context.Context) {
	prCtx, cancelFunc := context.WithCancel(ctx)
	p.cancelFunc = cancelFunc
	for i := uint64(0); i < p.n; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for {
				select {
				case event := <-p.events:
					if err := p.sender.Send(&event); err != nil {
						// ok
						p.workerPool.Submit(func() {
							// ...
						})
					} else {
						// err
						p.workerPool.Submit(func() {
							// ...
						})
					}
				case <-prCtx.Done():
					return
				}
			}
		}()
	}
}

func (p *producer) Close() {
	p.cancelFunc()
	p.wg.Wait()
}
