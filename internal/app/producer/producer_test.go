package producer

import (
	"testing"

	"github.com/gammazero/workerpool"
	"github.com/golang/mock/gomock"
	"github.com/ozonmp/omp-demo-api/internal/mocks"
	"github.com/ozonmp/omp-demo-api/internal/model"
)

func TestStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	sender := mocks.NewMockEventSender(ctrl)
	n := uint64(2)
	events := make(chan model.RetentionEvent, 10)
	workerPool := workerpool.New(3)
	producer := NewKafkaProducer(n, sender, events, workerPool)
	producer.Start()
	producer.Close()
}
