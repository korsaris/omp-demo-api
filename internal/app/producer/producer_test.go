package producer

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/golang/mock/gomock"
	"github.com/ozonmp/omp-demo-api/internal/mocks"
	"github.com/ozonmp/omp-demo-api/internal/model"
)

func retentionEventFactory(startID, count uint64) []model.RetentionEvent {
	res := make([]model.RetentionEvent, count)
	for index := range res {
		res[index] = model.RetentionEvent{
			ID:     startID + uint64(index),
			Type:   model.Created,
			Status: model.Deferred,
			Entity: &model.Retention{
				ID: startID + uint64(index),
			},
		}
	}

	return res
}

func TestStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	sender := mocks.NewMockEventSender(ctrl)
	n := uint64(2)
	events := make(chan model.RetentionEvent, 10)
	workerPool := workerpool.New(3)
	producer := NewKafkaProducer(n, sender, events, workerPool)
	producer.Start(context.Background())
	producer.Close()
}

func TestSendToDummy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eventsCount := 10

	workers := 4
	wpWorkers := 4
	sender := mocks.NewMockEventSender(ctrl)
	events := make(chan model.RetentionEvent, eventsCount)
	workerPool := workerpool.New(wpWorkers)

	producer := NewKafkaProducer(uint64(workers), sender, events, workerPool)

	eventsSendedInKafka := make([]model.RetentionEvent, 0)

	var kaffkaMockMutex sync.Mutex
	gomock.InOrder(
		sender.EXPECT().Send(gomock.Any()).Return(nil).
			AnyTimes().Do(func(event *model.RetentionEvent) {
			kaffkaMockMutex.Lock()
			eventsSendedInKafka = append(eventsSendedInKafka, *event)
			kaffkaMockMutex.Unlock()
		}),
	)

	expected := retentionEventFactory(uint64(0), uint64(eventsCount))
	for _, v := range expected {
		events <- v
	}

	producer.Start(context.Background())
	time.Sleep(time.Millisecond * 250)

	producer.Close()

	// Order is not guaranteed, need sort
	sort.SliceStable(eventsSendedInKafka, func(i, j int) bool {
		return eventsSendedInKafka[i].ID < eventsSendedInKafka[j].ID
	})

	if reflect.DeepEqual(eventsSendedInKafka, expected) == false {
		t.Errorf("res no equal expected\n res: %+v\nexpected: %+v\n", eventsSendedInKafka, expected)
	}
}
