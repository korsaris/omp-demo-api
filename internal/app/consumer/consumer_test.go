package consumer

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

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

	n := 2
	batchSize := 10
	consumeTimeout := time.Second * 5
	repo := mocks.NewMockEventRepo(ctrl)
	events := make(chan model.RetentionEvent)

	consumer := NewDbConsumer(uint64(n), uint64(batchSize), consumeTimeout, repo, events)
	consumer.Start(context.Background())
	consumer.Close()
}

func TestExtractFromDummy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	eventsCount := 10

	workers := 4
	batchSize := 5
	consumerTimeout := time.Millisecond * 200
	repo := mocks.NewMockEventRepo(ctrl)
	events := make(chan model.RetentionEvent, eventsCount)

	consumer := NewDbConsumer(uint64(workers), uint64(batchSize), consumerTimeout, repo, events)

	gomock.InOrder(
		repo.EXPECT().Lock(gomock.Any()).Return(retentionEventFactory(0, 5), nil),
		repo.EXPECT().Lock(gomock.Any()).Return(retentionEventFactory(5, 5), nil),
		repo.EXPECT().Lock(gomock.Any()).Return(retentionEventFactory(0, 0), nil).AnyTimes(),
	)

	consumer.Start(context.Background())
	time.Sleep(time.Millisecond * 100)

	res := make([]model.RetentionEvent, eventsCount)
	for i := range res {
		res[i] = <-events
	}

	consumer.Close()

	// Order is not guaranteed, need sort
	sort.SliceStable(res, func(i, j int) bool {
		return res[i].ID < res[j].ID
	})

	expected := retentionEventFactory(uint64(0), uint64(eventsCount))
	if reflect.DeepEqual(res, expected) == false {
		t.Errorf("res no equal expected\n res: %+v\nexpected: %+v\n", res, expected)
	}
}
