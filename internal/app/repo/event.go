package repo

import (
	"github.com/ozonmp/omp-demo-api/internal/model"
)

type EventRepo interface {
	Lock(n uint64) ([]model.RetentionEvent, error)
	Unlock(eventIDs []uint64) error

	Add(event []model.RetentionEvent) error
	Remove(eventIDs []uint64) error
}
