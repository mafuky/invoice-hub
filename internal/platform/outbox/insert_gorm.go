package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mafuky/invoice-hub/internal/platform/event"
	"github.com/mafuky/invoice-hub/internal/platform/gormx"
)

type outboxRow struct {
	ID            int64     `gorm:"column:id;primaryKey;autoIncrement"`
	EventID       string    `gorm:"column:event_id"`
	RoutingKey    string    `gorm:"column:routing_key"`
	Body          string    `gorm:"column:body;type:json"`
	Status        string    `gorm:"column:status;default:new"`
	Attempts      int       `gorm:"column:attempts"`
	NextAttemptAt time.Time `gorm:"column:next_attempt_at;default:CURRENT_TIMESTAMP(3)"`
	CreatedAt     time.Time `gorm:"column:created_at;->"`
}

func (outboxRow) TableName() string { return "outbox" }

var insertSessions = sync.OnceValues(func() (*gormx.Sessions, error) { return gormx.New(nil) })

// InsertGorm writes the event through GORM in the caller's transaction.
func InsertGorm(ctx context.Context, tx *sql.Tx, routingKey string, env event.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: marshal event %s: %w", env.EventID, err)
	}
	sessions, err := insertSessions()
	if err != nil {
		return fmt.Errorf("outbox: initialize gorm: %w", err)
	}
	row := outboxRow{EventID: env.EventID, RoutingKey: routingKey, Body: string(body)}
	if err := sessions.Session(ctx, tx).Create(&row).Error; err != nil {
		return fmt.Errorf("outbox: insert event %s (%s): %w", env.EventID, routingKey, err)
	}
	return nil
}
