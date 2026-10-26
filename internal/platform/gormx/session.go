// Package gormx binds GORM sessions to caller-owned SQL connections.
package gormx

import (
	"context"
	"database/sql"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Sessions shares GORM's schema cache and callbacks without retaining transactions.
type Sessions struct{ base *gorm.DB }

func New(db *sql.DB) (*Sessions, error) {
	base, err := gorm.Open(mysql.New(mysql.Config{Conn: db, SkipInitializeWithVersion: true}), &gorm.Config{
		SkipDefaultTransaction: true,
		DisableAutomaticPing:   true,
		Logger:                 logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, err
	}
	return &Sessions{base: base}, nil
}

// Session uses conn directly; only the caller may begin, commit or roll back a transaction.
func (s *Sessions) Session(ctx context.Context, conn gorm.ConnPool) *gorm.DB {
	db := s.base.WithContext(ctx)
	db.Statement.ConnPool = conn
	return db
}
