package gormdriver

import (
	"errors"
	"fmt"
	"strings"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"gorm.io/gorm"
)

// wrapError 将 GORM 错误映射为框架级 Sentinel Error。
func wrapError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case isErr(err, gorm.ErrRecordNotFound):
		return fmt.Errorf("%w: %v", contracts.ErrRecordNotFound, err)
	case isErr(err, gorm.ErrDuplicatedKey):
		return fmt.Errorf("%w: %v", contracts.ErrDuplicatedKey, err)
	case isErr(err, gorm.ErrInvalidTransaction):
		return fmt.Errorf("%w: %v", contracts.ErrInvalidTransaction, err)
	default:
		msg := err.Error()
		// MySQL: "Error 1213: Deadlock found when trying to get lock"
		// PostgreSQL: "deadlock detected"
		// SQLite: "database is locked"
		if strings.Contains(msg, "Deadlock") || strings.Contains(msg, "deadlock") ||
			strings.Contains(msg, "Error 1213") {
			return fmt.Errorf("%w: %v", contracts.ErrDeadlock, err)
		}
		if strings.Contains(msg, "Duplicate entry") || strings.Contains(msg, "duplicate key") ||
			strings.Contains(msg, "UNIQUE constraint failed") {
			return fmt.Errorf("%w: %v", contracts.ErrDuplicatedKey, err)
		}
		return err
	}
}

func isErr(err, target error) bool {
	return errors.Is(err, target)
}
