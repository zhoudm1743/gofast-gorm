package gormdriver

import (
	"context"
	"database/sql"
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
	// 事务终态后继续 Commit/Rollback：gorm 原样透出 database/sql 的
	// ErrTxDone（gofast.ErrInvalidTransaction 仅在 gorm 内部事务状态机命中），
	// 按 xorm 驱动同一语义映射为 ErrInvalidTransaction（§11.14 错误映射行）。
	case errors.Is(err, sql.ErrTxDone):
		return fmt.Errorf("%w: %v", contracts.ErrInvalidTransaction, err)
	default:
		msg := err.Error()
		// 查询超时（§5.16 ERR-05）：服务器端超时与 Go 侧 context 截止统一映射
		// ErrQueryTimeout——
		//   - PostgreSQL statement_timeout：SQLSTATE 57014
		//     "canceling statement due to statement timeout"
		//   - MySQL max_execution_time：Error 3024 "Query execution was
		//     interrupted, maximum statement execution time exceeded"
		//   - Go 侧 WithContext 截止：database/sql 透传 context.DeadlineExceeded
		if strings.Contains(msg, "statement timeout") ||
			strings.Contains(msg, "maximum statement execution time exceeded") ||
			errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("%w: %v", contracts.ErrQueryTimeout, err)
		}
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
