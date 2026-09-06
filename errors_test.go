package gormdriver

import (
	"errors"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"
	"gorm.io/gorm"
)

func TestWrapError_Nil(t *testing.T) {
	if err := wrapError(nil); err != nil {
		t.Errorf("wrapError(nil) = %v, 期望 nil", err)
	}
}

func TestWrapError_RecordNotFound(t *testing.T) {
	err := wrapError(gorm.ErrRecordNotFound)
	if !errors.Is(err, contracts.ErrRecordNotFound) {
		t.Errorf("wrapError(gorm.ErrRecordNotFound) 应包含 ErrRecordNotFound, 实际: %v", err)
	}
}

func TestWrapError_DuplicatedKey(t *testing.T) {
	err := wrapError(gorm.ErrDuplicatedKey)
	if !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("wrapError(gorm.ErrDuplicatedKey) 应包含 ErrDuplicatedKey, 实际: %v", err)
	}
}

func TestWrapError_DuplicatedKey_StringMatch(t *testing.T) {
	// MySQL 重复键错误
	err := wrapError(errors.New("Error 1062: Duplicate entry 'foo' for key 'idx_name'"))
	if !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("MySQL duplicate 应映射为 ErrDuplicatedKey, 实际: %v", err)
	}

	// PostgreSQL 重复键错误
	err = wrapError(errors.New("duplicate key value violates unique constraint"))
	if !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("PostgreSQL duplicate 应映射为 ErrDuplicatedKey, 实际: %v", err)
	}

	// SQLite 唯一约束错误
	err = wrapError(errors.New("UNIQUE constraint failed: users.email"))
	if !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("SQLite UNIQUE 应映射为 ErrDuplicatedKey, 实际: %v", err)
	}
}

func TestWrapError_Deadlock(t *testing.T) {
	// MySQL 死锁错误码 1213
	err := wrapError(errors.New("Error 1213: Deadlock found when trying to get lock; try restarting transaction"))
	if !errors.Is(err, contracts.ErrDeadlock) {
		t.Errorf("MySQL Error 1213 应映射为 ErrDeadlock, 实际: %v", err)
	}

	// PostgreSQL 死锁
	err = wrapError(errors.New("deadlock detected"))
	if !errors.Is(err, contracts.ErrDeadlock) {
		t.Errorf("PostgreSQL deadlock 应映射为 ErrDeadlock, 实际: %v", err)
	}
}

func TestWrapError_InvalidTransaction(t *testing.T) {
	err := wrapError(gorm.ErrInvalidTransaction)
	if !errors.Is(err, contracts.ErrInvalidTransaction) {
		t.Errorf("wrapError(gorm.ErrInvalidTransaction) 应包含 ErrInvalidTransaction, 实际: %v", err)
	}
}

func TestWrapError_Unknown(t *testing.T) {
	original := errors.New("some random database error")
	err := wrapError(original)
	if err == nil {
		t.Fatal("非哨兵错误不应返回 nil")
	}
	// 未知错误应原样返回，不包装
	if !errors.Is(err, original) {
		t.Errorf("未知错误应原样返回, 实际: %v", err)
	}
}

func TestIsErr(t *testing.T) {
	if !isErr(gorm.ErrRecordNotFound, gorm.ErrRecordNotFound) {
		t.Error("isErr(gorm.ErrRecordNotFound, gorm.ErrRecordNotFound) 应为 true")
	}
	if isErr(gorm.ErrRecordNotFound, gorm.ErrDuplicatedKey) {
		t.Error("isErr(gorm.ErrRecordNotFound, gorm.ErrDuplicatedKey) 应为 false")
	}
	if isErr(errors.New("some error"), gorm.ErrRecordNotFound) {
		t.Error("isErr(random, gorm.ErrRecordNotFound) 应为 false")
	}
}
