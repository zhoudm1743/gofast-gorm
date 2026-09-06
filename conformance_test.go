package gormdriver

// conformance_test.go 双驱动一致性套件接入（orm-tag-design.md §11.1/§11.17 U17）。
//
// 工厂构造要点（与 newTestGormDB 同模式）：内存 SQLite + registerAutoGenerateID
// Callback——缺 ID 回调时 FirstOrCreate 等内部创建路径不生成主键，套件 ID 断言失败。
//
// SQL 计数用 ConnPool 包装实现而非日志器计数：gorm caches 插件命中缓存时仍会
// 构建 SQL 并回调 Logger.Trace（stmt.SQL 非空即 Trace），日志器计数会把缓存命中
// 误计为 DB 访问；ConnPool 包装只统计真实连接池往返（事务内的语句经 *sql.Tx，
// 不经外层 ConnPool——套件的计数场景均在事务外，不受影响）。

import (
	"context"
	"database/sql"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/zhoudm1743/go-fast-framework/contracts"
	"github.com/zhoudm1743/go-fast-framework/database/drivertest"

	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
)

// countingConnPool 包装 gorm.ConnPool，统计真实 DB 往返次数。
type countingConnPool struct {
	gorm.ConnPool
	n *int64
}

func (p *countingConnPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	*p.n++
	return p.ConnPool.ExecContext(ctx, query, args...)
}

func (p *countingConnPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	*p.n++
	return p.ConnPool.QueryContext(ctx, query, args...)
}

func (p *countingConnPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	*p.n++
	return p.ConnPool.QueryRowContext(ctx, query, args...)
}

// BeginTx 透传事务开启（gorm.ConnPoolBeginner 语义）：wrapper 若不实现，
// gorm 的默认事务（Create/AutoMigrate 包裹）会直接返回 ErrInvalidTransaction。
// 返回内层 *sql.Tx（满足 gorm.ConnPool），事务内语句经 tx 不重复计数。
func (p *countingConnPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (gorm.ConnPool, error) {
	switch b := p.ConnPool.(type) {
	case gorm.ConnPoolBeginner:
		return b.BeginTx(ctx, opts)
	case gorm.TxBeginner:
		tx, err := b.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
		return tx, nil
	default:
		return nil, gorm.ErrInvalidTransaction
	}
}

// conformanceDriver 套件驱动：注入 *GormDriver + QueryCounter 能力。
type conformanceDriver struct {
	*GormDriver
	n *int64
}

// QueryCount 实现 drivertest.QueryCounter（§11.5 SQL 计数断言）。
func (d *conformanceDriver) QueryCount() int { return int(*d.n) }

// newGormConformanceDriver gorm 侧套件工厂：内存 SQLite + ID 回调 + ConnPool 计数。
// 日志器静默（避免套件输出噪音，缓存命中误计数问题已由 ConnPool 计数规避）。
func newGormConformanceDriver(t *testing.T) contracts.Driver {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger:                                   gormLogger.Discard,
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		t.Fatalf("创建一致性测试数据库失败: %v", err)
	}
	if err := registerAutoGenerateIDCallback(db); err != nil {
		t.Fatalf("注册 ID 回调失败: %v", err)
	}
	n := new(int64)
	pool := &countingConnPool{ConnPool: db.ConnPool, n: n}
	// 同时替换配置层与根 Statement 的 ConnPool：Session 克隆 Statement 时从
	// db.Statement.ConnPool 取值，只改 db.ConnPool 不会传播到后续会话。
	db.ConnPool = pool
	db.Statement.ConnPool = pool
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return &conformanceDriver{GormDriver: &GormDriver{db: db}, n: n}
}

// TestConformance 双驱动共享一致性套件（orm-tag-design.md §11.1）。
func TestConformance(t *testing.T) {
	drivertest.RunSuite(t, newGormConformanceDriver)
}
