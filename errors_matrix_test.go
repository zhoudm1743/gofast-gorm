package gormdriver

// errors_matrix_test.go：错误哨兵映射矩阵的 SQLite 侧（方案 §5.16 ERR 矩阵、§六 第 12 项）。
// 无 build tag：随默认 `go test` 在内存 SQLite 上运行；与 fault_integration_test.go
// 的 ERR_哨兵对照（PG/MySQL，integration tag）合成三方言对照表：
//
//	哨兵               | SQLite                       | MySQL                 | PostgreSQL
//	-------------------|------------------------------|-----------------------|--------------------------------
//	ErrRecordNotFound  | First 未命中（sql.ErrNoRows） | 同左                   | 同左
//	ErrDuplicatedKey   | UNIQUE constraint failed      | Error 1062 Duplicate  | duplicate key violates
//	ErrUnsupported     | FindInBatches batchSize<=0（query.go 驱动级防护，方言无关）；
//	                   | 非法 Where 类型差异固化：gormdriver.Where 原样透传 gorm，绑定期报底层
//	                   | 原始错误（如 "unsupported data type"），不映射 ErrUnsupported
//	                   | （与 fullcov_chain_integration_test.go 差异固化口径一致，xorm 侧
//	                   | 同类输入为链上 ErrUnsupported）。

import (
	"errors"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// gfltMatrixRow SQLite 错误矩阵模型（与集成侧 gfltFaultMatrixRow 分表）。
type gfltMatrixRow struct {
	ID   string `orm:"pk varchar(32) 'id'"`
	Name string `orm:"varchar(64) 'name'"`
}

func (gfltMatrixRow) TableName() string { return "gflt_matrix_rows" }

// TestFaultSQLite 错误哨兵矩阵的 SQLite 侧（表驱动）。
func TestFaultSQLite(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&gfltMatrixRow{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	q := drv.Query()

	cases := []struct {
		name string
		run  func() error
		check func(t *testing.T, err error)
	}{
		{
			name: "First 未命中 → ErrRecordNotFound",
			run: func() error {
				var row gfltMatrixRow
				return q.Where("id = ?", "no-such-row").First(&row)
			},
			check: func(t *testing.T, err error) {
				if !errors.Is(err, contracts.ErrRecordNotFound) {
					t.Errorf("期望 ErrRecordNotFound, 实际: %v", err)
				}
			},
		},
		{
			name: "重复主键 → ErrDuplicatedKey",
			run: func() error {
				if err := q.Create(&gfltMatrixRow{ID: "dup-1", Name: "first"}); err != nil {
					return err
				}
				return q.Create(&gfltMatrixRow{ID: "dup-1", Name: "second"})
			},
			check: func(t *testing.T, err error) {
				// SQLite 方言原文 "UNIQUE constraint failed: ..."，经 wrapError 字符串匹配映射
				if !errors.Is(err, contracts.ErrDuplicatedKey) {
					t.Errorf("期望 ErrDuplicatedKey, 实际: %v", err)
				}
			},
		},
		{
			name: "FindInBatches 非法 batchSize → ErrUnsupported",
			run: func() error {
				var rows []gfltMatrixRow
				return q.Model(&gfltMatrixRow{}).FindInBatches(&rows, 0, func(tx contracts.Query, batch int) error {
					return nil
				})
			},
			check: func(t *testing.T, err error) {
				// 驱动级防护（query.go FindInBatches 头部拦截，方言无关），三方言一致
				if !errors.Is(err, contracts.ErrUnsupported) {
					t.Errorf("期望 ErrUnsupported, 实际: %v", err)
				}
			},
		},
		{
			// 差异固化：gormdriver 的 Where/OrWhere/Not 对非法条件类型不做链上校验，
			// gorm 渲染/绑定期报原始错误（如 "unsupported data type"），不得映射
			// ErrUnsupported（契约差异：xorm 侧同类输入为链上 ErrUnsupported）。
			name: "非法 Where 类型不映射 ErrUnsupported（差异固化）",
			run: func() error {
				var row gfltMatrixRow
				return q.Where("id = ?", make(chan int)).First(&row)
			},
			check: func(t *testing.T, err error) {
				if errors.Is(err, contracts.ErrUnsupported) {
					t.Errorf("非法 Where 类型不应映射 ErrUnsupported（gorm 透传）, 实际: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		tc.check(t, tc.run())
	}
}
