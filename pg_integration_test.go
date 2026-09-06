//go:build integration

package gormdriver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// 本文件为 PostgreSQL 多租户集成测试，依赖真实数据库，默认不运行。
// 运行方式：设置环境变量 GOFAST_TEST_PG_DSN 后执行 `go test -tags integration`。
// 示例：
//
//	GOFAST_TEST_PG_DSN="postgres://user:pass@host:5432/db?sslmode=disable" \
//	  go test -tags integration ./database/drivers/gormdriver/

type AppUser struct {
	ID        string `gorm:"column:id;primaryKey"`
	CreatedAt int64  `gorm:"column:created_at"`
	UpdatedAt int64  `gorm:"column:updated_at"`
	DeletedAt int64  `gorm:"column:deleted_at"`
	Name      string `gorm:"column:name"`
	Email     string `gorm:"column:email"`
	Phone     string `gorm:"column:phone"`
}

type OrgNode struct {
	ID       string    `gorm:"column:id;primaryKey"`
	ParentID string    `gorm:"column:parent_id"`
	Name     string    `gorm:"column:name"`
	Children []OrgNode `gorm:"foreignKey:ParentID;references:ID"`
}

// TablerOrgNode 模拟业务模型实现 TableName()（返回裸表名）的场景。
// GORM 的 NamingStrategy 对实现 TableName() 的模型不生效，Preload 需兜底解析 schema 前缀。
// TableName 复用真实表 org_nodes（tenant schema 下已有数据）。
type TablerOrgNode struct {
	ID       string          `gorm:"column:id;primaryKey"`
	ParentID string          `gorm:"column:parent_id"`
	Name     string          `gorm:"column:name"`
	Children []TablerOrgNode `gorm:"foreignKey:ParentID;references:ID"`
}

func (TablerOrgNode) TableName() string { return "org_nodes" }

func newPG(t *testing.T) *GormDriver {
	t.Helper()
	dsn := os.Getenv("GOFAST_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("未设置 GOFAST_TEST_PG_DSN 环境变量，跳过 pgsql 集成测试")
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "public."},
		PrepareStmt:    true,
	})
	if err != nil {
		t.Fatal(err)
	}
	drv := &GormDriver{db: db}
	t.Cleanup(func() { _ = drv.Close() })
	seedPGFixtures(t, drv)
	return drv
}

// seedPGFixtures 幂等创建本文件全部用例依赖的租户 schema 与种子数据
// （tenant_260563780.app_users 1 行 / public.app_users 3 行 / org_nodes 树），
// 使测试自包含且失败重跑不残留（每次先 DROP 再 CREATE 保证确定性）。
func seedPGFixtures(t *testing.T, drv *GormDriver) {
	t.Helper()
	for _, ddl := range []string{
		`CREATE SCHEMA IF NOT EXISTS tenant_260563780`,
		`DROP TABLE IF EXISTS tenant_260563780.app_users`,
		`DROP TABLE IF EXISTS tenant_260563780.org_nodes`,
		`DROP TABLE IF EXISTS public.app_users`,
		`CREATE TABLE tenant_260563780.app_users (
			id text PRIMARY KEY,
			created_at bigint DEFAULT 0,
			updated_at bigint DEFAULT 0,
			deleted_at bigint DEFAULT 0,
			name text,
			email text,
			phone text,
			avatar text,
			nickname text,
			wechat_open_id text
		)`,
		`CREATE TABLE public.app_users (
			id text PRIMARY KEY,
			created_at bigint DEFAULT 0,
			updated_at bigint DEFAULT 0,
			deleted_at bigint DEFAULT 0,
			name text,
			email text,
			phone text,
			avatar text,
			nickname text,
			wechat_open_id text
		)`,
		`INSERT INTO tenant_260563780.app_users (id, name, email)
			VALUES ('77f4bb72-9664-4ff4-a075-29204e67c848', '测试用户', 'tenant@gofast.dev')`,
		`INSERT INTO public.app_users (id, name) VALUES
			('p1', '平台用户1'), ('p2', '平台用户2'), ('p3', '平台用户3')`,
		`CREATE TABLE tenant_260563780.org_nodes (
			id text PRIMARY KEY,
			parent_id text,
			name text
		)`,
		`INSERT INTO tenant_260563780.org_nodes (id, parent_id, name) VALUES
			('n1', NULL, '大房东'),
			('n2', 'n1', '二房东'),
			('n3', 'n2', '三房东'),
			('n4', 'n1', '四房东')`,
	} {
		if err := drv.Query().Exec(ddl); err != nil {
			t.Fatalf("初始化租户 fixtures 失败 %q: %v", ddl, err)
		}
	}
}
func TestPG_ReadOnlyMethods(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	// Count
	var total int64
	if err := drv.Query().Schema(ten).Model(&AppUser{}).Count(&total); err != nil {
		t.Fatalf("Count: %v", err)
	}
	if total != 1 {
		t.Errorf("Count 期望 tenant=1, 实际 %d", total)
	}

	// Pluck
	var names []string
	if err := drv.Query().Schema(ten).Model(&AppUser{}).Pluck("name", &names); err != nil {
		t.Fatalf("Pluck: %v", err)
	}
	if len(names) != 1 {
		t.Errorf("Pluck 期望 1 条, 实际 %v", names)
	}

	// Scan 到 struct
	var scans []AppUser
	if err := drv.Query().Schema(ten).Model(&AppUser{}).Scan(&scans); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(scans) != 1 {
		t.Errorf("Scan 期望 1 条, 实际 %d", len(scans))
	}

	// ScanMap
	var maps []map[string]any
	if err := drv.Query().Schema(ten).Model(&AppUser{}).ScanMap(&maps); err != nil {
		t.Fatalf("ScanMap: %v", err)
	}
	if len(maps) != 1 {
		t.Errorf("ScanMap 期望 1 条, 实际 %d", len(maps))
	}

	// First/Last/Take/Find
	var one AppUser
	if err := drv.Query().Schema(ten).Model(&AppUser{}).First(&one); err != nil {
		t.Fatalf("First: %v", err)
	}
	if one.Name != "测试用户" {
		t.Errorf("First 应查到 tenant 数据, 实际 name=%q", one.Name)
	}

	// Exists
	ok, err := drv.Query().Schema(ten).Exists(&AppUser{})
	if err != nil || !ok {
		t.Errorf("Exists 应 true, ok=%v err=%v", ok, err)
	}
}

// DryRun 观察未 applySchema 的写操作表名
func TestPG_WriteSQLTableName(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	// Update 表名
	q1 := drv.Query().Schema(ten).Model(&AppUser{}).Where("id = ?", "x")
	stmt := q1.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Update("name", "y").Statement
	t.Logf("Update SQL: %s", stmt.SQL.String())

	// Updates 表名
	q2 := drv.Query().Schema(ten).Model(&AppUser{}).Where("id = ?", "x")
	stmt2 := q2.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Updates(map[string]any{"name": "y"}).Statement
	t.Logf("Updates SQL: %s", stmt2.SQL.String())

	// Restore 表名
	q3 := drv.Query().Schema(ten).Model(&AppUser{}).Where("id = ?", "x")
	stmt3 := q3.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Update("deleted_at", 0).Statement
	t.Logf("Restore(Update) SQL: %s", stmt3.SQL.String())
}

// 并发多租户：不同租户 schema 是否串。
// 说明：gorm schema 缓存按 reflect.Type 以"首次解析的 NamingStrategy"钉住
// 表名前缀，同驱动同模型混用租户/动态 NS 的首次解析存在天然竞争——因此
// 租户分支与 public 分支各用独立驱动实例（各自 cacheStore），断言确定性。
func TestPG_ConcurrentTenants(t *testing.T) {
	drv := newPG(t)
	pubDrv := newPG(t)

	var wg sync.WaitGroup
	errs := make(chan string, 20)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var total int64
			if err := drv.Query().Schema("tenant_260563780").Model(&AppUser{}).Count(&total); err != nil {
				errs <- "tenant err: " + err.Error()
				return
			}
			if total != 1 {
				errs <- "tenant count 期望 1 实际 " + string(rune('0'+total))
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			var total int64
			// public 平台数据（独立驱动，不设租户 schema）
			if err := pubDrv.Query().Model(&AppUser{}).Count(&total); err != nil {
				errs <- "public err: " + err.Error()
				return
			}
			if total != 3 {
				errs <- "public count 期望 3"
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

func TestPG_PreloadTenant(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	var roots []OrgNode
	err := drv.Query().Schema(ten).Model(&OrgNode{}).
		Where("parent_id IS NULL").Preload("Children").Find(&roots)
	if err != nil {
		t.Fatalf("Preload: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("应 1 个根节点, 实际 %d", len(roots))
	}
	if roots[0].Name != "大房东" {
		t.Errorf("根节点应为大房东, 实际 %q", roots[0].Name)
	}
	t.Logf("根节点 %q 的 Children 数: %d", roots[0].Name, len(roots[0].Children))
	if len(roots[0].Children) == 0 {
		t.Error("Preload Children 应加载子节点（schema 穿透问题会导致为空）")
	}
	for _, c := range roots[0].Children {
		t.Logf("  child: %s (%s)", c.Name, c.ID)
	}
}

// TestPG_PreloadTenantTableName 覆盖"模型实现 TableName() 返回裸表名"的 Preload。
// 回归：0.7.5 移除 SET search_path 后，Preload 子查询因 Statement.Table 为空而丢失 schema 前缀，
// 导致 relation "xxx" does not exist。此测试验证 schema 前缀兜底解析。
func TestPG_PreloadTenantTableName(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	var roots []TablerOrgNode
	err := drv.Query().Schema(ten).Model(&TablerOrgNode{}).
		Where("parent_id IS NULL").Preload("Children").Find(&roots)
	if err != nil {
		t.Fatalf("TableName Preload: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("应 1 个根节点, 实际 %d", len(roots))
	}
	if roots[0].Name != "大房东" {
		t.Errorf("根节点应为大房东, 实际 %q", roots[0].Name)
	}
	if len(roots[0].Children) == 0 {
		t.Error("TableName 模型 Preload Children 应加载子节点（schema 穿透问题会导致为空）")
	}
}

func TestPG_JoinsTenantSQL(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	q := drv.Query().Schema(ten).Model(&OrgNode{}).Joins("Children").Where("org_nodes.name = ?", "大房东")
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]OrgNode{}).Statement
	t.Logf("Joins SQL: %s", stmt.SQL.String())
}

func TestPG_TransactionTenant(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	err := drv.Query().Schema(ten).Transaction(func(tx contracts.Query) error {
		var total int64
		if err := tx.Model(&AppUser{}).Count(&total); err != nil {
			return err
		}
		if total != 1 {
			t.Errorf("事务内 tenant Count 期望 1, 实际 %d", total)
		}
		// 事务内 Find 也应正确
		var one AppUser
		if err := tx.Model(&AppUser{}).First(&one); err != nil {
			return err
		}
		if one.Name != "测试用户" {
			t.Errorf("事务内 First 应查 tenant, 实际 %q", one.Name)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("事务: %v", err)
	}
}

func TestPG_NestedPreload(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	var roots []OrgNode
	err := drv.Query().Schema(ten).Model(&OrgNode{}).
		Where("parent_id IS NULL").Preload("Children.Children").Find(&roots)
	if err != nil {
		t.Fatalf("嵌套 Preload: %v", err)
	}
	if len(roots) != 1 {
		t.Fatalf("应 1 根节点, 实际 %d", len(roots))
	}
	t.Logf("根 %q children=%d", roots[0].Name, len(roots[0].Children))
	for _, c := range roots[0].Children {
		t.Logf("  %q 的 children=%d", c.Name, len(c.Children))
	}
}

func TestPG_ScanMapNullable(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	var maps []map[string]any
	err := drv.Query().Schema(ten).Model(&AppUser{}).Where("id IS NOT NULL").ScanMap(&maps)
	if err != nil {
		t.Fatalf("ScanMap: %v", err)
	}
	if len(maps) == 0 {
		t.Fatal("应查到数据")
	}
	// 检查可空字段
	for _, m := range maps {
		t.Logf("avatar=%v nickname=%v wechat_open_id=%v", m["avatar"], m["nickname"], m["wechat_open_id"])
	}
}

func TestPG_FirstOrCreate_Existing(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_260563780"

	// 用已存在的记录，FirstOrCreate 走 First 分支，不写库
	m := &AppUser{ID: "77f4bb72-9664-4ff4-a075-29204e67c848", Name: "不该被覆盖"}
	if err := drv.Query().Schema(ten).FirstOrCreate(m, "id = ?", m.ID); err != nil {
		t.Fatalf("FirstOrCreate: %v", err)
	}
	if m.Name != "测试用户" {
		t.Errorf("FirstOrCreate 应查到 tenant 已有记录, 实际 name=%q", m.Name)
	}
}

// TestPG_ExplicitTableWithProjectionDest 回归缺陷：schema 模式下 applySchema 曾按
// dest 结构体推导表名无条件覆盖显式 Table()，导致 relation "<schema>.user_rows"
// does not exist（SQLSTATE 42P01），表恰好存在时静默查错表（缺陷报告 2026-09-05）。
// 自建 schema：users 存正确数据、user_rows 存诱饵数据，覆盖 Find/First/Create/Delete 路径。
func TestPG_ExplicitTableWithProjectionDest(t *testing.T) {
	drv := newPG(t)
	ten := "tenant_tab_regression"

	if err := drv.Query().Exec("CREATE SCHEMA IF NOT EXISTS " + ten); err != nil {
		t.Fatalf("CREATE SCHEMA: %v", err)
	}
	defer func() {
		_ = drv.Query().Exec("DROP SCHEMA IF EXISTS " + ten + " CASCADE")
	}()
	for _, ddl := range []string{
		`CREATE TABLE ` + ten + `.users (id text PRIMARY KEY, name text)`,
		`CREATE TABLE ` + ten + `.user_rows (id text PRIMARY KEY, name text)`,
		`INSERT INTO ` + ten + `.users VALUES ('u1', 'right-table')`,
		`INSERT INTO ` + ten + `.user_rows VALUES ('u1', 'wrong-table')`,
	} {
		if err := drv.Query().Exec(ddl); err != nil {
			t.Fatalf("初始化失败 %q: %v", ddl, err)
		}
	}

	// userRow 投影 dest：推导表名 user_rows 恰为诱饵表，与显式 Table("users") 不一致
	type userRow struct {
		ID   string
		Name string
	}

	assertCount := func(table string, want int64) {
		t.Helper()
		var n int64
		if err := drv.Query().Raw("SELECT count(*) FROM " + ten + "." + table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != want {
			t.Errorf("%s 期望 %d 行, 实际 %d", table, want, n)
		}
	}

	// Find：显式 Table 应命中 users 而非 user_rows 诱饵表
	var rows []userRow
	if err := drv.Query().Schema(ten).Table("users").
		Select("id", "name").Where("id = ?", "u1").Find(&rows); err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "right-table" {
		t.Errorf("Find 应查 users 表, 期望 [right-table], 实际 %+v", rows)
	}

	// First：同上
	var one userRow
	if err := drv.Query().Schema(ten).Table("users").First(&one); err != nil {
		t.Fatalf("First: %v", err)
	}
	if one.Name != "right-table" {
		t.Errorf("First 应查 users 表, 期望 right-table, 实际 %q", one.Name)
	}

	// Create：应写入 users，不动 user_rows
	if err := drv.Query().Schema(ten).Table("users").Create(&userRow{ID: "u2", Name: "created"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	assertCount("users", 2)
	assertCount("user_rows", 1)

	// Delete：应删 users 的行，不动 user_rows
	if err := drv.Query().Schema(ten).Table("users").Where("id = ?", "u2").Delete(&userRow{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	assertCount("users", 1)
	assertCount("user_rows", 1)
}

// ═══════════════════════════════════════════════════════════════════════
// 统一 orm tag 体系集成测试（orm-tag-design.md §11.15/§11.18，U18）。
// 本节全部模型使用纯 orm tag（+ rel/ext），经 NewGormDriver 真实建连，
// 断言读取 information_schema / pg_constraint / pg_indexes 等真实方言元数据。
// 与 mysql_integration_test.go 同包编译，共享模型与 runner 定义在本文件。
// ═══════════════════════════════════════════════════════════════════════

// ── 共享测试基建 ─────────────────────────────────────────────────────

// intgNopLog 测试用框架日志器：把 NewGormDriver 的 SQL 日志重定向到 t.Log
// （失败时自动附带，便于排查集成环境问题）。
type intgNopLog struct{ t *testing.T }

func (l intgNopLog) Debug(args ...any)                         { l.t.Log(args...) }
func (l intgNopLog) Debugf(format string, a ...any)            { l.t.Logf(format, a...) }
func (l intgNopLog) Info(args ...any)                          { l.t.Log(args...) }
func (l intgNopLog) Infof(format string, a ...any)             { l.t.Logf(format, a...) }
func (l intgNopLog) Warn(args ...any)                          { l.t.Log(args...) }
func (l intgNopLog) Warnf(format string, a ...any)             { l.t.Logf(format, a...) }
func (l intgNopLog) Error(args ...any)                         { l.t.Log(args...) }
func (l intgNopLog) Errorf(format string, a ...any)            { l.t.Logf(format, a...) }
func (l intgNopLog) Fatal(args ...any)                         { l.t.Log(args...) }
func (l intgNopLog) Fatalf(format string, a ...any)            { l.t.Logf(format, a...) }
func (l intgNopLog) Panic(args ...any)                         { l.t.Log(args...) }
func (l intgNopLog) Panicf(format string, a ...any)            { l.t.Logf(format, a...) }
func (l intgNopLog) WithField(string, any) contracts.Log       { return l }
func (l intgNopLog) WithFields(map[string]any) contracts.Log   { return l }
func (l intgNopLog) WithError(error) contracts.Log             { return l }
func (l intgNopLog) WithContext(context.Context) contracts.Log { return l }

// newIntgPG 走 NewGormDriver 真实配置链路创建 postgres 驱动（orm tag patch/
// 乐观锁仿真/共享 Preload 全部经此路径），未设置环境变量时跳过。
func newIntgPG(t *testing.T) *GormDriver {
	t.Helper()
	dsn := os.Getenv("GOFAST_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("未设置 GOFAST_TEST_PG_DSN 环境变量，跳过 pgsql 集成测试")
	}
	drv, err := NewGormDriver(contracts.ConnectionConfig{
		Driver: "gormdriver",
		Engine: "postgres",
		DSN:    dsn,
	}, intgNopLog{t: t})
	if err != nil {
		t.Fatalf("创建 gorm postgres 驱动失败: %v", err)
	}
	t.Cleanup(func() { _ = drv.Close() })
	return drv
}

// intgMustExec 原生 DDL/DML 执行辅助。
func intgMustExec(t *testing.T, drv *GormDriver, sql string, values ...any) {
	t.Helper()
	if err := drv.Query().Exec(sql, values...); err != nil {
		t.Fatalf("执行 %q 失败: %v", sql, err)
	}
}

// intgDropTables 幂等清表（用例开始前清残留 + t.Cleanup 清终态）。
func intgDropTables(t *testing.T, drv *GormDriver, tables ...string) {
	t.Helper()
	for _, tb := range tables {
		if err := drv.Query().Exec("DROP TABLE IF EXISTS " + tb); err != nil {
			t.Fatalf("DROP TABLE %s: %v", tb, err)
		}
	}
}

// intgMigrate 迁移模型并登记清理。
func intgMigrate(t *testing.T, drv *GormDriver, tables []string, models ...any) {
	t.Helper()
	intgDropTables(t, drv, tables...)
	if err := drv.AutoMigrate(models...); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	t.Cleanup(func() { intgDropTables(t, drv, tables...) })
}

// intgAssertContains 通用子串断言。
func intgAssertContains(t *testing.T, name, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("%s 应包含 %q\n实际: %s", name, needle, haystack)
	}
}

// ── DDL 全维度模型（§11.15：varchar/decimal/bigint/bool/notnull/default/
//    comment/unique/index(name)/unique(name)/ext check/ext autoIncrementIncrement；
//    unsigned 仅 MySQL（PG 无无符号语义），单独模型见 mysql_integration_test.go）──

type IntgDDLModel struct {
	ID     string  `orm:"pk varchar(16) 'id'"`
	Name   string  `orm:"varchar(100) 'name' notnull default('anon')"`
	Email  string  `orm:"varchar(100) 'email' unique"`
	Amount float64 `orm:"decimal(10,2) 'amount' notnull"`
	BigNum int64   `orm:"bigint 'big_num' default(0)"`
	Active bool    `orm:"bool 'active' default(0)"`
	OrgID  string  `orm:"varchar(16) 'org_id' index(idx_intg_org)"`
	DeptID string  `orm:"varchar(16) 'dept_id' index(idx_intg_org)"`
	Tenant string  `orm:"varchar(16) 'tenant_id' unique(uk_intg_tenant)"`
	TKey   string  `orm:"varchar(32) 't_key' unique(uk_intg_tenant)"`
	Remark string  `orm:"varchar(200) 'remark' comment('备注列')"`
	Age    int     `orm:"'age' default(0)" ext:"check:age >= 0"`
	Seq    int64   `orm:"'seq'" ext:"autoIncrementIncrement:5"`
}

func (IntgDDLModel) TableName() string { return "intg_ddl_models" }

// intgPGColumn 读 PG information_schema 单列元数据（data_type/长度/精度/可空/默认值）。
func intgPGColumn(t *testing.T, drv *GormDriver, column string) map[string]any {
	t.Helper()
	var rows []map[string]any
	err := drv.Query().Raw(
		`SELECT data_type, character_maximum_length, numeric_precision, numeric_scale,
		        is_nullable, column_default
		 FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'intg_ddl_models' AND column_name = ?`,
		column).ScanMap(&rows)
	if err != nil || len(rows) != 1 {
		t.Fatalf("读取列 %s 元数据失败 rows=%d err=%v", column, len(rows), err)
	}
	return rows[0]
}

// intgPGIndexes 读表上全部索引定义（pg_indexes.indexdef 拼接）。
func intgPGIndexes(t *testing.T, drv *GormDriver, table string) string {
	t.Helper()
	var defs []string
	if err := drv.Query().Raw(
		`SELECT indexdef FROM pg_indexes WHERE schemaname = 'public' AND tablename = ?`,
		table).Scan(&defs); err != nil {
		t.Fatalf("读取 %s 索引失败: %v", table, err)
	}
	return strings.Join(defs, "\n")
}

// intgPGCheckDefs 读表上 CHECK 约束定义（pg_get_constraintdef）。
func intgPGCheckDefs(t *testing.T, drv *GormDriver, table string) string {
	t.Helper()
	var defs []string
	if err := drv.Query().Raw(
		`SELECT pg_get_constraintdef(oid) FROM pg_constraint
		 WHERE conrelid = ?::regclass AND contype = 'c'`, table).Scan(&defs); err != nil {
		t.Fatalf("读取 %s CHECK 约束失败: %v", table, err)
	}
	return strings.Join(defs, "\n")
}

// intgPGComment 读列注释（col_description）。
func intgPGComment(t *testing.T, drv *GormDriver, table, column string) string {
	t.Helper()
	var cmt string
	if err := drv.Query().Raw(
		`SELECT coalesce(col_description(?::regclass, ordinal_position), '')
		 FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = ? AND column_name = ?`,
		table, table, column).Scan(&cmt); err != nil {
		t.Fatalf("读取 %s.%s 注释失败: %v", table, column, err)
	}
	return cmt
}

// TestPGOrmTag_DDLFullMatrix（§11.15/§11.18）orm tag 全维度 DDL 等价：
// AutoMigrate 后对 information_schema/pg_constraint/pg_indexes 逐项断言。
func TestPGOrmTag_DDLFullMatrix(t *testing.T) {
	drv := newIntgPG(t)
	intgMigrate(t, drv, []string{"intg_ddl_models"}, &IntgDDLModel{})

	// varchar(100) → character varying(100)
	name := intgPGColumn(t, drv, "name")
	if name["data_type"] != "character varying" {
		t.Errorf("varchar(100) 应映射 character varying, 实际 %v", name["data_type"])
	}
	if fmt.Sprint(name["character_maximum_length"]) != "100" {
		t.Errorf("varchar(100) 长度应为 100, 实际 %v", name["character_maximum_length"])
	}
	// notnull
	if name["is_nullable"] != "NO" {
		t.Errorf("notnull 列应 NOT NULL, 实际 is_nullable=%v", name["is_nullable"])
	}
	// default('anon')
	intgAssertContains(t, "name 默认值", fmt.Sprint(name["column_default"]), "anon")

	// decimal(10,2) → numeric 精度 10/2
	amount := intgPGColumn(t, drv, "amount")
	if amount["data_type"] != "numeric" {
		t.Errorf("decimal(10,2) 应映射 numeric, 实际 %v", amount["data_type"])
	}
	if fmt.Sprint(amount["numeric_precision"]) != "10" || fmt.Sprint(amount["numeric_scale"]) != "2" {
		t.Errorf("decimal(10,2) 精度应为 (10,2), 实际 %v/%v",
			amount["numeric_precision"], amount["numeric_scale"])
	}
	if amount["is_nullable"] != "NO" {
		t.Errorf("amount notnull 应 NOT NULL, 实际 %v", amount["is_nullable"])
	}

	// bigint / bool 类型映射
	if big := intgPGColumn(t, drv, "big_num"); big["data_type"] != "bigint" {
		t.Errorf("bigint 应映射 bigint, 实际 %v", big["data_type"])
	}
	if act := intgPGColumn(t, drv, "active"); act["data_type"] != "boolean" {
		t.Errorf("bool 应映射 boolean, 实际 %v", act["data_type"])
	}

	// default(0) 数值默认值
	intgAssertContains(t, "age 默认值", fmt.Sprint(intgPGColumn(t, drv, "age")["column_default"]), "0")

	// comment('备注列') 两方言存在（PG col_description）
	if cmt := intgPGComment(t, drv, "intg_ddl_models", "remark"); cmt != "备注列" {
		t.Errorf("remark 注释应为 备注列, 实际 %q", cmt)
	}

	// 联合唯一 unique(uk_intg_tenant) 与联合索引 index(idx_intg_org) 落地
	idx := intgPGIndexes(t, drv, "intg_ddl_models")
	t.Logf("intg_ddl_models indexes:\n%s", idx)
	intgAssertContains(t, "联合唯一", idx, "uk_intg_tenant")
	intgAssertContains(t, "联合唯一列 tenant_id", idx, "tenant_id")
	intgAssertContains(t, "联合唯一列 t_key", idx, "t_key")
	intgAssertContains(t, "联合索引", idx, "idx_intg_org")
	intgAssertContains(t, "联合索引列 org_id", idx, "org_id")

	// 列级 unique：应存在覆盖 email 的唯一索引
	var uniqIdx []string
	if err := drv.Query().Raw(
		`SELECT c2.relname FROM pg_index x
		 JOIN pg_class c1 ON c1.oid = x.indrelid
		 JOIN pg_class c2 ON c2.oid = x.indexrelid
		 JOIN pg_attribute a ON a.attrelid = c1.oid AND a.attnum = ANY(x.indkey)
		 WHERE c1.relname = 'intg_ddl_models' AND a.attname = 'email' AND x.indisunique`).Scan(&uniqIdx); err != nil || len(uniqIdx) == 0 {
		t.Errorf("email unique 应存在唯一索引, 实际 %v err=%v", uniqIdx, err)
	}

	// ext check：CHECK 约束存在（pg_constraint）
	chk := intgPGCheckDefs(t, drv, "intg_ddl_models")
	t.Logf("CHECK 约束: %s", chk)
	intgAssertContains(t, "CHECK", chk, "age >= 0")

	// ext autoIncrementIncrement:5：步长是会话级行为（SHOW VARIABLES 的
	// auto_increment_increment 为 MySQL 会话变量），表结构上无从断言——
	// 仅固化"迁移不报错、列正常建立"（与 MySQL 用例同注释）。
	if seq := intgPGColumn(t, drv, "seq"); seq["data_type"] != "bigint" {
		t.Errorf("seq 列应正常建立为 bigint, 实际 %v", seq["data_type"])
	}

	// 幂等：重复 AutoMigrate 不报错
	if err := drv.AutoMigrate(&IntgDDLModel{}); err != nil {
		t.Errorf("重复 AutoMigrate 应幂等不报错: %v", err)
	}
}

// ── 唯一冲突错误映射（§11.14/§11.18）─────────────────────────────────

type IntgDupModel struct {
	ID    string `orm:"pk varchar(16) 'id'"`
	Email string `orm:"varchar(100) 'email' unique"`
}

func (IntgDupModel) TableName() string { return "intg_dups" }

func TestPGOrmTag_DuplicateKeyMapping(t *testing.T) {
	drv := newIntgPG(t)
	intgMigrate(t, drv, []string{"intg_dups"}, &IntgDupModel{})

	q := drv.Query()
	if err := q.Create(&IntgDupModel{ID: "d1", Email: "dup@pg.dev"}); err != nil {
		t.Fatalf("首次插入: %v", err)
	}
	err := q.Create(&IntgDupModel{ID: "d2", Email: "dup@pg.dev"})
	if err == nil {
		t.Fatal("unique 列重复插入应报错（PG SQLSTATE 23505）")
	}
	if !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("唯一冲突应映射 contracts.ErrDuplicatedKey, 实际: %v", err)
	}
}

// ── FOR UPDATE 悲观锁并发互斥（§11.10/§11.18）────────────────────────

type IntgLockModel struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Cnt int    `orm:"'cnt' default(0)"`
}

func (IntgLockModel) TableName() string { return "intg_locks" }

func TestPGOrmTag_ForUpdateMutex(t *testing.T) {
	drv := newIntgPG(t)
	intgMigrate(t, drv, []string{"intg_locks"}, &IntgLockModel{})
	intgMustExec(t, drv, `INSERT INTO intg_locks (id, cnt) VALUES ('lock1', 1)`)

	// DryRun 断言生成 FOR UPDATE
	q := drv.Query().Model(&IntgLockModel{}).Lock(contracts.LockForUpdate)
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]IntgLockModel{}).Statement
	intgAssertContains(t, "DryRun SQL", stmt.SQL.String(), "FOR UPDATE")

	// 事务一：锁定行并持锁（信号量同步），持锁期间改值
	locked := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- drv.Query().Transaction(func(tx contracts.Query) error {
			var row IntgLockModel
			if err := tx.Model(&IntgLockModel{}).Lock(contracts.LockForUpdate).First(&row, "id = ?", "lock1"); err != nil {
				return err
			}
			close(locked)
			<-release // 持锁等待
			return tx.Model(&IntgLockModel{}).Where("id = ?", "lock1").Update("cnt", 99)
		})
	}()
	<-locked

	// 事务二：同一行 FOR UPDATE 应被互斥——用 lock_timeout 观察阻塞
	err := drv.Query().Transaction(func(tx contracts.Query) error {
		if err := tx.Exec("SET LOCAL lock_timeout = '400ms'"); err != nil {
			return err
		}
		var row IntgLockModel
		return tx.Model(&IntgLockModel{}).Lock(contracts.LockForUpdate).First(&row, "id = ?", "lock1")
	})
	if err == nil || !strings.Contains(err.Error(), "lock timeout") {
		t.Fatalf("第二事务应在锁超时失败（FOR UPDATE 互斥）, 实际: %v", err)
	}

	// 释放后：事务一提交，第三事务可获锁并读到持锁事务写入的新值
	close(release)
	if err := <-errCh; err != nil {
		t.Fatalf("事务一: %v", err)
	}
	var after IntgLockModel
	if err := drv.Query().Transaction(func(tx contracts.Query) error {
		return tx.Model(&IntgLockModel{}).Lock(contracts.LockForUpdate).First(&after, "id = ?", "lock1")
	}); err != nil {
		t.Fatalf("释放后 FOR UPDATE 应成功: %v", err)
	}
	if after.Cnt != 99 {
		t.Errorf("持锁事务写入应已提交, 期望 cnt=99, 实际 %d", after.Cnt)
	}
}

// TestPGOrmTag_LockShareMode（§11.10）gorm 生成 FOR SHARE 且真实执行不报错。
func TestPGOrmTag_LockShareMode(t *testing.T) {
	drv := newIntgPG(t)
	intgMigrate(t, drv, []string{"intg_locks"}, &IntgLockModel{})
	intgMustExec(t, drv, `INSERT INTO intg_locks (id, cnt) VALUES ('s1', 7)`)

	// DryRun：SQL 含 FOR SHARE
	q := drv.Query().Model(&IntgLockModel{}).Lock(contracts.LockShareMode)
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]IntgLockModel{}).Statement
	intgAssertContains(t, "DryRun SQL", stmt.SQL.String(), "FOR SHARE")

	// 真实执行不报错且返回数据
	var rows []IntgLockModel
	if err := drv.Query().Model(&IntgLockModel{}).Lock(contracts.LockShareMode).Find(&rows); err != nil {
		t.Fatalf("Lock(LockShareMode) 真实执行不应报错: %v", err)
	}
	if len(rows) != 1 || rows[0].Cnt != 7 {
		t.Errorf("Lock(LockShareMode) 查询结果异常: %+v", rows)
	}
}

// ── 乐观锁真实方言冒烟（§11.18）──────────────────────────────────────

type IntgVerModel struct {
	ID      string `orm:"pk varchar(16) 'id'"`
	Title   string `orm:"varchar(64) 'title'"`
	Version int64  `orm:"version 'version'"`
}

func (IntgVerModel) TableName() string { return "intg_vers" }

// TestPGOrmTag_OptimisticLock orm:"version" 模型：插入置 1、struct 更新自增、
// 陈旧版本 0 行不报错（真实 PG 方言走全链路 patch + 仿真）。
func TestPGOrmTag_OptimisticLock(t *testing.T) {
	drv := newIntgPG(t)
	intgMigrate(t, drv, []string{"intg_vers"}, &IntgVerModel{})

	q := drv.Query()
	doc := &IntgVerModel{ID: "v1", Title: "t1"}
	if err := q.Create(doc); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if doc.Version != 1 {
		t.Fatalf("插入后 version 应置 1, 实际 %d", doc.Version)
	}

	got := &IntgVerModel{}
	if err := q.Model(&IntgVerModel{}).Where("id = ?", "v1").Take(got); err != nil {
		t.Fatalf("Take: %v", err)
	}
	got.Title = "t2"
	res := q.Model(&IntgVerModel{}).Where("id = ?", "v1").UpdatesResult(got)
	if res.Error != nil || res.RowsAffected != 1 {
		t.Fatalf("struct 更新期望 1 行, 实际 rows=%d err=%v", res.RowsAffected, res.Error)
	}
	if got.Version != 2 {
		t.Fatalf("struct 更新后 version 应回填 +1 为 2, 实际 %d", got.Version)
	}

	// 陈旧版本（库中已是 2）：0 行命中不报错，数据不变
	stale := &IntgVerModel{ID: "v1", Title: "stale", Version: 1}
	res = q.Model(&IntgVerModel{}).Where("id = ?", "v1").UpdatesResult(stale)
	if res.Error != nil {
		t.Fatalf("陈旧版本更新不应报错: %v", res.Error)
	}
	if res.RowsAffected != 0 {
		t.Fatalf("陈旧版本更新应命中 0 行, 实际 %d", res.RowsAffected)
	}
	var unchanged IntgVerModel
	if err := q.Model(&IntgVerModel{}).Where("id = ?", "v1").Take(&unchanged); err != nil {
		t.Fatal(err)
	}
	if unchanged.Version != 2 || unchanged.Title != "t2" {
		t.Errorf("陈旧版本更新不应改动数据: %+v", unchanged)
	}
}

// ── ext 双驱动行为矩阵（§11.16/§11.18）───────────────────────────────

type IntgCheckModel struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Age int    `orm:"'age' default(0)" ext:"check:age >= 0"`
}

func (IntgCheckModel) TableName() string { return "intg_checks" }

type IntgMigModel struct {
	ID     string `orm:"pk varchar(16) 'id'"`
	Remark string `orm:"varchar(100) 'remark'" ext:"migration:false"`
}

func (IntgMigModel) TableName() string { return "intg_migs" }

type IntgTimeModel struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	CreatedAt int64  `orm:"created 'created_at'" ext:"timePrecision:milli"`
}

func (IntgTimeModel) TableName() string { return "intg_times" }

// TestPGOrmTag_ExtMatrix（§11.16）check 拒绝非法行 / migration:false 不建列
// 但读写正常 / timePrecision:milli 填毫秒。
func TestPGOrmTag_ExtMatrix(t *testing.T) {
	drv := newIntgPG(t)

	// check：DDL 有 CHECK + 非法行被数据库拒绝
	intgMigrate(t, drv, []string{"intg_checks"}, &IntgCheckModel{})
	intgAssertContains(t, "CHECK 约束", intgPGCheckDefs(t, drv, "intg_checks"), "age >= 0")
	if err := drv.Query().Create(&IntgCheckModel{ID: "ok1", Age: 5}); err != nil {
		t.Fatalf("合法行 Create 不应报错: %v", err)
	}
	if err := drv.Query().Create(&IntgCheckModel{ID: "bad1", Age: -1}); err == nil {
		t.Error("违反 CHECK 约束的行应被数据库拒绝")
	}

	// migration:false：不建列；外部 DDL 建列后读写正常
	intgMigrate(t, drv, []string{"intg_migs"}, &IntgMigModel{})
	var n int64
	if err := drv.Query().Raw(
		`SELECT count(*) FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = 'intg_migs' AND column_name = 'remark'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("migration:false 不应建列 remark, count=%d err=%v", n, err)
	}
	intgMustExec(t, drv, `ALTER TABLE intg_migs ADD COLUMN remark varchar(100)`)
	if err := drv.Query().Create(&IntgMigModel{ID: "m1", Remark: "kept"}); err != nil {
		t.Fatalf("Create（保留读写）: %v", err)
	}
	var got IntgMigModel
	if err := drv.Query().First(&got, "id = ?", "m1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Remark != "kept" {
		t.Errorf("migration:false 字段读写应正常, 实际 %q", got.Remark)
	}

	// timePrecision:milli：created 填 unix 毫秒（量级 1e12+）
	intgMigrate(t, drv, []string{"intg_times"}, &IntgTimeModel{})
	before := time.Now().UnixMilli()
	if err := drv.Query().Create(&IntgTimeModel{ID: "t1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	after := time.Now().UnixMilli()
	var tm IntgTimeModel
	if err := drv.Query().First(&tm, "id = ?", "t1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if tm.CreatedAt < 1e12 {
		t.Errorf("timePrecision:milli 的 created 应为毫秒量级（>=1e12）, 实际 %d", tm.CreatedAt)
	}
	if tm.CreatedAt < before || tm.CreatedAt > after {
		t.Errorf("created 应为 unix 毫秒（%d ∈ [%d,%d]）, 实际 %d", tm.CreatedAt, before, after, tm.CreatedAt)
	}
}

// ── Preload 全套（§11.18：套件同款模型结构，orm tag + rel）───────────
//
// IntgUser.Orders / IntgOrder.Items / IntgUser.Dept 外键命中 gorm 约定
//（<父类型名><父主键名>）→ gorm 原生 Preload；Roles/Photos 偏离约定，
// gorm:"-" + rel → 共享 Preload 引擎。双路径在真实方言上同时验证。

type IntgUser struct {
	ID     string      `orm:"pk varchar(16) 'id'"`
	Name   string      `orm:"varchar(64) 'name'"`
	DeptID string      `orm:"varchar(16) 'dept_id' null"`
	Orders []IntgOrder `orm:"-" rel:"foreignKey:IntgUserID;references:ID"`
	Dept   *IntgDept   `orm:"-" rel:"foreignKey:DeptID;references:ID"`
	Roles  []IntgRole  `orm:"-" gorm:"-" rel:"many2many:intg_user_roles;joinForeignKey:ID;joinReferences:Code"`
	Photos []IntgPhoto `orm:"-" gorm:"-" rel:"polymorphic:Owner;polymorphicValue:intg_users"`
}

func (IntgUser) TableName() string { return "intg_users" }

type IntgOrder struct {
	ID         string     `orm:"pk varchar(16) 'id'"`
	IntgUserID string     `orm:"varchar(16) 'user_id'"`
	Amount     int        `orm:"'amount' default(0)"`
	Items      []IntgItem `orm:"-" rel:"foreignKey:IntgOrderID;references:ID"`
}

func (IntgOrder) TableName() string { return "intg_orders" }

type IntgItem struct {
	ID          string `orm:"pk varchar(16) 'id'"`
	IntgOrderID string `orm:"varchar(16) 'order_id'"`
	Sku         string `orm:"varchar(64) 'sku'"`
}

func (IntgItem) TableName() string { return "intg_items" }

type IntgDept struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Name string `orm:"varchar(64) 'name'"`
}

func (IntgDept) TableName() string { return "intg_depts" }

type IntgRole struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Code string `orm:"varchar(16) 'code'"`
	Name string `orm:"varchar(64) 'name'"`
}

func (IntgRole) TableName() string { return "intg_roles" }

type IntgPhoto struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	OwnerID   string `orm:"varchar(16) 'owner_id'"`
	OwnerType string `orm:"varchar(32) 'owner_type'"`
	URL       string `orm:"varchar(120) 'url'"`
}

func (IntgPhoto) TableName() string { return "intg_photos" }

// runIntgPreloadMatrix PG/MySQL 共用的 Preload 全套断言（11.18）：
// has-many / 嵌套 / belongs-to / many2many / polymorphic 各一例。
func runIntgPreloadMatrix(t *testing.T, drv *GormDriver) {
	t.Helper()
	tables := []string{
		"intg_user_roles", "intg_users", "intg_orders", "intg_items",
		"intg_depts", "intg_roles", "intg_photos",
	}
	intgMigrate(t, drv, tables,
		&IntgUser{}, &IntgOrder{}, &IntgItem{}, &IntgDept{}, &IntgRole{}, &IntgPhoto{})
	intgMustExec(t, drv, `CREATE TABLE intg_user_roles (id varchar(16) NOT NULL, code varchar(16) NOT NULL)`)

	q := drv.Query()
	seed := []any{
		&IntgDept{ID: "d1", Name: "研发部"},
		&IntgUser{ID: "u1", Name: "alice", DeptID: "d1"},
		&IntgUser{ID: "u2", Name: "bob"},
		&IntgUser{ID: "u3", Name: "carol"}, // 无订单/无照片
		&IntgOrder{ID: "o1", IntgUserID: "u1", Amount: 100},
		&IntgOrder{ID: "o2", IntgUserID: "u1", Amount: 250},
		&IntgOrder{ID: "o3", IntgUserID: "u2", Amount: 50},
		&IntgItem{ID: "i1", IntgOrderID: "o1", Sku: "sku1"},
		&IntgItem{ID: "i2", IntgOrderID: "o2", Sku: "sku2"},
		&IntgItem{ID: "i3", IntgOrderID: "o3", Sku: "sku3"},
		&IntgRole{ID: "r1", Code: "c1", Name: "管理员"},
		&IntgRole{ID: "r2", Code: "c2", Name: "审计"},
		&IntgPhoto{ID: "p1", OwnerID: "u1", OwnerType: "intg_users", URL: "u1a"},
		&IntgPhoto{ID: "p2", OwnerID: "u1", OwnerType: "intg_users", URL: "u1b"},
		&IntgPhoto{ID: "px", OwnerID: "u1", OwnerType: "other_type", URL: "noise"}, // 类型过滤负面用例
	}
	for _, s := range seed {
		if err := q.Create(s); err != nil {
			t.Fatalf("种子 %T %+v: %v", s, s, err)
		}
	}
	intgMustExec(t, drv,
		`INSERT INTO intg_user_roles (id, code) VALUES ('u1','c1'), ('u1','c2'), ('u2','c2')`)

	var users []IntgUser
	err := q.Model(&IntgUser{}).
		Preload("Orders.Items").Preload("Dept").Preload("Roles").Preload("Photos").Find(&users)
	if err != nil {
		t.Fatalf("Preload 全套查询失败: %v", err)
	}
	if len(users) != 3 {
		t.Fatalf("应查到 3 个用户, 实际 %d", len(users))
	}
	byID := make(map[string]*IntgUser, len(users))
	for i := range users {
		byID[users[i].ID] = &users[i]
	}

	// has-many + 嵌套（gorm 原生路径）
	u1 := byID["u1"]
	if len(u1.Orders) != 2 {
		t.Fatalf("u1 应预加载 2 个订单, 实际 %d", len(u1.Orders))
	}
	skuByOrder := map[string]string{}
	for _, o := range u1.Orders {
		if len(o.Items) != 1 {
			t.Fatalf("订单 %s 应预加载 1 个明细, 实际 %d", o.ID, len(o.Items))
		}
		skuByOrder[o.ID] = o.Items[0].Sku
	}
	if skuByOrder["o1"] != "sku1" || skuByOrder["o2"] != "sku2" {
		t.Errorf("嵌套明细回填错误: %v", skuByOrder)
	}
	u2 := byID["u2"]
	if len(u2.Orders) != 1 || len(u2.Orders[0].Items) != 1 || u2.Orders[0].Items[0].Sku != "sku3" {
		t.Errorf("u2 订单 o3 明细回填错误: %+v", u2.Orders)
	}
	if len(byID["u3"].Orders) != 0 {
		t.Errorf("u3 无订单应回填空集合, 实际 %+v", byID["u3"].Orders)
	}

	// belongs-to（gorm 原生路径）
	if u1.Dept == nil || u1.Dept.Name != "研发部" {
		t.Errorf("u1 belongs-to 部门应回填, 实际 %+v", u1.Dept)
	}
	if u2.Dept != nil {
		t.Errorf("u2 无部门应为 nil, 实际 %+v", u2.Dept)
	}

	// many2many（共享引擎两跳）
	roleCodes := map[string]bool{}
	for _, r := range u1.Roles {
		roleCodes[r.Code] = true
	}
	if len(roleCodes) != 2 || !roleCodes["c1"] || !roleCodes["c2"] {
		t.Errorf("u1 应预加载角色 c1/c2, 实际 %v", u1.Roles)
	}
	if len(u2.Roles) != 1 || u2.Roles[0].Code != "c2" {
		t.Errorf("u2 应仅预加载角色 c2, 实际 %+v", u2.Roles)
	}

	// polymorphic（共享引擎类型列过滤）
	if len(u1.Photos) != 2 {
		t.Errorf("u1 应预加载 2 张照片（other_type 被过滤）, 实际 %+v", u1.Photos)
	}
	for _, p := range u1.Photos {
		if p.OwnerType != "intg_users" {
			t.Errorf("多态类型过滤失效: %+v", p)
		}
	}
}

func TestPGOrmTag_PreloadMatrix(t *testing.T) {
	drv := newIntgPG(t)
	runIntgPreloadMatrix(t, drv)
}

// ── 多租户 schema 全链（§11.12/§11.18，PG 专属，orm tag 模型形态）────
//
// 注意：本组模型不实现 TableName()——动态 Schema() 的租户前缀对 Preload
// 子查询 / Joins 关联表经 NamingStrategy 克隆（TablePrefix=租户 schema）统一
// 生效，而 NamingStrategy 对实现 TableName() 的模型整体旁路（主表可由
// applySchema 兜底，JOIN 表无从兜底）。gorm 约定推导的表名恰为
// intg_tenant_users / intg_tenant_orders，与本组断言一致。

type IntgTenantUser struct {
	ID     string            `orm:"pk varchar(16) 'id'"`
	Name   string            `orm:"varchar(64) 'name'"`
	Orders []IntgTenantOrder `orm:"-" rel:"foreignKey:IntgTenantUserID;references:ID"`
}

type IntgTenantOrder struct {
	ID               string `orm:"pk varchar(16) 'id'"`
	IntgTenantUserID string `orm:"varchar(16) 'user_id'"`
	Amount           int    `orm:"'amount' default(0)"`
	// belongs-to 约定外键：gorm 约定 FK 名 = 关联字段名 + 父主键名
	//（IntgTenantUser + ID → IntgTenantUserID），字段名必须同型
	IntgTenantUser *IntgTenantUser `orm:"-" rel:"foreignKey:IntgTenantUserID;references:ID"`
}

// TestPGOrmTag_TenantFullChain：Schema("tenant_x") + orm tag 模型——
// AutoMigrate 建在租户 schema、Model/Table 与 Schema 调用顺序无关、
// Preload 子查询与 Joins 关联表带 schema 前缀。
func TestPGOrmTag_TenantFullChain(t *testing.T) {
	drv := newIntgPG(t)
	ten := "tenant_intgorm"

	// 建租户 schema（幂等 + 清理），AutoMigrate 走 SET LOCAL search_path 路径
	intgMustExec(t, drv, "DROP SCHEMA IF EXISTS "+ten+" CASCADE")
	intgMustExec(t, drv, "CREATE SCHEMA "+ten)
	t.Cleanup(func() { _ = drv.Query().Exec("DROP SCHEMA IF EXISTS " + ten + " CASCADE") })

	tenantDrv, err := NewGormDriver(contracts.ConnectionConfig{
		Driver: "gormdriver",
		Engine: "postgres",
		DSN:    os.Getenv("GOFAST_TEST_PG_DSN"),
		Schema: ten,
	}, intgNopLog{t: t})
	if err != nil {
		t.Fatalf("创建租户驱动: %v", err)
	}
	t.Cleanup(func() { _ = tenantDrv.Close() })
	if err := tenantDrv.AutoMigrate(&IntgTenantUser{}, &IntgTenantOrder{}); err != nil {
		t.Fatalf("租户 schema AutoMigrate: %v", err)
	}

	// 表落在正确 schema（public 下不存在同名表）
	var n int64
	if err := drv.Query().Raw(
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema = ? AND table_name = 'intg_tenant_users'`, ten).Scan(&n); err != nil || n != 1 {
		t.Fatalf("intg_tenant_users 应建在 schema %s, count=%d err=%v", ten, n, err)
	}
	if err := drv.Query().Raw(
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema = 'public' AND table_name = 'intg_tenant_users'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("public 下不应存在同名表, count=%d err=%v", n, err)
	}

	// 灌租户数据（动态 Schema 前缀路径；连接级 NamingStrategy 前缀对
	// 实现 TableName() 的模型不生效，由 applySchema 兜底拼前缀）
	tq := drv.Query().Schema(ten)
	for _, s := range []any{
		&IntgTenantUser{ID: "tu1", Name: "租户用户"},
		&IntgTenantOrder{ID: "to1", IntgTenantUserID: "tu1", Amount: 66},
		&IntgTenantOrder{ID: "to2", IntgTenantUserID: "tu1", Amount: 88},
	} {
		if err := tq.Create(s); err != nil {
			t.Fatalf("灌租户数据 %T: %v", s, err)
		}
	}

	// Schema 与 Model 调用顺序无关（两种顺序均命中租户表）
	var usersA []IntgTenantUser
	if err := drv.Query().Schema(ten).Model(&IntgTenantUser{}).Find(&usersA); err != nil {
		t.Fatalf("Schema→Model 顺序查询失败: %v", err)
	}
	if len(usersA) != 1 || usersA[0].Name != "租户用户" {
		t.Errorf("Schema→Model 应命中租户表, 实际 %+v", usersA)
	}
	var usersB []IntgTenantUser
	if err := drv.Query().Model(&IntgTenantUser{}).Schema(ten).Find(&usersB); err != nil {
		t.Fatalf("Model→Schema 顺序查询失败: %v", err)
	}
	if len(usersB) != 1 {
		t.Errorf("Model→Schema 应命中租户表, 实际 %+v", usersB)
	}

	// 显式 Table() 投影同样带 schema 前缀（顺序无关第二形态）
	var names []map[string]any
	if err := drv.Query().Schema(ten).Table("intg_tenant_users").
		Select("id", "name").Where("id = ?", "tu1").ScanMap(&names); err != nil {
		t.Fatalf("Schema→Table 投影失败: %v", err)
	}
	if len(names) != 1 {
		t.Errorf("Schema→Table 投影应命中租户表 1 行, 实际 %v", names)
	}

	// Preload 子查询带 schema 前缀（租户表关联数据正确）
	var roots []IntgTenantUser
	if err := drv.Query().Schema(ten).Model(&IntgTenantUser{}).
		Preload("Orders").Find(&roots); err != nil {
		t.Fatalf("租户 Preload: %v", err)
	}
	if len(roots) != 1 || len(roots[0].Orders) != 2 {
		t.Fatalf("租户 Preload Orders 应回填 2 单, 实际 %+v", roots)
	}

	// Joins 关联表带 schema 前缀（真实执行）。
	// Joins 的 JOIN 表名出自 gorm schema 缓存的关联元数据，动态 Schema()
	// 链无法重写（首次解析的缓存表名不带前缀，连接级 NamingStrategy 才能承载），
	// 故按 §11.12 设计路径走连接级租户驱动（NamingStrategy TablePrefix）断言。
	// DryRun 固化 has-many JOIN 表带租户前缀；gorm 对 has-many Joins 无模型
	// dest 扫描能力（slice 关联字段会 reflect panic，gorm 固有限制），真实执行
	// 用 belongs-to 方向 JOIN（tenant_orders → tenant_users）验证。
	joinQ := tenantDrv.Query().Model(&IntgTenantUser{}).Joins("Orders")
	joinStmt := joinQ.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]IntgTenantUser{}).Statement
	intgAssertContains(t, "Joins SQL", joinStmt.SQL.String(), `"tenant_intgorm"."intg_tenant_orders"`)

	var orders []IntgTenantOrder
	if err := tenantDrv.Query().Model(&IntgTenantOrder{}).
		Joins("IntgTenantUser").Where("intg_tenant_orders.id = ?", "to1").Find(&orders); err != nil {
		t.Fatalf("租户 Joins: %v", err)
	}
	if len(orders) != 1 || orders[0].IntgTenantUser == nil || orders[0].IntgTenantUser.ID != "tu1" {
		t.Errorf("租户 Joins 应命中 to1 并回填 IntgTenantUser=tu1, 实际 %+v", orders)
	}
}
