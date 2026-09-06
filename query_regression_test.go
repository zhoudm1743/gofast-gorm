package gormdriver

import (
	"context"
	"strings"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

type SoftTestModel struct {
	ID        string `gorm:"primaryKey;size:16"`
	Name      string `gorm:"size:100"`
	DeletedAt int64  `gorm:"column:deleted_at;default:0"`
}

func (m *SoftTestModel) AutoGenerateID() {}

func newSoftTable(t *testing.T) *GormDriver {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&SoftTestModel{}); err != nil {
		t.Fatal(err)
	}
	return &GormDriver{db: db}
}

// ── 钩子完整性 ─────────────────────────────────────────────────────

func TestCreateInBatches_Hooks(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&HookTestModel{}); err != nil {
		t.Fatal(err)
	}
	ms := []*HookTestModel{{ID: "b1", Name: "a"}, {ID: "b2", Name: "b"}}
	if err := drv.Query().CreateInBatches(ms, 1); err != nil {
		t.Fatalf("CreateInBatches 失败: %v", err)
	}
	if !ms[0].BeforeCreateCalled || !ms[1].BeforeCreateCalled {
		t.Error("CreateInBatches 应调用 BeforeCreate")
	}
	if !ms[0].AfterCreateCalled || !ms[1].AfterCreateCalled {
		t.Error("CreateInBatches 应调用 AfterCreate（当前疑似未调用）")
	}
}

// ── 软删除 ─────────────────────────────────────────────────────────

func TestSoftDelete_Lifecycle(t *testing.T) {
	drv := newSoftTable(t)
	q := drv.Query()

	if err := q.Create(&SoftTestModel{ID: "sd1", Name: "alive"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// 软删除
	if err := q.Model(&SoftTestModel{}).Where("id = ?", "sd1").Update("deleted_at", 1); err != nil {
		t.Fatalf("软删除: %v", err)
	}

	// OnlyTrashed 应查到
	var trashed []SoftTestModel
	if err := q.Model(&SoftTestModel{}).OnlyTrashed().Find(&trashed); err != nil {
		t.Fatalf("OnlyTrashed: %v", err)
	}
	if len(trashed) != 1 {
		t.Errorf("OnlyTrashed 期望 1 条, 实际 %d", len(trashed))
	}

	// Restore 恢复
	if err := q.Model(&SoftTestModel{}).Where("id = ?", "sd1").Restore(); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	var restored SoftTestModel
	if err := q.First(&restored, "id = ?", "sd1"); err != nil {
		t.Fatalf("恢复后 First: %v", err)
	}
	if restored.DeletedAt != 0 {
		t.Errorf("恢复后 DeletedAt 应为 0, 实际 %d", restored.DeletedAt)
	}
}

func TestSoftDelete_ForceDelete(t *testing.T) {
	drv := newSoftTable(t)
	q := drv.Query()
	if err := q.Create(&SoftTestModel{ID: "sd2", Name: "del"}); err != nil {
		t.Fatal(err)
	}
	if err := q.ForceDelete(&SoftTestModel{}, "id = ?", "sd2"); err != nil {
		t.Fatalf("ForceDelete: %v", err)
	}
	var count int64
	q.Model(&SoftTestModel{}).Unscoped().Count(&count)
	if count != 0 {
		t.Errorf("ForceDelete 后应 0 条, 实际 %d", count)
	}
}

// ── 分页边界 ───────────────────────────────────────────────────────

func TestPaginate_Edge(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	for i := 0; i < 25; i++ {
		if err := q.Create(&TestModel{ID: "p" + string(rune('a'+i/26)) + string(rune('a'+i%26)), Name: "n"}); err != nil {
			t.Fatal(err)
		}
	}
	var rows []TestModel
	if err := q.Model(&TestModel{}).Paginate(0, 0).Find(&rows); err != nil {
		t.Fatalf("Paginate(0,0): %v", err)
	}
	if len(rows) != 20 {
		t.Errorf("Paginate(0,0) 应默认 size=20, 实际 %d", len(rows))
	}

	rows = nil
	if err := q.Model(&TestModel{}).Paginate(2, 10).Find(&rows); err != nil {
		t.Fatalf("Paginate(2,10): %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("Paginate(2,10) 应 10 条, 实际 %d", len(rows))
	}
}

// ── 条件链 ─────────────────────────────────────────────────────────

func TestWhereOrNot(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	ids := []string{"w1", "w2", "w3"}
	names := []string{"alice", "bob", "carol"}
	for i := range ids {
		if err := q.Create(&TestModel{ID: ids[i], Name: names[i]}); err != nil {
			t.Fatal(err)
		}
	}
	var rows []TestModel
	if err := q.Model(&TestModel{}).Where("name = ?", "alice").OrWhere("name = ?", "bob").Find(&rows); err != nil {
		t.Fatalf("Where/OrWhere: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Where/OrWhere 期望 2 条, 实际 %d", len(rows))
	}

	rows = nil
	if err := q.Model(&TestModel{}).Not("name = ?", "carol").Find(&rows); err != nil {
		t.Fatalf("Not: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Not 期望 2 条, 实际 %d", len(rows))
	}
}

func TestOrderLimitOffset(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	for i := 0; i < 5; i++ {
		if err := q.Create(&TestModel{ID: "o" + string(rune('0'+i)), Name: "n" + string(rune('0'+i))}); err != nil {
			t.Fatal(err)
		}
	}
	var rows []TestModel
	if err := q.Model(&TestModel{}).Order("name desc").Limit(2).Offset(1).Find(&rows); err != nil {
		t.Fatalf("Order/Limit/Offset: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("期望 2 条, 实际 %d", len(rows))
	}
	if rows[0].Name != "n3" {
		t.Errorf("Order desc 后 Offset(1) 第一条应为 n3, 实际 %q", rows[0].Name)
	}
}

// ── Distinct / Group / Having ──────────────────────────────────────

func TestDistinctGroupHaving(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	// 用 TestModel 的 name 做 group 测试
	data := []struct{ id, name string }{{"d1", "x"}, {"d2", "x"}, {"d3", "y"}}
	for _, d := range data {
		if err := q.Create(&TestModel{ID: d.id, Name: d.name}); err != nil {
			t.Fatal(err)
		}
	}

	var names []string
	if err := q.Model(&TestModel{}).Distinct().Pluck("name", &names); err != nil {
		t.Fatalf("Distinct Pluck: %v", err)
	}
	if len(names) != 2 {
		t.Errorf("Distinct 期望 2 个不同 name, 实际 %v", names)
	}

	type agg struct {
		Name  string
		Count int64
	}
	var aggs []agg
	if err := q.Model(&TestModel{}).Select("name, count(*) as count").Group("name").Having("count(*) > ?", 1).Scan(&aggs); err != nil {
		t.Fatalf("Group/Having: %v", err)
	}
	if len(aggs) != 1 || aggs[0].Name != "x" || aggs[0].Count != 2 {
		t.Errorf("Group/Having 期望 [{x 2}], 实际 %+v", aggs)
	}
}

// ── Scopes ─────────────────────────────────────────────────────────

func TestScopes(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	for i, n := range []string{"apple", "apricot", "banana"} {
		if err := q.Create(&TestModel{ID: "sc" + string(rune('0'+i)), Name: n}); err != nil {
			t.Fatal(err)
		}
	}
	startWithA := func(q contracts.Query) contracts.Query {
		return q.Where("name LIKE ?", "a%")
	}
	var rows []TestModel
	if err := q.Model(&TestModel{}).Scopes(startWithA).Find(&rows); err != nil {
		t.Fatalf("Scopes: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("Scopes 期望 2 条, 实际 %d", len(rows))
	}
}

// ── 事务 ───────────────────────────────────────────────────────────

func TestTransaction_Commit(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	err := q.Transaction(func(tx contracts.Query) error {
		return tx.Create(&TestModel{ID: "tx1", Name: "in_tx"})
	})
	if err != nil {
		t.Fatalf("Transaction: %v", err)
	}
	var count int64
	q.Model(&TestModel{}).Count(&count)
	if count != 1 {
		t.Errorf("事务提交后应 1 条, 实际 %d", count)
	}
}

func TestTransaction_Rollback(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	err := q.Transaction(func(tx contracts.Query) error {
		if err := tx.Create(&TestModel{ID: "tx2", Name: "rollback"}); err != nil {
			return err
		}
		return gorm.ErrInvalidTransaction // 触发回滚
	})
	if err == nil {
		t.Fatal("事务应返回错误")
	}
	var count int64
	q.Model(&TestModel{}).Count(&count)
	if count != 0 {
		t.Errorf("回滚后应 0 条, 实际 %d", count)
	}
}

func TestBeginCommitRollback(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	tx := q.Begin()
	if err := tx.Create(&TestModel{ID: "bc1", Name: "x"}); err != nil {
		t.Fatalf("Begin/Create: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	tx2 := q.Begin()
	if err := tx2.Create(&TestModel{ID: "bc2", Name: "y"}); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var count int64
	q.Model(&TestModel{}).Count(&count)
	if count != 1 {
		t.Errorf("回滚后仅 commit 的 1 条应存在, 实际 %d", count)
	}
}

// ── 高级查询 ───────────────────────────────────────────────────────

func TestFirstOrCreate(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	m := &TestModel{ID: "foc1", Name: "alice"}
	if err := q.FirstOrCreate(m, "id = ?", "foc1"); err != nil {
		t.Fatalf("FirstOrCreate: %v", err)
	}
	var count int64
	q.Model(&TestModel{}).Count(&count)
	if count != 1 {
		t.Errorf("应 1 条, 实际 %d", count)
	}
	// 再次调用不应重复创建
	m2 := &TestModel{ID: "foc1", Name: "alice2"}
	if err := q.FirstOrCreate(m2, "id = ?", "foc1"); err != nil {
		t.Fatal(err)
	}
	q.Model(&TestModel{}).Count(&count)
	if count != 1 {
		t.Errorf("FirstOrCreate 重复调用仍应 1 条, 实际 %d", count)
	}
}

func TestFindInBatches(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	for i := 0; i < 7; i++ {
		if err := q.Create(&TestModel{ID: "fb" + string(rune('0'+i)), Name: "n"}); err != nil {
			t.Fatal(err)
		}
	}
	batchSizes := []int{}
	var all []TestModel
	if err := q.Model(&TestModel{}).FindInBatches(&all, 3, func(tx contracts.Query, batch int) error {
		batchSizes = append(batchSizes, batch)
		return nil
	}); err != nil {
		t.Fatalf("FindInBatches: %v", err)
	}
	if len(batchSizes) != 3 {
		t.Errorf("7 条按 3 分批应 3 批, 实际 %d 批 %v", len(batchSizes), batchSizes)
	}
}

func TestExists(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	if err := q.Create(&TestModel{ID: "ex1", Name: "x"}); err != nil {
		t.Fatal(err)
	}
	ok, err := q.Exists(&TestModel{}, "name = ?", "x")
	if err != nil || !ok {
		t.Errorf("Exists 应 true, ok=%v err=%v", ok, err)
	}
	ok, err = q.Exists(&TestModel{}, "name = ?", "none")
	if err != nil || ok {
		t.Errorf("Exists 应 false, ok=%v err=%v", ok, err)
	}
}

// ── ScanMap 边界 ───────────────────────────────────────────────────

func TestScanMap_EmptyTable(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	var rows []map[string]any
	if err := q.Model(&TestModel{}).ScanMap(&rows); err != nil {
		t.Fatalf("空表 ScanMap: %v", err)
	}
	if rows != nil && len(rows) != 0 {
		t.Errorf("空表 ScanMap 应空, 实际 %v", rows)
	}
}

// ── walkValues 钩子边界 ────────────────────────────────────────────

func TestWalkValues_Boundaries(t *testing.T) {
	count := 0
	fn := func(iface any) error {
		count++
		return nil
	}

	// nil 接口
	if err := walkValues(nil, fn); err != nil {
		t.Errorf("walkValues(nil) 不应报错: %v", err)
	}

	// nil 指针
	var m *TestModel
	if err := walkValues(m, fn); err != nil {
		t.Errorf("walkValues((*T)(nil)) 不应报错: %v", err)
	}

	// 空切片
	var empty []*TestModel
	if err := walkValues(empty, fn); err != nil {
		t.Errorf("walkValues(空切片) 不应报错: %v", err)
	}

	// 切片含 nil 元素
	mixed := []*TestModel{nil, {ID: "hn1", Name: "x"}, nil}
	if err := walkValues(mixed, fn); err != nil {
		t.Errorf("walkValues(含 nil 元素) 不应报错: %v", err)
	}
	if count != 1 {
		t.Errorf("walkValues 应跳过 nil 元素只计数 1, 实际 %d", count)
	}
}

// ── Raw / Exec ─────────────────────────────────────────────────────

func TestRawExec(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	if err := q.Exec("INSERT INTO test_models (id, name) VALUES (?, ?)", "re1", "raw"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	var rows []TestModel
	if err := q.Raw("SELECT * FROM test_models WHERE name = ?", "raw").Scan(&rows); err != nil {
		t.Fatalf("Raw/Scan: %v", err)
	}
	if len(rows) != 1 || rows[0].ID != "re1" {
		t.Errorf("Raw 查询期望 re1, 实际 %+v", rows)
	}
}

// ── Row / Rows ─────────────────────────────────────────────────────

func TestRowAndRows(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	if err := q.Create(&TestModel{ID: "rr1", Name: "alice"}); err != nil {
		t.Fatal(err)
	}
	if err := q.Create(&TestModel{ID: "rr2", Name: "bob"}); err != nil {
		t.Fatal(err)
	}

	rows, err := q.Model(&TestModel{}).Order("id").Rows()
	if err != nil {
		t.Fatalf("Rows: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("rows.Scan: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Errorf("Rows 期望 2 条, 实际 %d", count)
	}
}

// ── WithContext / Debug ────────────────────────────────────────────

func TestWithContext(t *testing.T) {
	drv := newTestDriverWithTable(t)
	ctx := context.Background()
	q := drv.Query().WithContext(ctx)
	if err := q.Model(&TestModel{}).Count(new(int64)); err != nil {
		t.Fatalf("WithContext Count: %v", err)
	}
	_ = q.Debug()
}

// ── Lock ───────────────────────────────────────────────────────────

func TestLock_SQL(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query().Model(&TestModel{}).Lock(contracts.LockForUpdate)
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]TestModel{}).Statement
	t.Logf("Lock ForUpdate SQL: %s", stmt.SQL.String())
}

// ── Unscoped ───────────────────────────────────────────────────────

func TestUnscoped(t *testing.T) {
	drv := newSoftTable(t)
	q := drv.Query()
	if err := q.Create(&SoftTestModel{ID: "un1", Name: "x"}); err != nil {
		t.Fatal(err)
	}
	// 软删除（直接改 deleted_at）
	if err := q.Model(&SoftTestModel{}).Where("id = ?", "un1").Update("deleted_at", 1); err != nil {
		t.Fatal(err)
	}
	var all []SoftTestModel
	if err := q.Unscoped().Model(&SoftTestModel{}).Find(&all); err != nil {
		t.Fatalf("Unscoped Find: %v", err)
	}
	if len(all) != 1 {
		t.Errorf("Unscoped 应查到已删除记录, 实际 %d", len(all))
	}
}

// 带默认 schema 前缀的驱动（模拟 cfg.Schema="public" 场景）
func newPrefixedDriver(t *testing.T) *GormDriver {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{TablePrefix: "public."},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &GormDriver{db: db}
}

func TestSchema_DynamicTablePrefix(t *testing.T) {
	drv := newPrefixedDriver(t)

	// 未设置租户 schema：表名沿用默认 "public."
	q := drv.Query().Model(&TestModel{})
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]TestModel{}).Statement
	t.Logf("无 Schema SQL: %s", stmt.SQL.String())
	if !strings.Contains(stmt.SQL.String(), "public") {
		t.Errorf("无 Schema 应带 public 前缀, SQL: %s", stmt.SQL.String())
	}

	// 设置租户 schema：表名应切换为 tenant1，而非 public
	q2 := drv.Query().Schema("tenant1").Model(&TestModel{})
	stmt2 := q2.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]TestModel{}).Statement
	t.Logf("Schema(tenant1) SQL: %s", stmt2.SQL.String())
	if !strings.Contains(stmt2.SQL.String(), "tenant1") {
		t.Errorf("Schema(tenant1) 表名应带 tenant1 前缀, SQL: %s", stmt2.SQL.String())
	}
	if strings.Contains(stmt2.SQL.String(), "public") {
		t.Errorf("Schema(tenant1) 不应再带 public 前缀, SQL: %s", stmt2.SQL.String())
	}
}

func TestSchema_AfterModel(t *testing.T) {
	drv := newPrefixedDriver(t)
	// Schema 在 Model 之后调用，也应保留 Model 与 Where 条件并切换 schema 前缀
	q := drv.Query().Model(&TestModel{}).Schema("tenant1").Where("name = ?", "x")
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]TestModel{}).Statement
	sql := stmt.SQL.String()
	t.Logf("Schema(Model 之后) SQL: %s", sql)
	if !strings.Contains(sql, "tenant1") {
		t.Errorf("表名应带 tenant1 前缀, SQL: %s", sql)
	}
	if !strings.Contains(sql, "name = ") {
		t.Errorf("Where 条件应保留, SQL: %s", sql)
	}
}

// ── Schema 显式表名不被 dest 覆盖回归 ─────────────────────────────
// 回归缺陷：schema 模式下 applySchema 曾按 dest 结构体推导表名无条件覆盖
// 调用方显式设置的 Table()/Model()，投影结构体（推导表名与实际表不一致）场景
// 会 42P01 或静默查错表（缺陷报告 2026-09-05，stitch-mes）。

// userRow 投影结构体：NamingStrategy 推导表名为 user_rows，恰为诱饵表名，
// 与显式 Table("users") 不一致（缺陷报告同款场景）。
type userRow struct {
	ID   string
	Name string
}

// tablerUserRow 实现 TableName() 返回裸表名：NamingStrategy 对其不生效，依赖 applySchema 兜底拼 schema 前缀。
type tablerUserRow struct {
	ID   string
	Name string
}

func (tablerUserRow) TableName() string { return "users" }

// tenantUser 普通模型：NamingStrategy 推导表名为 tenant_users。
type tenantUser struct {
	ID   string
	Name string
}

// newTenantSchemaDriver 打开内存 SQLite 并 ATTACH 为独立租户 schema tenant1。
// tenant1.users 存正确数据，tenant1.user_rows / tenant1.tenant_users 存诱饵数据，
// 用于捕获最危险的失败模式——不报错但静默查错表。
// ATTACH 是连接级状态，必须限制连接池单连接，避免查询落到未 ATTACH 的连接上。
func newTenantSchemaDriver(t *testing.T) *GormDriver {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	for _, ddl := range []string{
		`ATTACH DATABASE ':memory:' AS tenant1`,
		`CREATE TABLE tenant1.users (id TEXT PRIMARY KEY, name TEXT)`,
		`CREATE TABLE tenant1.user_rows (id TEXT PRIMARY KEY, name TEXT)`,
		`CREATE TABLE tenant1.tenant_users (id TEXT PRIMARY KEY, name TEXT)`,
		`INSERT INTO tenant1.users VALUES ('u1', 'right-table')`,
		`INSERT INTO tenant1.user_rows VALUES ('u1', 'wrong-table')`,
		`INSERT INTO tenant1.tenant_users VALUES ('t1', 'right-table')`,
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("初始化租户 schema 失败 %q: %v", ddl, err)
		}
	}
	return &GormDriver{db: db}
}

func countTenantTable(t *testing.T, drv *GormDriver, table string) int64 {
	t.Helper()
	var n int64
	if err := drv.Query().Raw("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestSchema_ExplicitTableFindNotOverriddenByDest(t *testing.T) {
	drv := newTenantSchemaDriver(t)
	var rows []userRow
	if err := drv.Query().Schema("tenant1").Table("users").
		Select("id", "name").Where("id = ?", "u1").Find(&rows); err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "right-table" {
		t.Errorf("显式 Table(users) 应查 users 表, 期望 [right-table], 实际 %+v", rows)
	}
}

func TestSchema_ExplicitTableFirstNotOverriddenByDest(t *testing.T) {
	drv := newTenantSchemaDriver(t)
	var row userRow
	if err := drv.Query().Schema("tenant1").Table("users").
		Select("id", "name").First(&row); err != nil {
		t.Fatalf("First: %v", err)
	}
	if row.Name != "right-table" {
		t.Errorf("显式 Table(users) First 应查 users 表, 期望 right-table, 实际 %q", row.Name)
	}
}

func TestSchema_ExplicitTableCreateNotOverriddenByDest(t *testing.T) {
	drv := newTenantSchemaDriver(t)
	if err := drv.Query().Schema("tenant1").Table("users").
		Create(&userRow{ID: "u2", Name: "created"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := countTenantTable(t, drv, "tenant1.users"); got != 2 {
		t.Errorf("Create 应写入 users 表（期望 2 行）, 实际 %d", got)
	}
	if got := countTenantTable(t, drv, "tenant1.user_rows"); got != 1 {
		t.Errorf("Create 不应动 user_rows 诱饵表（期望仍 1 行）, 实际 %d", got)
	}
}

func TestSchema_ModelFindNotOverriddenByDest(t *testing.T) {
	drv := newTenantSchemaDriver(t)
	var rows []userRow
	if err := drv.Query().Schema("tenant1").Model(&tenantUser{}).
		Select("id", "name").Find(&rows); err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "right-table" {
		t.Errorf("Model(tenantUser) 应查 tenant_users 表, 期望 [right-table], 实际 %+v", rows)
	}
}

func TestSchema_TablerDestFallbackStillApplied(t *testing.T) {
	drv := newTenantSchemaDriver(t)
	// 无显式 Table/Model：TableName() 模型绕过 NamingStrategy，applySchema 兜底应拼前缀查 users
	var rows []tablerUserRow
	if err := drv.Query().Schema("tenant1").Find(&rows); err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "right-table" {
		t.Errorf("TableName() dest 应由兜底拼前缀查 users, 期望 [right-table], 实际 %+v", rows)
	}
}

func TestSchema_PlainDestNamingStrategyPrefix(t *testing.T) {
	drv := newTenantSchemaDriver(t)
	// 无显式 Table/Model：普通 dest 由 Schema() 设置的 NamingStrategy 前缀在执行期解析
	var rows []tenantUser
	if err := drv.Query().Schema("tenant1").Find(&rows); err != nil {
		t.Fatalf("Find: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "right-table" {
		t.Errorf("普通 dest 应经 NamingStrategy 前缀查 tenant_users, 期望 [right-table], 实际 %+v", rows)
	}
}
