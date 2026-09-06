package gormdriver

import (
	"errors"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// 测试用的内存 SQLite 驱动
func newTestDriver(t *testing.T) *GormDriver {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("创建测试数据库失败: %v", err)
	}
	return &GormDriver{db: db}
}

// 带模型的测试驱动
func newTestDriverWithTable(t *testing.T) *GormDriver {
	t.Helper()
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&TestModel{}); err != nil {
		t.Fatalf("自动迁移失败: %v", err)
	}
	return drv
}

// TestModel 测试用模型
type TestModel struct {
	ID   string `gorm:"primaryKey;size:16"`
	Name string `gorm:"size:100"`
}

func (m *TestModel) AutoGenerateID() {
	// 测试中手动设置 ID
}

// ── Exists 测试 ─────────────────────────────────────────────────────

func TestExists_EmptyConds(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	// 空表 + 空条件：不应 panic
	exists, err := q.Exists(&TestModel{})
	if err != nil {
		t.Fatalf("Exists(空表, 无条件) 返回错误: %v", err)
	}
	if exists {
		t.Error("空表 Exists 应返回 false")
	}
}

func TestExists_WithData(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	// 插入数据
	if err := q.Create(&TestModel{ID: "test001", Name: "foo"}); err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	// 无条件：应返回 true
	exists, err := q.Exists(&TestModel{})
	if err != nil {
		t.Fatalf("Exists 返回错误: %v", err)
	}
	if !exists {
		t.Error("有数据时 Exists 应返回 true")
	}
}

func TestExists_WithConds_Match(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "test002", Name: "bar"}); err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	exists, err := q.Exists(&TestModel{}, "name = ?", "bar")
	if err != nil {
		t.Fatalf("Exists 返回错误: %v", err)
	}
	if !exists {
		t.Error("匹配条件时 Exists 应返回 true")
	}
}

func TestExists_WithConds_NoMatch(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "test003", Name: "baz"}); err != nil {
		t.Fatalf("插入测试数据失败: %v", err)
	}

	exists, err := q.Exists(&TestModel{}, "name = ?", "nonexistent")
	if err != nil {
		t.Fatalf("Exists 返回错误: %v", err)
	}
	if exists {
		t.Error("不匹配条件时 Exists 应返回 false")
	}
}

// ── ScanMap 测试 ────────────────────────────────────────────────────

func TestScanMap(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	// 插入数据
	if err := q.Create(&TestModel{ID: "sm001", Name: "alice"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}
	if err := q.Create(&TestModel{ID: "sm002", Name: "bob"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var rows []map[string]any
	if err := q.Model(&TestModel{}).ScanMap(&rows); err != nil {
		t.Fatalf("ScanMap 失败: %v", err)
	}

	if len(rows) != 2 {
		t.Errorf("期望 2 行, 实际 %d", len(rows))
	}

	if rows[0]["name"] != "alice" {
		t.Errorf("第一行 name 期望 alice, 实际 %v", rows[0]["name"])
	}
}

// ── 钩子测试 ─────────────────────────────────────────────────────────

type HookTestModel struct {
	ID   string `gorm:"primaryKey;size:16"`
	Name string `gorm:"size:100"`

	BeforeCreateCalled bool
	AfterCreateCalled  bool
	BeforeUpdateCalled bool
	AfterUpdateCalled  bool
	BeforeDeleteCalled bool
	AfterDeleteCalled  bool
	AfterFindCalled    bool
}

func (m *HookTestModel) AutoGenerateID() {
	// 测试中手动设置
}

func (m *HookTestModel) OnBeforeCreate(q contracts.Query) error {
	m.BeforeCreateCalled = true
	return nil
}

func (m *HookTestModel) OnAfterCreate(q contracts.Query) error {
	m.AfterCreateCalled = true
	return nil
}

func (m *HookTestModel) OnBeforeUpdate(q contracts.Query) error {
	m.BeforeUpdateCalled = true
	return nil
}

func (m *HookTestModel) OnAfterUpdate(q contracts.Query) error {
	m.AfterUpdateCalled = true
	return nil
}

func (m *HookTestModel) OnBeforeDelete(q contracts.Query) error {
	m.BeforeDeleteCalled = true
	return nil
}

func (m *HookTestModel) OnAfterDelete(q contracts.Query) error {
	m.AfterDeleteCalled = true
	return nil
}

func (m *HookTestModel) OnAfterFind(q contracts.Query) error {
	m.AfterFindCalled = true
	return nil
}

func TestHooks_Create(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&HookTestModel{}); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}

	m := &HookTestModel{ID: "hook001", Name: "test"}
	if err := drv.Query().Create(m); err != nil {
		t.Fatalf("Create 失败: %v", err)
	}

	if !m.BeforeCreateCalled {
		t.Error("BeforeCreate 钩子未被调用")
	}
	if !m.AfterCreateCalled {
		t.Error("AfterCreate 钩子未被调用")
	}
}

func TestHooks_Save(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&HookTestModel{}); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}

	// 先创建
	m := &HookTestModel{ID: "hook002", Name: "original"}
	if err := drv.Query().Create(m); err != nil {
		t.Fatalf("Create 失败: %v", err)
	}

	// 重置钩子标志
	m.BeforeCreateCalled = false
	m.AfterCreateCalled = false

	// 再 Save（触发 Update 钩子）
	m.Name = "updated"
	if err := drv.Query().Save(m); err != nil {
		t.Fatalf("Save 失败: %v", err)
	}

	if !m.BeforeUpdateCalled {
		t.Error("Save 时应调用 BeforeUpdate 钩子")
	}
	if !m.AfterUpdateCalled {
		t.Error("Save 时应调用 AfterUpdate 钩子")
	}
}

func TestHooks_Delete(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&HookTestModel{}); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}

	m := &HookTestModel{ID: "hook003", Name: "delete_me"}
	if err := drv.Query().Create(m); err != nil {
		t.Fatalf("Create 失败: %v", err)
	}

	if err := drv.Query().Delete(m); err != nil {
		t.Fatalf("Delete 失败: %v", err)
	}

	if !m.BeforeDeleteCalled {
		t.Error("BeforeDelete 钩子未被调用")
	}
	if !m.AfterDeleteCalled {
		t.Error("AfterDelete 钩子未被调用")
	}
}

func TestHooks_Find(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&HookTestModel{}); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}

	m := &HookTestModel{ID: "hook004", Name: "find_me"}
	if err := drv.Query().Create(m); err != nil {
		t.Fatalf("Create 失败: %v", err)
	}

	var found HookTestModel
	if err := drv.Query().First(&found, "id = ?", "hook004"); err != nil {
		t.Fatalf("First 失败: %v", err)
	}

	if !found.AfterFindCalled {
		t.Error("AfterFind 钩子未被调用")
	}
}

func TestHooks_NoHooksModel(t *testing.T) {
	drv := newTestDriverWithTable(t)

	// 使用普通 TestModel（不实现钩子），验证不会出错
	m := &TestModel{ID: "hook005", Name: "no_hooks"}
	if err := drv.Query().Create(m); err != nil {
		t.Fatalf("无钩子模型的 Create 不应失败: %v", err)
	}
}

// ── wrapError 集成测试 ──────────────────────────────────────────────

func TestCreate_DuplicateKey(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "dup001", Name: "first"}); err != nil {
		t.Fatalf("首次创建失败: %v", err)
	}

	// 重复主键
	err := q.Create(&TestModel{ID: "dup001", Name: "second"})
	if err == nil {
		t.Fatal("重复主键应返回错误")
	}
	if !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("重复主键应映射为 ErrDuplicatedKey, 实际: %v", err)
	}
}

func TestFirst_RecordNotFound(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	var m TestModel
	err := q.First(&m, "id = ?", "nonexistent")
	if err == nil {
		t.Fatal("RecordNotFound 应返回错误")
	}
	if !errors.Is(err, contracts.ErrRecordNotFound) {
		t.Errorf("RecordNotFound 应映射为 ErrRecordNotFound, 实际: %v", err)
	}
}

// ── Select("*") 回归测试 ─────────────────────────────────────────────

func TestSelectStar_Find(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "star001", Name: "alice"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var rows []TestModel
	if err := q.Model(&TestModel{}).Select("*").Find(&rows); err != nil {
		t.Fatalf("Select(*) 后 Find 不应报错: %v", err)
	}
	if len(rows) != 1 || rows[0].Name != "alice" {
		t.Errorf("期望查到 alice 且字段完整, 实际 %+v", rows)
	}
}

func TestSelectStar_Pluck(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "star002", Name: "bob"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var names []string
	if err := q.Model(&TestModel{}).Select("*").Pluck("name", &names); err != nil {
		t.Fatalf("Select(*) 后 Pluck 不应报错: %v", err)
	}
	if len(names) != 1 || names[0] != "bob" {
		t.Errorf("期望 [bob], 实际 %v", names)
	}
}

func TestSelectStar_Omit(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "star003", Name: "carol"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var rows []TestModel
	if err := q.Model(&TestModel{}).Select("*").Omit("name").Find(&rows); err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if rows[0].ID == "" {
		t.Error("Omit name 后 ID 仍应被查询到")
	}
	if rows[0].Name != "" {
		t.Errorf("Omit name 后 Name 应为空, 实际 %q", rows[0].Name)
	}
}

func TestSelectStar_OverridesSelect(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "star004", Name: "dave"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var rows []TestModel
	if err := q.Model(&TestModel{}).Select("id").Select("*").Find(&rows); err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if rows[0].Name != "dave" {
		t.Errorf("Select(id) 后再 Select(*) 应恢复全字段, Name 实际 %q", rows[0].Name)
	}
}

// ── SQL 表达式（X-08）测试 ──────────────────────────────────────────

// ExprModel 表达式测试用模型（含 count 计数字段，验证数据库端原子表达式更新）
type ExprModel struct {
	ID    string `gorm:"primaryKey;size:16"`
	Name  string `gorm:"size:100"`
	Count int64  `gorm:"column:count"`
}

func (m *ExprModel) AutoGenerateID() {
	// 测试中手动设置 ID
}

// newTestDriverWithExprTable 创建带 ExprModel 表的测试驱动
func newTestDriverWithExprTable(t *testing.T) *GormDriver {
	t.Helper()
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&ExprModel{}); err != nil {
		t.Fatalf("自动迁移失败: %v", err)
	}
	return drv
}

func TestGormExpr_Update(t *testing.T) {
	drv := newTestDriverWithExprTable(t)
	q := drv.Query()

	if err := q.Create(&ExprModel{ID: "expr001", Name: "alice", Count: 10}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 数据库端原子自增：SET count = count + 5
	if err := q.Model(&ExprModel{}).Where("id = ?", "expr001").Update("count", contracts.Expr("count + ?", 5)); err != nil {
		t.Fatalf("Update(表达式) 失败: %v", err)
	}

	var got ExprModel
	if err := q.First(&got, "id = ?", "expr001"); err != nil {
		t.Fatalf("回读失败: %v", err)
	}
	if got.Count != 15 {
		t.Errorf("count 期望 15, 实际 %d", got.Count)
	}
}

func TestGormExpr_Updates(t *testing.T) {
	drv := newTestDriverWithExprTable(t)
	q := drv.Query()

	if err := q.Create(&ExprModel{ID: "expr002", Name: "bob", Count: 1}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 表达式与普通值混合
	values := map[string]any{
		"count": contracts.Expr("count + ?", 1),
		"name":  "x",
	}
	if err := q.Model(&ExprModel{}).Where("id = ?", "expr002").Updates(values); err != nil {
		t.Fatalf("Updates(混合表达式) 失败: %v", err)
	}

	// 调用方传入的 map 不得被修改（表达式值应保持原样）
	if _, ok := values["count"].(contracts.SQLExpression); !ok {
		t.Errorf("调用方 map 中的表达式值被篡改: %T %v", values["count"], values["count"])
	}
	if values["name"] != "x" {
		t.Errorf("调用方 map 中的普通值被篡改: %v", values["name"])
	}

	var got ExprModel
	if err := q.First(&got, "id = ?", "expr002"); err != nil {
		t.Fatalf("回读失败: %v", err)
	}
	if got.Count != 2 {
		t.Errorf("count 期望 2, 实际 %d", got.Count)
	}
	if got.Name != "x" {
		t.Errorf("name 期望 x, 实际 %q", got.Name)
	}
}

func TestGormExpr_ExecResult(t *testing.T) {
	drv := newTestDriverWithExprTable(t)
	q := drv.Query()

	// INSERT 命中 1 行
	res := q.ExecResult("INSERT INTO expr_models (id, name, count) VALUES (?, ?, ?)", "expr003", "carol", 7)
	if res.Error != nil {
		t.Fatalf("ExecResult(INSERT) 失败: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Errorf("INSERT RowsAffected 期望 1, 实际 %d", res.RowsAffected)
	}

	// UPDATE 命中 1 行
	res = q.ExecResult("UPDATE expr_models SET count = count + 1 WHERE id = ?", "expr003")
	if res.Error != nil {
		t.Fatalf("ExecResult(UPDATE) 失败: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Errorf("UPDATE RowsAffected 期望 1, 实际 %d", res.RowsAffected)
	}

	// UPDATE 未命中：Error 为 nil 但 RowsAffected = 0，经 IsZeroRow 判定
	res = q.ExecResult("UPDATE expr_models SET count = count + 1 WHERE id = ?", "missing")
	if res.Error != nil {
		t.Fatalf("ExecResult(未命中) 不应报错: %v", res.Error)
	}
	if !res.IsZeroRow() {
		t.Errorf("未命中应 IsZeroRow, 实际 RowsAffected=%d, Error=%v", res.RowsAffected, res.Error)
	}

	// 回读验证原生 SQL 表达式生效
	var got ExprModel
	if err := q.First(&got, "id = ?", "expr003"); err != nil {
		t.Fatalf("回读失败: %v", err)
	}
	if got.Count != 8 {
		t.Errorf("count 期望 8, 实际 %d", got.Count)
	}
}

func TestGormExpr_UpdateWithWhere(t *testing.T) {
	drv := newTestDriverWithExprTable(t)
	q := drv.Query()

	if err := q.Create(&ExprModel{ID: "expr004", Name: "dave", Count: 0}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}
	if err := q.Create(&ExprModel{ID: "expr005", Name: "eve", Count: 0}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 链上 Where 与表达式组合：只影响目标行
	if err := q.Model(&ExprModel{}).Where("id = ?", "expr004").Update("count", contracts.Expr("count + ?", 3)); err != nil {
		t.Fatalf("Update 失败: %v", err)
	}

	var rows []ExprModel
	if err := q.Model(&ExprModel{}).Order("id").Find(&rows); err != nil {
		t.Fatalf("查询失败: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("期望 2 行, 实际 %d", len(rows))
	}
	for _, row := range rows {
		want := int64(0)
		if row.ID == "expr004" {
			want = 3
		}
		if row.Count != want {
			t.Errorf("id=%s count 期望 %d, 实际 %d", row.ID, want, row.Count)
		}
	}
}

func TestGormExpr_ResultVariants(t *testing.T) {
	drv := newTestDriverWithExprTable(t)
	q := drv.Query()

	if err := q.Create(&ExprModel{ID: "expr006", Name: "frank", Count: 100}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// UpdateResult 携带表达式：数据库端原子扣减
	res := q.Model(&ExprModel{}).Where("id = ?", "expr006").UpdateResult("count", contracts.Expr("count - ?", 30))
	if res.Error != nil {
		t.Fatalf("UpdateResult 失败: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Errorf("UpdateResult RowsAffected 期望 1, 实际 %d", res.RowsAffected)
	}

	// UpdatesResult 表达式与普通值混合
	res = q.Model(&ExprModel{}).Where("id = ?", "expr006").UpdatesResult(map[string]any{
		"count": contracts.Expr("count * 2"),
		"name":  "g",
	})
	if res.Error != nil {
		t.Fatalf("UpdatesResult 失败: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Errorf("UpdatesResult RowsAffected 期望 1, 实际 %d", res.RowsAffected)
	}

	var got ExprModel
	if err := q.First(&got, "id = ?", "expr006"); err != nil {
		t.Fatalf("回读失败: %v", err)
	}
	if got.Count != 140 { // (100 - 30) * 2
		t.Errorf("count 期望 140, 实际 %d", got.Count)
	}
	if got.Name != "g" {
		t.Errorf("name 期望 g, 实际 %q", got.Name)
	}

	// 未命中：RowsAffected 语义不变 → IsZeroRow
	res = q.Model(&ExprModel{}).Where("id = ?", "missing").UpdateResult("count", contracts.Expr("count + ?", 1))
	if res.Error != nil {
		t.Fatalf("UpdateResult(未命中) 不应报错: %v", res.Error)
	}
	if !res.IsZeroRow() {
		t.Errorf("未命中应 IsZeroRow, 实际 RowsAffected=%d", res.RowsAffected)
	}
}
