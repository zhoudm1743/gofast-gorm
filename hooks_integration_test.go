//go:build integration

package gormdriver

// hooks_integration_test.go —— 模型钩子（7 个 On*）真实库集成（双驱动测试方案
// §5.13 HK-01/HK-02、§六 第 11 项）：把 SQLite 单测（query_test.go TestHooks_*）
// 与 drivertest suiteHooks 的关键用例提升到 PG/MySQL 双方言验证。
//
// 运行方式（docker-compose.yml 环境）：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -count=1 -run 'TestHooks' -v .
//
// 覆盖场景（PG/MySQL 双方言断言一致，无 dialect 分支）：
//   - HK-01：Create 触发 OnBeforeCreate/OnAfterCreate 各恰好一次，数据落库；
//   - HK-01：Save 双分支钩子（按驱动实现实测固化——gorm 驱动 Save 统一走
//     invokeBeforeUpdate/invokeAfterUpdate 链：空主键插入分支与非空主键更新
//     分支均触发 Update 系钩子、不触发 Create 系钩子，见 query.go Save）；
//   - HK-02：OnBeforeCreate 返回业务错误 → Create 中断不写库（行数不变），
//     错误原样透传（errors.Is 命中且为同实例，不经 Sentinel 包装）；
//   - HK-04：链式 Update/Updates 不触发任何模型钩子（Model bean 计数位恒零）。
//
// 计数位用 int 而非 bool：可精确断言"各触发恰好一次"，捕获重复触发回归。

import (
	"errors"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// ── 测试模型与钩子样板 ───────────────────────────────────────────────

// gfhkErrBoom 业务钩子返回的自定义错误（§11.14：原样透传，不被 Sentinel 包装吞没）。
var gfhkErrBoom = errors.New("gfhk boom: 业务钩子自定义错误")

// gfhkHookModel 全钩子模型：实现 contracts 全部 7 个 On* 钩子 + IDAutoGenerator，
// 用 int 计数位验证钩子触发次数与时机。计数位 orm:"-" 不落库（schema_patch
// "-" 忽略五联）；FailHook 非空时对应钩子返回 gfhkErrBoom（错误中断用例）。
type gfhkHookModel struct {
	ID       string `orm:"pk varchar(16) 'id'"`
	Name     string `orm:"varchar(64) 'name'"`
	FailHook string `orm:"varchar(16) 'fail_hook' null"`

	BeforeCreateCalled int `orm:"-"`
	AfterCreateCalled  int `orm:"-"`
	BeforeUpdateCalled int `orm:"-"`
	AfterUpdateCalled  int `orm:"-"`
	BeforeDeleteCalled int `orm:"-"`
	AfterDeleteCalled  int `orm:"-"`
	AfterFindCalled    int `orm:"-"`
}

func (gfhkHookModel) TableName() string { return "gfhk_hooks" }

func (m *gfhkHookModel) AutoGenerateID() {
	// 测试中显式设置 ID（Save 插入分支不经 invokeBeforeCreate，ID 保持空串）
}

func (m *gfhkHookModel) hookFails(name string) bool {
	return m.FailHook == name
}

// hookTotal 计数位总和（"全零"断言用）。
func (m *gfhkHookModel) hookTotal() int {
	return m.BeforeCreateCalled + m.AfterCreateCalled +
		m.BeforeUpdateCalled + m.AfterUpdateCalled +
		m.BeforeDeleteCalled + m.AfterDeleteCalled + m.AfterFindCalled
}

// OnBeforeCreate 实现 contracts.BeforeCreator。
func (m *gfhkHookModel) OnBeforeCreate(q contracts.Query) error {
	m.BeforeCreateCalled++
	if m.hookFails("before_create") {
		return gfhkErrBoom
	}
	return nil
}

// OnAfterCreate 实现 contracts.AfterCreator。
func (m *gfhkHookModel) OnAfterCreate(q contracts.Query) error {
	m.AfterCreateCalled++
	if m.hookFails("after_create") {
		return gfhkErrBoom
	}
	return nil
}

// OnBeforeUpdate 实现 contracts.BeforeUpdater。
func (m *gfhkHookModel) OnBeforeUpdate(q contracts.Query) error {
	m.BeforeUpdateCalled++
	if m.hookFails("before_update") {
		return gfhkErrBoom
	}
	return nil
}

// OnAfterUpdate 实现 contracts.AfterUpdater。
func (m *gfhkHookModel) OnAfterUpdate(q contracts.Query) error {
	m.AfterUpdateCalled++
	if m.hookFails("after_update") {
		return gfhkErrBoom
	}
	return nil
}

// OnBeforeDelete 实现 contracts.BeforeDeleter。
func (m *gfhkHookModel) OnBeforeDelete(q contracts.Query) error {
	m.BeforeDeleteCalled++
	if m.hookFails("before_delete") {
		return gfhkErrBoom
	}
	return nil
}

// OnAfterDelete 实现 contracts.AfterDeleter。
func (m *gfhkHookModel) OnAfterDelete(q contracts.Query) error {
	m.AfterDeleteCalled++
	if m.hookFails("after_delete") {
		return gfhkErrBoom
	}
	return nil
}

// OnAfterFind 实现 contracts.AfterFinder。
func (m *gfhkHookModel) OnAfterFind(q contracts.Query) error {
	m.AfterFindCalled++
	if m.hookFails("after_find") {
		return gfhkErrBoom
	}
	return nil
}

// ── 入口与共享 runner ────────────────────────────────────────────────

// TestHooks_PG 钩子矩阵（pgsql 方言）。
func TestHooks_PG(t *testing.T) {
	drv := newIntgPG(t)
	runGfhkHookMatrix(t, drv)
}

// TestHooks_MySQL 钩子矩阵（mysql 方言）。
func TestHooks_MySQL(t *testing.T) {
	drv := newIntgMySQL(t)
	runGfhkHookMatrix(t, drv)
}

// runGfhkHookMatrix PG/MySQL 共用的钩子关键用例矩阵（方言无差异预期，
// 双方言断言完全一致）。
func runGfhkHookMatrix(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfhk_hooks"}, &gfhkHookModel{})
	q := drv.Query()

	// 回读辅助：按主键取整行（gorm 语义：未命中报错）。
	mustTake := func(t *testing.T, id string) gfhkHookModel {
		t.Helper()
		var got gfhkHookModel
		if err := q.Model(&gfhkHookModel{}).Where("id = ?", id).Take(&got); err != nil {
			t.Fatalf("回读 %q: %v", id, err)
		}
		return got
	}

	t.Run("HK-01_Create触发BeforeAfterCreate且落库", func(t *testing.T) {
		m := &gfhkHookModel{ID: "gfhk-c1", Name: "create"}
		if err := q.Create(m); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if m.BeforeCreateCalled != 1 {
			t.Errorf("OnBeforeCreate 应恰好触发一次, 实际 %d 次", m.BeforeCreateCalled)
		}
		if m.AfterCreateCalled != 1 {
			t.Errorf("OnAfterCreate 应恰好触发一次, 实际 %d 次", m.AfterCreateCalled)
		}
		// 其余钩子不应被 Create 带动
		if m.BeforeUpdateCalled+m.AfterUpdateCalled+
			m.BeforeDeleteCalled+m.AfterDeleteCalled+m.AfterFindCalled != 0 {
			t.Errorf("Create 不应触发 Create 系以外钩子: %+v", m)
		}
		// 数据真实落库（真实方言读写闭环）
		got := mustTake(t, "gfhk-c1")
		if got.Name != "create" {
			t.Errorf("落库数据不一致, 期望 name=create, 实际 %q", got.Name)
		}
	})

	t.Run("HK-01_Save双分支钩子", func(t *testing.T) {
		// gorm 驱动实测语义：Save 统一走 update 钩子链（query.go Save 无分支
		// 地调用 invokeBeforeUpdate/invokeAfterUpdate），gorm 原生 db.Save 负责
		// 按主键零值路由 INSERT/UPDATE——两个分支的契约钩子序列一致。

		// 插入分支（空主键 → db.Save 走 INSERT）：触发 Update 系钩子
		ins := &gfhkHookModel{Name: "save-ins"}
		if err := q.Save(ins); err != nil {
			t.Fatalf("Save(插入分支): %v", err)
		}
		if ins.BeforeUpdateCalled != 1 || ins.AfterUpdateCalled != 1 {
			t.Errorf("Save 插入分支应各触发一次 Update 系钩子, 实际 before=%d after=%d",
				ins.BeforeUpdateCalled, ins.AfterUpdateCalled)
		}
		if ins.BeforeCreateCalled != 0 || ins.AfterCreateCalled != 0 {
			t.Errorf("Save 插入分支不应触发 Create 系钩子: %+v", ins)
		}
		var gotIns []gfhkHookModel
		if err := q.Model(&gfhkHookModel{}).Where("name = ?", "save-ins").Find(&gotIns); err != nil {
			t.Fatalf("回读 Save 插入分支: %v", err)
		}
		if len(gotIns) != 1 {
			t.Fatalf("Save 插入分支应真实落库 1 行, 实际 %d 行", len(gotIns))
		}

		// 更新分支（非空主键 → db.Save 走 UPDATE）：触发 Update 系钩子
		up := &gfhkHookModel{ID: "gfhk-sv2", Name: "before-save"}
		if err := q.Create(up); err != nil {
			t.Fatalf("种子 Create: %v", err)
		}
		up.BeforeCreateCalled, up.AfterCreateCalled = 0, 0 // 重置 Create 阶段计数
		up.Name = "after-save"
		if err := q.Save(up); err != nil {
			t.Fatalf("Save(更新分支): %v", err)
		}
		if up.BeforeUpdateCalled != 1 || up.AfterUpdateCalled != 1 {
			t.Errorf("Save 更新分支应各触发一次 Update 系钩子, 实际 before=%d after=%d",
				up.BeforeUpdateCalled, up.AfterUpdateCalled)
		}
		if up.BeforeCreateCalled != 0 || up.AfterCreateCalled != 0 {
			t.Errorf("Save 更新分支不应触发 Create 系钩子: %+v", up)
		}
		if got := mustTake(t, "gfhk-sv2"); got.Name != "after-save" {
			t.Errorf("Save 更新分支落库数据不一致, 期望 name=after-save, 实际 %q", got.Name)
		}
	})

	t.Run("HK-02_钩子错误中断Create", func(t *testing.T) {
		bad := &gfhkHookModel{ID: "gfhk-bad", Name: "boom", FailHook: "before_create"}
		err := q.Create(bad)
		if err == nil {
			t.Fatal("OnBeforeCreate 业务错误应中断 Create")
		}
		if !errors.Is(err, gfhkErrBoom) {
			t.Errorf("业务错误应可被 errors.Is 命中, 实际: %v", err)
		}
		if err != gfhkErrBoom { // wrapError 对非 Sentinel 错误原样透传（同实例）
			t.Errorf("业务错误应原样透传（同实例）, 实际: %v", err)
		}
		if bad.AfterCreateCalled != 0 {
			t.Errorf("Before 钩子中断后不应继续触发 AfterCreate: %+v", bad)
		}
		// 中断即不写库：行数不变
		var n int64
		if err := q.Model(&gfhkHookModel{}).Where("id = ?", "gfhk-bad").Count(&n); err != nil {
			t.Fatalf("Count: %v", err)
		}
		if n != 0 {
			t.Errorf("钩子错误中断后不应落库, 实际 %d 行", n)
		}
	})

	t.Run("HK-04_UpdateUpdates不触发模型钩子", func(t *testing.T) {
		seed := &gfhkHookModel{ID: "gfhk-up", Name: "orig"}
		if err := q.Create(seed); err != nil {
			t.Fatalf("种子 Create: %v", err)
		}
		// 链式 Update/Updates 不触发任何模型钩子：以 Model bean 的计数位为
		// 直接观察点（驱动若误调用钩子，计数位必落在 bean 上）
		bean := &gfhkHookModel{}
		if err := q.Model(bean).Where("id = ?", "gfhk-up").Update("name", "u2"); err != nil {
			t.Fatalf("Update: %v", err)
		}
		if bean.hookTotal() != 0 {
			t.Errorf("Update 不应触发模型钩子, 计数: %+v", bean)
		}
		bean2 := &gfhkHookModel{}
		if err := q.Model(bean2).Where("id = ?", "gfhk-up").Updates(map[string]any{"name": "u3"}); err != nil {
			t.Fatalf("Updates: %v", err)
		}
		if bean2.hookTotal() != 0 {
			t.Errorf("Updates 不应触发模型钩子, 计数: %+v", bean2)
		}
		// 数据真实更新
		if got := mustTake(t, "gfhk-up"); got.Name != "u3" {
			t.Errorf("Updates 数据未落库, 期望 name=u3, 实际 %q", got.Name)
		}
	})
}
