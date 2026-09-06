package gormdriver

// optimistic_lock_test.go：orm:"version" 乐观锁写路径仿真矩阵（设计文档 7.4 / 11.3）。
// 对照 7.4 表逐条：Create 置 1 / Save 双分支+upsert 回落 / struct Updates 自增回填 /
// 陈旧版本 0 行不报错 / Updates(map) 不参与 / Update 单列不参与 / Delete 不参与 /
// 并发仅一方成功。

import (
	"sync"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"
)

type verModel struct {
	ID      string `orm:"pk varchar(16) 'id'"`
	Name    string `orm:"varchar(100) 'name'"`
	Version int64  `orm:"version 'version'"`
}

func newVerDriver(t *testing.T) *GormDriver {
	t.Helper()
	drv := newTestDriver(t)
	// :memory: 每个连接是独立库，并发用例需限制单连接（与 newTenantSchemaDriver 同理）
	sqlDB, err := drv.db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	if err := drv.AutoMigrate(&verModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	return drv
}

func mustLoadVer(t *testing.T, q contracts.Query, id string) *verModel {
	t.Helper()
	var got verModel
	if err := q.First(&got, "id = ?", id); err != nil {
		t.Fatalf("加载 %s: %v", id, err)
	}
	return &got
}

func TestVersion_CreateSetsOne(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()

	// 零值 → 强制置 1
	m := &verModel{ID: "v1", Name: "n"}
	if err := q.Create(m); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if m.Version != 1 {
		t.Errorf("Create 后内存 version 应置 1, 实际 %d", m.Version)
	}
	if got := mustLoadVer(t, q, "v1"); got.Version != 1 {
		t.Errorf("Create 后落库 version 应为 1, 实际 %d", got.Version)
	}

	// CreateInBatches 同样置 1
	ms := []*verModel{{ID: "b1", Name: "x"}, {ID: "b2", Name: "y"}}
	if err := q.CreateInBatches(ms, 2); err != nil {
		t.Fatalf("CreateInBatches: %v", err)
	}
	for _, m := range ms {
		if m.Version != 1 {
			t.Errorf("CreateInBatches 后 %s version 应置 1, 实际 %d", m.ID, m.Version)
		}
	}
	if got := mustLoadVer(t, q, "b2"); got.Version != 1 {
		t.Errorf("CreateInBatches 落库 version 应为 1, 实际 %d", got.Version)
	}
}

func TestVersion_UpdatesStructIncrements(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()
	if err := q.Create(&verModel{ID: "v1", Name: "old"}); err != nil {
		t.Fatal(err)
	}

	// 加载（version=1）→ struct 更新：WHERE version=1，SET version+1，回填 2
	row := mustLoadVer(t, q, "v1")
	row.Name = "new"
	if err := q.Model(row).Updates(row); err != nil {
		t.Fatalf("Updates(struct): %v", err)
	}
	if row.Version != 2 {
		t.Errorf("成功 struct 更新后内存 version 应回填 2, 实际 %d", row.Version)
	}
	if got := mustLoadVer(t, q, "v1"); got.Version != 2 || got.Name != "new" {
		t.Errorf("落库期望 version=2/name=new, 实际 version=%d/name=%q", got.Version, got.Name)
	}
}

func TestVersion_UpdatesStructStaleNoError(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()
	if err := q.Create(&verModel{ID: "v1", Name: "old"}); err != nil {
		t.Fatal(err)
	}

	// 陈旧版本：WHERE version=99 不命中 → 0 行、不报错、落库不变
	stale := verModel{ID: "v1", Name: "hijack", Version: 99}
	if err := q.Model(&stale).Updates(&stale); err != nil {
		t.Fatalf("陈旧版本更新不应报错: %v", err)
	}
	if got := mustLoadVer(t, q, "v1"); got.Version != 1 || got.Name != "old" {
		t.Errorf("陈旧更新不应生效, 实际 version=%d/name=%q", got.Version, got.Name)
	}
}

func TestVersion_UpdatesResultStruct(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()
	if err := q.Create(&verModel{ID: "v1", Name: "old"}); err != nil {
		t.Fatal(err)
	}
	row := mustLoadVer(t, q, "v1")
	row.Name = "res"
	res := q.Model(row).UpdatesResult(row)
	if res.Error != nil {
		t.Fatalf("UpdatesResult: %v", res.Error)
	}
	if res.RowsAffected != 1 {
		t.Errorf("成功更新 RowsAffected 期望 1, 实际 %d", res.RowsAffected)
	}
	if row.Version != 2 {
		t.Errorf("UpdatesResult 后内存 version 应回填 2, 实际 %d", row.Version)
	}

	// 陈旧：0 行且不报错
	stale := verModel{ID: "v1", Name: "x", Version: 42}
	res = q.Model(&stale).UpdatesResult(&stale)
	if res.Error != nil {
		t.Fatalf("陈旧 UpdatesResult 不应报错: %v", res.Error)
	}
	if res.RowsAffected != 0 || !res.IsZeroRow() {
		t.Errorf("陈旧 UpdatesResult 应 0 行 IsZeroRow, 实际 %+v", res)
	}
}

func TestVersion_UpdatesMapNotInvolved(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()
	if err := q.Create(&verModel{ID: "v1", Name: "old"}); err != nil {
		t.Fatal(err)
	}

	// Updates(map) 不参与版本控制：更新生效且 version 不自增
	if err := q.Model(&verModel{}).Where("id = ?", "v1").Updates(map[string]any{"name": "mapped"}); err != nil {
		t.Fatalf("Updates(map): %v", err)
	}
	if got := mustLoadVer(t, q, "v1"); got.Version != 1 || got.Name != "mapped" {
		t.Errorf("Updates(map) 不应触碰 version, 实际 version=%d/name=%q", got.Version, got.Name)
	}
}

func TestVersion_UpdateSingleNotInvolved(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()
	if err := q.Create(&verModel{ID: "v1", Name: "old"}); err != nil {
		t.Fatal(err)
	}

	if err := q.Model(&verModel{}).Where("id = ?", "v1").Update("name", "single"); err != nil {
		t.Fatalf("Update 单列: %v", err)
	}
	if got := mustLoadVer(t, q, "v1"); got.Version != 1 || got.Name != "single" {
		t.Errorf("Update 单列不参与版本控制, 实际 version=%d/name=%q", got.Version, got.Name)
	}

	res := q.Model(&verModel{}).Where("id = ?", "v1").UpdateResult("name", "single2")
	if res.Error != nil || res.RowsAffected != 1 {
		t.Fatalf("UpdateResult: %+v", res)
	}
	if got := mustLoadVer(t, q, "v1"); got.Version != 1 {
		t.Errorf("UpdateResult 单列不参与版本控制, 实际 version=%d", got.Version)
	}
}

func TestVersion_DeleteNotInvolved(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()
	if err := q.Create(&verModel{ID: "v1", Name: "doomed"}); err != nil {
		t.Fatal(err)
	}

	// Delete 不带版本条件，正常删除
	if err := q.Delete(&verModel{}, "id = ?", "v1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	var count int64
	q.Model(&verModel{}).Count(&count)
	if count != 0 {
		t.Errorf("Delete 后应 0 行, 实际 %d", count)
	}
}

func TestVersion_SaveBothBranches(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()

	// 插入分支（空主键）→ version 置 1
	m := &verModel{ID: "s1", Name: "v1"}
	if err := q.Save(m); err != nil {
		t.Fatalf("Save(插入): %v", err)
	}
	if m.Version != 1 {
		t.Errorf("Save 插入分支 version 应置 1, 实际 %d", m.Version)
	}

	// 更新分支 → WHERE version=1 + 自增 + 回填 2
	m.Name = "v2"
	if err := q.Save(m); err != nil {
		t.Fatalf("Save(更新): %v", err)
	}
	if m.Version != 2 {
		t.Errorf("Save 更新分支后内存 version 应回填 2, 实际 %d", m.Version)
	}
	if got := mustLoadVer(t, q, "s1"); got.Version != 2 || got.Name != "v2" {
		t.Errorf("Save 更新分支落库期望 version=2/name=v2, 实际 version=%d/name=%q", got.Version, got.Name)
	}
}

func TestVersion_SaveUpsertFallback(t *testing.T) {
	drv := newVerDriver(t)
	q := drv.Query()
	if err := q.Create(&verModel{ID: "exist1", Name: "a"}); err != nil {
		t.Fatal(err)
	}

	// 更新 0 行回落 INSERT：数据库中不存在的行（PK 不存在）→ 回落插入 version 置 1
	fallback := &verModel{ID: "brand-new", Name: "inserted", Version: 7}
	if err := q.Save(fallback); err != nil {
		t.Fatalf("Save(upsert 回落): %v", err)
	}
	got := mustLoadVer(t, q, "brand-new")
	if got.Version != 1 || got.Name != "inserted" {
		t.Errorf("回落插入 version 应置 1, 实际 version=%d/name=%q", got.Version, got.Name)
	}
}

func TestVersion_ConcurrentOnlyOneWins(t *testing.T) {
	drv := newVerDriver(t)
	if err := drv.Query().Create(&verModel{ID: "race1", Name: "init"}); err != nil {
		t.Fatal(err)
	}

	// 两方都基于 version=1 发起 struct 更新：仅一方成功（各用独立查询链，单次尝试）
	var mu sync.Mutex
	var aOK, bOK bool
	var wg sync.WaitGroup
	attempt := func(id, name string, report func()) {
		defer wg.Done()
		row := verModel{ID: id, Name: name, Version: 1}
		if err := drv.Query().Model(&row).Updates(&row); err == nil && row.Version == 2 {
			mu.Lock()
			report()
			mu.Unlock()
		}
	}
	wg.Add(2)
	go attempt("race1", "from-a", func() { aOK = true })
	go attempt("race1", "from-b", func() { bOK = true })
	wg.Wait()

	if aOK == bOK {
		t.Errorf("并发 struct 更新应恰好一方成功（version 1→2）, 实际 aOK=%v bOK=%v", aOK, bOK)
	}
	got := mustLoadVer(t, drv.Query(), "race1")
	if got.Version != 2 {
		t.Errorf("最终落库 version 应为 2, 实际 %d", got.Version)
	}
	winner := got.Name
	if winner != "from-a" && winner != "from-b" {
		t.Errorf("获胜方内容异常: %q", winner)
	}
}
