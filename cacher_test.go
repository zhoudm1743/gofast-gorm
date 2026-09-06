package gormdriver

import (
	"testing"
	"time"

	"github.com/zhoudm1743/go-fast-framework/cache"
	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// testCache 测试用的 contracts.Cache 实现，基于框架内存 store。
type testCache struct {
	contracts.CacheStore
}

func (c *testCache) Store(name string) contracts.CacheStore { return c.CacheStore }

func newTestCache() contracts.Cache {
	return &testCache{CacheStore: cache.NewMemoryStore(16, 0)}
}

// newTestDriverWithCaches 创建已启用查询缓存插件的测试驱动。
func newTestDriverWithCaches(t *testing.T) *GormDriver {
	t.Helper()
	drv := newTestDriverWithTable(t)
	if err := drv.EnableCaches(newTestCache()); err != nil {
		t.Fatalf("启用查询缓存失败: %v", err)
	}
	return drv
}

// TestQueryCache_HitAndMiss 验证 Cache() 按需缓存与命中行为。
func TestQueryCache_HitAndMiss(t *testing.T) {
	drv := newTestDriverWithCaches(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "c001", Name: "alice"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 第一次 Cache() 查询：走数据库，写入缓存
	var m1 TestModel
	if err := q.Cache().First(&m1, "id = ?", "c001"); err != nil {
		t.Fatalf("首次缓存查询失败: %v", err)
	}
	if m1.Name != "alice" {
		t.Fatalf("首次查询结果错误: %v", m1.Name)
	}

	// 用原生 SQL 直接改库（绕过 caches 插件的失效回调），模拟数据被外部修改
	if err := q.Exec("UPDATE test_models SET name = 'bob' WHERE id = ?", "c001"); err != nil {
		t.Fatalf("直接改库失败: %v", err)
	}

	// 第二次 Cache() 查询：应命中缓存，返回旧值 alice
	var m2 TestModel
	if err := q.Cache().First(&m2, "id = ?", "c001"); err != nil {
		t.Fatalf("命中缓存查询失败: %v", err)
	}
	if m2.Name != "alice" {
		t.Fatalf("缓存命中失败，期望 alice 实际 %v", m2.Name)
	}

	// 未调用 Cache() 的查询：不走缓存，应返回新值 bob
	var m3 TestModel
	if err := q.First(&m3, "id = ?", "c001"); err != nil {
		t.Fatalf("非缓存查询失败: %v", err)
	}
	if m3.Name != "bob" {
		t.Fatalf("非缓存查询应返回最新值，期望 bob 实际 %v", m3.Name)
	}
}

// TestQueryCache_InvalidateOnWrite 验证写操作自动失效缓存。
func TestQueryCache_InvalidateOnWrite(t *testing.T) {
	drv := newTestDriverWithCaches(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "c002", Name: "alice"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var m1 TestModel
	if err := q.Cache().First(&m1, "id = ?", "c002"); err != nil {
		t.Fatalf("首次缓存查询失败: %v", err)
	}
	if m1.Name != "alice" {
		t.Fatalf("首次查询结果错误: %v", m1.Name)
	}

	// 写操作（Update）应触发 Invalidate，清空查询缓存
	if err := q.Model(&TestModel{}).Where("id = ?", "c002").Update("name", "bob"); err != nil {
		t.Fatalf("更新失败: %v", err)
	}

	// 再次 Cache() 查询：缓存已失效，应走数据库返回新值 bob
	var m2 TestModel
	if err := q.Cache().First(&m2, "id = ?", "c002"); err != nil {
		t.Fatalf("失效后缓存查询失败: %v", err)
	}
	if m2.Name != "bob" {
		t.Fatalf("写操作应使缓存失效，期望 bob 实际 %v", m2.Name)
	}
}

// TestQueryCache_CustomTTLAndTag 验证 TTL 与自定义标签选项。
func TestQueryCache_CustomTTLAndTag(t *testing.T) {
	drv := newTestDriverWithCaches(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "c003", Name: "carol"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 使用自定义 TTL 与标签
	var m1 TestModel
	err := q.Cache(
		contracts.CacheTTLOption(time.Minute),
		contracts.CacheTagsOption("users"),
	).First(&m1, "id = ?", "c003")
	if err != nil {
		t.Fatalf("带选项的缓存查询失败: %v", err)
	}
	if m1.Name != "carol" {
		t.Fatalf("查询结果错误: %v", m1.Name)
	}

	// 命中自定义标签对应的缓存
	var m2 TestModel
	if err := q.Cache(contracts.CacheTagsOption("users")).First(&m2, "id = ?", "c003"); err != nil {
		t.Fatalf("再次缓存查询失败: %v", err)
	}
	if m2.Name != "carol" {
		t.Fatalf("缓存命中结果错误: %v", m2.Name)
	}
}

// TestQueryCache_DisabledPluginNoOp 验证未启用插件时 Cache() 无副作用。
func TestQueryCache_DisabledPluginNoOp(t *testing.T) {
	drv := newTestDriverWithTable(t) // 未启用缓存插件
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "c004", Name: "dave"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 调用 Cache() 不应报错，查询正常返回
	var m1 TestModel
	if err := q.Cache().First(&m1, "id = ?", "c004"); err != nil {
		t.Fatalf("未启用插件时 Cache() 查询失败: %v", err)
	}
	if m1.Name != "dave" {
		t.Fatalf("查询结果错误: %v", m1.Name)
	}
}

// TestQueryCache_RowNoPanic 验证 Cache() 后 Row()/Rows() 不被缓存短路：
// 游标类终结方法命中缓存会返回空游标导致 panic/报错，应自动剥离缓存标记。
func TestQueryCache_RowNoPanic(t *testing.T) {
	drv := newTestDriverWithCaches(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "c005", Name: "eve"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// Row() 单行扫描
	var id, name string
	if err := q.Cache().Model(&TestModel{}).Select("id", "name").
		Where("id = ?", "c005").Row().Scan(&id, &name); err != nil {
		t.Fatalf("Cache().Row() 扫描失败: %v", err)
	}
	if name != "eve" {
		t.Fatalf("Row() 结果错误: %v", name)
	}

	// Rows() 多行遍历
	rows, err := q.Cache().Model(&TestModel{}).Select("id", "name").Rows()
	if err != nil {
		t.Fatalf("Cache().Rows() 失败: %v", err)
	}
	defer rows.Close()
	cnt := 0
	for rows.Next() {
		cnt++
	}
	if cnt != 1 {
		t.Fatalf("Rows() 遍历行数错误: %d", cnt)
	}
}

// TestQueryCache_ScanMap 验证 ScanMap 与缓存结合正常。
func TestQueryCache_ScanMap(t *testing.T) {
	drv := newTestDriverWithCaches(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "c006", Name: "fay"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var m1, m2 []map[string]any
	if err := q.Cache().Model(&TestModel{}).Select("id", "name").ScanMap(&m1); err != nil {
		t.Fatalf("首次 ScanMap 失败: %v", err)
	}
	if err := q.Cache().Model(&TestModel{}).Select("id", "name").ScanMap(&m2); err != nil {
		t.Fatalf("二次 ScanMap 失败: %v", err)
	}
	if len(m1) != 1 || len(m2) != 1 {
		t.Fatalf("ScanMap 行数错误: %d/%d", len(m1), len(m2))
	}
	if m1[0]["name"] != m2[0]["name"] {
		t.Fatalf("ScanMap 两次结果不一致: %v vs %v", m1, m2)
	}
}

// TestQueryCache_LockBypassesCache 验证悲观锁查询自动剥离缓存标记：
// FOR UPDATE 命中缓存会跳过 DB 执行导致锁不生效，应始终走数据库。
func TestQueryCache_LockBypassesCache(t *testing.T) {
	drv := newTestDriverWithCaches(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "c007", Name: "grace"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	var m1 TestModel
	if err := q.Cache().Lock(contracts.LockForUpdate).First(&m1, "id = ?", "c007"); err != nil {
		t.Fatalf("加锁查询失败: %v", err)
	}
	if m1.Name != "grace" {
		t.Fatalf("加锁查询结果错误: %v", m1.Name)
	}

	// 改库后再次加锁查询：若被缓存应返回旧值，剥离后应返回新值
	if err := q.Exec("UPDATE test_models SET name = 'grace_v2' WHERE id = ?", "c007"); err != nil {
		t.Fatalf("直接改库失败: %v", err)
	}
	var m2 TestModel
	if err := q.Cache().Lock(contracts.LockForUpdate).First(&m2, "id = ?", "c007"); err != nil {
		t.Fatalf("二次加锁查询失败: %v", err)
	}
	if m2.Name != "grace_v2" {
		t.Fatalf("悲观锁查询不应被缓存，期望 grace_v2 实际 %v", m2.Name)
	}
}

// testModelLite 与 TestModel 同表查询时使用的不同类型，用于验证缓存 key 的类型隔离。
type testModelLite struct {
	ID   string
	Name string
}

// TestQueryCache_DestTypeIsolation 验证缓存 key 包含 dest 类型：
// 同一 SQL 被不同 dest 类型复用时，不应共享缓存（避免返回错误类型的错误数据）。
func TestQueryCache_DestTypeIsolation(t *testing.T) {
	drv := newTestDriverWithCaches(t)
	q := drv.Query()

	if err := q.Create(&TestModel{ID: "c008", Name: "type_a"}); err != nil {
		t.Fatalf("插入失败: %v", err)
	}

	// 第一次：TestModel 类型查询并写入缓存
	var m1 TestModel
	if err := q.Cache().First(&m1, "id = ?", "c008"); err != nil {
		t.Fatalf("TestModel 首次缓存查询失败: %v", err)
	}
	if m1.Name != "type_a" {
		t.Fatalf("查询结果错误: %v", m1.Name)
	}

	// 原生 SQL 直接改库（绕过缓存失效回调）
	if err := q.Exec("UPDATE test_models SET name = 'type_b' WHERE id = ?", "c008"); err != nil {
		t.Fatalf("直接改库失败: %v", err)
	}

	// 第二次：testModelLite 类型查询同一表、同一 SQL。
	// 若 key 不区分类型，会命中 TestModel 的缓存返回旧值 type_a；
	// 类型隔离生效时应回源读到新值 type_b。
	var lite testModelLite
	if err := q.Cache().Table("test_models").First(&lite, "id = ?", "c008"); err != nil {
		t.Fatalf("testModelLite 缓存查询失败: %v", err)
	}
	if lite.Name != "type_b" {
		t.Fatalf("类型隔离失效：testModelLite 应回源读到 type_b，实际命中缓存旧值 %v", lite.Name)
	}

	// 第三次：TestModel 类型再次查询，应命中自身缓存返回旧值 type_a
	var m2 TestModel
	if err := q.Cache().First(&m2, "id = ?", "c008"); err != nil {
		t.Fatalf("TestModel 二次缓存查询失败: %v", err)
	}
	if m2.Name != "type_a" {
		t.Fatalf("TestModel 应命中自身缓存，期望 type_a 实际 %v", m2.Name)
	}
}
