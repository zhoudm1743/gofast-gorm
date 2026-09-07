//go:build integration

package gormdriver

// fullcov_read_integration_test.go —— contracts.Query 读终结方法真实库全覆盖
// （分组 2：Find / First / Last / Take / Count / Scan / Pluck / Row / Rows /
// ScanMap / Exists + 查询缓存 CC-01~05 / LK-04 / RAW-04）。
//
// 对齐 gofast-xorm 侧 fullcov_read_integration_test.go 的用例矩阵
// （fc2_ → gfc2_ 前缀），方法论一致：每个子用例 DROP 重建表、断言一律回读
// 真实数据验证。语义以 gorm 侧驱动实现为准（query.go / cacher.go 先读后写，
// 关键行为全部经 /tmp 独立探针在真实 PG(pgx v5.6)/MySQL(go-sql-driver v1.8)
// 上实测核实，gorm v1.31.1）。
//
// 差异固化条目（相对 xorm 侧同名矩阵，断言处均带 // 差异固化 注释）：
//   1. Find 空表：gorm 预分配非 nil 空切片（scan.go MakeSlice(type,0,20)），
//      xorm 零行不预置（nil/空均可）；
//   2. Row()/Rows() 构建器链：gorm 直接执行（GormQuery.Row/Rows 仅剥离缓存
//      标记后代理 db.Row()/db.Rows()），xorm 侧报"仅支持 Raw"；
//   3. ScanMap NULL 形态（Q-10）：gorm 两侧言 NULL int 列与 NULL 文本列
//      一律为 nil（实测：pgx/go-sql-driver 经 database/sql Scan 进 *any 时
//      NULL→nil）；xorm 侧为 pgx NULL INT4→int32(0)、文本 NULL→""；
//   4. Scan 标量零行：gorm 静默保持 dest 原值不报错（xorm 模板无对应场景，
//      其 count(*) 恒有行）；NULL 标量 Scan 进 string/int64 会报
//      "converting NULL to string|int64 is unsupported"（实测，未入矩阵断言）；
//   5. Count：gorm 在无 GROUP BY 时自动剥离链上 ORDER BY（finisher_api），
//      与 xorm X-06 同语义，PG 无 42803；裸链（无 Table/Model）报
//      "Table not set"（xorm 同样报错，文案不同）；
//   6. Exists 的 dest 预填充值不参与条件构建（实现为
//      Model(dest).Where(conds...).Limit(1).Count，无主键内联）；
//   7. 查询缓存：计数口径 gets/hits/puts 与 xorm 一致；Lock(FOR UPDATE) 与
//      Row()/Rows() 自动剥离缓存标记（query.go withoutCache，LK-04 对照），
//      缓存 key 绑定 dest 类型（cacheKey 追加 reflect.TypeOf，CC-05）。
//
// 运行（docker-compose.yml 环境）：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	  go test -tags integration -run 'TestFullCovRead' -v .
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -run 'TestFullCovRead' -v .

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhoudm1743/go-fast-framework/cache"
	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// ── 测试模型（gorm tag，照 pg_integration_test.go 风格；表前缀 gfc2_）────

// gfc2User 主模型：文本主键（升降序语义跨方言确定）、数值列、可空文本列
// （note 供 ScanMap 的 NULL 表示断言）与可空数值列（age 供 NULL int 断言）。
type gfc2User struct {
	ID   string `gorm:"column:id;primaryKey;size:16"`
	Name string `gorm:"column:name;size:64"`
	Cnt  int64  `gorm:"column:cnt"`
	Note string `gorm:"column:note;size:64"`
	Age  *int64 `gorm:"column:age"`
}

func (gfc2User) TableName() string { return "gfc2_users" }

// gfc2UserProj 投影结构体：与 gfc2_users 部分同列布局、无 TableName，
// 查询须显式 Table()（否则 gorm 按命名策略推导出不存在的 gfc2_user_projs）。
type gfc2UserProj struct {
	ID   string `gorm:"column:id"`
	Name string `gorm:"column:name"`
}

// gfc2UserLite 与 gfc2User 同表的另一 dest 类型（无 TableName，配 Table 用），
// 用于查询缓存的 dest 类型隔离断言（CC-05）。
type gfc2UserLite struct {
	ID   string `gorm:"column:id;primaryKey;size:16"`
	Name string `gorm:"column:name;size:64"`
}

// ── 种子与断言基建 ─────────────────────────────────────────────────────

// gfc2AgePtr int64 → *int64（可空数值列种子辅助）。
func gfc2AgePtr(v int64) *int64 { return &v }

// gfc2Reset 用例开始前 DROP 重建 gfc2_users（intgDropTables→intgMigrate 链路），
// t.Cleanup 再清一次（防用例中断残留）。
func gfc2Reset(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc2_users"}, &gfc2User{})
}

// gfc2Seed 灌入三行基线：插入顺序 3→1→2 与主键顺序 1<2<3 错位，
// 使 First/Last 的排序断言能区分"显式主键排序"与"存储序/插入序"。
// bob 的 note/age 以原生 SQL 省略列写 NULL（gorm struct Create 会把零值
// string 写成空串而非 NULL，统一经原生 INSERT 固化 NULL 来源）。
// 注意：两次 Create 会触发查询缓存全部失效（写失效语义），缓存矩阵依赖此副作用。
func gfc2Seed(t *testing.T, drv *GormDriver) {
	t.Helper()
	q := drv.Query()
	if err := q.Create(&gfc2User{ID: "3", Name: "carol", Cnt: 30, Note: "note3", Age: gfc2AgePtr(33)}); err != nil {
		t.Fatalf("种子 3: %v", err)
	}
	if err := q.Create(&gfc2User{ID: "1", Name: "alice", Cnt: 10, Note: "note1", Age: gfc2AgePtr(11)}); err != nil {
		t.Fatalf("种子 1: %v", err)
	}
	intgMustExec(t, drv,
		`INSERT INTO gfc2_users (id, name, cnt) VALUES (?, ?, ?)`,
		"2", "bob", int64(20))
}

// gfc2NamesByID 将查询结果整理为 id→name 映射（行序无关断言辅助）。
func gfc2NamesByID(rows []gfc2User) map[string]string {
	m := make(map[string]string, len(rows))
	for _, r := range rows {
		m[r.ID] = r.Name
	}
	return m
}

// ── 缓存计数替身（照 gofast-xorm cacher_test.go 思路，gfc2 前缀防撞名）──

// gfc2CountStore 带调用计数的 CacheStore 装饰器。gormdriver 的 gormCacher
// 走 Store().Get（Get 计数）与 Store().Tags(...).Put（TaggedCache Put 计数）
// 两条路径，两处都计数才能观测完整缓存读写。
type gfc2CountStore struct {
	contracts.CacheStore

	mu   sync.Mutex
	gets int // Get 调用总次数（含未命中）
	hits int // Get 返回非 nil 的次数（命中）
	puts int // Put 写入次数（含 Tags 包装路径）
}

func (s *gfc2CountStore) Get(key string, def ...any) any {
	s.mu.Lock()
	s.gets++
	s.mu.Unlock()
	v := s.CacheStore.Get(key, def...)
	if v != nil {
		s.mu.Lock()
		s.hits++
		s.mu.Unlock()
	}
	return v
}

func (s *gfc2CountStore) Tags(tags ...string) contracts.TaggedCache {
	return &gfc2CountTagged{TaggedCache: s.CacheStore.Tags(tags...), s: s}
}

// gfc2CountTagged 对 Tags() 返回的 TaggedCache 的 Put 计数后透传。
type gfc2CountTagged struct {
	contracts.TaggedCache
	s *gfc2CountStore
}

func (t *gfc2CountTagged) Put(key string, value any, ttl time.Duration) error {
	t.s.mu.Lock()
	t.s.puts++
	t.s.mu.Unlock()
	return t.TaggedCache.Put(key, value, ttl)
}

// gfc2CountCache 测试用 contracts.Cache 实现：所有 store 名路由到同一计数
// store（默认 CacheConfig.Store 为空串，单 store 足够覆盖）。
type gfc2CountCache struct {
	*gfc2CountStore
}

func (c *gfc2CountCache) Store(_ string) contracts.CacheStore { return c.gfc2CountStore }

func gfc2NewCountCache() *gfc2CountCache {
	return &gfc2CountCache{gfc2CountStore: &gfc2CountStore{CacheStore: cache.NewMemoryStore(16, 0)}}
}

// gfc2CountSnapshot 缓存计数快照（增量断言基线）。
type gfc2CountSnapshot struct{ gets, hits, puts int }

func gfc2Snapshot(s *gfc2CountStore) gfc2CountSnapshot {
	gets, hits, puts := s.snapshot()
	return gfc2CountSnapshot{gets: gets, hits: hits, puts: puts}
}

// snapshot 原子读取计数。
func (s *gfc2CountStore) snapshot() (gets, hits, puts int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets, s.hits, s.puts
}

// gfc2AssertCounts 断言缓存读写计数绝对值（自 EnableCaches 起累计），
// stage 标注断言所处阶段便于定位。
func gfc2AssertCounts(t *testing.T, s *gfc2CountStore, stage string, wantGets, wantHits, wantPuts int) {
	t.Helper()
	gets, hits, puts := s.snapshot()
	if gets != wantGets || hits != wantHits || puts != wantPuts {
		t.Fatalf("%s: 缓存计数不符 get=%d(期望 %d) hit=%d(期望 %d) put=%d(期望 %d)",
			stage, gets, wantGets, hits, wantHits, puts, wantPuts)
	}
}

// gfc2AssertDelta 断言相对基线快照的计数增量。
func gfc2AssertDelta(t *testing.T, s *gfc2CountStore, base gfc2CountSnapshot, stage string, dGets, dHits, dPuts int) {
	t.Helper()
	gfc2AssertCounts(t, s, stage, base.gets+dGets, base.hits+dHits, base.puts+dPuts)
}

// ── 共享 runner：PG/MySQL 双入口均执行同一矩阵 ────────────────────────

func runFullCovRead(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	// 缓存计数 store 由第 9 组创建（EnableCaches），第 10 组复用同一实例
	//（插件按驱动注册，无法二次绑定新 cacher），以基线快照做增量断言。
	var readStore *gfc2CountStore

	// ── 1. Find ───────────────────────────────────────────────────
	t.Run("Find_全量_指针切片_conds变参_投影_空表非nil", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv)
		q := drv.Query()

		// 无条件全量：3 行全回（行序不保证，按集合断言）
		var all []gfc2User
		if err := q.Model(&gfc2User{}).Find(&all); err != nil {
			t.Fatalf("Find 全量: %v", err)
		}
		names := gfc2NamesByID(all)
		if len(names) != 3 || names["1"] != "alice" || names["2"] != "bob" || names["3"] != "carol" {
			t.Errorf("Find 全量应返回 3 行基线数据, 实际 %+v", all)
		}
		// 可空数值列：bob 经原生 INSERT 省略列为 NULL → *int64 nil；alice 33/11 正常
		for i := range all {
			if all[i].ID == "2" && all[i].Age != nil {
				t.Errorf("bob 的 age 应为 NULL(nil), 实际 %v", *all[i].Age)
			}
			if all[i].ID == "3" && (all[i].Age == nil || *all[i].Age != 33) {
				t.Errorf("carol 的 age 应 33, 实际 %v", all[i].Age)
			}
		}

		// 指针元素切片形态
		var allPtrs []*gfc2User
		if err := q.Model(&gfc2User{}).Find(&allPtrs); err != nil {
			t.Fatalf("Find []*T: %v", err)
		}
		if len(allPtrs) != 3 {
			t.Errorf("Find []*T 应返回 3 行, 实际 %d", len(allPtrs))
		}
		for _, p := range allPtrs {
			if p == nil {
				t.Fatalf("Find []*T 元素不应为 nil: %+v", allPtrs)
			}
		}

		// conds 变参单条件
		var one []gfc2User
		if err := q.Model(&gfc2User{}).Find(&one, "id = ?", "2"); err != nil {
			t.Fatalf("Find conds: %v", err)
		}
		if len(one) != 1 || one[0].Name != "bob" || one[0].Cnt != 20 {
			t.Errorf("Find conds 应命中 bob, 实际 %+v", one)
		}

		// conds 与链上 Where 取 AND 交集（name=bob 且 cnt=20 → 命中；cnt>=30 → 空）
		var and []gfc2User
		if err := q.Model(&gfc2User{}).Where("name = ?", "bob").Find(&and, "cnt >= ?", int64(20)); err != nil {
			t.Fatalf("Find conds+Where: %v", err)
		}
		if len(and) != 1 || and[0].ID != "2" {
			t.Errorf("conds 与链上 Where 应取 AND, 实际 %+v", and)
		}
		var andEmpty []gfc2User
		if err := q.Model(&gfc2User{}).Where("name = ?", "bob").Find(&andEmpty, "cnt >= ?", int64(30)); err != nil {
			t.Fatalf("Find conds+Where 空集: %v", err)
		}
		if len(andEmpty) != 0 {
			t.Errorf("AND 空集应 0 行, 实际 %+v", andEmpty)
		}

		// 投影结构体切片（显式 Table + Select + Order）
		var projs []gfc2UserProj
		if err := q.Table("gfc2_users").Select("id", "name").
			Where("cnt >= ?", int64(20)).Order("id").Find(&projs); err != nil {
			t.Fatalf("Find 投影: %v", err)
		}
		if len(projs) != 2 || projs[0].ID != "2" || projs[0].Name != "bob" || projs[1].Name != "carol" {
			t.Errorf("投影 Find 应按 id 升序返回 bob/carol, 实际 %+v", projs)
		}

		// 空表 → len 0 不报错
		// 差异固化：gorm 对空切片 dest 预分配（scan.go MakeSlice(type,0,20)），
		// 空结果为非 nil 空切片；xorm 侧零行不预置（nil/空均可）。
		gfc2Reset(t, drv)
		var empty []gfc2User
		if err := q.Model(&gfc2User{}).Find(&empty); err != nil {
			t.Fatalf("空表 Find: %v", err)
		}
		if len(empty) != 0 {
			t.Errorf("空表 Find 应 0 行, 实际 %+v", empty)
		}
		if empty == nil {
			t.Error("空表 Find 应返回非 nil 空切片（gorm 预分配语义）")
		}
		// 空表 + conds 同样 0 行
		var emptyConds []gfc2User
		if err := q.Model(&gfc2User{}).Find(&emptyConds, "id = ?", "x"); err != nil {
			t.Fatalf("空表 Find conds: %v", err)
		}
		if len(emptyConds) != 0 {
			t.Errorf("空表 Find conds 应 0 行, 实际 %+v", emptyConds)
		}
	})

	// ── 2. First / Last / Take ────────────────────────────────────
	t.Run("FirstLastTake_主键序_conds_未命中ErrRecordNotFound", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv)
		q := drv.Query()

		// 乱序插入 [3,1,2]：First 必须主键升序取 1、Last 主键降序取 3
		var first gfc2User
		if err := q.Model(&gfc2User{}).First(&first); err != nil {
			t.Fatalf("First: %v", err)
		}
		if first.ID != "1" || first.Name != "alice" {
			t.Errorf("First 应按主键升序取 id=1, 实际 %+v", first)
		}
		var last gfc2User
		if err := q.Model(&gfc2User{}).Last(&last); err != nil {
			t.Fatalf("Last: %v", err)
		}
		if last.ID != "3" || last.Name != "carol" {
			t.Errorf("Last 应按主键降序取 id=3, 实际 %+v", last)
		}

		// Take 不排序：取任意一行且数据完整
		var take gfc2User
		if err := q.Model(&gfc2User{}).Take(&take); err != nil {
			t.Fatalf("Take: %v", err)
		}
		switch take.ID {
		case "1", "2", "3":
		default:
			t.Errorf("Take 应取到基线三行之一, 实际 %+v", take)
		}

		// 三者带 conds：命中正确行
		if err := q.Model(&gfc2User{}).First(&first, "id = ?", "2"); err != nil {
			t.Fatalf("First conds: %v", err)
		}
		if first.Name != "bob" {
			t.Errorf("First conds 应命中 bob, 实际 %+v", first)
		}
		if err := q.Model(&gfc2User{}).Last(&last, "cnt = ?", int64(10)); err != nil {
			t.Fatalf("Last conds: %v", err)
		}
		if last.ID != "1" {
			t.Errorf("Last conds 应命中 alice, 实际 %+v", last)
		}
		var take2 gfc2User
		if err := q.Model(&gfc2User{}).Take(&take2, "id = ?", "3"); err != nil {
			t.Fatalf("Take conds: %v", err)
		}
		if take2.Name != "carol" {
			t.Errorf("Take conds 应命中 carol, 实际 %+v", take2)
		}

		// 三者未命中：均映射 contracts.ErrRecordNotFound（ERR-01，gorm.ErrRecordNotFound
		// 经 wrapError 包装，errors.Is 可判）
		for _, tc := range []struct {
			name string
			fn   func(dest any, conds ...any) error
		}{
			{"First", func(dest any, conds ...any) error { return q.Model(&gfc2User{}).First(dest, conds...) }},
			{"Last", func(dest any, conds ...any) error { return q.Model(&gfc2User{}).Last(dest, conds...) }},
			{"Take", func(dest any, conds ...any) error { return q.Model(&gfc2User{}).Take(dest, conds...) }},
		} {
			var miss gfc2User
			err := tc.fn(&miss, "id = ?", "no-such-id")
			if !errors.Is(err, contracts.ErrRecordNotFound) {
				t.Errorf("%s 未命中应返回 ErrRecordNotFound, 实际 %v", tc.name, err)
			}
		}

		// 空表 First：同样 ErrRecordNotFound（Last/Take 同实现，抽其一即可）
		gfc2Reset(t, drv)
		var none gfc2User
		if err := q.Model(&gfc2User{}).First(&none); !errors.Is(err, contracts.ErrRecordNotFound) {
			t.Errorf("空表 First 应返回 ErrRecordNotFound, 实际 %v", err)
		}
	})

	// ── 3. Count ──────────────────────────────────────────────────
	t.Run("Count_全量_条件_Order剥离_空表_裸链报错", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv)
		q := drv.Query()

		// 无条件（Model 链）
		var n int64
		if err := q.Model(&gfc2User{}).Count(&n); err != nil {
			t.Fatalf("Count 无条件: %v", err)
		}
		if n != 3 {
			t.Errorf("Count 应 3, 实际 %d", n)
		}

		// 链上 Where string 条件
		var nCond int64
		if err := q.Model(&gfc2User{}).Where("cnt >= ?", int64(20)).Count(&nCond); err != nil {
			t.Fatalf("Count 条件: %v", err)
		}
		if nCond != 2 {
			t.Errorf("Count cnt>=20 应 2, 实际 %d", nCond)
		}

		// 链上 Where map 等值条件
		var nMap int64
		if err := q.Model(&gfc2User{}).Where(map[string]any{"name": "bob"}).Count(&nMap); err != nil {
			t.Fatalf("Count map 条件: %v", err)
		}
		if nMap != 1 {
			t.Errorf("Count name=bob 应 1, 实际 %d", nMap)
		}

		// 无命中条件 → 0（不报错）
		var nZero int64
		if err := q.Model(&gfc2User{}).Where("cnt > ?", int64(1000)).Count(&nZero); err != nil {
			t.Fatalf("Count 无命中: %v", err)
		}
		if nZero != 0 {
			t.Errorf("Count 无命中应 0, 实际 %d", nZero)
		}

		// 链上 ORDER BY：Count 必须剥离排序，PG 下聚合列不在排序列会 42803（X-06）。
		// 差异固化：gorm 在无 GROUP BY 时自动删除 ORDER BY 子句（finisher_api Count），
		// 与 xorm X-06 同语义；探针实测两方言均不报错。
		var nOrder int64
		if err := q.Model(&gfc2User{}).Order("name").Count(&nOrder); err != nil {
			t.Fatalf("Count Order(name)（应剥离排序）: %v", err)
		}
		if nOrder != 3 {
			t.Errorf("Count Order(name) 应 3, 实际 %d", nOrder)
		}

		// Table 显式链等价
		var nTable int64
		if err := q.Table("gfc2_users").Count(&nTable); err != nil {
			t.Fatalf("Count Table: %v", err)
		}
		if nTable != 3 {
			t.Errorf("Count Table 应 3, 实际 %d", nTable)
		}

		// 空表 → 0 不报错
		gfc2Reset(t, drv)
		var nEmpty int64
		if err := q.Model(&gfc2User{}).Count(&nEmpty); err != nil {
			t.Fatalf("空表 Count: %v", err)
		}
		if nEmpty != 0 {
			t.Errorf("空表 Count 应 0, 实际 %d", nEmpty)
		}

		// 裸链（无 Table/Model）→ 报错
		// 差异固化：gorm 报 "Table not set, please set it like: db.Model(&user)
		// or db.Table(\"users\")"（xorm 同样报错，文案不同）。
		var nBare int64
		err := q.Count(&nBare)
		if err == nil {
			t.Error("无表 Count 应报错（要求链上显式 Table()/Model()）")
		} else if !strings.Contains(err.Error(), "Table not set") {
			t.Errorf("无表 Count 报错应含 Table not set, 实际 %v", err)
		}
	})

	// ── 4. Scan ───────────────────────────────────────────────────
	t.Run("Scan_标量_结构体_未命中零值dest不覆写", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv)
		q := drv.Query()

		// Raw 标量（count 聚合）
		var cnt int64
		if err := q.Raw("SELECT count(*) FROM gfc2_users WHERE cnt >= ?", int64(20)).Scan(&cnt); err != nil {
			t.Fatalf("Scan 标量: %v", err)
		}
		if cnt != 2 {
			t.Errorf("Scan count 应 2, 实际 %d", cnt)
		}

		// Raw 标量（文本列）
		var name string
		if err := q.Raw("SELECT name FROM gfc2_users WHERE id = ?", "1").Scan(&name); err != nil {
			t.Fatalf("Scan 文本标量: %v", err)
		}
		if name != "alice" {
			t.Errorf("Scan name 应 alice, 实际 %q", name)
		}

		// 链式标量（Select 单列）
		var sel int64
		if err := q.Model(&gfc2User{}).Select("cnt").Where("id = ?", "2").Scan(&sel); err != nil {
			t.Fatalf("Scan 链式标量: %v", err)
		}
		if sel != 20 {
			t.Errorf("Scan Select(cnt) 应 20, 实际 %d", sel)
		}

		// Raw 单行 → 单 struct（投影列缺失字段保持零值）
		var rawOne gfc2User
		if err := q.Raw("SELECT id, name FROM gfc2_users WHERE id = ?", "1").Scan(&rawOne); err != nil {
			t.Fatalf("Scan Raw 单行: %v", err)
		}
		if rawOne.ID != "1" || rawOne.Name != "alice" || rawOne.Cnt != 0 {
			t.Errorf("Scan Raw 单行应命中 alice 且缺失列保持零值, 实际 %+v", rawOne)
		}

		// 链式单 struct
		var chainOne gfc2User
		if err := q.Model(&gfc2User{}).Where("id = ?", "3").Scan(&chainOne); err != nil {
			t.Fatalf("Scan 链式单行: %v", err)
		}
		if chainOne.Name != "carol" || chainOne.Cnt != 30 {
			t.Errorf("Scan 链式单行应命中 carol, 实际 %+v", chainOne)
		}

		// dest 复用回归：Scan 不把 dest 旧值并入 WHERE（gorm Scan 无主键内联），
		// 已填充 dest 只作输出，二次查询正常覆盖
		var reused gfc2User
		if err := q.Model(&gfc2User{}).Where("id = ?", "1").Scan(&reused); err != nil {
			t.Fatalf("Scan 复用首次: %v", err)
		}
		if reused.Name != "alice" {
			t.Fatalf("Scan 复用首次应命中 alice, 实际 %+v", reused)
		}
		if err := q.Model(&gfc2User{}).Where("id = ?", "2").Scan(&reused); err != nil {
			t.Fatalf("Scan 复用二次: %v", err)
		}
		if reused.Name != "bob" {
			t.Errorf("Scan 复用二次应覆盖为 bob（旧值不得作条件）, 实际 %+v", reused)
		}

		// 链式集合（有序）
		var rows []gfc2User
		if err := q.Model(&gfc2User{}).Where("cnt >= ?", int64(20)).Order("id").Scan(&rows); err != nil {
			t.Fatalf("Scan 集合: %v", err)
		}
		if len(rows) != 2 || rows[0].Name != "bob" || rows[1].Name != "carol" {
			t.Errorf("Scan 集合应按 id 升序返回 bob/carol, 实际 %+v", rows)
		}

		// 单 struct 未命中：gorm Find 语义 → 不报错、dest 保持零值
		var miss gfc2User
		if err := q.Model(&gfc2User{}).Where("id = ?", "zz").Scan(&miss); err != nil {
			t.Fatalf("Scan 未命中不应报错: %v", err)
		}
		if miss.ID != "" || miss.Name != "" || miss.Cnt != 0 {
			t.Errorf("Scan 未命中 dest 应保持零值, 实际 %+v", miss)
		}

		// 标量未命中：零行时 gorm 静默保持 dest 原值不报错
		// 差异固化：gorm 标量 dest 零行不覆写（探针实测两方言 -1 原样保留）。
		var missScalar int64 = -1
		if err := q.Raw("SELECT cnt FROM gfc2_users WHERE id = ?", "zz").Scan(&missScalar); err != nil {
			t.Fatalf("Scan 标量未命中不应报错: %v", err)
		}
		if missScalar != -1 {
			t.Errorf("Scan 标量未命中应保持 dest 原值 -1, 实际 %d", missScalar)
		}

		// 空表集合：0 行不报错
		gfc2Reset(t, drv)
		var empty []gfc2User
		if err := q.Model(&gfc2User{}).Scan(&empty); err != nil {
			t.Fatalf("空表 Scan: %v", err)
		}
		if len(empty) != 0 {
			t.Errorf("空表 Scan 应 0 行, 实际 %+v", empty)
		}
		// 空表 Raw 聚合标量 → 0（聚合恒有一行）
		var n0 int64 = -1
		if err := q.Raw("SELECT count(*) FROM gfc2_users").Scan(&n0); err != nil {
			t.Fatalf("空表 Scan 标量: %v", err)
		}
		if n0 != 0 {
			t.Errorf("空表 Scan 标量应 0, 实际 %d", n0)
		}
	})

	// ── 5. Pluck ──────────────────────────────────────────────────
	t.Run("Pluck_单列_条件_空结果", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv)
		q := drv.Query()

		// 文本列 → []string（Order 保证行序确定性）
		var names []string
		if err := q.Model(&gfc2User{}).Order("id").Pluck("name", &names); err != nil {
			t.Fatalf("Pluck name: %v", err)
		}
		if len(names) != 3 || names[0] != "alice" || names[1] != "bob" || names[2] != "carol" {
			t.Errorf("Pluck name 应按 id 序返回 [alice bob carol], 实际 %v", names)
		}

		// 数值列 → []int64
		var cnts []int64
		if err := q.Model(&gfc2User{}).Order("id").Pluck("cnt", &cnts); err != nil {
			t.Fatalf("Pluck cnt: %v", err)
		}
		if len(cnts) != 3 || cnts[0] != 10 || cnts[1] != 20 || cnts[2] != 30 {
			t.Errorf("Pluck cnt 应按 id 序返回 [10 20 30], 实际 %v", cnts)
		}

		// 链上 Where 条件过滤
		var filtered []string
		if err := q.Model(&gfc2User{}).Where("cnt >= ?", int64(20)).Order("id").Pluck("name", &filtered); err != nil {
			t.Fatalf("Pluck 条件: %v", err)
		}
		if len(filtered) != 2 || filtered[0] != "bob" || filtered[1] != "carol" {
			t.Errorf("Pluck cnt>=20 应按 id 序返回 [bob carol], 实际 %v", filtered)
		}

		// 无命中条件 → 空结果不报错
		var none []string
		if err := q.Model(&gfc2User{}).Where("id = ?", "zz").Pluck("name", &none); err != nil {
			t.Fatalf("Pluck 无命中: %v", err)
		}
		if len(none) != 0 {
			t.Errorf("Pluck 无命中应 0 值, 实际 %v", none)
		}

		// 空表 → 空结果不报错
		gfc2Reset(t, drv)
		var empty []string
		if err := q.Model(&gfc2User{}).Pluck("name", &empty); err != nil {
			t.Fatalf("空表 Pluck: %v", err)
		}
		if len(empty) != 0 {
			t.Errorf("空表 Pluck 应 0 值, 实际 %v", empty)
		}
	})

	// ── 6. Row / Rows ─────────────────────────────────────────────
	t.Run("RowRows_Raw构建器链_游标生命周期_未命中ErrNoRows", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv)
		q := drv.Query()

		// Row()：Raw 原生链单行扫描
		var id, name string
		if err := q.Raw("SELECT id, name FROM gfc2_users WHERE id = ?", "1").Row().Scan(&id, &name); err != nil {
			t.Fatalf("Row().Scan: %v", err)
		}
		if id != "1" || name != "alice" {
			t.Errorf("Row() 应扫描到 alice, 实际 %q/%q", id, name)
		}

		// Row()：Raw 未命中 → sql.ErrNoRows 原样透出
		var missID, missName string
		if err := q.Raw("SELECT id, name FROM gfc2_users WHERE id = ?", "zz").
			Row().Scan(&missID, &missName); !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("Row() 未命中应返回 sql.ErrNoRows, 实际 %v", err)
		}

		// Row()：构建器链
		// 差异固化：gorm 侧构建器链 Row() 直接执行（GormQuery.Row 仅剥离缓存标记
		// 后代理 db.Row()），xorm 侧报"仅支持 Raw"。
		var bID, bName string
		if err := q.Model(&gfc2User{}).Select("id", "name").Where("id = ?", "3").
			Row().Scan(&bID, &bName); err != nil {
			t.Fatalf("构建器链 Row().Scan: %v", err)
		}
		if bID != "3" || bName != "carol" {
			t.Errorf("构建器链 Row() 应扫描到 carol, 实际 %q/%q", bID, bName)
		}

		// Rows()：Raw 原生链游标迭代（Next/Scan/Close/Columns）
		rows, err := q.Raw("SELECT id, name, cnt FROM gfc2_users WHERE cnt >= ? ORDER BY id", int64(10)).Rows()
		if err != nil {
			t.Fatalf("Rows(): %v", err)
		}
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Rows().Columns: %v", err)
		}
		if len(cols) != 3 || cols[0] != "id" || cols[1] != "name" || cols[2] != "cnt" {
			t.Errorf("Rows() 列名异常: %v", cols)
		}
		type rowT struct {
			id, name string
			cnt      int64
		}
		var got []rowT
		for rows.Next() {
			var r rowT
			if err := rows.Scan(&r.id, &r.name, &r.cnt); err != nil {
				t.Fatalf("Rows().Scan: %v", err)
			}
			got = append(got, r)
		}
		if err := rows.Close(); err != nil {
			t.Fatalf("Rows().Close: %v", err)
		}
		if len(got) != 3 || got[0].name != "alice" || got[1].name != "bob" || got[2].name != "carol" {
			t.Errorf("Rows() 应按 id 序返回 3 行, 实际 %+v", got)
		}

		// Rows()：Raw 空结果集 → 迭代 0 行无错
		emptyRows, err := q.Raw("SELECT id FROM gfc2_users WHERE id = ?", "zz").Rows()
		if err != nil {
			t.Fatalf("Rows() 空集: %v", err)
		}
		n := 0
		for emptyRows.Next() {
			n++
		}
		_ = emptyRows.Close()
		if n != 0 {
			t.Errorf("Rows() 空集应 0 行, 实际 %d", n)
		}

		// Rows()：构建器链
		// 差异固化：gorm 侧构建器链 Rows() 直接执行（xorm 侧报"仅支持 Raw"）。
		bRows, err := q.Model(&gfc2User{}).Select("id", "name").Order("id").Rows()
		if err != nil {
			t.Fatalf("构建器链 Rows(): %v", err)
		}
		bn := 0
		firstID := ""
		for bRows.Next() {
			var rid, rname string
			if err := bRows.Scan(&rid, &rname); err != nil {
				t.Fatalf("构建器链 Rows().Scan: %v", err)
			}
			if bn == 0 {
				firstID = rid
			}
			bn++
		}
		_ = bRows.Close()
		if bn != 3 || firstID != "1" {
			t.Errorf("构建器链 Rows() 应按 id 序迭代 3 行（首行 id=1）, 实际 %d 行首行 %q", bn, firstID)
		}

		// 用后连接不泄漏：Close 后连续多查询均正常（游标未释放会耗尽连接池使后续查询阻塞）
		for i := 0; i < 3; i++ {
			rr, err := q.Raw("SELECT id FROM gfc2_users ORDER BY id").Rows()
			if err != nil {
				t.Fatalf("泄漏观测轮 %d Rows(): %v", i, err)
			}
			c := 0
			for rr.Next() {
				c++
			}
			if err := rr.Close(); err != nil {
				t.Fatalf("泄漏观测轮 %d Close: %v", i, err)
			}
			if c != 3 {
				t.Errorf("泄漏观测轮 %d 应迭代 3 行, 实际 %d", i, c)
			}
		}
		var after int64
		if err := q.Model(&gfc2User{}).Count(&after); err != nil || after != 3 {
			t.Errorf("游标 Close 后查询应正常（count=3）, 实际 %d err=%v", after, err)
		}
	})

	// ── 7. ScanMap 与 NULL 形态（Q-10 差异固化重点）─────────────────
	t.Run("ScanMap_值形态_NULL差异固化_追加语义", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv)
		q := drv.Query()

		var rows []map[string]any
		if err := q.Model(&gfc2User{}).Order("id").ScanMap(&rows); err != nil {
			t.Fatalf("ScanMap: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("ScanMap 应 3 行, 实际 %d", len(rows))
		}
		// 行序由 Order("id") 保证：alice → bob → carol
		r0 := rows[0]
		for _, key := range []string{"id", "name", "cnt", "note", "age"} {
			if _, ok := r0[key]; !ok {
				t.Errorf("ScanMap 行应含列 %q, 实际键 %v", key, gfc2KeysOf(r0))
			}
		}
		// 非空文本列归一为 string：PG（pgx）原生 string；MySQL（go-sql-driver）
		// 返回 []byte，ScanMap 内 []byte→string 归一（实测两方言最终均为 string）
		if s, ok := r0["name"].(string); !ok || s != "alice" {
			t.Errorf("ScanMap name 应归一为 string alice, 实际 %T %v", r0["name"], r0["name"])
		}
		if s, ok := rows[2]["note"].(string); !ok || s != "note3" {
			t.Errorf("ScanMap note3 归一异常, 实际 %T %v", rows[2]["note"], rows[2]["note"])
		}
		// 数值列按 fmt.Sprint 断言（容器类型差异：int64 等）
		if fmt.Sprint(r0["cnt"]) != "10" {
			t.Errorf("ScanMap cnt 应 10, 实际 %T %v", r0["cnt"], r0["cnt"])
		}
		if fmt.Sprint(r0["age"]) != "11" {
			t.Errorf("ScanMap age 应 11, 实际 %T %v", r0["age"], r0["age"])
		}

		// NULL 列形态（Q-10 差异固化）：bob 行 note/age 均为数据库 NULL。
		// gorm 侧实测（gormdriver ScanMap → db.Rows() → sql.Rows.Scan 进 *any）：
		//   - PG（pgx v5.6）：NULL int → nil、NULL text → nil
		//   - MySQL（go-sql-driver v1.8）：NULL int → nil、NULL text → nil
		// 对照 xorm 口径：pgx NULL INT4→int32(0)、MySQL→nil、文本 NULL→""——
		// gorm 侧不做容器归一，两侧言统一为 nil（database/sql 对 *any dest 的
		// NULL→nil 语义）。
		switch dialect {
		case "pg":
			// 实测：pgx NULL 列 Scan 进 *any → nil
			gfc2AssertNil(t, "PG NULL note", rows[1]["note"])
			gfc2AssertNil(t, "PG NULL age", rows[1]["age"])
		case "mysql":
			// 实测：go-sql-driver NULL 列 Scan 进 *any → nil
			gfc2AssertNil(t, "MySQL NULL note", rows[1]["note"])
			gfc2AssertNil(t, "MySQL NULL age", rows[1]["age"])
		}

		// dest 追加而非替换：预置一行后追加 3 行
		var appended = []map[string]any{{"pre": "seed"}}
		if err := q.Model(&gfc2User{}).ScanMap(&appended); err != nil {
			t.Fatalf("ScanMap 追加: %v", err)
		}
		if len(appended) != 4 || appended[0]["pre"] != "seed" {
			t.Errorf("ScanMap 应追加进 dest（预置 1 + 3）, 实际 %d 行", len(appended))
		}

		// Raw 路径：列名即 SELECT 标签
		var rawRows []map[string]any
		if err := q.Raw("SELECT id, name FROM gfc2_users WHERE id = ?", "2").ScanMap(&rawRows); err != nil {
			t.Fatalf("ScanMap Raw: %v", err)
		}
		if len(rawRows) != 1 {
			t.Fatalf("ScanMap Raw 应 1 行, 实际 %d", len(rawRows))
		}
		if s, ok := rawRows[0]["name"].(string); !ok || s != "bob" {
			t.Errorf("ScanMap Raw 应命中 bob, 实际 %T %v", rawRows[0]["name"], rawRows[0]["name"])
		}

		// 空表 → 0 行追加、不报错
		gfc2Reset(t, drv)
		var empty []map[string]any
		if err := q.Model(&gfc2User{}).ScanMap(&empty); err != nil {
			t.Fatalf("空表 ScanMap: %v", err)
		}
		if len(empty) != 0 {
			t.Errorf("空表 ScanMap 应 0 行, 实际 %v", empty)
		}
	})

	// ── 8. Exists ─────────────────────────────────────────────────
	t.Run("Exists_空表_有数据_conds_预填充不收窄", func(t *testing.T) {
		gfc2Reset(t, drv)
		q := drv.Query()

		// 空表 → false, nil
		if ok, err := q.Model(&gfc2User{}).Exists(&gfc2User{}); err != nil {
			t.Fatalf("空表 Exists: %v", err)
		} else if ok {
			t.Error("空表 Exists 应 false")
		}

		gfc2Seed(t, drv)
		// 有数据 → true
		if ok, err := q.Model(&gfc2User{}).Exists(&gfc2User{}); err != nil {
			t.Fatalf("Exists 有数据: %v", err)
		} else if !ok {
			t.Error("有数据 Exists 应 true")
		}

		// conds 命中 / 不命中
		if ok, err := q.Model(&gfc2User{}).Exists(&gfc2User{}, "id = ?", "2"); err != nil {
			t.Fatalf("Exists conds 命中: %v", err)
		} else if !ok {
			t.Error("Exists conds 命中应 true")
		}
		if ok, err := q.Model(&gfc2User{}).Exists(&gfc2User{}, "id = ?", "zz"); err != nil {
			t.Fatalf("Exists conds 不命中: %v", err)
		} else if ok {
			t.Error("Exists conds 不命中应 false")
		}

		// 链上 Where
		if ok, err := q.Model(&gfc2User{}).Where("cnt >= ?", int64(30)).Exists(&gfc2User{}); err != nil {
			t.Fatalf("Exists 链上 Where 命中: %v", err)
		} else if !ok {
			t.Error("Exists 链上 Where 命中应 true")
		}
		if ok, err := q.Model(&gfc2User{}).Where("cnt >= ?", int64(1000)).Exists(&gfc2User{}); err != nil {
			t.Fatalf("Exists 链上 Where 不命中: %v", err)
		} else if ok {
			t.Error("Exists 链上 Where 不命中应 false")
		}

		// dest 预填充不收窄条件
		// 差异固化：gormdriver Exists 实现为 Model(dest).Where(conds).Limit(1).Count，
		// dest 的主键/字段值不并入 WHERE（与 Find/First 的主键内联语义不同）。
		// 预填 ID=1 但 conds 指向 cnt>1000 → 仅按 conds 判 false；
		// 预填 ID=1 但 conds 指向 id=2 → 仅按 conds 判 true（若 dest 主键并入
		// 条件则 id=1 AND id=2 应为 false）。
		if ok, err := q.Model(&gfc2User{}).Exists(&gfc2User{ID: "1"}, "cnt > ?", int64(1000)); err != nil {
			t.Fatalf("Exists dest 预填充+不命中 conds: %v", err)
		} else if ok {
			t.Error("Exists dest 预填充不应放宽条件（cnt>1000 应 false）")
		}
		if ok, err := q.Model(&gfc2User{}).Exists(&gfc2User{ID: "1"}, "id = ?", "2"); err != nil {
			t.Fatalf("Exists dest 预填充+命中 conds: %v", err)
		} else if !ok {
			t.Error("Exists dest 主键不应并入条件（id=2 应 true）")
		}
	})

	// ── 9. 查询缓存（CC-01/02/04/05，真实库 + EnableCaches）──────────
	// 放倒数第二：EnableCaches 对驱动实例全局生效且不可关闭，避免影响前面用例。
	t.Run("查询缓存_未启用无副作用_命中回源_写失效_键隔离", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv)
		q := drv.Query()

		// CC-04（未启用缓存插件）：Cache() 零缓存副作用——直改库后立即读到新值
		var cc4a, cc4b gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("id = ?", "2").First(&cc4a); err != nil {
			t.Fatalf("CC-04 首查: %v", err)
		}
		if cc4a.Name != "bob" {
			t.Fatalf("CC-04 首查应命中 bob, 实际 %+v", cc4a)
		}
		intgMustExec(t, drv, `UPDATE gfc2_users SET name = ? WHERE id = ?`, "bob_cc4", "2")
		if err := q.Cache().Model(&gfc2User{}).Where("id = ?", "2").First(&cc4b); err != nil {
			t.Fatalf("CC-04 二查: %v", err)
		}
		if cc4b.Name != "bob_cc4" {
			t.Errorf("未启用插件时 Cache() 不应缓存（应读到新值 bob_cc4）, 实际 %q", cc4b.Name)
		}
		intgMustExec(t, drv, `UPDATE gfc2_users SET name = ? WHERE id = ?`, "bob", "2")

		// 启用缓存插件（重复调用安全；同一驱动仅首次注册）
		tc := gfc2NewCountCache()
		if err := drv.EnableCaches(tc); err != nil {
			t.Fatalf("EnableCaches: %v", err)
		}
		store := tc.gfc2CountStore
		readStore = store
		// CC-04 佐证：启用前 Cache() 未产生任何缓存读写（计数从 0 开始）
		gfc2AssertCounts(t, store, "启用时零残留", 0, 0, 0)

		// CC-01 首次 Find 回源回填（get=1 hit=0 put=1）
		var v1 []gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("cnt >= ?", int64(20)).Order("id").Find(&v1); err != nil {
			t.Fatalf("缓存 Find 首查: %v", err)
		}
		if len(v1) != 2 || v1[0].Name != "bob" || v1[1].Name != "carol" {
			t.Fatalf("缓存 Find 结果异常: %+v", v1)
		}
		gfc2AssertCounts(t, store, "Find 首查回源", 1, 0, 1)

		// 同链再查命中缓存（不回源）
		var v2 []gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("cnt >= ?", int64(20)).Order("id").Find(&v2); err != nil {
			t.Fatalf("缓存 Find 再查: %v", err)
		}
		if len(v2) != 2 || v2[1].Name != "carol" {
			t.Fatalf("命中缓存 Find 应 2 行, 实际 %d", len(v2))
		}
		gfc2AssertCounts(t, store, "Find 再查命中", 2, 1, 1)

		// 绕开驱动写层直改库（Exec 不触发失效）：同链仍命中缓存返回旧结果集
		intgMustExec(t, drv, `INSERT INTO gfc2_users (id, name, cnt, note, age) VALUES (?, ?, ?, ?, ?)`,
			"4", "dave", int64(40), "note4", gfc2AgePtr(44))
		var v3 []gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("cnt >= ?", int64(20)).Order("id").Find(&v3); err != nil {
			t.Fatalf("缓存 Find 三查: %v", err)
		}
		if len(v3) != 2 {
			t.Errorf("直改库后应命中旧缓存（仍 2 行），实际 %d 行", len(v3))
		}
		gfc2AssertCounts(t, store, "Find 命中旧值", 3, 2, 1)

		// CC-02 写失效——Create：失效全部缓存，同链回源读到新行
		if err := q.Create(&gfc2User{ID: "5", Name: "eve", Cnt: 50, Note: "note5", Age: gfc2AgePtr(55)}); err != nil {
			t.Fatalf("缓存失效 Create: %v", err)
		}
		var v4 []gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("cnt >= ?", int64(20)).Order("id").Find(&v4); err != nil {
			t.Fatalf("写失效后 Find: %v", err)
		}
		if len(v4) != 4 || v4[0].Name != "bob" || v4[3].Name != "eve" {
			t.Errorf("写失效后应回源 4 行（bob/carol/dave/eve）, 实际 %+v", v4)
		}
		gfc2AssertCounts(t, store, "Create 失效后回源", 4, 2, 2)

		// Count 同样走缓存（标量 dest，键与 Find 隔离）
		var total int64
		if err := q.Cache().Model(&gfc2User{}).Count(&total); err != nil {
			t.Fatalf("缓存 Count: %v", err)
		}
		if total != 5 {
			t.Errorf("缓存 Count 应 5, 实际 %d", total)
		}
		gfc2AssertCounts(t, store, "Count 回源", 5, 2, 3)

		// CC-02 写失效——Update：失效后 Count 回源、Find B 回源读到 alice 新值
		if err := q.Model(&gfc2User{}).Where("id = ?", "1").Update("cnt", 15); err != nil {
			t.Fatalf("缓存失效 Update: %v", err)
		}
		var total2 int64
		if err := q.Cache().Model(&gfc2User{}).Count(&total2); err != nil {
			t.Fatalf("Update 失效后 Count: %v", err)
		}
		if total2 != 5 {
			t.Errorf("Update 失效后 Count 应回源 5, 实际 %d", total2)
		}
		gfc2AssertCounts(t, store, "Update 失效后 Count 回源", 6, 2, 4)
		var v5 []gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("cnt >= ?", int64(15)).Order("id").Find(&v5); err != nil {
			t.Fatalf("Update 失效后 Find B: %v", err)
		}
		if len(v5) != 5 || v5[0].ID != "1" {
			t.Errorf("Update 失效后应回源含 alice 新值（cnt>=15 共 5 行）, 实际 %+v", v5)
		}
		gfc2AssertCounts(t, store, "Update 失效后 Find 回源", 7, 2, 5)

		// CC-02 写失效——Delete：失效后 Count 回源变 4
		if err := q.Model(&gfc2User{}).Where("id = ?", "5").Delete(&gfc2User{}); err != nil {
			t.Fatalf("缓存失效 Delete: %v", err)
		}
		var total3 int64
		if err := q.Cache().Model(&gfc2User{}).Count(&total3); err != nil {
			t.Fatalf("Delete 失效后 Count: %v", err)
		}
		if total3 != 4 {
			t.Errorf("Delete 失效后 Count 应回源 4, 实际 %d", total3)
		}
		gfc2AssertCounts(t, store, "Delete 失效后回源", 8, 2, 6)

		// CC-02 写失效——Save（无 version 模型走原生全列更新）：失效后回源新值
		if err := q.Save(&gfc2User{ID: "2", Name: "bobby", Cnt: 20, Note: "note2b"}); err != nil {
			t.Fatalf("缓存失效 Save: %v", err)
		}
		var total4 int64
		if err := q.Cache().Model(&gfc2User{}).Count(&total4); err != nil {
			t.Fatalf("Save 失效后 Count: %v", err)
		}
		if total4 != 4 {
			t.Errorf("Save 失效后 Count 应回源 4, 实际 %d", total4)
		}
		gfc2AssertCounts(t, store, "Save 失效后回源", 9, 2, 7)
		var v6 []gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("cnt >= ?", int64(20)).Order("id").Find(&v6); err != nil {
			t.Fatalf("Save 失效后 Find: %v", err)
		}
		if len(v6) != 3 || v6[0].Name != "bobby" {
			t.Errorf("Save 失效后应回源读到 bobby（3 行）, 实际 %+v", v6)
		}
		gfc2AssertCounts(t, store, "Save 失效后 Find 回源", 10, 2, 8)

		// CC-05 缓存键隔离——dest 类型隔离（cacheKey 追加 dest 类型）：
		// 同表同条件的查询，gfc2User 与 gfc2UserLite 不共享缓存。
		var u1 gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("id = ?", "3").First(&u1); err != nil {
			t.Fatalf("CC-05 gfc2User 首查: %v", err)
		}
		if u1.Name != "carol" {
			t.Fatalf("CC-05 gfc2User 首查应 carol, 实际 %+v", u1)
		}
		gfc2AssertCounts(t, store, "CC-05 首查回源", 11, 2, 9)
		// 直改库绕开失效
		intgMustExec(t, drv, `UPDATE gfc2_users SET name = ? WHERE id = ?`, "carol2", "3")
		// 不同 dest 类型：回源读到新值（类型隔离生效）
		var lite gfc2UserLite
		if err := q.Cache().Table("gfc2_users").Where("id = ?", "3").First(&lite); err != nil {
			t.Fatalf("CC-05 gfc2UserLite 查询: %v", err)
		}
		if lite.Name != "carol2" {
			t.Errorf("dest 类型隔离失效：gfc2UserLite 应回源读到 carol2, 实际命中旧值 %q", lite.Name)
		}
		gfc2AssertCounts(t, store, "CC-05 类型隔离回源", 12, 2, 10)
		// 原 dest 类型再次查询：命中自身旧缓存
		var u2 gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("id = ?", "3").First(&u2); err != nil {
			t.Fatalf("CC-05 gfc2User 二查: %v", err)
		}
		if u2.Name != "carol" {
			t.Errorf("gfc2User 应命中自身缓存旧值 carol, 实际 %q", u2.Name)
		}
		gfc2AssertCounts(t, store, "CC-05 原类型命中", 13, 3, 10)

		// CC-05 不同 Where 不同键：链上条件不同 → 缓存键不同（互不串值）。
		// 注意 bobby/carol2 为上文 Save/直改库后的现值，顺带验证回源读到真实数据。
		var w1, w2 []gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("cnt = ?", int64(20)).Find(&w1); err != nil {
			t.Fatalf("CC-05 Where(20): %v", err)
		}
		if err := q.Cache().Model(&gfc2User{}).Where("cnt = ?", int64(30)).Find(&w2); err != nil {
			t.Fatalf("CC-05 Where(30): %v", err)
		}
		if len(w1) != 1 || w1[0].Name != "bobby" || len(w2) != 1 || w2[0].Name != "carol2" {
			t.Errorf("不同 Where 应各自回源且互不串值, 实际 %+v / %+v", w1, w2)
		}
		gfc2AssertCounts(t, store, "CC-05 不同 Where 独立键", 15, 3, 12)
	})

	// ── 10. 缓存 TTL 过期 + 锁/游标剥离（CC-03 / LK-04）──────────────
	// 放最后：沿用本驱动已注册的缓存插件，以基线快照做增量计数断言。
	t.Run("缓存TTL过期_锁与游标剥离缓存标记", func(t *testing.T) {
		gfc2Reset(t, drv)
		gfc2Seed(t, drv) // 两次 Create 已触发全量失效，组内从干净缓存开始
		q := drv.Query()
		if readStore == nil {
			t.Fatalf("缓存计数 store 未初始化（第 9 组未执行）")
		}
		store := readStore
		base := gfc2Snapshot(store)

		// CC-03 TTL 过期：短命 TTL（100ms）后回源
		var n1 int64
		if err := q.Cache(contracts.CacheTTLOption(100 * time.Millisecond)).Model(&gfc2User{}).Count(&n1); err != nil {
			t.Fatalf("TTL Count 首查: %v", err)
		}
		if n1 != 3 {
			t.Fatalf("TTL Count 首查应 3, 实际 %d", n1)
		}
		gfc2AssertDelta(t, store, base, "TTL 首查回源", 1, 0, 1)
		// TTL 内同链再查：命中
		var n2 int64
		if err := q.Cache(contracts.CacheTTLOption(100 * time.Millisecond)).Model(&gfc2User{}).Count(&n2); err != nil {
			t.Fatalf("TTL Count 再查: %v", err)
		}
		if n2 != 3 {
			t.Fatalf("TTL 内命中应 3, 实际 %d", n2)
		}
		gfc2AssertDelta(t, store, base, "TTL 内命中", 2, 1, 1)
		// 过 TTL 后同链三查：条目过期 → 回源重填
		time.Sleep(250 * time.Millisecond)
		var n3 int64
		if err := q.Cache(contracts.CacheTTLOption(100 * time.Millisecond)).Model(&gfc2User{}).Count(&n3); err != nil {
			t.Fatalf("TTL 过期后 Count: %v", err)
		}
		if n3 != 3 {
			t.Fatalf("TTL 过期回源应 3, 实际 %d", n3)
		}
		gfc2AssertDelta(t, store, base, "TTL 过期回源", 3, 1, 2)

		// 对照：默认 TTL（5min）查询两次第二次命中 → 证明过期由 TTL 驱动
		var d1 gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("id = ?", "1").First(&d1); err != nil {
			t.Fatalf("默认 TTL First 首查: %v", err)
		}
		if d1.Name != "alice" {
			t.Fatalf("默认 TTL First 首查应 alice, 实际 %+v", d1)
		}
		gfc2AssertDelta(t, store, base, "默认 TTL 首查回源", 4, 1, 3)
		var d2 gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("id = ?", "1").First(&d2); err != nil {
			t.Fatalf("默认 TTL First 再查: %v", err)
		}
		if d2.Name != "alice" {
			t.Fatalf("默认 TTL 命中应 alice, 实际 %+v", d2)
		}
		gfc2AssertDelta(t, store, base, "默认 TTL 命中", 5, 2, 3)

		// LK-04 锁查询自动剥离缓存标记（withoutCache）：直改库后 Lock+Cache
		// 查询始终回源新值，且计数零增长（未进缓存读写层）。
		intgMustExec(t, drv, `UPDATE gfc2_users SET name = ? WHERE id = ?`, "aliceLK", "1")
		var l1 gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("id = ?", "1").
			Lock(contracts.LockForUpdate).First(&l1); err != nil {
			t.Fatalf("Lock+Cache First: %v", err)
		}
		if l1.Name != "aliceLK" {
			t.Errorf("悲观锁查询不应被缓存（期望新值 aliceLK）, 实际 %q", l1.Name)
		}
		gfc2AssertDelta(t, store, base, "Lock 剥离缓存标记", 5, 2, 3)
		var l2 gfc2User
		if err := q.Cache().Model(&gfc2User{}).Where("id = ?", "1").
			Lock(contracts.LockForUpdate).First(&l2); err != nil {
			t.Fatalf("Lock+Cache First 二查: %v", err)
		}
		if l2.Name != "aliceLK" {
			t.Errorf("二次锁查询应仍回源新值 aliceLK, 实际 %q", l2.Name)
		}
		gfc2AssertDelta(t, store, base, "Lock 二查仍剥离", 5, 2, 3)

		// Row()/Rows() 游标类终结方法自动剥离缓存标记（避免命中缓存返回空游标）
		var rID, rName string
		if err := q.Cache().Model(&gfc2User{}).Select("id", "name").Where("id = ?", "1").
			Row().Scan(&rID, &rName); err != nil {
			t.Fatalf("Cache+Row().Scan: %v", err)
		}
		if rID != "1" || rName != "aliceLK" {
			t.Errorf("Cache+Row() 应回源新值, 实际 %q/%q", rID, rName)
		}
		gfc2AssertDelta(t, store, base, "Row 剥离缓存标记", 5, 2, 3)
		rows, err := q.Cache().Model(&gfc2User{}).Select("id", "name").Order("id").Rows()
		if err != nil {
			t.Fatalf("Cache+Rows(): %v", err)
		}
		rn := 0
		for rows.Next() {
			rn++
		}
		_ = rows.Close()
		if rn != 3 {
			t.Errorf("Cache+Rows() 应迭代 3 行, 实际 %d", rn)
		}
		gfc2AssertDelta(t, store, base, "Rows 剥离缓存标记", 5, 2, 3)
	})
}

// gfc2KeysOf 输出 map 键集合（断言诊断辅助）。
func gfc2KeysOf(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// gfc2AssertNil 断言值为 nil（ScanMap NULL 形态断言辅助）。
func gfc2AssertNil(t *testing.T, stage string, v any) {
	t.Helper()
	if v != nil {
		t.Errorf("%s 应为 nil, 实际 %T %v", stage, v, v)
	}
}

// ── PG / MySQL 双入口 ────────────────────────────────────────────────

func TestFullCovRead_PG(t *testing.T) {
	runFullCovRead(t, newIntgPG(t), "pg")
}

func TestFullCovRead_MySQL(t *testing.T) {
	runFullCovRead(t, newIntgMySQL(t), "mysql")
}
