//go:build integration

package gormdriver

// fullcov_ext_integration_test.go —— ext 域真实库全量覆盖（分组 gfc5，对齐 xorm 侧
// gofast-xorm/fullcov_ext_integration_test.go 的用例矩阵）：
// 软删除全生命周期（SD-01~08）/ 悲观锁互斥（LK-01/02）/ FOR SHARE（LK-03）/
// Joins 功能断言（Q-08）/ 预加载 + 事务联动（PL-02/PL-04/TX-07）。
//
// 运行方式（docker-compose.yml 环境）：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	  go test -tags integration -run 'TestFullCovExt_PG$' -v .
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -run 'TestFullCovExt_MySQL$' -v .
//
// 软删除双形态（方案 §5.4，gorm 侧特有的关键差异维度）：
//   - 形态一（业务级）：框架 database.SoftDelete 同款——deleted_at 为普通 int64
//     列（orm tag index/default(0)，无任何行为 tag）。gorm 对该形态无原生软删
//     语义：Delete 是物理删除，软删标记由业务经 Update 写入，普通查询不自动
//     过滤（需显式 Where("deleted_at = 0") 业务 scope）。OnlyTrashed/Restore/
//     ForceDelete 对 int64 列均可直接执行。
//   - 形态二（gorm 原生）：gorm.DeletedAt 类型列（timestamp/datetime）。gorm
//     Delete 自动转软删（写入当前时间）、普通查询自动过滤 deleted_at IS NULL、
//     Preload/Update 子句自动附加 IS NULL。但框架扩展 OnlyTrashed/Restore 硬
//     编码 "deleted_at != 0"/"deleted_at = 0"，与 timestamp 列形态不兼容
//     （PG 直接报类型错误、MySQL 靠隐式转换部分工作）——差异在用例中如实固化。
//
// 其余覆盖：
//   - Lock：PG lock_timeout / MySQL innodb_lock_wait_timeout 双方言真实互斥
//    （LK-01/02，gorm 补 MySQL 互斥新用例）；FOR SHARE 真执行并发 + 写阻塞
//    （LK-03，差异固化：xorm 侧 LockShareMode 为 no-op，gorm 侧 clause.Locking
//     真实生成 FOR SHARE）。
//   - Joins：缺省 INNER / LEFT（无匹配补 NULL）/ 带参 ON / 自连接（别名）/
//     非法串（差异固化：gorm 原样透传 → 数据库语法错误，而非 xorm 的
//     ErrUnsupported）/ PG 下 schema 前缀关联表（gorm Joins 串不自动加前缀，
//     需调用方拼接，演示 GetSchema() 用法）。
//   - Preload：conds 过滤（原生 + 共享引擎双路径）、callback 排序限量（仅引擎
//     路径，差异固化：contracts 形态 callback 在 gorm 原生路径报错）、
//     FindInBatches 逐批预加载（双路径）、事务内 Preload（TX-07）、事务内写
//     对外部查询缓存的失效时机（TX-07，内存 Cache 断言）。
//
// 锁/并发用例全部经 gfc5WaitCh/gfc5WaitErrCh（select + time.After 30s）防悬挂；
// goroutine 内不调用 t.Fatalf（以 error 返回值/通道传递失败）。

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"gorm.io/gorm"
)

// ── 测试模型（前缀 gfc5_，全部显式 TableName）──────────────────────────

// gfc5Soft 软删除模型·形态一：框架业务级（database.SoftDelete 同款）——
// deleted_at 普通 int64 列，gorm 无原生软删语义。
type gfc5Soft struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	Name      string `orm:"varchar(64) 'name'"`
	DeletedAt int64  `orm:"'deleted_at' index default(0)"`
}

func (gfc5Soft) TableName() string { return "gfc5_softs" }

// gfc5SoftG 软删除模型·形态二：gorm 原生 gorm.DeletedAt（timestamp/datetime 列，
// gorm 自动软删 + IS NULL 过滤）。
type gfc5SoftG struct {
	ID        string         `gorm:"column:id;primaryKey;size:16"`
	Name      string         `gorm:"column:name;size:64"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

func (gfc5SoftG) TableName() string { return "gfc5_softgs" }

// gfc5Unique 唯一键 + 业务级软删共存模型（SD-07 形态一）。
type gfc5Unique struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	Email     string `orm:"varchar(100) 'email' unique"`
	DeletedAt int64  `orm:"'deleted_at' index default(0)"`
}

func (gfc5Unique) TableName() string { return "gfc5_uniques" }

// gfc5UniqueG 唯一键 + gorm 原生软删共存模型（SD-07 形态二）。
type gfc5UniqueG struct {
	ID        string         `gorm:"column:id;primaryKey;size:16"`
	Email     string         `gorm:"column:email;size:100;unique"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

func (gfc5UniqueG) TableName() string { return "gfc5_uniquegs" }

// gfc5Lock 悲观锁场景模型（LK-01/02/03）。
type gfc5Lock struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Cnt int64  `orm:"'cnt' default(0)"`
}

func (gfc5Lock) TableName() string { return "gfc5_locks" }

// gfc5User / gfc5Order Joins 场景主表与子表（列名不重名，免去 SELECT 歧义）。
type gfc5User struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Name string `orm:"varchar(64) 'name'"`
	Cnt  int64  `orm:"'cnt' default(0)"`
}

func (gfc5User) TableName() string { return "gfc5_users" }

type gfc5Order struct {
	ID     string `orm:"pk varchar(16) 'id'"`
	UserID string `orm:"varchar(16) 'user_id'"`
	Amount int64  `orm:"'amount' default(0)"`
}

func (gfc5Order) TableName() string { return "gfc5_orders" }

// ── SD-06 模型：业务级软删 + 关联（gorm 原生 Preload 路径）─────────────

type gfc5SoftOrder struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	UserID    string `orm:"varchar(16) 'user_id'"`
	Amount    int64  `orm:"'amount' default(0)"`
	DeletedAt int64  `orm:"'deleted_at' index default(0)"`
}

func (gfc5SoftOrder) TableName() string { return "gfc5_soft_orders" }

type gfc5SoftUser struct {
	ID        string          `orm:"pk varchar(16) 'id'"`
	Name      string          `orm:"varchar(64) 'name'"`
	DeletedAt int64           `orm:"'deleted_at' index default(0)"`
	Orders    []gfc5SoftOrder `orm:"-" gorm:"foreignKey:UserID;references:ID"`
}

func (gfc5SoftUser) TableName() string { return "gfc5_soft_users" }

// ── SD-06 模型：gorm 原生软删 + 关联（Preload 自动 IS NULL 过滤）────────

type gfc5SoftGOrder struct {
	ID        string         `gorm:"column:id;primaryKey;size:16"`
	UserID    string         `gorm:"column:user_id;size:16"`
	Amount    int64          `gorm:"column:amount;default:0"`
	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index"`
}

func (gfc5SoftGOrder) TableName() string { return "gfc5_softg_orders" }

type gfc5SoftGUser struct {
	ID        string           `gorm:"column:id;primaryKey;size:16"`
	Name      string           `gorm:"column:name;size:64"`
	DeletedAt gorm.DeletedAt   `gorm:"column:deleted_at;index"`
	Orders    []gfc5SoftGOrder `gorm:"foreignKey:UserID;references:ID"`
}

func (gfc5SoftGUser) TableName() string { return "gfc5_softg_users" }

// ── Preload / TX-07 模型（表组 gfc5_pre_*，原生与共享引擎 dest 各一）────

type gfc5PreOrder struct {
	ID     string `orm:"pk varchar(16) 'id'"`
	UserID string `orm:"varchar(16) 'user_id'"`
	Amount int64  `orm:"'amount' default(0)"`
}

func (gfc5PreOrder) TableName() string { return "gfc5_pre_orders" }

// gfc5PreUser gorm 原生 Preload 路径（gorm 关联 tag，非 "-"）。
type gfc5PreUser struct {
	ID     string         `orm:"pk varchar(16) 'id'"`
	Name   string         `orm:"varchar(64) 'name'"`
	Orders []gfc5PreOrder `orm:"-" gorm:"foreignKey:UserID;references:ID"`
}

func (gfc5PreUser) TableName() string { return "gfc5_pre_users" }

// gfc5EngUser 共享 Preload 引擎路径 dest（gorm:"-" + rel tag），与 gfc5PreUser
// 同表 gfc5_pre_users，用于双路径一致性对照。
type gfc5EngUser struct {
	ID     string         `orm:"pk varchar(16) 'id'"`
	Name   string         `orm:"varchar(64) 'name'"`
	Orders []gfc5PreOrder `orm:"-" gorm:"-" rel:"foreignKey:UserID;references:ID"`
}

func (gfc5EngUser) TableName() string { return "gfc5_pre_users" }

// ── 防悬挂与断言辅助（gfc5 前缀）────────────────────────────────────────

// gfc5WaitCh 带超时的通道等待（锁/并发用例防悬挂）。
func gfc5WaitCh(t *testing.T, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("等待 %s 超时", name)
	}
}

// gfc5WaitErrCh 带超时的错误通道接收。
func gfc5WaitErrCh(t *testing.T, ch <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(30 * time.Second):
		t.Fatalf("等待 %s 超时", name)
		return nil
	}
}

// gfc5CountRaw 原生 SQL 计数锚点（不含任何驱动侧过滤）。
func gfc5CountRaw(t *testing.T, drv *GormDriver, table string) int64 {
	t.Helper()
	var n int64
	if err := drv.Query().Raw("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("原生计数 %s: %v", table, err)
	}
	return n
}

// gfc5NilLike SQL NULL 经方言归一后的形态（nil / 空串/空字节 / 数值零——如 pgx
// 将 NULL INT4 归一为 int32(0)）。
func gfc5NilLike(v any) bool {
	if v == nil {
		return true
	}
	switch x := v.(type) {
	case string:
		return x == ""
	case []byte:
		return len(x) == 0
	}
	return false
}

// gfc5MapStr ScanMap 字符串归一（string/[]byte 等方言形态 → string）。
func gfc5MapStr(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	}
	return fmt.Sprint(v)
}

// gfc5MapNum ScanMap 数值归一（int64/int32/float64/string/[]byte → int64）。
func gfc5MapNum(v any) int64 {
	var out int64
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	case string:
		for _, c := range n {
			if c < '0' || c > '9' {
				return out
			}
			out = out*10 + int64(c-'0')
		}
	case []byte:
		for _, c := range n {
			if c < '0' || c > '9' {
				return out
			}
			out = out*10 + int64(c-'0')
		}
	}
	return out
}

// gfc5HasAll 断言金额列表恰含全部期望值（INNER 无匹配不产生行）。
func gfc5HasAll(got []int64, want ...int64) bool {
	for _, w := range want {
		found := false
		for _, g := range got {
			if g == w {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return len(got) == len(want)
}

// ── 分组入口 ───────────────────────────────────────────────────────────

// TestFullCovExt_PG PostgreSQL 全量覆盖入口。
func TestFullCovExt_PG(t *testing.T) {
	runGfc5Ext(t, newIntgPG(t), "pg")
}

// TestFullCovExt_MySQL MySQL 全量覆盖入口（PG 专属场景在 runner 内跳过）。
func TestFullCovExt_MySQL(t *testing.T) {
	runGfc5Ext(t, newIntgMySQL(t), "mysql")
}

// runGfc5Ext 共享 runner：按域分组，dialect 区分方言分支（"pg"/"mysql"）。
func runGfc5Ext(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	t.Run("Lock_ForUpdate_Mutex", func(t *testing.T) { runGfc5LockMutex(t, drv, dialect) })
	t.Run("Lock_ShareMode", func(t *testing.T) { runGfc5LockShare(t, drv, dialect) })
	t.Run("SoftDelete_BusinessForm", func(t *testing.T) { runGfc5SoftBiz(t, drv, dialect) })
	t.Run("SoftDelete_GormNativeForm", func(t *testing.T) { runGfc5SoftNative(t, drv, dialect) })
	t.Run("SoftDelete_Rel_Unique_Save", func(t *testing.T) { runGfc5SoftRel(t, drv, dialect) })
	t.Run("Joins", func(t *testing.T) { runGfc5Joins(t, drv, dialect) })
	t.Run("Preload_Tx", func(t *testing.T) { runGfc5PreloadTx(t, drv, dialect) })
}

// ── 软删除全生命周期·形态一：框架业务级（普通 int64 deleted_at 列）──────

// runGfc5SoftBiz（SD-01~05 形态一）：
// 差异固化（gorm vs xorm deleted tag 模型）：
//   - gorm 下 Delete 对业务级形态是【物理删除】（gorm 不识别普通 deleted_at 列），
//     与 xorm deleted tag 模型（软删保留）行为相反——框架软删必须业务级经
//     Update 写入（database.SoftDelete 设计如此，双驱动一致的形态约定）。
//   - 普通查询不自动过滤软删行，需显式 Where("deleted_at = 0") 业务 scope。
//   - OnlyTrashed（Unscoped + deleted_at != 0）/Restore（置 0）/ForceDelete
//     （物理删除）对 int64 列两方言均可直接执行，语义与 xorm 侧一致。
func runGfc5SoftBiz(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc5_softs"}, &gfc5Soft{})

	q := drv.Query()
	for _, s := range []*gfc5Soft{
		{ID: "a", Name: "alice"},
		{ID: "b", Name: "bob"},
		{ID: "c", Name: "carol"},
	} {
		if err := q.Create(s); err != nil {
			t.Fatalf("种子 %s: %v", s.ID, err)
		}
	}
	_ = dialect // int64 列形态两方言 SQL 兼容，无需方言分支

	// 基线：普通查询（无 scope）3 行——差异固化：业务级形态无自动过滤
	var base []gfc5Soft
	if err := q.Model(&gfc5Soft{}).Order("id").Find(&base); err != nil {
		t.Fatalf("基线普通 Find: %v", err)
	}
	if len(base) != 3 {
		t.Fatalf("基线普通查询应 3 行, 实际 %d", len(base))
	}

	// SD-01（形态一差异固化）：Delete = 物理删除
	if err := q.Model(&gfc5Soft{}).Where("id = ?", "a").Delete(&gfc5Soft{}); err != nil {
		t.Fatalf("Delete a: %v", err)
	}
	if n := gfc5CountRaw(t, drv, "gfc5_softs"); n != 2 {
		t.Errorf("gorm 对业务级软删形态的 Delete 应物理移除行（差异固化：与 xorm deleted tag 软删保留相反）, 原生计数应 2, 实际 %d", n)
	}

	// 业务级软删 b：Update 写 deleted_at（database.SoftDelete 语义）
	now := time.Now().Unix()
	if err := q.Model(&gfc5Soft{}).Where("id = ?", "b").Update("deleted_at", now); err != nil {
		t.Fatalf("业务软删 b: %v", err)
	}
	// SD-01：软删后物理行保留
	if n := gfc5CountRaw(t, drv, "gfc5_softs"); n != 2 {
		t.Errorf("业务级软删应物理保留行, 原生计数应 2, 实际 %d", n)
	}
	// SD-01：普通查询不自动过滤（差异固化），业务 scope 过滤 + Count 同步
	var noScope []gfc5Soft
	if err := q.Model(&gfc5Soft{}).Order("id").Find(&noScope); err != nil {
		t.Fatalf("无 scope Find: %v", err)
	}
	if len(noScope) != 2 {
		t.Errorf("业务级形态普通查询不过滤软删行（差异固化）, 应 2 行, 实际 %d: %+v", len(noScope), noScope)
	}
	var scoped []gfc5Soft
	if err := q.Model(&gfc5Soft{}).Where("deleted_at = ?", 0).Order("id").Find(&scoped); err != nil {
		t.Fatalf("业务 scope Find: %v", err)
	}
	if len(scoped) != 1 || scoped[0].ID != "c" {
		t.Errorf("业务 scope（deleted_at = 0）应仅剩 [c], 实际 %+v", scoped)
	}
	var aliveCnt int64
	if err := q.Model(&gfc5Soft{}).Where("deleted_at = ?", 0).Count(&aliveCnt); err != nil || aliveCnt != 1 {
		t.Errorf("业务 scope Count 应同步为 1, 实际 %d (err=%v)", aliveCnt, err)
	}

	// SD-02：Unscoped 查回全部且 deleted_at 非 0（int64 形态）
	var all []gfc5Soft
	if err := q.Unscoped().Model(&gfc5Soft{}).Order("id").Find(&all); err != nil {
		t.Fatalf("Unscoped Find: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("Unscoped 应查回 2 行, 实际 %d", len(all))
	}
	for _, r := range all {
		if r.ID == "b" && r.DeletedAt == 0 {
			t.Errorf("软删行 b 的 deleted_at 应非 0, 实际 %d", r.DeletedAt)
		}
		if r.ID == "c" && r.DeletedAt != 0 {
			t.Errorf("未删行 c 的 deleted_at 应为 0, 实际 %d", r.DeletedAt)
		}
	}

	// SD-03：OnlyTrashed 只查已删行（int64 列两方言均可执行 deleted_at != 0）
	var trashed []gfc5Soft
	if err := q.OnlyTrashed().Model(&gfc5Soft{}).Order("id").Find(&trashed); err != nil {
		t.Fatalf("OnlyTrashed Find: %v", err)
	}
	if len(trashed) != 1 || trashed[0].ID != "b" {
		t.Errorf("OnlyTrashed 应只查已删行 [b], 实际 %+v", trashed)
	}

	// SD-04：Restore 置 0 后业务 scope 可见
	if err := q.Model(&gfc5Soft{}).Where("id = ?", "b").Restore(); err != nil {
		t.Fatalf("Restore b: %v", err)
	}
	var restored []gfc5Soft
	if err := q.Model(&gfc5Soft{}).Where("deleted_at = ?", 0).Order("id").Find(&restored); err != nil {
		t.Fatalf("Restore 后 scope Find: %v", err)
	}
	if len(restored) != 2 {
		t.Errorf("Restore 后业务 scope 应恢复 [b c] 2 行, 实际 %d: %+v", len(restored), restored)
	}
	var empty []gfc5Soft
	if err := q.OnlyTrashed().Model(&gfc5Soft{}).Find(&empty); err != nil {
		t.Fatalf("Restore 后 OnlyTrashed: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("Restore 后 OnlyTrashed 应为空, 实际 %+v", empty)
	}
	var bRow gfc5Soft
	if err := q.Unscoped().Model(&gfc5Soft{}).Where("id = ?", "b").First(&bRow); err != nil {
		t.Fatalf("Unscoped 回读 b: %v", err)
	}
	if bRow.DeletedAt != 0 {
		t.Errorf("Restore 后 b 的 deleted_at 应置 0, 实际 %d", bRow.DeletedAt)
	}

	// SD-05：ForceDelete 物理删除不可恢复
	if err := q.Model(&gfc5Soft{}).Where("id = ?", "c").Update("deleted_at", now); err != nil {
		t.Fatalf("业务软删 c: %v", err)
	}
	if err := q.Model(&gfc5Soft{}).Where("id = ?", "c").ForceDelete(&gfc5Soft{}); err != nil {
		t.Fatalf("ForceDelete c: %v", err)
	}
	if n := gfc5CountRaw(t, drv, "gfc5_softs"); n != 1 {
		t.Errorf("ForceDelete 应物理移除行, 原生计数应 1, 实际 %d", n)
	}
	var left []gfc5Soft
	if err := q.OnlyTrashed().Model(&gfc5Soft{}).Find(&left); err != nil {
		t.Fatalf("ForceDelete 后 OnlyTrashed: %v", err)
	}
	if len(left) != 0 {
		t.Errorf("ForceDelete 后 OnlyTrashed 应为空, 实际 %+v", left)
	}
	var remain []gfc5Soft
	if err := q.Unscoped().Model(&gfc5Soft{}).Order("id").Find(&remain); err != nil {
		t.Fatalf("ForceDelete 后 Unscoped Find: %v", err)
	}
	if len(remain) != 1 || remain[0].ID != "b" {
		t.Errorf("ForceDelete 后 Unscoped 应仅剩 [b], 实际 %+v", remain)
	}
}

// ── 软删除全生命周期·形态二：gorm 原生 gorm.DeletedAt ──────────────────

// runGfc5SoftNative（SD-01~05 形态二）：
// gorm 原生软删全链：Delete 写 timestamp、普通查询/Count 自动 IS NULL 过滤、
// Unscoped 查回；OnlyTrashed/Restore 与 timestamp 形态不兼容（差异固化 +
// 疑似缺陷固化，见组内注释）；ForceDelete 真删。
func runGfc5SoftNative(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc5_softgs"}, &gfc5SoftG{})

	q := drv.Query()
	for _, s := range []*gfc5SoftG{
		{ID: "sg1", Name: "alice"},
		{ID: "sg2", Name: "bob"},
		{ID: "sg3", Name: "carol"},
	} {
		if err := q.Create(s); err != nil {
			t.Fatalf("种子 %s: %v", s.ID, err)
		}
	}

	// 基线：3 行全部可见
	var base []gfc5SoftG
	if err := q.Model(&gfc5SoftG{}).Order("id").Find(&base); err != nil {
		t.Fatalf("基线普通 Find: %v", err)
	}
	if len(base) != 3 {
		t.Fatalf("基线普通查询应 3 行, 实际 %d", len(base))
	}

	// SD-01：Delete → gorm 原生软删（物理行保留）+ 普通查询过滤 + Count 同步
	if err := q.Model(&gfc5SoftG{}).Where("id = ?", "sg1").Delete(&gfc5SoftG{}); err != nil {
		t.Fatalf("软删除 sg1: %v", err)
	}
	if n := gfc5CountRaw(t, drv, "gfc5_softgs"); n != 3 {
		t.Fatalf("gorm 原生软删不应物理移除行, 原生计数应 3, 实际 %d", n)
	}
	var normal []gfc5SoftG
	if err := q.Model(&gfc5SoftG{}).Order("id").Find(&normal); err != nil {
		t.Fatalf("软删后普通 Find: %v", err)
	}
	if len(normal) != 2 {
		t.Errorf("gorm 原生软删后普通查询应自动过滤（剩 2）, 实际 %d: %+v", len(normal), normal)
	}
	var cnt int64
	if err := q.Model(&gfc5SoftG{}).Count(&cnt); err != nil || cnt != 2 {
		t.Errorf("Count 应与普通查询同步过滤, 期望 2, 实际 %d (err=%v)", cnt, err)
	}

	// SD-02：Unscoped 查回且 deleted_at 非 0（timestamp 形态即 Valid=true）
	var all []gfc5SoftG
	if err := q.Unscoped().Model(&gfc5SoftG{}).Order("id").Find(&all); err != nil {
		t.Fatalf("Unscoped Find: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("Unscoped 应查回 3 行, 实际 %d", len(all))
	}
	for _, r := range all {
		if r.ID == "sg1" && !r.DeletedAt.Valid {
			t.Errorf("软删行 sg1 的 deleted_at 应非零（Valid=true）, 实际 %+v", r.DeletedAt)
		}
		if r.ID != "sg1" && r.DeletedAt.Valid {
			t.Errorf("未删行 %s 的 deleted_at 应为 NULL（Valid=false）, 实际 %+v", r.ID, r.DeletedAt)
		}
	}

	// SD-03：OnlyTrashed——双驱动一致（已修复对齐）：驱动条件改为跨类型
	// CAST 比较（int64 列 <> '0' / 时间列 NULL 恒假排除），gorm.DeletedAt
	// 形态在 PG/MySQL 均正常查回已删行（修复前硬编码 "deleted_at != 0"
	// 在 PG 报 42883 类型不兼容）。
	var trashed []gfc5SoftG
	if err := q.OnlyTrashed().Model(&gfc5SoftG{}).Order("id").Find(&trashed); err != nil {
		t.Fatalf("gorm.DeletedAt 形态 OnlyTrashed: %v", err)
	}
	if len(trashed) != 1 || trashed[0].ID != "sg1" || !trashed[0].DeletedAt.Valid {
		t.Errorf("OnlyTrashed 应只查已删行 [sg1], 实际 %+v", trashed)
	}

	// SD-04：Restore——双驱动一致（已修复对齐）：驱动探测 gorm.DeletedAt
	// 形态写 NULL（修复前硬编码置 0：PG 42804 / MySQL 拒零日期，恢复失效）。
	// 恢复后普通查询（IS NULL 过滤）应重新可见。
	if err := q.Model(&gfc5SoftG{}).Where("id = ?", "sg1").Restore(); err != nil {
		t.Fatalf("gorm.DeletedAt 形态 Restore: %v", err)
	}
	var restored gfc5SoftG
	if err := q.Model(&gfc5SoftG{}).Where("id = ?", "sg1").First(&restored); err != nil {
		t.Fatalf("Restore 后普通查询应重新可见: %v", err)
	}
	if restored.DeletedAt.Valid {
		t.Errorf("Restore 后 deleted_at 应为 NULL（Valid=false）, 实际 %+v", restored.DeletedAt)
	}
	var visCnt int64
	if err := q.Model(&gfc5SoftG{}).Count(&visCnt); err != nil || visCnt != 3 {
		t.Errorf("Restore 后 Count 应 3, 实际 %d (err=%v)", visCnt, err)
	}
	if n := gfc5CountRaw(t, drv, "gfc5_softgs"); n != 3 {
		t.Errorf("Restore 路径不应物理移除行, 原生计数应 3, 实际 %d", n)
	}
	// 重新软删 sg1，恢复后续 ForceDelete 清场语义
	if err := q.Model(&gfc5SoftG{}).Where("id = ?", "sg1").Delete(&gfc5SoftG{}); err != nil {
		t.Fatalf("重新软删 sg1: %v", err)
	}

	// SD-05：ForceDelete 物理删除不可恢复（先删 sg2，再清 sg3/sg1）
	if err := q.Model(&gfc5SoftG{}).Where("id = ?", "sg2").ForceDelete(&gfc5SoftG{}); err != nil {
		t.Fatalf("ForceDelete sg2: %v", err)
	}
	if n := gfc5CountRaw(t, drv, "gfc5_softgs"); n != 2 {
		t.Errorf("ForceDelete 应物理移除行, 原生计数应 2, 实际 %d", n)
	}
	var left []gfc5SoftG
	if err := q.Unscoped().Model(&gfc5SoftG{}).Order("id").Find(&left); err != nil {
		t.Fatalf("ForceDelete 后 Unscoped Find: %v", err)
	}
	if len(left) != 2 {
		t.Fatalf("ForceDelete 后 Unscoped 应剩 2 行, 实际 %+v", left)
	}
	for _, r := range left {
		if r.ID == "sg2" {
			t.Errorf("ForceDelete 后 sg2 不应再出现（物理删除不可恢复）: %+v", r)
		}
	}
	// 已软删行 sg1 同样可被 ForceDelete 真删
	if err := q.Model(&gfc5SoftG{}).Where("id = ?", "sg1").ForceDelete(&gfc5SoftG{}); err != nil {
		t.Fatalf("ForceDelete 已软删行 sg1: %v", err)
	}
	if err := q.Model(&gfc5SoftG{}).Where("id = ?", "sg3").ForceDelete(&gfc5SoftG{}); err != nil {
		t.Fatalf("ForceDelete sg3: %v", err)
	}
	if n := gfc5CountRaw(t, drv, "gfc5_softgs"); n != 0 {
		t.Errorf("全部 ForceDelete 后表应空, 实际 %d", n)
	}
}

// ── 软删 + 关联 / 唯一键 / Save（SD-06/07/08，双形态各跑一轮）──────────

func runGfc5SoftRel(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()

	t.Run("SD-06_软删关联预加载", func(t *testing.T) { runGfc5SoftRelPreload(t, drv, dialect) })
	t.Run("SD-07_软删占唯一键", func(t *testing.T) { runGfc5SoftRelUnique(t, drv, dialect) })
	t.Run("SD-08_Save命中软删行", func(t *testing.T) { runGfc5SoftRelSave(t, drv, dialect) })
}

// runGfc5SoftRelPreload（SD-06）父行软删后子表 Preload 行为 + 子行软删过滤。
func runGfc5SoftRelPreload(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	now := time.Now().Unix()
	q := drv.Query()

	// ── 形态一（业务级）：gorm 原生 Preload 不识别普通 deleted_at 列 ──
	intgMigrate(t, drv, []string{"gfc5_soft_users", "gfc5_soft_orders"}, &gfc5SoftUser{}, &gfc5SoftOrder{})
	for _, s := range []any{
		&gfc5SoftUser{ID: "pa", Name: "alive-parent"},
		&gfc5SoftUser{ID: "pb", Name: "trashed-parent"},
		&gfc5SoftOrder{ID: "po1", UserID: "pa", Amount: 100},
		&gfc5SoftOrder{ID: "po4", UserID: "pa", Amount: 999},
		&gfc5SoftOrder{ID: "po2", UserID: "pb", Amount: 50},
	} {
		if err := q.Create(s); err != nil {
			t.Fatalf("种子 %T %+v: %v", s, s, err)
		}
	}
	// 业务级软删标记：子行 po4、父行 pb
	if err := q.Model(&gfc5SoftOrder{}).Where("id = ?", "po4").Update("deleted_at", now); err != nil {
		t.Fatalf("业务软删 po4: %v", err)
	}
	if err := q.Model(&gfc5SoftUser{}).Where("id = ?", "pb").Update("deleted_at", now); err != nil {
		t.Fatalf("业务软删 pb: %v", err)
	}

	// ① 普通查询 + Preload：父行不过滤（软删父行 pb 仍可见）、子行不过滤（po4 回填）
	// ——差异固化：业务级形态一切过滤均需业务显式表达。
	var rowsB []gfc5SoftUser
	if err := q.Model(&gfc5SoftUser{}).Order("id").Preload("Orders").Find(&rowsB); err != nil {
		t.Fatalf("形态一 Preload Find: %v", err)
	}
	if len(rowsB) != 2 {
		t.Errorf("形态一普通查询应返回 2 父行（软删 pb 不过滤——差异固化）, 实际 %d: %+v", len(rowsB), rowsB)
	}
	for _, u := range rowsB {
		switch u.ID {
		case "pa":
			if len(u.Orders) != 2 {
				t.Errorf("形态一 pa 应回填 2 订单（软删 po4 不过滤——差异固化）, 实际 %+v", u.Orders)
			}
		case "pb":
			if len(u.Orders) != 1 {
				t.Errorf("形态一 pb 应回填 1 订单, 实际 %+v", u.Orders)
			}
		}
	}
	// ② 业务 scope 父行过滤 + Preload conds 过滤软删子行（对齐 drivertest 套件口径）
	var rowsB2 []gfc5SoftUser
	if err := q.Model(&gfc5SoftUser{}).Where("deleted_at = ?", 0).Order("id").
		Preload("Orders", "deleted_at = ?", 0).Find(&rowsB2); err != nil {
		t.Fatalf("形态一 scope + conds Preload: %v", err)
	}
	if len(rowsB2) != 1 || rowsB2[0].ID != "pa" {
		t.Errorf("形态一业务 scope 父行应仅 [pa], 实际 %+v", rowsB2)
	} else if len(rowsB2[0].Orders) != 1 || rowsB2[0].Orders[0].ID != "po1" {
		t.Errorf("形态一 Preload conds 应过滤软删子行（期望仅 [po1]）, 实际 %+v", rowsB2[0].Orders)
	}

	// ── 形态二（gorm 原生）：父行/子行软删均被自动 IS NULL 过滤 ──
	intgMigrate(t, drv, []string{"gfc5_softg_users", "gfc5_softg_orders"}, &gfc5SoftGUser{}, &gfc5SoftGOrder{})
	for _, s := range []any{
		&gfc5SoftGUser{ID: "ga", Name: "alive-parent"},
		&gfc5SoftGUser{ID: "gb", Name: "trashed-parent"},
		&gfc5SoftGOrder{ID: "go1", UserID: "ga", Amount: 100},
		&gfc5SoftGOrder{ID: "go4", UserID: "ga", Amount: 999},
		&gfc5SoftGOrder{ID: "go2", UserID: "gb", Amount: 50},
	} {
		if err := q.Create(s); err != nil {
			t.Fatalf("种子 %T %+v: %v", s, s, err)
		}
	}
	if err := q.Model(&gfc5SoftGOrder{}).Where("id = ?", "go4").Delete(&gfc5SoftGOrder{}); err != nil {
		t.Fatalf("软删 go4: %v", err)
	}
	if err := q.Model(&gfc5SoftGUser{}).Where("id = ?", "gb").Delete(&gfc5SoftGUser{}); err != nil {
		t.Fatalf("软删 gb: %v", err)
	}

	// ① 父行软删自动过滤：普通 Find 仅剩 ga（差异固化 vs 形态一不过滤）
	var rowsG []gfc5SoftGUser
	if err := q.Model(&gfc5SoftGUser{}).Order("id").Preload("Orders").Find(&rowsG); err != nil {
		t.Fatalf("形态二 Preload Find: %v", err)
	}
	if len(rowsG) != 1 || rowsG[0].ID != "ga" {
		t.Fatalf("形态二软删父行 gb 应被自动过滤（差异固化）, 实际 %+v", rowsG)
	}
	// ② 子行软删过滤：gorm 原生 Preload 自动排除已删子行 go4
	if len(rowsG[0].Orders) != 1 || rowsG[0].Orders[0].ID != "go1" {
		t.Errorf("形态二 Preload 应自动过滤软删子行（期望仅 [go1]）, 实际 %+v", rowsG[0].Orders)
	}
}

// runGfc5SoftRelUnique（SD-07）软删行仍占唯一键——重复插入仍 ErrDuplicatedKey
// （业务坑位固化，双形态各一轮）。
func runGfc5SoftRelUnique(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	q := drv.Query()

	// 形态一：业务级软删（Update 置 deleted_at），物理行仍在 → 唯一键仍占位
	intgMigrate(t, drv, []string{"gfc5_uniques"}, &gfc5Unique{})
	if err := q.Create(&gfc5Unique{ID: "uq1", Email: "dup@gofast.dev"}); err != nil {
		t.Fatalf("形态一首次插入: %v", err)
	}
	if err := q.Model(&gfc5Unique{}).Where("id = ?", "uq1").
		Update("deleted_at", time.Now().Unix()); err != nil {
		t.Fatalf("形态一业务软删: %v", err)
	}
	err := q.Create(&gfc5Unique{ID: "uq2", Email: "dup@gofast.dev"})
	if err == nil || !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("形态一软删行仍应占唯一键（重复插入 ErrDuplicatedKey——业务坑位固化）, 实际: %v", err)
	}

	// 形态二：gorm 原生软删（Delete 写 timestamp），物理行仍在 → 唯一键仍占位
	intgMigrate(t, drv, []string{"gfc5_uniquegs"}, &gfc5UniqueG{})
	if err := q.Create(&gfc5UniqueG{ID: "ug1", Email: "dup@gofast.dev"}); err != nil {
		t.Fatalf("形态二首次插入: %v", err)
	}
	if err := q.Model(&gfc5UniqueG{}).Where("id = ?", "ug1").Delete(&gfc5UniqueG{}); err != nil {
		t.Fatalf("形态二软删: %v", err)
	}
	err = q.Create(&gfc5UniqueG{ID: "ug2", Email: "dup@gofast.dev"})
	if err == nil || !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("形态二软删行仍应占唯一键（gorm 无 partial unique 自动处理）, 实际: %v", err)
	}
}

// runGfc5SoftRelSave（SD-08）Save 命中已软删行的行为（实测源码推导后如实固化）。
func runGfc5SoftRelSave(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	q := drv.Query()

	// 形态一（业务级）：Save 全字段 UPDATE 直接命中物理行（无软删 scope），
	// deleted_at 一并被零值覆盖 → 行"复活"。
	intgMigrate(t, drv, []string{"gfc5_softs"}, &gfc5Soft{})
	if err := q.Create(&gfc5Soft{ID: "s8", Name: "before"}); err != nil {
		t.Fatalf("形态一 Create: %v", err)
	}
	if err := q.Model(&gfc5Soft{}).Where("id = ?", "s8").
		Update("deleted_at", time.Now().Unix()); err != nil {
		t.Fatalf("形态一业务软删: %v", err)
	}
	if err := q.Save(&gfc5Soft{ID: "s8", Name: "resurrected", DeletedAt: 0}); err != nil {
		t.Fatalf("形态一 Save 命中软删行不应报错: %v", err)
	}
	if n := gfc5CountRaw(t, drv, "gfc5_softs"); n != 1 {
		t.Errorf("形态一 Save 应命中原行（无新增）, 原生计数应 1, 实际 %d", n)
	}
	var backB []gfc5Soft
	if err := q.Model(&gfc5Soft{}).Where("deleted_at = ?", 0).Find(&backB); err != nil {
		t.Fatalf("形态一 Save 后回读: %v", err)
	}
	if len(backB) != 1 || backB[0].Name != "resurrected" {
		// 差异固化：Save 全字段覆盖将软删标记清零 → 软删行被复活
		t.Errorf("形态一 Save 应复活软删行（全字段覆盖 deleted_at=0）, 实际 %+v", backB)
	}

	// 形态二（gorm 原生）：Save 的全字段 UPDATE 带 deleted_at IS NULL scope
	// 命中 0 行 → gorm 内置 0 行回落 upsert（PG ON CONFLICT / MySQL ON
	// DUPLICATE KEY UPDATE ALL）→ 行复活且 deleted_at 置 NULL。
	intgMigrate(t, drv, []string{"gfc5_softgs"}, &gfc5SoftG{})
	if err := q.Create(&gfc5SoftG{ID: "g8", Name: "before"}); err != nil {
		t.Fatalf("形态二 Create: %v", err)
	}
	if err := q.Model(&gfc5SoftG{}).Where("id = ?", "g8").Delete(&gfc5SoftG{}); err != nil {
		t.Fatalf("形态二软删: %v", err)
	}
	if err := q.Save(&gfc5SoftG{ID: "g8", Name: "resurrected"}); err != nil {
		// 差异固化：gorm Save 0 行回落 upsert 路径预期不报错
		t.Fatalf("形态二 Save 命中软删行（upsert 回落）不应报错: %v", err)
	}
	if n := gfc5CountRaw(t, drv, "gfc5_softgs"); n != 1 {
		t.Errorf("形态二 Save upsert 回落不应产生重复行, 原生计数应 1, 实际 %d", n)
	}
	var backG []gfc5SoftG
	if err := q.Model(&gfc5SoftG{}).Find(&backG); err != nil {
		t.Fatalf("形态二 Save 后回读: %v", err)
	}
	if len(backG) != 1 || backG[0].Name != "resurrected" || backG[0].DeletedAt.Valid {
		// 差异固化：upsert 回落将 deleted_at 置 NULL → 行复活且可见
		t.Errorf("形态二 Save 应经 upsert 回落复活软删行（deleted_at=NULL）, 实际 %+v", backG)
	}
}

// ── 悲观锁互斥（LK-01/02）──────────────────────────────────────────────

// runGfc5LockMutex FOR UPDATE 真实行锁互斥：事务一持锁改值，事务二同行加锁
// 被阻塞至超时报错；释放后第三事务读到新值。
//   - PG：SET LOCAL lock_timeout = '400ms'（事务级，随事务销毁）。
//   - MySQL：SET SESSION innodb_lock_wait_timeout = 1（会话级，事务绑定单连接
//     故随后语句同连接生效；回池后仅影响"锁等待 >1s"场景，正常读写不受影响）。
//
// 差异固化：xorm 侧因超时参数难以稳定控制未做 MySQL 互斥（普通 FOR UPDATE 不
// 报错代替），gorm 侧 clause.Locking 真实生成 FOR UPDATE，两方言均可全量断言。
func runGfc5LockMutex(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc5_locks"}, &gfc5Lock{})
	intgMustExec(t, drv, `INSERT INTO gfc5_locks (id, cnt) VALUES ('lock1', 1)`)

	// DryRun：SQL 生成 FOR UPDATE
	q := drv.Query().Model(&gfc5Lock{}).Lock(contracts.LockForUpdate)
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]gfc5Lock{}).Statement
	intgAssertContains(t, "FOR UPDATE DryRun SQL", stmt.SQL.String(), "FOR UPDATE")

	// 事务一：FOR UPDATE 锁行并持锁，持锁期间改值（goroutine 内不 t.Fatalf，
	// 释放等待超时以 error 返回防悬挂）
	locked := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- drv.Query().Transaction(func(tx contracts.Query) error {
			var row gfc5Lock
			if err := tx.Model(&gfc5Lock{}).Lock(contracts.LockForUpdate).First(&row, "id = ?", "lock1"); err != nil {
				return err
			}
			close(locked)
			select {
			case <-release:
			case <-time.After(30 * time.Second):
				return fmt.Errorf("等待事务一释放信号超时")
			}
			return tx.Model(&gfc5Lock{}).Where("id = ?", "lock1").UpdateResult("cnt", 99).Error
		})
	}()
	gfc5WaitCh(t, locked, "事务一持锁")

	// 事务二：同行 FOR UPDATE 被互斥——方言超时参数观察阻塞
	var err error
	if dialect == "pg" {
		err = drv.Query().Transaction(func(tx contracts.Query) error {
			if e := tx.Exec("SET LOCAL lock_timeout = '400ms'"); e != nil {
				return e
			}
			var row gfc5Lock
			return tx.Model(&gfc5Lock{}).Lock(contracts.LockForUpdate).First(&row, "id = ?", "lock1")
		})
		if err == nil || !strings.Contains(err.Error(), "lock timeout") {
			t.Fatalf("PG 第二事务应在 lock_timeout 内报锁超时（FOR UPDATE 互斥）, 实际: %v", err)
		}
	} else {
		err = drv.Query().Transaction(func(tx contracts.Query) error {
			if e := tx.Exec("SET SESSION innodb_lock_wait_timeout = 1"); e != nil {
				return e
			}
			var row gfc5Lock
			return tx.Model(&gfc5Lock{}).Lock(contracts.LockForUpdate).First(&row, "id = ?", "lock1")
		})
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "lock wait timeout") {
			t.Fatalf("MySQL 第二事务应在 innodb_lock_wait_timeout 内报锁等待超时（FOR UPDATE 互斥）, 实际: %v", err)
		}
	}

	// 释放后：事务一提交，第三事务可获锁并读到新值
	close(release)
	if err := gfc5WaitErrCh(t, errCh, "事务一结果"); err != nil {
		t.Fatalf("事务一: %v", err)
	}
	var after gfc5Lock
	if err := drv.Query().Transaction(func(tx contracts.Query) error {
		return tx.Model(&gfc5Lock{}).Lock(contracts.LockForUpdate).First(&after, "id = ?", "lock1")
	}); err != nil {
		t.Fatalf("释放后 FOR UPDATE 应成功: %v", err)
	}
	if after.Cnt != 99 {
		t.Errorf("持锁事务写入应已提交, 期望 cnt=99, 实际 %d", after.Cnt)
	}
}

// ── FOR SHARE（LK-03）──────────────────────────────────────────────────

// runGfc5LockShare gorm FOR SHARE 真实执行（差异固化：xorm 侧 LockShareMode 为
// 文档化 no-op，gorm 侧 clause.Locking{Strength: "SHARE"} 真实生成 FOR SHARE）：
//   - DryRun 断言 SQL 含 FOR SHARE；
//   - 两事务同时 FOR SHARE 可并发（S 锁兼容）；
//   - 持 S 锁期间他事务 UPDATE 同行被阻塞至超时 → 证明锁真实生效。
func runGfc5LockShare(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc5_locks"}, &gfc5Lock{})
	intgMustExec(t, drv, `INSERT INTO gfc5_locks (id, cnt) VALUES ('lock1', 7)`)

	// DryRun：SQL 生成 FOR SHARE
	q := drv.Query().Model(&gfc5Lock{}).Lock(contracts.LockShareMode)
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]gfc5Lock{}).Statement
	intgAssertContains(t, "FOR SHARE DryRun SQL", stmt.SQL.String(), "FOR SHARE")

	// 事务一：FOR SHARE 持锁（goroutine 内不 t.Fatalf）
	locked := make(chan struct{})
	release := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- drv.Query().Transaction(func(tx contracts.Query) error {
			var row gfc5Lock
			if err := tx.Model(&gfc5Lock{}).Lock(contracts.LockShareMode).First(&row, "id = ?", "lock1"); err != nil {
				return err
			}
			close(locked)
			select {
			case <-release:
			case <-time.After(30 * time.Second):
				return fmt.Errorf("等待 FOR SHARE 持锁事务释放信号超时")
			}
			return nil
		})
	}()
	gfc5WaitCh(t, locked, "FOR SHARE 事务一持锁")

	// 并发 FOR SHARE：另一事务同查询应成功（S 锁相互兼容 → 真执行而非 no-op 的旁证）
	var share2 gfc5Lock
	if err := drv.Query().Transaction(func(tx contracts.Query) error {
		return tx.Model(&gfc5Lock{}).Lock(contracts.LockShareMode).First(&share2, "id = ?", "lock1")
	}); err != nil {
		t.Fatalf("并发 FOR SHARE 应成功（S 锁兼容）: %v", err)
	}
	if share2.Cnt != 7 {
		t.Errorf("并发 FOR SHARE 读值异常: %+v", share2)
	}

	// 持 S 锁期间：他事务 UPDATE 同行被阻塞至超时 → FOR SHARE 锁真实生效
	var err error
	if dialect == "pg" {
		err = drv.Query().Transaction(func(tx contracts.Query) error {
			if e := tx.Exec("SET LOCAL lock_timeout = '400ms'"); e != nil {
				return e
			}
			return tx.Model(&gfc5Lock{}).Where("id = ?", "lock1").Update("cnt", 77)
		})
		if err == nil || !strings.Contains(err.Error(), "lock timeout") {
			t.Errorf("PG 持 S 锁期间 UPDATE 应在 lock_timeout 内报错, 实际: %v", err)
		}
	} else {
		err = drv.Query().Transaction(func(tx contracts.Query) error {
			if e := tx.Exec("SET SESSION innodb_lock_wait_timeout = 1"); e != nil {
				return e
			}
			return tx.Model(&gfc5Lock{}).Where("id = ?", "lock1").Update("cnt", 77)
		})
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "lock wait timeout") {
			t.Errorf("MySQL 持 S 锁期间 UPDATE 应在锁等待超时内报错, 实际: %v", err)
		}
	}

	// 释放后：事务一提交，被阻塞事务的更新已回滚，数据不变
	close(release)
	if err := gfc5WaitErrCh(t, errCh, "FOR SHARE 事务一结果"); err != nil {
		t.Fatalf("FOR SHARE 事务一: %v", err)
	}
	var final gfc5Lock
	if err := drv.Query().Model(&gfc5Lock{}).Where("id = ?", "lock1").First(&final); err != nil {
		t.Fatalf("释放后回读: %v", err)
	}
	if final.Cnt != 7 {
		t.Errorf("被阻塞事务的更新应已回滚, 期望 cnt=7, 实际 %d", final.Cnt)
	}
}

// ── Joins 功能断言（Q-08）──────────────────────────────────────────────

// runGfc5Joins 缺省 INNER / LEFT 显式 / 带参 ON / 自连接（别名）/ 非法串报错
// （差异固化：gorm 原样透传 → 数据库语法错误，而非 xorm 的 ErrUnsupported）/
// PG 下 schema 前缀关联表（gorm Joins 串不自动加前缀，需调用方拼接）。
func runGfc5Joins(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	// seedGfc5JoinTables 重建 gfc5_users/gfc5_orders 并灌入三用户三订单
	seedGfc5JoinTables := func() {
		t.Helper()
		intgMigrate(t, drv, []string{"gfc5_users", "gfc5_orders"}, &gfc5User{}, &gfc5Order{})
		q := drv.Query()
		for _, s := range []any{
			&gfc5User{ID: "u1", Name: "alice", Cnt: 1},
			&gfc5User{ID: "u2", Name: "bob", Cnt: 2},
			&gfc5User{ID: "u3", Name: "carol", Cnt: 2}, // 无订单：LEFT JOIN 补 NULL
			&gfc5Order{ID: "o1", UserID: "u1", Amount: 100},
			&gfc5Order{ID: "o2", UserID: "u1", Amount: 250},
			&gfc5Order{ID: "o3", UserID: "u2", Amount: 50},
		} {
			if err := q.Create(s); err != nil {
				t.Fatalf("种子 %T: %v", s, err)
			}
		}
	}
	// joinRows 执行联表 ScanMap：uid/amount 可能因方言以 string/int64 出现，
	// 比较一律经 gfc5MapNum 归一
	joinRows := func(chain contracts.Query) []map[string]any {
		t.Helper()
		var dest []map[string]any
		if err := chain.ScanMap(&dest); err != nil {
			t.Fatalf("联表 ScanMap: %v", err)
		}
		return dest
	}
	groupByUser := func(rows []map[string]any) map[string][]int64 {
		out := make(map[string][]int64, len(rows))
		for _, m := range rows {
			uid := gfc5MapStr(m["uid"])
			out[uid] = append(out[uid], gfc5MapNum(m["amount"]))
		}
		return out
	}

	seedGfc5JoinTables()

	// ① 缺省 INNER JOIN（无前缀关键字）：u1 两单(100/250)、u2 一单(50)、u3 无行
	inner := groupByUser(joinRows(drv.Query().Table("gfc5_users").
		Select("gfc5_users.id AS uid", "gfc5_orders.amount").
		Joins("JOIN gfc5_orders ON gfc5_orders.user_id = gfc5_users.id").
		Order("gfc5_users.id")))
	if _, dup := inner["u3"]; dup {
		t.Errorf("缺省 INNER JOIN 不应含无订单用户 u3, 实际 %v", inner)
	}
	if !gfc5HasAll(inner["u1"], 100, 250) || !gfc5HasAll(inner["u2"], 50) {
		t.Errorf("缺省 INNER JOIN 结果异常: %v", inner)
	}

	// ② LEFT JOIN：u3 无订单 → 该行存在且 amount 为 NULL/零值形态
	left := groupByUser(joinRows(drv.Query().Table("gfc5_users").
		Select("gfc5_users.id AS uid", "gfc5_orders.amount").
		Joins("LEFT JOIN gfc5_orders ON gfc5_orders.user_id = gfc5_users.id").
		Order("gfc5_users.id")))
	if !gfc5HasAll(left["u1"], 100, 250) || !gfc5HasAll(left["u2"], 50) {
		t.Errorf("LEFT JOIN 金额回读异常: %v", left)
	}
	{
		var u3row map[string]any
		for _, m := range joinRows(drv.Query().Table("gfc5_users").
			Select("gfc5_users.id AS uid", "gfc5_orders.amount").
			Joins("LEFT JOIN gfc5_orders ON gfc5_orders.user_id = gfc5_users.id").
			Where("gfc5_users.id = ?", "u3")) {
			u3row = m
		}
		if u3row == nil {
			t.Error("LEFT JOIN 应包含无订单用户 u3 行")
		} else if !gfc5NilLike(u3row["amount"]) {
			t.Errorf("u3 无订单 amount 应为 NULL/零值形态, 实际 %#v", u3row["amount"])
		}
	}

	// ③ JOIN 条件带 args 占位符（amount >= 100 → 命中 u1 的 100/250 两单）
	argRows := groupByUser(joinRows(drv.Query().Table("gfc5_users").
		Select("gfc5_users.id AS uid", "gfc5_orders.amount").
		Joins("INNER JOIN gfc5_orders ON gfc5_orders.user_id = gfc5_users.id AND gfc5_orders.amount >= ?", 100).
		Order("gfc5_users.id")))
	if !gfc5HasAll(argRows["u1"], 100, 250) || len(argRows["u2"]) != 0 {
		t.Errorf("带参 JOIN 应命中 u1 两单(>=100), 实际 %v", argRows)
	}

	// ④ 自连接（别名）：同 cnt 不同 id 的 peer 对（u2↔u3，cnt 均为 2）
	selfIDs := make(map[string]bool)
	for _, m := range joinRows(drv.Query().Table("gfc5_users peer").
		Select("u.id AS uid").
		Joins("JOIN gfc5_users u ON u.cnt = peer.cnt AND u.id != peer.id")) {
		selfIDs[gfc5MapStr(m["uid"])] = true
	}
	if !selfIDs["u2"] || !selfIDs["u3"] || len(selfIDs) != 2 {
		t.Errorf("自连接应恰命中 peer 对 uid {u2, u3}（u2↔u3）, 实际 %v", selfIDs)
	}

	// ⑤ 非法 JOIN 串——差异固化：gorm 的 Joins 为原样透传，非法串由数据库报
	// 语法错误（PG 42601 / MySQL 1064），不映射 contracts.ErrUnsupported
	//（xorm 侧同类输入报 ErrUnsupported）。
	var bogus []map[string]any
	err := drv.Query().Table("gfc5_users").
		Joins("BOGUS JOIN gfc5_orders ON gfc5_orders.user_id = gfc5_users.id").ScanMap(&bogus)
	if err == nil {
		t.Error("非法 JOIN 串应被数据库拒绝")
	} else if errors.Is(err, contracts.ErrUnsupported) {
		t.Errorf("非法 JOIN 串应透传数据库错误而非 ErrUnsupported（差异固化）, 实际: %v", err)
	} else {
		t.Logf("非法 JOIN 串报数据库错误（差异固化）: %v", err)
	}

	// ⑥ PG：Schema(ten) 下关联表——gorm Joins 串原样透传不自动加 schema 前缀，
	// 关联表需调用方显式拼接（经 GetSchema()），主表前缀由 schemaTable 生成。
	if dialect == "pg" {
		ten := "gfc5_jten"
		intgMustExec(t, drv, "DROP SCHEMA IF EXISTS "+ten+" CASCADE")
		intgMustExec(t, drv, "CREATE SCHEMA "+ten)
		t.Cleanup(func() { _ = drv.Query().Exec("DROP SCHEMA IF EXISTS " + ten + " CASCADE") })
		for _, ddl := range []string{
			`CREATE TABLE ` + ten + `.gfc5_users (id varchar(16) PRIMARY KEY, name varchar(64), cnt bigint NOT NULL DEFAULT 0)`,
			`CREATE TABLE ` + ten + `.gfc5_orders (id varchar(16) PRIMARY KEY, user_id varchar(16), amount bigint)`,
			`INSERT INTO ` + ten + `.gfc5_users VALUES ('ju1', 'tenant-alice', 1)`,
			`INSERT INTO ` + ten + `.gfc5_users VALUES ('ju2', 'tenant-bob', 2)`,
			`INSERT INTO ` + ten + `.gfc5_orders VALUES ('jo1', 'ju1', 100)`,
			`INSERT INTO ` + ten + `.gfc5_orders VALUES ('jo2', 'ju2', 50)`,
		} {
			intgMustExec(t, drv, ddl)
		}
		full := drv.Query().Schema(ten)
		tenRows := groupByUser(joinRows(full.Table("gfc5_users").
			Select("gfc5_users.id AS uid", "gfc5_orders.amount").
			Joins("JOIN " + full.GetSchema() + ".gfc5_orders ON " + full.GetSchema() + ".gfc5_orders.user_id = " + full.GetSchema() + ".gfc5_users.id").
			Order("gfc5_users.id")))
		if !gfc5HasAll(tenRows["ju1"], 100) || !gfc5HasAll(tenRows["ju2"], 50) {
			t.Errorf("schema JOIN 应命中租户两表, 实际 %v", tenRows)
		}
	} else {
		t.Log("MySQL 无 schema 概念：schema 前缀关联表场景跳过（PG 专属）")
	}
}

// ── 预加载 + 事务联动（PL-02/PL-04/TX-07）──────────────────────────────

func runGfc5PreloadTx(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	intgMigrate(t, drv, []string{"gfc5_pre_users", "gfc5_pre_orders"}, &gfc5PreUser{}, &gfc5PreOrder{})

	q := drv.Query()
	for _, s := range []any{
		&gfc5PreUser{ID: "u1", Name: "alice"},
		&gfc5PreUser{ID: "u2", Name: "bob"},
		&gfc5PreUser{ID: "u3", Name: "carol"}, // 无订单
		&gfc5PreOrder{ID: "o1", UserID: "u1", Amount: 100},
		&gfc5PreOrder{ID: "o2", UserID: "u1", Amount: 250},
		&gfc5PreOrder{ID: "o3", UserID: "u2", Amount: 50},
	} {
		if err := q.Create(s); err != nil {
			t.Fatalf("种子 %T %+v: %v", s, s, err)
		}
	}

	t.Run("PL-02_conds与callback", func(t *testing.T) {
		// 原生路径 conds 过滤：u1 仅回填 amount>100 的 [o2]
		var rows []gfc5PreUser
		if err := q.Model(&gfc5PreUser{}).Order("id").Preload("Orders", "amount > ?", 100).Find(&rows); err != nil {
			t.Fatalf("原生 conds Preload: %v", err)
		}
		if len(rows) != 3 {
			t.Fatalf("应查到 3 用户, 实际 %d", len(rows))
		}
		if len(rows[0].Orders) != 1 || rows[0].Orders[0].ID != "o2" {
			t.Errorf("原生 conds 应仅回填 [o2], 实际 %+v", rows[0].Orders)
		}
		if len(rows[1].Orders) != 0 || rows[2].Orders == nil || len(rows[2].Orders) != 0 {
			t.Errorf("无命中子行的用户应回填空集合, 实际 u2=%+v u3=%+v", rows[1].Orders, rows[2].Orders)
		}

		// 共享引擎路径 conds 过滤：语义一致（同表同数据，dest 换引擎形态类型）
		var eRows []gfc5EngUser
		if err := q.Model(&gfc5EngUser{}).Order("id").Preload("Orders", "amount > ?", 100).Find(&eRows); err != nil {
			t.Fatalf("引擎 conds Preload: %v", err)
		}
		if len(eRows[0].Orders) != 1 || eRows[0].Orders[0].ID != "o2" {
			t.Errorf("引擎 conds 应仅回填 [o2], 实际 %+v", eRows[0].Orders)
		}

		// 共享引擎路径 callback 排序 + 限量：引擎契约 3——conds/callbacks 作用于
		// 本层【批量 IN 子查询】（全局生效），故 Limit(1) 是全局 top1：
		// u1 命中全局最大 [o2(250)]，u2 分不到配额（[]）——与 xorm 同引擎一致。
		var eRows2 []gfc5EngUser
		if err := q.Model(&gfc5EngUser{}).Order("id").Preload("Orders",
			func(cq contracts.Query) contracts.Query {
				return cq.Order("amount DESC").Limit(1)
			}).Find(&eRows2); err != nil {
			t.Fatalf("引擎 callback Preload: %v", err)
		}
		if len(eRows2[0].Orders) != 1 || eRows2[0].Orders[0].Amount != 250 {
			t.Errorf("引擎 callback 全局 top1 期望 u1=[o2(250)], 实际 %+v", eRows2[0].Orders)
		}
		if len(eRows2[1].Orders) != 0 {
			t.Errorf("引擎 callback Limit 全局语义下 u2 应为空（配额被全局 top1 占用）, 实际 %+v", eRows2[1].Orders)
		}

		// 差异固化（疑似驱动缺口）：contracts 形态的 Preload callback 仅共享引擎
		// 路径支持——gorm 原生路径未做 func(contracts.Query) → func(*gorm.DB) 转
		// 换，回调被当作条件值透传（生成 "id IN (func)" 类条件）→ 执行报错。
		// 记录现状不改驱动；若未来驱动补转换，此断言应改为双路径行为一致。
		var nRows []gfc5PreUser
		err := q.Model(&gfc5PreUser{}).Order("id").Preload("Orders",
			func(cq contracts.Query) contracts.Query { return cq.Order("amount DESC") }).Find(&nRows)
		if err == nil {
			t.Errorf("contracts 形态 callback 在 gorm 原生路径应报错（驱动未转换回调形态——差异固化）, 实际成功: %+v", nRows)
		} else {
			t.Logf("原生路径 contracts callback 报错（差异固化，疑似驱动缺口）: %v", err)
		}
	})

	t.Run("PL-04_FindInBatches逐批预加载", func(t *testing.T) {
		// 追加 u4/u5（每用户 1 单）→ 5 用户按批大小 2 分 3 批
		for _, s := range []any{
			&gfc5PreUser{ID: "u4", Name: "dave"},
			&gfc5PreUser{ID: "u5", Name: "eve"},
			&gfc5PreOrder{ID: "o4", UserID: "u4", Amount: 400},
			&gfc5PreOrder{ID: "o5", UserID: "u5", Amount: 500},
		} {
			if err := q.Create(s); err != nil {
				t.Fatalf("种子 %T: %v", s, err)
			}
		}

		// 原生路径：gorm 每批 Find 均执行预加载，回调内 dest 即当前批（已回填）
		// 种子订单归属：u1→[o1,o2]（2 单）、u2→[o3]、u3→无、u4→[o4]、u5→[o5]
		wantOrders := map[string]int{"u1": 2, "u2": 1, "u3": 0, "u4": 1, "u5": 1}
		var batches []int
		var gotUsers []gfc5PreUser
		dest := &[]gfc5PreUser{}
		if err := q.Model(&gfc5PreUser{}).Order("id").Preload("Orders").
			FindInBatches(dest, 2, func(tx contracts.Query, batch int) error {
				batches = append(batches, batch)
				cp := make([]gfc5PreUser, len(*dest))
				copy(cp, *dest)
				gotUsers = append(gotUsers, cp...)
				return nil
			}); err != nil {
			t.Fatalf("原生 FindInBatches 逐批预加载: %v", err)
		}
		if fmt.Sprint(batches) != "[1 2 3]" {
			t.Errorf("5 用户批大小 2 应分 3 批, 实际 %v", batches)
		}
		if len(gotUsers) != 5 {
			t.Fatalf("逐批累计应 5 用户, 实际 %d", len(gotUsers))
		}
		for _, u := range gotUsers {
			if len(u.Orders) != wantOrders[u.ID] {
				t.Errorf("用户 %s 应逐批预加载 %d 订单, 实际 %d: %+v", u.ID, wantOrders[u.ID], len(u.Orders), u.Orders)
			}
		}

		// 共享引擎路径：驱动 FindInBatches 包装每批执行 runEnginePreloads
		var eBatches []int
		var eGot []gfc5EngUser
		eDest := &[]gfc5EngUser{}
		if err := q.Model(&gfc5EngUser{}).Order("id").Preload("Orders").
			FindInBatches(eDest, 2, func(tx contracts.Query, batch int) error {
				eBatches = append(eBatches, batch)
				cp := make([]gfc5EngUser, len(*eDest))
				copy(cp, *eDest)
				for _, u := range cp {
					if len(u.Orders) != wantOrders[u.ID] {
						return fmt.Errorf("批次 %d 引擎用户 %s 应预加载 %d 单, 实际 %d: %+v",
							batch, u.ID, wantOrders[u.ID], len(u.Orders), u.Orders)
					}
				}
				eGot = append(eGot, cp...)
				return nil
			}); err != nil {
			t.Fatalf("引擎 FindInBatches 逐批预加载: %v", err)
		}
		if fmt.Sprint(eBatches) != "[1 2 3]" || len(eGot) != 5 {
			t.Fatalf("引擎路径应 3 批累计 5 用户, 实际批 %v 用户 %d", eBatches, len(eGot))
		}
	})

	t.Run("TX-07_事务内Preload与缓存失效", func(t *testing.T) {
		// ① 事务内 Preload 正常回填
		var txRows []gfc5PreUser
		if err := drv.Query().Transaction(func(tx contracts.Query) error {
			return tx.Model(&gfc5PreUser{}).Preload("Orders").Where("id = ?", "u1").Find(&txRows)
		}); err != nil {
			t.Fatalf("事务内 Preload: %v", err)
		}
		if len(txRows) != 1 || len(txRows[0].Orders) != 2 {
			t.Errorf("事务内 Preload 应回填 u1 的 2 订单, 实际 %+v", txRows)
		}

		// ② 事务内写对外部查询缓存的失效时机：写语句执行时即失效（而非延迟到
		// 提交后）——若失效不及时，提交后的缓存查询将命中旧值 alice 而失败。
		// （内存 Cache 配合断言；xorm 侧同款场景依赖 countCacheStore 计数，
		//   gorm 侧 testCache 为纯内存 store，以数据新旧互相印证。）
		if err := drv.EnableCaches(newTestCache()); err != nil {
			t.Fatalf("EnableCaches: %v", err)
		}
		cachedAlice := func() []gfc5PreUser {
			t.Helper()
			var rows []gfc5PreUser
			if err := drv.Query().Cache().Model(&gfc5PreUser{}).Where("id = ?", "u1").Find(&rows); err != nil {
				t.Fatalf("缓存查询失败: %v", err)
			}
			return rows
		}

		// 首次：回源（name=alice）并写入缓存
		if first := cachedAlice(); len(first) != 1 || first[0].Name != "alice" {
			t.Fatalf("首次缓存查询应回源返回 alice, 实际 %+v", first)
		}
		// 事务内更新（写回调在 UPDATE 执行时同步失效查询缓存），提交
		if err := drv.Query().Transaction(func(tx contracts.Query) error {
			return tx.Model(&gfc5PreUser{}).Where("id = ?", "u1").Update("name", "cache-flushed")
		}); err != nil {
			t.Fatalf("事务内更新: %v", err)
		}
		// 提交后：缓存应已失效并回源新值
		if second := cachedAlice(); len(second) != 1 || second[0].Name != "cache-flushed" {
			t.Errorf("事务内写应使外部查询缓存失效（回源返回 cache-flushed）, 实际 %+v", second)
		}
	})
}
