//go:build integration

package gormdriver

// model_pk_integration_test.go —— X-09 全表更新事故的 gorm 侧基准对照矩阵
// （与 gofast-xorm/model_pk_integration_test.go 逐组对齐，防 gorm 升级回归）。
// gorm 的 Model(&bean) 主键并入 WHERE 是 gorm 原生行为，本文件把它逐形态
// 锁定为基准：任何 gorm 升级破坏该行为时此处即刻红灯。
//
// 运行方式（docker-compose.yml 环境）：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -run 'GormModelPK' -v .
//
// 覆盖场景（对齐 xorm 侧 7 组 + PG 租户 schema 第三入口）：
//   - 事故同款路径：First 回填主键 → Model(&bean).Updates(map) 无显式 Where
//   - Update(col, val) 单列、UpdatesResult/UpdateResult 变体 RowsAffected
//   - Updates(struct)：零值字段跳过语义下仍只收敛主键行
//   - 表达式 UPDATE（X-08 通道）：Update Expr / Updates 混合 map + 链上 Where AND
//   - Model 主键与链上显式 Where 取 AND 交集（不交集 → 0 行不改数据）
//   - 复合主键：双列全非零 → 双列条件；仅一列非零 → 只收敛该列
//   - 零主键现状锁定：范围由链上 Where 决定；零主键且无 Where → gorm 原生
//     ErrMissingWhereClause 防线拦截（见 runGormModelPKMatrix 第七组注释）
//   - PG 专属：schema-per-tenant（Schema + Model(&bean).Updates），
//     即 stitch-mes 事故发生的真实部署形态，public 同名表不得串扰
//
// 零主键场景的 gorm 实测结论（gorm@v1.31.1 源码逐行确认，两个真库入口均按此断言）：
//   - callbacks/update.go ConvertToAssignments：Model(&bean) 主键字段非零时逐列
//     AddClause(clause.Where{Eq})，与链上 Where 同入 WHERE 子句（AND 交集）；
//   - callbacks/helper.go checkMissingWhereConditions：无任何 WHERE 条件时
//     AddError(gorm.ErrMissingWhereClause)（"WHERE conditions required"），
//     且 Update 回调在错误非 nil 时跳过 Exec——即零主键 + 无 Where 的更新
//     返回错误、0 行受影响、数据不变。gorm 原生防线使 X-09 式全表更新
//     在 gorm 侧无法发生；本文件将其固化为升级回归红灯。

import (
	"errors"
	"strings"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"gorm.io/gorm"
)

// ── 测试模型 ─────────────────────────────────────────────────────────

// gxpkUser 单主键模型（gorm tag primaryKey；TableName 固定裸表名，避免
// NamingStrategy 推导差异——同时是租户 schema 前缀兜底路径的前提）。
type gxpkUser struct {
	ID   string `gorm:"column:id;primaryKey;size:16"`
	Name string `gorm:"column:name;size:64"`
	Cnt  int64  `gorm:"column:cnt"`
}

func (gxpkUser) TableName() string { return "gxpk_users" }

// gxpkPair 复合主键模型：验证部分非零主键的收敛行为。
type gxpkPair struct {
	TenantID string `gorm:"column:tenant_id;primaryKey;size:16"`
	UserID   string `gorm:"column:user_id;primaryKey;size:16"`
	Name     string `gorm:"column:name;size:64"`
}

func (gxpkPair) TableName() string { return "gxpk_pairs" }

// ── 共用基建 ─────────────────────────────────────────────────────────

// gxpkReadUser 按主键回读单行（链上显式 Where，不经 Model 主键路径），
// 零值 Model 不附加主键条件，回读只由显式 Where 决定。
func gxpkReadUser(t *testing.T, q contracts.Query, id string) gxpkUser {
	t.Helper()
	var row gxpkUser
	if err := q.Model(&gxpkUser{}).Where("id = ?", id).First(&row); err != nil {
		t.Fatalf("回读 %q: %v", id, err)
	}
	return row
}

// gxpkSeedUsers 重建表并灌入三行基线数据（a=10 / b=20 / c=30）。
func gxpkSeedUsers(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gxpk_users"}, &gxpkUser{})
	q := drv.Query()
	for _, u := range []*gxpkUser{
		{ID: "a", Name: "alice", Cnt: 10},
		{ID: "b", Name: "bob", Cnt: 20},
		{ID: "c", Name: "carol", Cnt: 30},
	} {
		if err := q.Create(u); err != nil {
			t.Fatalf("种子 %s: %v", u.ID, err)
		}
	}
}

// runGormModelPKMatrix PG/MySQL 共用的 X-09 行为矩阵（gorm 侧基准对照）。
func runGormModelPKMatrix(t *testing.T, drv *GormDriver) {
	t.Helper()

	t.Run("事故路径_First回填主键_Updates只改一行", func(t *testing.T) {
		gxpkSeedUsers(t, drv)
		q := drv.Query()

		// 事故报告同款写法：First 填充主键 → Model(&user).Updates(map)，链上无 Where。
		// gorm 原生行为：Model bean 主键非零 → WHERE id = 'a' 自动并入，只改 a 行。
		var bean gxpkUser
		if err := q.First(&bean, "id = ?", "a"); err != nil {
			t.Fatalf("First: %v", err)
		}
		if bean.ID != "a" {
			t.Fatalf("First 应回填主键 a, 实际 %q", bean.ID)
		}
		if err := q.Model(&bean).Updates(map[string]any{"name": "诶嘿"}); err != nil {
			t.Fatalf("Updates: %v", err)
		}
		// 目标行被改
		if got := gxpkReadUser(t, q, "a"); got.Name != "诶嘿" || got.Cnt != 10 {
			t.Errorf("目标行 a 期望 诶嘿/cnt=10, 实际 %q/%d", got.Name, got.Cnt)
		}
		// 非目标行逐字段断言未被波及（全表更新回归检测的核心口径）
		if got := gxpkReadUser(t, q, "b"); got.Name != "bob" || got.Cnt != 20 {
			t.Errorf("非目标行 b 被改写（全表更新回归）: %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "c"); got.Name != "carol" || got.Cnt != 30 {
			t.Errorf("非目标行 c 被改写（全表更新回归）: %q/%d", got.Name, got.Cnt)
		}
	})

	t.Run("单列Update与Result变体", func(t *testing.T) {
		gxpkSeedUsers(t, drv)
		q := drv.Query()

		// 单列 Update：主键收敛只改 b
		if err := q.Model(&gxpkUser{ID: "b"}).Update("name", "b-single"); err != nil {
			t.Fatalf("Update: %v", err)
		}
		if got := gxpkReadUser(t, q, "b"); got.Name != "b-single" || got.Cnt != 20 {
			t.Errorf("目标行 b 期望 b-single/cnt=20, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "a"); got.Name != "alice" || got.Cnt != 10 {
			t.Errorf("非目标行 a 被改写（全表更新回归）: %q/%d", got.Name, got.Cnt)
		}

		// UpdatesResult：主键命中 1 行
		res := q.Model(&gxpkUser{ID: "b"}).UpdatesResult(map[string]any{"name": "b2"})
		if res.Error != nil || res.RowsAffected != 1 {
			t.Errorf("UpdatesResult 期望 1 行, 实际 RowsAffected=%d Error=%v", res.RowsAffected, res.Error)
		}
		// 主键未命中：0 行且无错（gorm 对 UPDATE 不命中不报 ErrRecordNotFound）
		res = q.Model(&gxpkUser{ID: "missing"}).UpdateResult("name", "x")
		if res.Error != nil || !res.IsZeroRow() {
			t.Errorf("主键未命中期望 0 行无错, 实际 RowsAffected=%d Error=%v", res.RowsAffected, res.Error)
		}
		if got := gxpkReadUser(t, q, "a"); got.Name != "alice" || got.Cnt != 10 {
			t.Errorf("主键未命中不应波及任何行: a 实际 %q/%d", got.Name, got.Cnt)
		}
	})

	t.Run("Updates结构体路径", func(t *testing.T) {
		gxpkSeedUsers(t, drv)
		q := drv.Query()

		// struct 更新：零值字段（Cnt=0）跳过 + 主键条件仍来自 Model bean，只收敛 c 行
		bean := gxpkUser{ID: "c"}
		if err := q.Model(&bean).Updates(gxpkUser{Name: "c-struct"}); err != nil {
			t.Fatalf("Updates(struct): %v", err)
		}
		if got := gxpkReadUser(t, q, "c"); got.Name != "c-struct" || got.Cnt != 30 {
			t.Errorf("目标行 c 期望 c-struct/cnt=30（零值字段跳过不改 cnt）, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "a"); got.Name != "alice" || got.Cnt != 10 {
			t.Errorf("非目标行 a 被改写（全表更新回归）: %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "b"); got.Name != "bob" || got.Cnt != 20 {
			t.Errorf("非目标行 b 被改写（全表更新回归）: %q/%d", got.Name, got.Cnt)
		}
	})

	t.Run("表达式UPDATE通道", func(t *testing.T) {
		gxpkSeedUsers(t, drv)
		q := drv.Query()

		// Update 单列表达式（X-08）：只命中主键行，数据库端原子计算
		if err := q.Model(&gxpkUser{ID: "b"}).Update("cnt", contracts.Expr("cnt + ?", 5)); err != nil {
			t.Fatalf("Update 表达式: %v", err)
		}
		if got := gxpkReadUser(t, q, "b"); got.Cnt != 25 || got.Name != "bob" {
			t.Errorf("目标行 b 期望 cnt=25/name=bob, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "a"); got.Cnt != 10 || got.Name != "alice" {
			t.Errorf("非目标行 a 被表达式全表更新（回归）: %q/%d", got.Name, got.Cnt)
		}

		// Updates 混合表达式 map：主键与链上 Where 叠加（AND 交集）仍只作用 c 行
		if err := q.Model(&gxpkUser{ID: "c"}).Where("cnt > ?", 0).
			Updates(map[string]any{"cnt": contracts.Expr("cnt * ?", 2), "name": "c-expr"}); err != nil {
			t.Fatalf("Updates 混合表达式: %v", err)
		}
		if got := gxpkReadUser(t, q, "c"); got.Cnt != 60 || got.Name != "c-expr" {
			t.Errorf("目标行 c 期望 cnt=60/name=c-expr, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "a"); got.Cnt != 10 || got.Name != "alice" {
			t.Errorf("非目标行 a 被表达式全表更新（回归）: %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "b"); got.Cnt != 25 || got.Name != "bob" {
			t.Errorf("AND 交集不应外溢至 b 行, 实际 %q/%d", got.Name, got.Cnt)
		}
	})

	t.Run("主键与显式Where取AND交集", func(t *testing.T) {
		gxpkSeedUsers(t, drv)
		q := drv.Query()

		// 主键 a AND name 不匹配 → 交集空 → 0 行不改数据且无错
		res := q.Model(&gxpkUser{ID: "a"}).Where("name = ?", "no-such").
			UpdatesResult(map[string]any{"name": "x"})
		if res.Error != nil || !res.IsZeroRow() {
			t.Errorf("交集为空期望 0 行无错, 实际 RowsAffected=%d Error=%v", res.RowsAffected, res.Error)
		}
		if got := gxpkReadUser(t, q, "a"); got.Name != "alice" || got.Cnt != 10 {
			t.Errorf("a 不应被改写, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "b"); got.Name != "bob" || got.Cnt != 20 {
			t.Errorf("交集空更新不应波及 b 行, 实际 %q/%d", got.Name, got.Cnt)
		}
	})

	t.Run("复合主键", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gxpk_pairs"}, &gxpkPair{})
		q := drv.Query()
		for _, p := range []*gxpkPair{
			{TenantID: "t1", UserID: "u1", Name: "t1u1"},
			{TenantID: "t1", UserID: "u2", Name: "t1u2"},
			{TenantID: "t2", UserID: "u1", Name: "t2u1"},
		} {
			if err := q.Create(p); err != nil {
				t.Fatalf("种子 %+v: %v", p, err)
			}
		}

		// 双列非零：双列等值条件 AND → 精确命中单行
		if err := q.Model(&gxpkPair{TenantID: "t1", UserID: "u1"}).
			Updates(map[string]any{"name": "hit"}); err != nil {
			t.Fatalf("复合主键 Updates: %v", err)
		}
		var rows []gxpkPair
		if err := q.Model(&gxpkPair{}).Where("name = ?", "hit").Find(&rows); err != nil {
			t.Fatalf("回读 hit: %v", err)
		}
		if len(rows) != 1 || rows[0].TenantID != "t1" || rows[0].UserID != "u1" {
			t.Errorf("复合主键应只命中 t1/u1, 实际 %+v", rows)
		}
		var t1u2 gxpkPair
		if err := q.Model(&gxpkPair{}).Where("tenant_id = ? AND user_id = ?", "t1", "u2").Take(&t1u2); err != nil {
			t.Fatalf("回读 t1/u2: %v", err)
		}
		if t1u2.Name != "t1u2" {
			t.Errorf("复合主键更新不应波及 t1/u2, 实际 %q", t1u2.Name)
		}

		// 仅一列非零：只收敛该列（tenant_id='t2' 下全部行，与 xorm 行为对齐）
		if err := q.Model(&gxpkPair{TenantID: "t2"}).
			Updates(map[string]any{"name": "t2-all"}); err != nil {
			t.Fatalf("部分主键 Updates: %v", err)
		}
		var t2rows []gxpkPair
		if err := q.Model(&gxpkPair{}).Where("name = ?", "t2-all").Find(&t2rows); err != nil {
			t.Fatalf("回读 t2-all: %v", err)
		}
		if len(t2rows) != 1 || t2rows[0].TenantID != "t2" || t2rows[0].UserID != "u1" {
			t.Errorf("部分主键应只收敛 t2 的 1 行, 实际 %+v", t2rows)
		}
		// t1 下两行不受部分主键更新波及
		var t1rows []gxpkPair
		if err := q.Model(&gxpkPair{}).Where("tenant_id = ?", "t1").Find(&t1rows); err != nil {
			t.Fatalf("回读 t1: %v", err)
		}
		if len(t1rows) != 2 || t1rows[0].Name == "t2-all" || t1rows[1].Name == "t2-all" {
			t.Errorf("部分主键更新不应波及 t1 行, 实际 %+v", t1rows)
		}
	})

	t.Run("零主键现状_范围由链上Where决定", func(t *testing.T) {
		gxpkSeedUsers(t, drv)
		q := drv.Query()

		// 零主键 + 链上 Where：gorm 不附加主键条件，范围完全由 Where 决定，只改 a 行
		if err := q.Model(&gxpkUser{}).Where("id = ?", "a").
			Updates(map[string]any{"name": "a-only"}); err != nil {
			t.Fatalf("零主键+Where Updates: %v", err)
		}
		if got := gxpkReadUser(t, q, "a"); got.Name != "a-only" || got.Cnt != 10 {
			t.Errorf("零主键+Where 应只改 a 行, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "b"); got.Name != "bob" || got.Cnt != 20 {
			t.Errorf("零主键+Where 不应波及 b 行, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "c"); got.Name != "carol" || got.Cnt != 30 {
			t.Errorf("零主键+Where 不应波及 c 行, 实际 %q/%d", got.Name, got.Cnt)
		}

		// 零主键 + 无任何 Where：gorm 原生防线（X-09 的关键基准点）。
		// 实测行为（gorm@v1.31.1 callbacks/helper.go checkMissingWhereConditions +
		// callbacks/update.go）：不报 ErrMissingWhereClause 之外的错——确切语义是
		// AddError(gorm.ErrMissingWhereClause)（"WHERE conditions required"）后跳过
		// Exec：返回错误、RowsAffected=0、数据不变。gorm 侧不存在"零主键静默全表
		// 更新"路径；若 gorm 升级放行该形态（AllowGlobalUpdate 语义变化），下列
		// 断言即刻红灯。
		err := q.Model(&gxpkUser{}).Updates(map[string]any{"name": "全表??"})
		if err == nil {
			t.Fatal("零主键且无 Where 的 Updates 应被 gorm ErrMissingWhereClause 拦截, 实际无错（疑似全表更新回归）")
		}
		if !errors.Is(err, gorm.ErrMissingWhereClause) && !strings.Contains(err.Error(), "WHERE conditions required") {
			t.Errorf("零主键无 Where 应报 ErrMissingWhereClause（WHERE conditions required）, 实际: %v", err)
		}
		// 0 行受影响：三行数据逐字段不变
		if got := gxpkReadUser(t, q, "a"); got.Name != "a-only" || got.Cnt != 10 {
			t.Errorf("被拦截的更新不应改动 a 行, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "b"); got.Name != "bob" || got.Cnt != 20 {
			t.Errorf("被拦截的更新不应改动 b 行（全表更新回归）, 实际 %q/%d", got.Name, got.Cnt)
		}
		if got := gxpkReadUser(t, q, "c"); got.Name != "carol" || got.Cnt != 30 {
			t.Errorf("被拦截的更新不应改动 c 行（全表更新回归）, 实际 %q/%d", got.Name, got.Cnt)
		}

		// Result 变体同一防线：Error 非空 + 0 行
		res := q.Model(&gxpkUser{}).UpdatesResult(map[string]any{"name": "全表??"})
		if res.Error == nil || res.RowsAffected != 0 {
			t.Errorf("零主键无 Where UpdatesResult 期望 Error!=nil 且 0 行, 实际 rows=%d err=%v", res.RowsAffected, res.Error)
		}
		if res.Error == nil || res.IsZeroRow() {
			t.Errorf("被拦截的更新不应判为 IsZeroRow（那是'成功 0 命中'语义）, 实际 err=%v", res.Error)
		}
	})
}

// ── PG 入口：public schema 矩阵 + schema-per-tenant 事故形态 ─────────

func TestGormModelPK_PG(t *testing.T) {
	runGormModelPKMatrix(t, newIntgPG(t))
}

// TestGormModelPK_PGTenantSchema stitch-mes 事故的真实部署形态：
// schema-per-tenant 下 Schema(ten).First 回填 + Model(&bean).Updates 必须
// 带租户 schema 前缀且只命中 bean 主键行；public 下的同名表数据不受影响
// （gorm 侧 Model() 在 schema 链上经 Statement.Parse 解析裸表名后显式拼
// "tenant." 前缀；模型实现 TableName() 时由 applySchema 兜底，先例见
// pg_integration_test.go 的 TablePrefix "public." 用法）。
func TestGormModelPK_PGTenantSchema(t *testing.T) {
	drv := newIntgPG(t)
	ten := "tenant_gxpk"

	intgMustExec(t, drv, "DROP SCHEMA IF EXISTS "+ten+" CASCADE")
	intgMustExec(t, drv, "CREATE SCHEMA "+ten)
	t.Cleanup(func() { _ = drv.Query().Exec("DROP SCHEMA IF EXISTS " + ten + " CASCADE") })
	intgDropTables(t, drv, "public.gxpk_users")

	// 租户表与 public 同名表各灌数据：验证互不串扰（裸 DDL，先例为
	// pg_integration_test.go 租户测试的 schema 内建表写法）
	intgMustExec(t, drv, `CREATE TABLE `+ten+`.gxpk_users (id varchar(16) PRIMARY KEY, name varchar(64), cnt bigint)`)
	intgMustExec(t, drv, `CREATE TABLE public.gxpk_users (id varchar(16) PRIMARY KEY, name varchar(64), cnt bigint)`)
	intgMustExec(t, drv, `INSERT INTO `+ten+`.gxpk_users VALUES ('a','t-alice',10),('b','t-bob',20)`)
	intgMustExec(t, drv, `INSERT INTO public.gxpk_users VALUES ('a','p-alice',1)`)

	// 事故场景还原：租户 schema 下 First 回填 → Model(&bean).Updates 无显式 Where
	q := drv.Query().Schema(ten)
	var bean gxpkUser
	if err := q.First(&bean, "id = ?", "a"); err != nil {
		t.Fatalf("租户 First: %v", err)
	}
	if bean.Name != "t-alice" {
		t.Fatalf("First 应命中租户表, 实际 %q", bean.Name)
	}
	if err := q.Model(&bean).Updates(map[string]any{"name": "租户改名"}); err != nil {
		t.Fatalf("租户 Updates: %v", err)
	}

	// 租户内：只改 a 行，b 行逐字段不变
	if got := gxpkReadUser(t, q, "a"); got.Name != "租户改名" || got.Cnt != 10 {
		t.Errorf("租户目标行期望 租户改名/cnt=10, 实际 %q/%d", got.Name, got.Cnt)
	}
	if got := gxpkReadUser(t, q, "b"); got.Name != "t-bob" || got.Cnt != 20 {
		t.Errorf("租户非目标行被改写（全表更新回归）: %q/%d", got.Name, got.Cnt)
	}
	// public 同名表不受租户更新波及（逐字段）
	var pub gxpkUser
	if err := drv.Query().First(&pub, "id = ?", "a"); err != nil {
		t.Fatalf("public 回读: %v", err)
	}
	if pub.Name != "p-alice" || pub.Cnt != 1 {
		t.Errorf("public 同名表不应被租户更新波及, 实际 %q/%d", pub.Name, pub.Cnt)
	}
}

// ── MySQL 入口 ───────────────────────────────────────────────────────

func TestGormModelPK_MySQL(t *testing.T) {
	runGormModelPKMatrix(t, newIntgMySQL(t))
}
