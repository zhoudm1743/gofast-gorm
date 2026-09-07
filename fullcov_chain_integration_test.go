//go:build integration

package gormdriver

// fullcov_chain_integration_test.go —— 分组 1：contracts.Query 链式条件构建方法
// （Table / Model / Select / Omit / Where / OrWhere / Not / Order / Limit /
// Offset / Group / Having / Distinct / Paginate + 聚合投影）真实库全覆盖集成
// 测试，对齐 gofast-xorm 侧同名文件 fullcov_chain_integration_test.go 的用例
// 矩阵（fc1c_ → gfc1c_ 前缀）。
//
// 语义基准（全部经 gorm v1.31.1 源码走读 + PG/MySQL 真库探针逐条核实）：
//   - Where/OrWhere/Not 支持 string+args / clause.Expression（gorm 侧 builder
//     类型，对应 xorm 的 builder.Cond）/ map[string]any 三种条件类型；
//   - IN ? / IN (?) 切片展开、空切片 → IN (NULL) 恒假、NOT IN、前置普通占位符
//     参数不错位、双 IN，与 xorm 语义一致；
//   - Order+Count 剥离 ORDER BY（无 GROUP BY 时，gorm finisher 内建行为，
//     PG 42803 回归）；Group+Count 返回分组数（gorm 以 RowsAffected 覆盖）；
//   - Paginate 经 utils.PageUtil.Normalize 归一（page<1→1、size<1→20）。
//
// 差异固化条目（相对 xorm 侧同名矩阵，断言处均带注释）：
//   1. 非法条件类型不上报 ErrUnsupported：gormdriver.Where/OrWhere/Not 直接
//      透传 gorm.BuildCondition，非法标量/切片被解释为「主键等值/IN」条件——
//      MySQL 数值强转静默过滤（0 行/全量），PG pgx 无法把 float/int 编码为
//      text 主键参数而报底层编码错误（均非 contracts.ErrUnsupported）；
//   2. OrWhere 三条件混合优先级：Where(a).OrWhere(b).Where(c) 在 gorm 渲染为
//      a OR b AND c（SQL 优先级 = a OR (b AND c)），xorm 为 (a OR b) AND c，
//      数据可分辨（u1 是否命中）；
//   3. 连续 Order 叠加而非覆盖：gorm clause.OrderBy MergeClause 追加列，
//      Order("age").Order("id DESC") = ORDER BY age, id DESC（xorm 以最后一次
//      为准）；Order(非 string) 静默忽略不报错；
//   4. 仅 Offset 无兜底：gorm 渲染裸 OFFSET，PG 可用、MySQL 1064 语法错误
//      （xorm 以大 LIMIT 兜底两方言可用）；Limit(0) 渲染 LIMIT 0 返回 0 行
//      （xorm 视为未设置）；Limit(负)/Offset(0/负) 不渲染子句 = 全量；
//   5. Select 非法变参悬空绑定：Select("id", 42) 静默把 42 渲染为未消费绑定
//      参数，执行期报 "expected 0 arguments, got 1"（xorm 链上即报
//      ErrUnsupported）；Select(非 string) gorm AddError 原始错误透出；
//   6. Model 非法入参：Model(42) 报 gorm "unsupported data type" 原始错误；
//      Model(nil) 不报错、回退按 dest 解析表名返回全量（xorm 均为链上
//      ErrUnsupported）；
//   7. 链式派生别名：gorm 仅在会话首个链式方法处克隆（Query() 经
//      Session(NewDB) 获 clone=2 会话），从已用链再派生会与源链共享底层
//      Statement 互相污染（xorm 每次链式调用克隆，派生互不影响）——测试以
//      「会话根分叉互不影响」锁定可用口径，并固化深分叉别名的真实行为；
//   8. dest 内联主键条件（Q-11）：Find（切片 dest）/Exists（Count 路径）
//      不收窄；First 的内联已由驱动层 one() 屏蔽（零值副本承接，双驱动一致
//      不收窄）；FirstOrCreate 传入同模型类型且主键非零的结构体 dest 时仍按
//      gorm 原生收窄：未命中回落 Create 撞重复主键 ErrDuplicatedKey
//      （xorm 侧按链条件命中回填不新建——差异固化保留）；
//   9. Having 带参占位符：gorm 完整支持（xorm Session.Having 仅收 string，
//      链上报 ErrUnsupported）——AGG-02 差异固化双断言的 gorm 正向侧。
//
// 运行（docker-compose.yml 环境）：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	  go test -tags integration -run 'TestFullCovChain_PG' -v .
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -run 'TestFullCovChain_MySQL' -v .

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ── 测试模型（表名固定 gfc1c_ 前缀，gorm column tag，照 gfc3User 风格）────

// gfc1cUser 主表模型：id 主键 + 全列数据，供投影/条件/排序/分页断言。
type gfc1cUser struct {
	ID    string `gorm:"column:id;primaryKey;size:32"`
	Name  string `gorm:"column:name;size:64"`
	Age   int    `gorm:"column:age"`
	City  string `gorm:"column:city;size:32"`
	Email string `gorm:"column:email;size:64"`
	Score int64  `gorm:"column:score"`
}

func (gfc1cUser) TableName() string { return "gfc1c_users" }

// gfc1cOrder 分组聚合表：category 分组、amount 聚合。
type gfc1cOrder struct {
	ID       string `gorm:"column:id;primaryKey;size:32"`
	Category string `gorm:"column:category;size:16"`
	Amount   int64  `gorm:"column:amount"`
}

func (gfc1cOrder) TableName() string { return "gfc1c_orders" }

// gfc1cProj 投影结构体 dest：派生表名 gfc1c_user_projs 故意不存在——
// 若 dest 推导表名覆盖了显式 Table()，查询将因 relation 不存在而失败（Q-04 诱饵）。
type gfc1cProj struct {
	ID   string `gorm:"column:id"`
	Name string `gorm:"column:name"`
}

func (gfc1cProj) TableName() string { return "gfc1c_user_projs" }

// gfc1cAgg Group 聚合的投影结构体：列名与 Select 别名一一对应
// （gorm Scan 对异型 dest 会重新解析 schema，字段按列名回填）。
type gfc1cAgg struct {
	Category string `gorm:"column:category"`
	Cnt      int64  `gorm:"column:cnt"`
}

func (gfc1cAgg) TableName() string { return "gfc1c_aggs" }

// ── 种子数据 ─────────────────────────────────────────────────────────
//
// 固定 5 行用户（年龄/城市构造区分 AND/OR/NOT 语义的命中集合）：
//
//	u1 alice 20 beijing    u2 alice 21 beijing
//	u3 bob   30 shanghai   u4 carol 25 beijing
//	u5 bob   30 guangzhou
//
// 固定 6 行订单（3 个分组）：
//	a: o1/o2/o3（amount 10/20/30，共 3 行）
//	b: o4/o5（amount 5/15，共 2 行）
//	c: o6（amount 100，共 1 行）
//
// 全量聚合基准：count=6、sum=180、avg=30、max=100、min=5。

const (
	gfc1cUsersTable  = "gfc1c_users"
	gfc1cOrdersTable = "gfc1c_orders"
)

func gfc1cResetUsers(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{gfc1cUsersTable}, &gfc1cUser{})
	q := drv.Query()
	for _, u := range []gfc1cUser{
		{ID: "u1", Name: "alice", Age: 20, City: "beijing", Email: "u1@g.com", Score: 100},
		{ID: "u2", Name: "alice", Age: 21, City: "beijing", Email: "u2@g.com", Score: 200},
		{ID: "u3", Name: "bob", Age: 30, City: "shanghai", Email: "u3@g.com", Score: 300},
		{ID: "u4", Name: "carol", Age: 25, City: "beijing", Email: "u4@g.com", Score: 400},
		{ID: "u5", Name: "bob", Age: 30, City: "guangzhou", Email: "u5@g.com", Score: 500},
	} {
		tmp := u
		if err := q.Create(&tmp); err != nil {
			t.Fatalf("种子 %s: %v", u.ID, err)
		}
	}
}

func gfc1cSeedOrders(t *testing.T, drv *GormDriver) {
	t.Helper()
	q := drv.Query()
	for _, o := range []gfc1cOrder{
		{ID: "o1", Category: "a", Amount: 10},
		{ID: "o2", Category: "a", Amount: 20},
		{ID: "o3", Category: "a", Amount: 30},
		{ID: "o4", Category: "b", Amount: 5},
		{ID: "o5", Category: "b", Amount: 15},
		{ID: "o6", Category: "c", Amount: 100},
	} {
		tmp := o
		if err := q.Create(&tmp); err != nil {
			t.Fatalf("种子 %s: %v", o.ID, err)
		}
	}
}

func gfc1cResetOrders(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{gfc1cOrdersTable}, &gfc1cOrder{})
	gfc1cSeedOrders(t, drv)
}

// ── 断言与辅助（全部 gfc1c 前缀，仅复用 intg* 既有基建）──────────────

// gfc1cIDs 提取结果行 ID 列表（保留查询返回顺序，供排序断言逐位比对）。
func gfc1cIDs(rows []gfc1cUser) []string {
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	return ids
}

// gfc1cAssertOrdered 断言查询返回顺序与期望一致（用于显式 Order 链）。
func gfc1cAssertOrdered(t *testing.T, got []gfc1cUser, want ...string) {
	t.Helper()
	ids := gfc1cIDs(got)
	if len(ids) != len(want) {
		t.Fatalf("行数期望 %d（%v）, 实际 %d（%v）", len(want), want, len(ids), ids)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("第 %d 行期望 %s, 实际 %v（完整: %v）", i, want[i], ids, ids)
		}
	}
}

// gfc1cAssertSet 断言查询返回的行 ID 集合（顺序无关，剔除无 ORDER BY 的行序差异）。
func gfc1cAssertSet(t *testing.T, got []gfc1cUser, want ...string) {
	t.Helper()
	ids := gfc1cIDs(got)
	sort.Strings(ids)
	w := append([]string(nil), want...)
	sort.Strings(w)
	if len(ids) != len(w) {
		t.Fatalf("ID 集合期望 %v, 实际 %v", w, ids)
	}
	for i := range w {
		if ids[i] != w[i] {
			t.Fatalf("ID 集合期望 %v, 实际 %v", w, ids)
		}
	}
}

// gfc1cErrRawNonNil 断言「底层原始错误」路径：gormdriver 链上无类型校验，
// 非法入参以 gorm/database 层错误透出（差异固化），不得映射为 ErrUnsupported。
func gfc1cErrRawNonNil(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s 期望底层报错, 实际 nil", name)
		return
	}
	if errors.Is(err, contracts.ErrUnsupported) {
		t.Errorf("%s 差异固化: gorm 链上无校验, 不应映射 ErrUnsupported, 实际: %v", name, err)
	}
}

// gfc1cErrNotUnsupported 软断言：若报错则不得是 ErrUnsupported
// （方言间「静默 0 行 / 底层编码错误」两态均可接受的场景用）。
func gfc1cErrNotUnsupported(t *testing.T, name string, err error) {
	t.Helper()
	if err != nil && errors.Is(err, contracts.ErrUnsupported) {
		t.Errorf("%s 不应以 ErrUnsupported 透出, 实际: %v", name, err)
	}
}

// gfc1cDryFindStmt DryRun 观察链上最终 SQL 与绑定参数（不触库）。
func gfc1cDryFindStmt(t *testing.T, q contracts.Query, dest any) *gorm.Statement {
	t.Helper()
	gq, ok := q.(*GormQuery)
	if !ok {
		t.Fatalf("gfc1cDryFindStmt 仅适用于 *GormQuery")
	}
	tx := gq.db.Session(&gorm.Session{DryRun: true}).Find(dest)
	if tx.Error != nil {
		t.Fatalf("DryRun 构建 SQL 失败: %v", tx.Error)
	}
	return tx.Statement
}

// gfc1cAsInt64 把聚合 Scan/ScanMap 里的数值列归一为 int64：
// 两方言驱动返回形态不一（pgx 对 sum/avg 等给 string，MySQL 给 []byte/int64）。
func gfc1cAsInt64(t *testing.T, name string, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case nil:
		return 0
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case float64:
		return int64(n)
	case []byte:
		return gfc1cAsInt64(t, name, string(n))
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return int64(f)
		}
	}
	t.Fatalf("%s 无法归一为 int64: %T(%v)", name, v, v)
	return 0
}

// gfc1cAsFloat 把 avg 等小数聚合列归一为 float64（PG numeric → string、
// MySQL decimal → []byte、部分驱动直接 float64）。
func gfc1cAsFloat(t *testing.T, name string, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case nil:
		return 0
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case []byte:
		return gfc1cAsFloat(t, name, string(n))
	case string:
		if f, err := strconv.ParseFloat(strings.TrimSpace(n), 64); err == nil {
			return f
		}
	}
	t.Fatalf("%s 无法归一为 float64: %T(%v)", name, v, v)
	return 0
}

// ── 共享 runner（PG/MySQL 共用）──────────────────────────────────────

func runFullCovChain(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	q := drv.Query()

	t.Run("Table与Model显式表名", func(t *testing.T) {
		gfc1cResetUsers(t, drv)
		// 本组 Model+Table 优先级断言引用 orders 的 6 行基线，须一并准备
		gfc1cResetOrders(t, drv)

		// Table 裸表名读取：Find + Where + Order + Count
		var rows []gfc1cUser
		if err := q.Table(gfc1cUsersTable).Where("city = ?", "beijing").Order("id").Find(&rows); err != nil {
			t.Fatalf("Table Find: %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u1", "u2", "u4")
		var n int64
		if err := q.Table(gfc1cUsersTable).Count(&n); err != nil {
			t.Fatalf("Table Count: %v", err)
		}
		if n != 5 {
			t.Errorf("Table Count 期望 5, 实际 %d", n)
		}

		// Q-04：显式 Table 后投影结构体 dest 不覆盖表名——
		// gfc1cProj 派生表名 gfc1c_user_projs 不存在，若被覆盖将报 relation 不存在
		var projs []gfc1cProj
		if err := q.Table(gfc1cUsersTable).Select("id", "name").Where("id = ?", "u1").Find(&projs); err != nil {
			t.Fatalf("Table+投影 dest Find（应命中 gfc1c_users 而非 gfc1c_user_projs）: %v", err)
		}
		if len(projs) != 1 || projs[0].ID != "u1" || projs[0].Name != "alice" {
			t.Errorf("显式 Table + 投影 dest 应命中 users 数据, 实际 %+v", projs)
		}

		// Model 链上解析表名：读路径
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("city = ?", "beijing").Order("id").Find(&rows); err != nil {
			t.Fatalf("Model Find: %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u1", "u2", "u4")

		// 显式表名优先级：Model 之后再 Table，以 Table 为准
		//（DryRun SQL 固化 FROM 表名；Count 固化命中 orders 的 6 行而非 users 的 5 行）
		stmt := gfc1cDryFindStmt(t, q.Model(&gfc1cUser{}).Table(gfc1cOrdersTable), &[]gfc1cUser{})
		if !strings.Contains(stmt.SQL.String(), gfc1cOrdersTable) {
			t.Errorf("Model+Table 应以显式 Table 为准, SQL: %s", stmt.SQL.String())
		}
		if strings.Contains(stmt.SQL.String(), gfc1cUsersTable) {
			t.Errorf("Model+Table 不应回退 Model 表名, SQL: %s", stmt.SQL.String())
		}
		n = 0
		if err := q.Model(&gfc1cUser{}).Table(gfc1cOrdersTable).Count(&n); err != nil {
			t.Fatalf("Model+Table Count: %v", err)
		}
		if n != 6 {
			t.Errorf("Model+Table Count 期望 orders 的 6 行, 实际 %d", n)
		}

		// 差异固化：Model 非法入参（非 struct）——gorm 链上不报错，执行期报
		// gorm "unsupported data type" 原始错误（xorm 为链上 ErrUnsupported）
		var r []gfc1cUser
		gfc1cErrRawNonNil(t, "Model(int).Find", q.Model(42).Find(&r))

		// 差异固化：Model(nil) 不报错——gorm 回退按 dest 解析表名返回全量
		//（xorm 侧 Model(nil) 链上报 ErrUnsupported）
		rows = nil
		if err := q.Model(nil).Find(&rows); err != nil {
			t.Fatalf("Model(nil).Find 差异固化应不报错: %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u3", "u4", "u5")
	})

	t.Run("Where条件形态", func(t *testing.T) {
		gfc1cResetUsers(t, drv)

		// string+args 单条件
		var rows []gfc1cUser
		if err := q.Model(&gfc1cUser{}).Where("city = ?", "beijing").Find(&rows); err != nil {
			t.Fatalf("Where string: %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u4")

		// string+args 多条件链式：AND 语义
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("city = ?", "beijing").Where("age >= ?", 21).Find(&rows); err != nil {
			t.Fatalf("Where AND 链: %v", err)
		}
		gfc1cAssertSet(t, rows, "u2", "u4")

		// clause.Expression（gorm 侧 builder 类型）：Eq / Expr（带参表达式）
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where(clause.Eq{Column: "name", Value: "bob"}).Find(&rows); err != nil {
			t.Fatalf("Where clause.Eq: %v", err)
		}
		gfc1cAssertSet(t, rows, "u3", "u5")
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where(clause.Expr{SQL: "age >= ?", Vars: []any{30}}).Find(&rows); err != nil {
			t.Fatalf("Where clause.Expr: %v", err)
		}
		gfc1cAssertSet(t, rows, "u3", "u5")

		// map[string]any：多键等值 AND；键值不匹配 0 行（证明 AND 而非 OR）
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where(map[string]any{"city": "beijing", "name": "alice"}).Find(&rows); err != nil {
			t.Fatalf("Where map: %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2")
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where(map[string]any{"city": "beijing", "name": "bob"}).Find(&rows); err != nil {
			t.Fatalf("Where map 不匹配: %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("map 多键不匹配应 0 行, 实际 %v", gfc1cIDs(rows))
		}

		// 差异固化：非法标量（float）——gorm BuildCondition 把它解释为
		// 「主键 = 值」条件（DryRun 固化绑定参数），不做链上类型校验：
		// MySQL 数值强转静默 0 行；PG pgx 无法把 float 编码为 text 主键参数
		// 而报底层编码错误。两态均非 contracts.ErrUnsupported（xorm 为链上报错）。
		stmt := gfc1cDryFindStmt(t, q.Model(&gfc1cUser{}).Where(3.14), &[]gfc1cUser{})
		if len(stmt.Vars) != 1 {
			t.Fatalf("Where(float) 应渲染为主键等值单参数, vars=%v", stmt.Vars)
		}
		if fv, ok := stmt.Vars[0].(float64); !ok || fv != 3.14 {
			t.Errorf("Where(float) 差异固化应把 3.14 作为主键等值参数, vars=%v", stmt.Vars)
		}
		var r []gfc1cUser
		err := q.Model(&gfc1cUser{}).Where(3.14).Find(&r)
		gfc1cErrNotUnsupported(t, "Where(float).Find", err)
		if err == nil && len(r) != 0 {
			t.Errorf("Where(float) 按主键等值过滤应 0 行, 实际 %v", gfc1cIDs(r))
		}

		// 非法 map 类型（map[string]int 非 map[string]any）：两方言均在参数
		// 编码/绑定阶段报底层错误（差异固化，非 ErrUnsupported）
		var n int64
		gfc1cErrRawNonNil(t, "Where(map[string]int).Count", q.Model(&gfc1cUser{}).Where(map[string]int{"age": 20}).Count(&n))
	})

	t.Run("IN切片展开", func(t *testing.T) {
		gfc1cResetUsers(t, drv)

		// IN ? 切片展开
		var rows []gfc1cUser
		if err := q.Model(&gfc1cUser{}).Where("city IN ?", []string{"beijing", "shanghai"}).Find(&rows); err != nil {
			t.Fatalf("Where city IN ?: %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u3", "u4")

		// IN (?) 形态同样展开
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("city IN (?)", []string{"beijing"}).Find(&rows); err != nil {
			t.Fatalf("Where city IN (?): %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u4")

		// 空切片 → IN (NULL) 恒假：0 行不报错（IN ? 与 IN (?) 两种形态，
		// gorm 与 xorm 的 IN (NULL) 语义一致，无差异）
		for _, cond := range []string{"city IN ?", "city IN (?)"} {
			rows = nil
			if err := q.Model(&gfc1cUser{}).Where(cond, []string{}).Find(&rows); err != nil {
				t.Fatalf("空切片 %s: %v", cond, err)
			}
			if len(rows) != 0 {
				t.Errorf("空切片 %s 应 0 行, 实际 %d", cond, len(rows))
			}
		}

		// NOT IN ? 同样展开
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("id NOT IN ?", []string{"u1", "u5"}).Find(&rows); err != nil {
			t.Fatalf("Where NOT IN ?: %v", err)
		}
		gfc1cAssertSet(t, rows, "u2", "u3", "u4")

		// 普通占位符与 IN 混合：参数索引不错位（X-05 回归场景）
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("name = ? AND city IN ?", "alice", []string{"beijing", "shanghai"}).Find(&rows); err != nil {
			t.Fatalf("Where 混合占位符: %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2")

		// 连续两个 IN 占位符：逐位消费参数
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("city IN ? AND name IN ?", []string{"beijing"}, []string{"alice", "carol"}).Find(&rows); err != nil {
			t.Fatalf("Where 双 IN: %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u4")
	})

	t.Run("OrWhere与Not组合", func(t *testing.T) {
		gfc1cResetUsers(t, drv)

		// string × string：Where(a).OrWhere(b) = a OR b
		var rows []gfc1cUser
		if err := q.Model(&gfc1cUser{}).Where("city = ?", "beijing").OrWhere("city = ?", "shanghai").Find(&rows); err != nil {
			t.Fatalf("OrWhere 并集: %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u3", "u4")

		// 无前置 Where 的单独 OrWhere：展平为普通条件
		rows = nil
		if err := q.Model(&gfc1cUser{}).OrWhere("city = ?", "guangzhou").Find(&rows); err != nil {
			t.Fatalf("单独 OrWhere: %v", err)
		}
		gfc1cAssertSet(t, rows, "u5")

		// 三条件混合：差异固化——gorm 渲染 a OR b AND c（SQL 优先级
		// = a OR (b AND c)），xorm 侧为 (a OR b) AND c。数据可分辨：
		// gorm 语义下 beijing 全部命中（多出 u1）。
		rows = nil
		if err := q.Model(&gfc1cUser{}).
			Where("city = ?", "beijing").OrWhere("city = ?", "shanghai").Where("age >= ?", 21).
			Find(&rows); err != nil {
			t.Fatalf("三条件混合: %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u3", "u4")

		// map × OrWhere：Where(string).OrWhere(map) 并集
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("city = ?", "beijing").OrWhere(map[string]any{"name": "bob"}).Find(&rows); err != nil {
			t.Fatalf("OrWhere(map): %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u3", "u4", "u5")

		// clause.Expression × OrWhere：Where(Expr).OrWhere(Eq) 并集
		rows = nil
		if err := q.Model(&gfc1cUser{}).
			Where(clause.Expr{SQL: "age >= ?", Vars: []any{25}}).OrWhere(clause.Eq{Column: "name", Value: "alice"}).
			Find(&rows); err != nil {
			t.Fatalf("OrWhere(clause.Eq): %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u3", "u4", "u5")

		// OrWhere 与 IN 切片展开组合：展开先于 OR 落链
		rows = nil
		if err := q.Model(&gfc1cUser{}).
			Where("age < ?", 21).OrWhere("city IN ?", []string{"shanghai", "guangzhou"}).
			Find(&rows); err != nil {
			t.Fatalf("OrWhere(IN ?): %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u3", "u5")

		// Not(string) 单独使用：NOT (name = alice) → 排除 alice 两行
		rows = nil
		if err := q.Model(&gfc1cUser{}).Not("name = ?", "alice").Find(&rows); err != nil {
			t.Fatalf("Not(string): %v", err)
		}
		gfc1cAssertSet(t, rows, "u3", "u4", "u5")

		// Where 后接 Not(string)：AND NOT
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("age >= ?", 25).Not("name = ?", "bob").Find(&rows); err != nil {
			t.Fatalf("Where+Not: %v", err)
		}
		gfc1cAssertSet(t, rows, "u4")

		// Not(clause.Eq)：等值取反（gorm NegationBuild → name <> 'bob'）
		rows = nil
		if err := q.Model(&gfc1cUser{}).Not(clause.Eq{Column: "name", Value: "bob"}).Find(&rows); err != nil {
			t.Fatalf("Not(clause.Eq): %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u4")

		// Not(clause.Expr) 带参表达式：参数展开后取反
		rows = nil
		if err := q.Model(&gfc1cUser{}).Not(clause.Expr{SQL: "age < ?", Vars: []any{25}}).Find(&rows); err != nil {
			t.Fatalf("Not(clause.Expr): %v", err)
		}
		gfc1cAssertSet(t, rows, "u3", "u4", "u5")

		// Where 后接 Not(map)：AND NOT 等值
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("age >= ?", 25).Not(map[string]any{"city": "beijing"}).Find(&rows); err != nil {
			t.Fatalf("Where+Not(map): %v", err)
		}
		gfc1cAssertSet(t, rows, "u3", "u5")

		// Not(map) 单独使用
		rows = nil
		if err := q.Model(&gfc1cUser{}).Not(map[string]any{"city": "shanghai"}).Find(&rows); err != nil {
			t.Fatalf("Not(map): %v", err)
		}
		gfc1cAssertSet(t, rows, "u1", "u2", "u4", "u5")

		// Not 与 IN 切片展开组合：先展开为 NOT (id IN (?,?)) 再取反
		rows = nil
		if err := q.Model(&gfc1cUser{}).Not("id IN ?", []string{"u1", "u5"}).Find(&rows); err != nil {
			t.Fatalf("Not(IN ?): %v", err)
		}
		gfc1cAssertSet(t, rows, "u2", "u3", "u4")

		// 差异固化：非法标量进 OrWhere——gorm 解释为「OR id = 1.5」主键条件：
		// MySQL varchar 主键与 float 比较，各行 CAST 为 DOUBLE 均非 1.5 → 恒假
		// 静默返回空集；PG pgx 编码 float→text 主键参数失败报底层错误。
		// 均非 ErrUnsupported（xorm 链上即报）。
		rows = nil
		err := q.Model(&gfc1cUser{}).OrWhere(1.5).Find(&rows)
		gfc1cErrNotUnsupported(t, "OrWhere(float).Find", err)
		if dialect == "mysql" {
			if err != nil {
				t.Errorf("OrWhere(float) MySQL 差异固化应静默: %v", err)
			} else {
				gfc1cAssertSet(t, rows)
			}
		} else if err == nil {
			t.Errorf("OrWhere(float) PG 差异固化应报底层编码错误, 实际命中 %v", gfc1cIDs(rows))
		}

		// 差异固化：非法切片进 Not——gorm 解释为「NOT id = 1」：
		// MySQL 返回全量 5 行；PG 底层编码错误（非 ErrUnsupported）。
		rows = nil
		err = q.Model(&gfc1cUser{}).Not([]int{1}).Find(&rows)
		gfc1cErrNotUnsupported(t, "Not(slice).Find", err)
		if dialect == "mysql" {
			if err != nil {
				t.Errorf("Not(slice) MySQL 差异固化应静默: %v", err)
			} else {
				gfc1cAssertSet(t, rows, "u1", "u2", "u3", "u4", "u5")
			}
		} else if err == nil {
			t.Errorf("Not(slice) PG 差异固化应报底层编码错误, 实际命中 %v", gfc1cIDs(rows))
		}
	})

	t.Run("Select投影", func(t *testing.T) {
		gfc1cResetUsers(t, drv)

		// 单列：仅 name 被投影，其余字段零值
		var rows []gfc1cUser
		if err := q.Model(&gfc1cUser{}).Select("name").Find(&rows); err != nil {
			t.Fatalf("Select(name): %v", err)
		}
		if len(rows) != 5 {
			t.Fatalf("Select(name) 应 5 行, 实际 %d", len(rows))
		}
		for _, r := range rows {
			if r.Name == "" || r.ID != "" || r.City != "" || r.Email != "" || r.Score != 0 || r.Age != 0 {
				t.Errorf("Select(name) 投影不符（仅 name 应有值）: %+v", r)
			}
		}

		// 多列变参拼接：id+name 两列有值，其余零值
		rows = nil
		if err := q.Model(&gfc1cUser{}).Select("id", "name").Find(&rows); err != nil {
			t.Fatalf("Select(id,name): %v", err)
		}
		if len(rows) != 5 {
			t.Fatalf("Select(id,name) 应 5 行, 实际 %d", len(rows))
		}
		for _, r := range rows {
			if r.ID == "" || r.Name == "" || r.Email != "" || r.Score != 0 {
				t.Errorf("Select(id,name) 投影不符: %+v", r)
			}
		}

		// "*" no-op：回归默认全列（gormdriver 对 "*" 清空 Selects）
		rows = nil
		if err := q.Model(&gfc1cUser{}).Select("*").Where("id = ?", "u1").Find(&rows); err != nil {
			t.Fatalf("Select(*): %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("Select(*) 应 1 行, 实际 %d", len(rows))
		}
		for _, r := range rows {
			if r.ID != "u1" || r.Name == "" || r.Email == "" || r.Score == 0 || r.Age == 0 || r.City == "" {
				t.Errorf("Select(*) 应全列回填, 实际 %+v", r)
			}
		}

		// 非 string 主参：gorm Select AddError → 执行期原始错误透出
		//（差异固化：非 ErrUnsupported，xorm 为链上报错）
		var r4 []gfc1cUser
		gfc1cErrRawNonNil(t, "Select(int).Find", q.Model(&gfc1cUser{}).Select(123).Find(&r4))

		// 差异固化：Select("id", 42)——gorm 把非 string 变参渲染为未消费的
		// 悬空绑定参数，执行期报 "expected 0 arguments, got 1"
		//（xorm 侧 Select(string,int) 链上即报 ErrUnsupported）
		gfc1cErrRawNonNil(t, "Select(string,int).Find", q.Model(&gfc1cUser{}).Select("id", 42).Find(&r4))
	})

	t.Run("Omit与Distinct", func(t *testing.T) {
		gfc1cResetUsers(t, drv)

		// 单列排除：email 保持零值，其余列正常回填
		var rows []gfc1cUser
		if err := q.Model(&gfc1cUser{}).Omit("email").Where("id = ?", "u1").Find(&rows); err != nil {
			t.Fatalf("Omit(email): %v", err)
		}
		if len(rows) != 1 {
			t.Fatalf("Omit 应 1 行, 实际 %d", len(rows))
		}
		if rows[0].Email != "" {
			t.Errorf("Omit(email) 后 email 应为空, 实际 %q", rows[0].Email)
		}
		if rows[0].ID != "u1" || rows[0].Name == "" || rows[0].City == "" || rows[0].Score == 0 || rows[0].Age == 0 {
			t.Errorf("Omit(email) 后其余列应回填, 实际 %+v", rows[0])
		}

		// 多列排除 + 无 Where：全表仍 5 行，被排除列全部零值
		rows = nil
		if err := q.Model(&gfc1cUser{}).Omit("name", "email").Find(&rows); err != nil {
			t.Fatalf("Omit(name,email): %v", err)
		}
		if len(rows) != 5 {
			t.Fatalf("Omit 多列应 5 行, 实际 %d", len(rows))
		}
		for _, r := range rows {
			if r.Name != "" || r.Email != "" {
				t.Errorf("Omit(name,email) 后两列应为空, 实际 %+v", r)
			}
			if r.ID == "" || r.City == "" || r.Score == 0 {
				t.Errorf("Omit 后其余列应有值, 实际 %+v", r)
			}
		}

		// Distinct 单列：3 个不同 city
		rows = nil
		if err := q.Model(&gfc1cUser{}).Distinct("city").Find(&rows); err != nil {
			t.Fatalf("Distinct(city): %v", err)
		}
		if len(rows) != 3 {
			t.Errorf("Distinct(city) 期望 3 行, 实际 %d: %v", len(rows), gfc1cIDs(rows))
		}
		seen := map[string]bool{}
		for _, r := range rows {
			if r.City == "" {
				t.Errorf("Distinct 后 city 应有值: %+v", r)
			}
			if seen[r.City] {
				t.Errorf("Distinct(city) 出现重复 city %q", r.City)
			}
			seen[r.City] = true
		}

		// Distinct 多列（Q-05）：4 个不同 (city,name) 组合
		rows = nil
		if err := q.Model(&gfc1cUser{}).Distinct("city", "name").Find(&rows); err != nil {
			t.Fatalf("Distinct(city,name): %v", err)
		}
		if len(rows) != 4 {
			t.Errorf("Distinct(city,name) 期望 4 行, 实际 %d", len(rows))
		}
		pairs := map[[2]string]bool{}
		for _, r := range rows {
			k := [2]string{r.City, r.Name}
			if pairs[k] {
				t.Errorf("Distinct(city,name) 出现重复组合 %v", k)
			}
			pairs[k] = true
		}

		// 去重结果与 Where 组合仍生效
		rows = nil
		if err := q.Model(&gfc1cUser{}).Distinct("name").Where("age < ?", 25).Find(&rows); err != nil {
			t.Fatalf("Distinct+Where: %v", err)
		}
		if len(rows) != 1 || rows[0].Name != "alice" {
			t.Errorf("Distinct(name)+age<25 期望仅 alice, 实际 %+v", rows)
		}

		// 差异固化：Distinct(非 string) —— gorm Select AddError 原始错误透出
		//（非 ErrUnsupported；xorm 为链上报错）
		var r []gfc1cUser
		gfc1cErrRawNonNil(t, "Distinct(int).Find", q.Model(&gfc1cUser{}).Distinct(42).Find(&r))
	})

	t.Run("OrderLimitOffset", func(t *testing.T) {
		gfc1cResetUsers(t, drv)
		gfc1cResetOrders(t, drv)

		// 单字段升序（带 id 决胜，行序确定）
		var rows []gfc1cUser
		if err := q.Model(&gfc1cUser{}).Order("name ASC, id ASC").Find(&rows); err != nil {
			t.Fatalf("Order(name): %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u1", "u2", "u3", "u5", "u4")

		// 多字段（单次 Order 内逗号分隔）：age 降序 + id 升序决胜
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order("age DESC, id ASC").Find(&rows); err != nil {
			t.Fatalf("Order(多字段): %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u3", "u5", "u4", "u2", "u1")

		// 差异固化：连续两次 Order 叠加而非覆盖（gorm OrderBy MergeClause
		// 追加列；xorm 以最后一次为准）→ ORDER BY age, id DESC
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order("age").Order("id DESC").Find(&rows); err != nil {
			t.Fatalf("Order 叠加: %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u1", "u2", "u4", "u5", "u3")

		// Q-06：Count 剥离 ORDER BY（无 GROUP BY 时）——带排序链 Count 不报
		// PG 42803 且行数正确
		var n int64
		if err := q.Table(gfc1cOrdersTable).Order("category DESC").Count(&n); err != nil {
			t.Fatalf("Order+Count: %v", err)
		}
		if n != 6 {
			t.Errorf("Order+Count 期望 6, 实际 %d", n)
		}

		// 差异固化：Order(非 string) 静默忽略（gorm 仅收 string/
		// clause.OrderByColumn，其余不加排序子句、不报错）
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order(123).Find(&rows); err != nil {
			t.Errorf("Order(int) 差异固化应静默忽略: %v", err)
		} else if len(rows) != 5 {
			t.Errorf("Order(int) 静默后应返回全量 5 行, 实际 %d", len(rows))
		}

		// 仅 Limit
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order("id").Limit(2).Find(&rows); err != nil {
			t.Fatalf("仅 Limit: %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u1", "u2")

		// 仅 Offset：差异固化——gorm 渲染裸 OFFSET（无 LIMIT 兜底）：
		// PG 支持 OFFSET 无 LIMIT；MySQL 1064 语法错误（xorm 以大 LIMIT
		// 兜底两方言可用）
		rows = nil
		err := q.Model(&gfc1cUser{}).Order("id").Offset(2).Find(&rows)
		if dialect == "mysql" {
			if err == nil {
				t.Errorf("仅 Offset MySQL 差异固化应为 1064 语法错误, 实际命中 %v", gfc1cIDs(rows))
			}
		} else {
			if err != nil {
				t.Fatalf("仅 Offset PG: %v", err)
			}
			gfc1cAssertOrdered(t, rows, "u3", "u4", "u5")
		}

		// Limit + Offset
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order("id").Limit(2).Offset(1).Find(&rows); err != nil {
			t.Fatalf("Limit+Offset: %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u2", "u3")

		// Offset 越界 → 空结果
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order("id").Limit(5).Offset(7).Find(&rows); err != nil {
			t.Fatalf("Offset 越界: %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("Offset(7) 应 0 行, 实际 %v", gfc1cIDs(rows))
		}

		// 差异固化：Limit(0) 渲染 LIMIT 0 → 0 行（xorm 视为未设置返回全量）
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order("id").Limit(0).Find(&rows); err != nil {
			t.Fatalf("Limit(0): %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("Limit(0) 差异固化应 LIMIT 0 空, 实际 %v", gfc1cIDs(rows))
		}

		// Limit(负数)：不渲染 LIMIT 子句，返回全量（与 xorm 一致）
		for _, lim := range []int{-1, -5} {
			rows = nil
			if err := q.Model(&gfc1cUser{}).Order("id").Limit(lim).Find(&rows); err != nil {
				t.Fatalf("Limit(%d): %v", lim, err)
			}
			gfc1cAssertOrdered(t, rows, "u1", "u2", "u3", "u4", "u5")
		}

		// Offset(0) 与 Offset(负数)：不渲染 OFFSET 子句，返回全量（与 xorm 一致）
		for _, off := range []int{0, -1} {
			rows = nil
			if err := q.Model(&gfc1cUser{}).Order("id").Offset(off).Find(&rows); err != nil {
				t.Fatalf("Offset(%d): %v", off, err)
			}
			gfc1cAssertOrdered(t, rows, "u1", "u2", "u3", "u4", "u5")
		}
	})

	t.Run("Paginate分页", func(t *testing.T) {
		gfc1cResetUsers(t, drv)

		// Q-07：Count 两阶段——先 Count 总数，再取分页数据
		var total int64
		if err := q.Model(&gfc1cUser{}).Count(&total); err != nil {
			t.Fatalf("Count: %v", err)
		}
		if total != 5 {
			t.Fatalf("Count 期望 5, 实际 %d", total)
		}

		// 中间页：page2 size2 → 第 3、4 行
		var rows []gfc1cUser
		if err := q.Model(&gfc1cUser{}).Order("id").Paginate(2, 2).Find(&rows); err != nil {
			t.Fatalf("Paginate(2,2): %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u3", "u4")

		// 尾页：page3 size2 → 仅第 5 行
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order("id").Paginate(3, 2).Find(&rows); err != nil {
			t.Fatalf("Paginate(3,2): %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u5")

		// 越界：page4 size2 → 0 行
		rows = nil
		if err := q.Model(&gfc1cUser{}).Order("id").Paginate(4, 2).Find(&rows); err != nil {
			t.Fatalf("Paginate(4,2): %v", err)
		}
		if len(rows) != 0 {
			t.Errorf("Paginate(4,2) 越界应 0 行, 实际 %v", gfc1cIDs(rows))
		}

		// 非法归一（utils.PageUtil.Normalize：page<1→1、size<1→20）
		for _, ps := range [][2]int{{0, 0}, {-2, -1}} {
			rows = nil
			if err := q.Model(&gfc1cUser{}).Order("id").Paginate(ps[0], ps[1]).Find(&rows); err != nil {
				t.Fatalf("Paginate(%d,%d): %v", ps[0], ps[1], err)
			}
			gfc1cAssertOrdered(t, rows, "u1", "u2", "u3", "u4", "u5")
		}

		// 与 Where/Order 组合：beijing 3 行 size2 → 第 2 页仅 u4
		rows = nil
		if err := q.Model(&gfc1cUser{}).Where("city = ?", "beijing").Order("id").Paginate(2, 2).Find(&rows); err != nil {
			t.Fatalf("Where+Paginate: %v", err)
		}
		gfc1cAssertOrdered(t, rows, "u4")
	})

	t.Run("Group与Count分组统计", func(t *testing.T) {
		gfc1cResetOrders(t, drv)

		// AGG-01：Group + Count 返回分组数（gorm 以 RowsAffected 覆盖 count）
		var n int64
		if err := q.Table(gfc1cOrdersTable).Group("category").Count(&n); err != nil {
			t.Fatalf("Group Count: %v", err)
		}
		if n != 3 {
			t.Errorf("Group 后 Count 期望 3 组, 实际 %d", n)
		}

		// Group + Select 聚合：逐组行数回读（真实数据验证分组正确性）
		var aggs []gfc1cAgg
		if err := q.Model(&gfc1cOrder{}).
			Select("category, count(*) AS cnt").Group("category").Scan(&aggs); err != nil {
			t.Fatalf("Group Scan: %v", err)
		}
		if len(aggs) != 3 {
			t.Fatalf("Group Scan 期望 3 组, 实际 %+v", aggs)
		}
		got := map[string]int64{}
		for _, a := range aggs {
			got[a.Category] = a.Cnt
		}
		if got["a"] != 3 || got["b"] != 2 || got["c"] != 1 {
			t.Errorf("分组行数期望 {a:3 b:2 c:1}, 实际 %v", got)
		}

		// Where 前置过滤 + Group：amount>=20 剔除 o1/o4/o5 → 2 组
		n = 0
		if err := q.Model(&gfc1cOrder{}).Where("amount >= ?", 20).Group("category").Count(&n); err != nil {
			t.Fatalf("Where+Group Count: %v", err)
		}
		if n != 2 {
			t.Errorf("Where+Group Count 期望 2 组, 实际 %d", n)
		}
	})

	t.Run("Having过滤", func(t *testing.T) {
		gfc1cResetOrders(t, drv)

		// AGG-02：内联常量条件（聚合后过滤，仅 count > 1 的组）
		var aggs []gfc1cAgg
		if err := q.Model(&gfc1cOrder{}).
			Select("category, count(*) AS cnt").Group("category").
			Having("count(*) > 1").Scan(&aggs); err != nil {
			t.Fatalf("Group+Having Scan: %v", err)
		}
		if len(aggs) != 2 {
			t.Fatalf("Having count(*) > 1 期望 2 组, 实际 %+v", aggs)
		}
		for _, a := range aggs {
			if a.Cnt <= 1 {
				t.Errorf("Having count(*) > 1 后仍出现 cnt=%d 的组 %s", a.Cnt, a.Category)
			}
		}

		// 列条件：category <> 'c'
		aggs = nil
		if err := q.Model(&gfc1cOrder{}).
			Select("category, count(*) AS cnt").Group("category").
			Having("category <> 'c'").Scan(&aggs); err != nil {
			t.Fatalf("Group+Having 列条件: %v", err)
		}
		if len(aggs) != 2 {
			t.Fatalf("Having 列条件期望 2 组, 实际 %+v", aggs)
		}
		for _, a := range aggs {
			if a.Category == "c" {
				t.Errorf("Having category <> 'c' 后不应出现 c 组: %+v", aggs)
			}
		}

		// 差异固化：带参占位符——gorm 完整支持（xorm Session.Having 仅收
		// string、链上报 ErrUnsupported；gorm 侧行为有效即本断言）
		aggs = nil
		if err := q.Model(&gfc1cOrder{}).
			Select("category, count(*) AS cnt").Group("category").
			Having("count(*) > ?", 1).Scan(&aggs); err != nil {
			t.Fatalf("Having(带参) 差异固化 gorm 应支持: %v", err)
		}
		if len(aggs) != 2 {
			t.Fatalf("Having(带参) 期望 2 组, 实际 %+v", aggs)
		}
		for _, a := range aggs {
			if a.Category != "a" && a.Category != "b" {
				t.Errorf("Having(带参) 后组集合应为 {a,b}, 实际含 %s", a.Category)
			}
		}
	})

	t.Run("Select聚合表达式", func(t *testing.T) {
		// AGG-05：先建空表断言空集形态，再灌种子断言常规聚合
		intgMigrate(t, drv, []string{gfc1cOrdersTable}, &gfc1cOrder{})

		// 空表 + 无 Group：count/sum 返回 1 行，NULL 和经 gorm 双指针扫描归零
		var empty struct {
			Cnt   int64
			Total int64
		}
		if err := q.Table(gfc1cOrdersTable).Where("1 = 0").
			Select("count(*) AS cnt, sum(amount) AS total").Scan(&empty); err != nil {
			t.Fatalf("空表聚合: %v", err)
		}
		if empty.Cnt != 0 || empty.Total != 0 {
			t.Errorf("空表聚合期望 {0 0}, 实际 %+v", empty)
		}

		// 空表 + Group：0 行
		var eg []gfc1cAgg
		if err := q.Table(gfc1cOrdersTable).Where("1 = 0").
			Select("category, count(*) AS cnt").Group("category").Scan(&eg); err != nil {
			t.Fatalf("空表分组聚合: %v", err)
		}
		if len(eg) != 0 {
			t.Errorf("空表分组聚合应 0 行, 实际 %+v", eg)
		}

		gfc1cSeedOrders(t, drv)

		// AGG-03：count/sum/avg/max/min 经 Scan 回收（基准 6/180/30/100/5）
		var agg struct {
			Cnt       int64
			Total     int64
			AvgAmount float64
			MaxAmount int64
			MinAmount int64
		}
		if err := q.Table(gfc1cOrdersTable).
			Select("count(*) AS cnt, sum(amount) AS total, avg(amount) AS avg_amount, " +
				"max(amount) AS max_amount, min(amount) AS min_amount").
			Scan(&agg); err != nil {
			t.Fatalf("Scan 聚合: %v", err)
		}
		if agg.Cnt != 6 || agg.Total != 180 || agg.MaxAmount != 100 || agg.MinAmount != 5 {
			t.Errorf("聚合期望 {6 180 _ 100 5}, 实际 %+v", agg)
		}
		if avg := gfc1cAsFloat(t, "avg", agg.AvgAmount); avg != 30 {
			t.Errorf("avg 期望 30, 实际 %v", avg)
		}

		// AGG-03：ScanMap 形态回收分组聚合（数值列两方言驱动形态不一，经 helper 归一）
		var maps []map[string]any
		if err := q.Table(gfc1cOrdersTable).
			Select("category, count(*) AS cnt, sum(amount) AS total").
			Group("category").ScanMap(&maps); err != nil {
			t.Fatalf("ScanMap 聚合: %v", err)
		}
		if len(maps) != 3 {
			t.Fatalf("ScanMap 聚合期望 3 组, 实际 %d: %v", len(maps), maps)
		}
		gotCnt := map[string]int64{}
		gotTotal := map[string]int64{}
		for _, m := range maps {
			cat := fmt.Sprint(m["category"])
			gotCnt[cat] = gfc1cAsInt64(t, "cnt", m["cnt"])
			gotTotal[cat] = gfc1cAsInt64(t, "total", m["total"])
		}
		if gotCnt["a"] != 3 || gotCnt["b"] != 2 || gotCnt["c"] != 1 {
			t.Errorf("ScanMap 分组计数期望 {a:3 b:2 c:1}, 实际 %v", gotCnt)
		}
		if gotTotal["a"] != 60 || gotTotal["b"] != 20 || gotTotal["c"] != 100 {
			t.Errorf("ScanMap 分组和期望 {a:60 b:20 c:100}, 实际 %v", gotTotal)
		}

		// AGG-04：聚合 + Where 前置过滤（amount>=20 → 组 a 剩 2 行、组 c 1 行）
		var aggs []gfc1cAgg
		if err := q.Model(&gfc1cOrder{}).
			Select("category, count(*) AS cnt").Where("amount >= ?", 20).
			Group("category").Scan(&aggs); err != nil {
			t.Fatalf("Where+Group 聚合: %v", err)
		}
		if len(aggs) != 2 {
			t.Fatalf("Where+Group 期望 2 组, 实际 %+v", aggs)
		}
		byCat := map[string]int64{}
		for _, a := range aggs {
			byCat[a.Category] = a.Cnt
		}
		if byCat["a"] != 2 || byCat["c"] != 1 {
			t.Errorf("Where+Group 期望 {a:2 c:1}, 实际 %v", byCat)
		}

		// AGG-04：组排序 + TopN 组（cnt 降序取第 1 组 → a:3）
		aggs = nil
		if err := q.Model(&gfc1cOrder{}).
			Select("category, count(*) AS cnt").Group("category").
			Order("cnt DESC").Limit(1).Scan(&aggs); err != nil {
			t.Fatalf("TopN 组聚合: %v", err)
		}
		if len(aggs) != 1 || aggs[0].Category != "a" || aggs[0].Cnt != 3 {
			t.Errorf("TopN 组期望 [{a 3}], 实际 %+v", aggs)
		}
	})

	t.Run("链式不可变与dest条件", func(t *testing.T) {
		gfc1cResetUsers(t, drv)

		// Q-09（可用口径）：从同一会话根 q 出发派生的链互不影响
		base := q.Model(&gfc1cUser{}).Order("id")
		filtered := q.Model(&gfc1cUser{}).Order("id").Where("city = ?", "beijing")
		limited := q.Model(&gfc1cUser{}).Order("id").Limit(2)

		var all, beijing, two []gfc1cUser
		if err := base.Find(&all); err != nil {
			t.Fatalf("原链 Find: %v", err)
		}
		gfc1cAssertOrdered(t, all, "u1", "u2", "u3", "u4", "u5")

		if err := filtered.Find(&beijing); err != nil {
			t.Fatalf("派生 Where 链 Find: %v", err)
		}
		gfc1cAssertOrdered(t, beijing, "u1", "u2", "u4")

		if err := limited.Find(&two); err != nil {
			t.Fatalf("派生 Limit 链 Find: %v", err)
		}
		gfc1cAssertOrdered(t, two, "u1", "u2")

		// 原链复用执行结果不变
		all = nil
		if err := base.Find(&all); err != nil {
			t.Fatalf("原链复用 Find: %v", err)
		}
		gfc1cAssertOrdered(t, all, "u1", "u2", "u3", "u4", "u5")

		// 差异固化：深分叉别名——gorm 仅在会话首个链式方法处克隆
		//（Session(NewDB) clone=2 → 首个链式方法后 clone=0），从已用链再派生
		// 会与源链共享底层 Statement 互相污染（xorm 每次链式调用克隆、
		// 派生互不影响）。此处固化 gorm 真实行为：
		// deepFiltered 的 Where 会回流到 deep。
		deep := q.Model(&gfc1cUser{}).Order("id")
		deepFiltered := deep.Where("city = ?", "beijing")
		var deepRows, deepAgain []gfc1cUser
		if err := deepFiltered.Find(&deepRows); err != nil {
			t.Fatalf("深分叉链 Find: %v", err)
		}
		gfc1cAssertOrdered(t, deepRows, "u1", "u2", "u4")
		if err := deep.Find(&deepAgain); err != nil {
			t.Fatalf("被派生的源链 Find: %v", err)
		}
		gfc1cAssertOrdered(t, deepAgain, "u1", "u2", "u4")

		// Q-11：复用已填充的切片 dest 再查询，不作查询条件（gorm Find 会
		// 重置切片 dest，且 dest 内容不并入 WHERE）
		var reused []gfc1cUser
		if err := q.Model(&gfc1cUser{}).Order("id").Find(&reused); err != nil {
			t.Fatalf("首次 Find: %v", err)
		}
		if len(reused) != 5 {
			t.Fatalf("首次 Find 应 5 行, 实际 %d", len(reused))
		}
		if err := q.Model(&gfc1cUser{}).Where("city = ?", "guangzhou").Order("id").Find(&reused); err != nil {
			t.Fatalf("复用 dest Find: %v", err)
		}
		gfc1cAssertOrdered(t, reused, "u5")

		// Q-11：Exists 不收窄——已填充 dest（u1/beijing）不并入条件，
		// 仅链上 Where 生效（guangzhou 存在 → true；若按 dest 收窄将为 false）
		populated := &gfc1cUser{ID: "u1", Name: "alice", Age: 20, City: "beijing", Email: "u1@g.com", Score: 100}
		ok, err := q.Model(&gfc1cUser{}).Where("city = ?", "guangzhou").Exists(populated)
		if err != nil {
			t.Fatalf("Exists: %v", err)
		}
		if !ok {
			t.Errorf("Exists 差异固化应不收窄（dest u1 不并入条件）, 实际 false")
		}

		// Q-11（双驱动一致，已修复对齐）：First 传入同模型且主键非零的 dest
		// 不收窄——gorm 原生 BuildQuerySQL 会给非零主键 struct dest 内联主键
		// 条件（驱动层 one() 以零值副本承接屏蔽），与 xorm NoAutoCondition 口径
		// 一致：链上 Where(city=beijing) 生效 → 回填 u1（beijing 首行）
		firstDest := gfc1cUser{ID: "u3", Name: "bob", Age: 30, City: "shanghai", Email: "u3@g.com", Score: 300}
		if err := q.Model(&gfc1cUser{}).Where("city = ?", "beijing").Order("id").First(&firstDest); err != nil {
			t.Fatalf("First(populated dest) 不应收窄, 实际: %v", err)
		}
		if firstDest.ID != "u1" {
			t.Errorf("First(populated dest) 应按链条件回填 u1, 实际 %+v", firstDest)
		}

		// Q-11 差异固化：FirstOrCreate 的查询段仍按 gorm 原生收窄 → 未命中
		// 回落 Create → 撞 u3 重复主键 ErrDuplicatedKey（xorm 侧按链条件命中
		// u1、回填不新建；FirstOrCreate 内部走 gorm FirstOrCreate，未做副本
		// 屏蔽——修复需重实现其 attrs/assigns 机制，风险大，本轮差异固化）
		fcDest := gfc1cUser{ID: "u3", Name: "bob", Age: 30, City: "shanghai", Email: "u3@g.com", Score: 300}
		err = q.Model(&gfc1cUser{}).Where("city = ?", "beijing").FirstOrCreate(&fcDest)
		if !errors.Is(err, contracts.ErrDuplicatedKey) {
			t.Errorf("FirstOrCreate(populated dest) 差异固化应收窄未命中后回落 Create 撞重复主键, 实际: %v", err)
		}
		var n int64
		if err := q.Model(&gfc1cUser{}).Count(&n); err != nil {
			t.Fatalf("FirstOrCreate 后 Count: %v", err)
		}
		if n != 5 {
			t.Errorf("FirstOrCreate 失败后不应落库, 期望 5 行, 实际 %d", n)
		}
	})
}

// ── 入口（PG / MySQL 成对，共享 runner）───────────────────────────────

// TestFullCovChain_PG 分组 1 链式条件构建：PostgreSQL 真实库全覆盖。
func TestFullCovChain_PG(t *testing.T) {
	runFullCovChain(t, newIntgPG(t), "pg")
}

// TestFullCovChain_MySQL 分组 1 链式条件构建：MySQL 真实库全覆盖。
func TestFullCovChain_MySQL(t *testing.T) {
	runFullCovChain(t, newIntgMySQL(t), "mysql")
}
