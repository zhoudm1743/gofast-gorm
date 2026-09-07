//go:build integration

package gormdriver

// fullcov_rawtx_integration_test.go —— contracts.Query 真实库全覆盖（分组 4）：
// 原生 SQL + 事务 + 链式杂项。PG/MySQL 双真库跑同一共享 runner（runFullCovRawTx），
// 对齐 xorm 侧同名文件的用例矩阵。
// 覆盖方法：Raw / Row / Rows / Exec / ExecResult / Transaction / Begin / Commit /
// Rollback / SavePoint / RollbackTo / WithContext / Debug / Scopes / Paginate /
// GetSchema / Schema。
//
// 运行方式（docker-compose.yml 环境）：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	  go test -tags integration -run 'TestFullCovRawTx' -v .
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -run 'TestFullCovRawTx' -v .
//
// 覆盖形态（与 xorm 模板分组对齐，gorm 侧差异以「差异固化」注释就地说明）：
//   - Raw：+Find 进模型切片、+Scan 标量（count/count(*) 别名/单列/未命中保持零值）、
//     '?' 参数绑定、Row 单行（未命中 sql.ErrNoRows）、Rows 游标（迭代/Columns/Close）
//   - Exec：原生 DDL（建表/加列）与带参 DML（INSERT/UPDATE/DELETE）落库生效，
//     动态建出的表 ORM 可继续读写
//   - ExecResult：INSERT/UPDATE/DELETE RowsAffected、未命中 0 行 IsZeroRow、
//     重复键错误映射 contracts.ErrDuplicatedKey
//   - Transaction：正常提交外部可见（含事务内 Count 会话复用）、业务错误整体回滚且
//     原样透传（errors.Is + 同一错误实例）、ORM 错误（重复键）自动回滚、
//     事务内读写一致（未提交本事务可见 / 回滚后外部不可见）、TxOption 变体
//     （gorm 走真实 sql.TxOptions，与 xorm 降级忽略不同——差异固化）
//   - Begin/Commit、Begin/Rollback 可见性；重复 Commit / Commit 后 Rollback /
//     Rollback 后 Commit / 重复 Rollback → contracts.ErrInvalidTransaction（TX-02 全矩阵）
//   - SavePoint/RollbackTo：保存点后插入消失、保存点前保留、回滚后事务可继续写并提交；
//     mysql 方言生成未加引号的 SAVEPOINT <name>（普通标识符合法，1064 回归点）
//   - WithContext：已取消 ctx 读/写均报错且不落库；有效 ctx 正常执行
//   - Debug：文档化 no-op，链可继续正常执行
//   - Scopes：多个作用域依次应用（AND + Order）、片段复用、原链不被污染；
//     nil 作用域行为差异（xorm 跳过 / gorm 执行期 panic，差异固化仅记录）
//   - Paginate：非法 page/size 归一 1/20（utils.PageUtil.Normalize）、
//     中间页/尾页不足一页/越界空结果、Count+分页两阶段
//   - PG 专属：事务内 DDL 可回滚（MySQL 的 DDL 隐式提交，分支跳过）、
//     GetSchema + Schema() 后原生 SQL 拼 schema 限定表名（MySQL 分支跳过）

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// ── 测试模型 ─────────────────────────────────────────────────────────
//
// 数值列全部用 int64（两方言统一 BIGINT），避免跨方言 int 扫描/绑定差异。
// TableName() 固定表名 gfc4_*（分组 4 前缀）。

type gfc4User struct {
	ID     string `orm:"pk varchar(16) 'id'"`
	Name   string `orm:"varchar(64) 'name'"`
	Status int64  `orm:"'status'"`
	Cnt    int64  `orm:"'cnt'"`
}

func (gfc4User) TableName() string { return "gfc4_users" }

// gfc4ExecRow 供 Exec 动态 DDL 建出的表（gfc4_exec_tbl）使用——ORM 读写
// 非迁移建表（先 Exec CREATE TABLE 再 ORM 读写）的场景。
type gfc4ExecRow struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Note string `orm:"varchar(64) 'note'"`
	Cnt  int64  `orm:"'cnt'"`
}

func (gfc4ExecRow) TableName() string { return "gfc4_exec_tbl" }

// ── 共用种子 / 断言辅助 ───────────────────────────────────────────────

// gfc4SeedBasic 重建 gfc4_users 并灌入三行基线（a/b status=1；c status=2）。
func gfc4SeedBasic(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
	q := drv.Query()
	for _, u := range []*gfc4User{
		{ID: "a", Name: "alice", Status: 1, Cnt: 10},
		{ID: "b", Name: "bob", Status: 1, Cnt: 20},
		{ID: "c", Name: "carol", Status: 2, Cnt: 30},
	} {
		if err := q.Create(u); err != nil {
			t.Fatalf("种子 %s: %v", u.ID, err)
		}
	}
}

// gfc4SeedPaged 重建 gfc4_users 并灌入 23 行：u01..u13 status=1，u14..u23 status=2。
// 供 Paginate 分页矩阵（筛选 status=1 共 13 行）与归一化（23 行全量）使用。
func gfc4SeedPaged(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
	q := drv.Query()
	for i := 1; i <= 23; i++ {
		st := int64(2)
		if i <= 13 {
			st = 1
		}
		u := &gfc4User{ID: fmt.Sprintf("u%02d", i), Name: fmt.Sprintf("name-%02d", i), Status: st, Cnt: int64(i * 10)}
		if err := q.Create(u); err != nil {
			t.Fatalf("种子 %s: %v", u.ID, err)
		}
	}
}

// gfc4CountRaw 任意 count 标量查询（Raw+Scan 链路，顺带覆盖 count 扫描）。
func gfc4CountRaw(t *testing.T, drv *GormDriver, query string) int64 {
	t.Helper()
	var n int64
	if err := drv.Query().Raw(query).Scan(&n); err != nil {
		t.Fatalf("count 查询 %q: %v", query, err)
	}
	return n
}

// gfc4CountUsers 整表行数。
func gfc4CountUsers(t *testing.T, drv *GormDriver) int64 {
	t.Helper()
	return gfc4CountRaw(t, drv, "SELECT count(*) FROM gfc4_users")
}

// gfc4FindAll 全表查询并返回按 id 升序的行。
func gfc4FindAll(t *testing.T, q contracts.Query) []gfc4User {
	t.Helper()
	var rows []gfc4User
	if err := q.Model(&gfc4User{}).Order("id").Find(&rows); err != nil {
		t.Fatalf("Find 全表: %v", err)
	}
	return rows
}

// gfc4FindByID 按主键回读单行（显式 Where）。
func gfc4FindByID(t *testing.T, q contracts.Query, id string) gfc4User {
	t.Helper()
	var row gfc4User
	if err := q.Model(&gfc4User{}).Where("id = ?", id).First(&row); err != nil {
		t.Fatalf("回读 %q: %v", id, err)
	}
	return row
}

// gfc4AssertIDs 断言 rows 的主键序列与 want 完全一致（按序）。
func gfc4AssertIDs(t *testing.T, stage string, rows []gfc4User, want string) {
	t.Helper()
	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.ID)
	}
	if got := strings.Join(ids, ","); got != want {
		t.Errorf("%s: 主键序列应为 %q, 实际 %q", stage, want, got)
	}
}

// ── 共享 runner（PG/MySQL 双入口调用；dialect 供方言差异分支）──────────

func runFullCovRawTx(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	q := drv.Query()

	t.Run("Raw_Find模型切片_参数绑定", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		// 单参绑定 + AND 组合过滤 + 排序
		var rows []gfc4User
		if err := q.Raw("SELECT id, name, status, cnt FROM gfc4_users WHERE status = ? ORDER BY id", int64(1)).
			Find(&rows); err != nil {
			t.Fatalf("Raw+Find 单参: %v", err)
		}
		if len(rows) != 2 || rows[0].ID != "a" || rows[1].ID != "b" {
			t.Fatalf("Raw+Find 应命中 a,b 两行, 实际 %+v", rows)
		}
		if rows[0].Name != "alice" || rows[0].Cnt != 10 {
			t.Errorf("a 行回读值错误: %+v", rows[0])
		}

		// 多参绑定：cnt >= ? 且 status <> ?
		var rows2 []gfc4User
		if err := q.Raw("SELECT id, name, status, cnt FROM gfc4_users WHERE cnt >= ? AND status <> ? ORDER BY id DESC",
			int64(20), int64(2)).Find(&rows2); err != nil {
			t.Fatalf("Raw+Find 多参: %v", err)
		}
		if len(rows2) != 1 || rows2[0].ID != "b" || rows2[0].Name != "bob" {
			t.Errorf("多参 Raw 应命中 b, 实际 %+v", rows2)
		}

		// 同一查询语义重复构建执行（链不可变，两次独立终结）
		var again []gfc4User
		if err := q.Raw("SELECT id FROM gfc4_users WHERE status = ? ORDER BY id", int64(2)).Find(&again); err != nil {
			t.Fatalf("Raw 二次执行: %v", err)
		}
		if len(again) != 1 || again[0].ID != "c" {
			t.Errorf("Raw 二次执行应命中 c, 实际 %+v", again)
		}
	})

	t.Run("Raw_Scan标量_Count别名_未命中", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		// count(*) → int64（单值回收）
		if n := gfc4CountUsers(t, drv); n != 3 {
			t.Fatalf("count 应 3, 实际 %d", n)
		}
		// count(*) AS 别名 → struct 单字段回收
		var counted struct {
			Total int64
		}
		if err := q.Raw("SELECT count(*) AS total FROM gfc4_users").Scan(&counted); err != nil {
			t.Fatalf("Scan count 别名: %v", err)
		}
		if counted.Total != 3 {
			t.Errorf("count(*) AS total 应 3, 实际 %d", counted.Total)
		}
		// 单列字符串 + 参数绑定
		var name string
		if err := q.Raw("SELECT name FROM gfc4_users WHERE id = ?", "a").Scan(&name); err != nil {
			t.Fatalf("Scan 单列: %v", err)
		}
		if name != "alice" {
			t.Errorf("Scan 单列应 alice, 实际 %q", name)
		}
		// 单列数值
		var cnt int64
		if err := q.Raw("SELECT cnt FROM gfc4_users WHERE id = ?", "b").Scan(&cnt); err != nil {
			t.Fatalf("Scan 数值列: %v", err)
		}
		if cnt != 20 {
			t.Errorf("Scan 数值列应 20, 实际 %d", cnt)
		}
		// 未命中：无行保持零值不报错（gorm Scan 语义）
		var miss string
		if err := q.Raw("SELECT name FROM gfc4_users WHERE id = ?", "no-such").Scan(&miss); err != nil {
			t.Fatalf("Scan 未命中应无错: %v", err)
		}
		if miss != "" {
			t.Errorf("Scan 未命中应保持零值, 实际 %q", miss)
		}
		// Raw+Scan 单 struct（Get 语义）
		var one gfc4User
		if err := q.Raw("SELECT id, name, status, cnt FROM gfc4_users WHERE id = ?", "c").Scan(&one); err != nil {
			t.Fatalf("Raw+Scan struct: %v", err)
		}
		if one.ID != "c" || one.Name != "carol" || one.Cnt != 30 {
			t.Errorf("Raw+Scan struct 值错误: %+v", one)
		}
	})

	t.Run("Exec_DDL建表加列_ORM可读写", func(t *testing.T) {
		intgDropTables(t, drv, "gfc4_exec_tbl")
		t.Cleanup(func() { intgDropTables(t, drv, "gfc4_exec_tbl") })
		// DDL：Exec 建表（两方言通用类型）
		if err := q.Exec(`CREATE TABLE gfc4_exec_tbl (id varchar(16) PRIMARY KEY, note varchar(64), cnt bigint)`); err != nil {
			t.Fatalf("Exec 建表: %v", err)
		}
		// 动态建出的表：ORM 写入（模型 TableName 指向该表）
		if err := q.Create(&gfc4ExecRow{ID: "e1", Note: "orm-first", Cnt: 1}); err != nil {
			t.Fatalf("ORM 写动态表: %v", err)
		}
		// 带参 DML 插入第二行
		if err := q.Exec("INSERT INTO gfc4_exec_tbl (id, note, cnt) VALUES (?, ?, ?)", "e2", "raw-second", int64(2)); err != nil {
			t.Fatalf("Exec 插入: %v", err)
		}
		// DDL 加列（差异固化：MySQL DDL 隐式提交，此处非事务内 DDL，两方言一致）
		if err := q.Exec("ALTER TABLE gfc4_exec_tbl ADD COLUMN extra varchar(64)"); err != nil {
			t.Fatalf("Exec 加列: %v", err)
		}
		if err := q.Exec("UPDATE gfc4_exec_tbl SET extra = ? WHERE id = ?", "widened", "e1"); err != nil {
			t.Fatalf("Exec 回填新列: %v", err)
		}
		// ORM 读动态表（gorm 显式列 SELECT，规避 PrepareStmt 缓存计划变更）
		var rows []gfc4ExecRow
		if err := q.Model(&gfc4ExecRow{}).Order("id").Find(&rows); err != nil {
			t.Fatalf("ORM 读动态表: %v", err)
		}
		if len(rows) != 2 || rows[0].ID != "e1" || rows[0].Note != "orm-first" || rows[1].ID != "e2" {
			t.Errorf("动态表 ORM 回读错误: %+v", rows)
		}
		// 加列后 ORM 仍可写
		if err := q.Model(&gfc4ExecRow{}).Where("id = ?", "e2").Update("note", "orm-updated"); err != nil {
			t.Fatalf("加列后 ORM 更新: %v", err)
		}
		// 新列原生回读
		var extra string
		if err := q.Raw("SELECT extra FROM gfc4_exec_tbl WHERE id = ?", "e1").Scan(&extra); err != nil {
			t.Fatalf("回读 extra: %v", err)
		}
		if extra != "widened" {
			t.Errorf("Exec 加列后 extra 应 widened, 实际 %q", extra)
		}
		if n := gfc4CountRaw(t, drv, "SELECT count(*) FROM gfc4_exec_tbl"); n != 2 {
			t.Errorf("动态表应 2 行, 实际 %d", n)
		}
	})

	t.Run("Exec_带参DML落库回读", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		// 带参 INSERT 落库
		if err := q.Exec("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "d", "dora", int64(2), int64(40)); err != nil {
			t.Fatalf("Exec INSERT: %v", err)
		}
		if got := gfc4FindByID(t, q, "d"); got.Name != "dora" || got.Cnt != 40 {
			t.Errorf("Exec INSERT 落库值错误: %+v", got)
		}
		// 带参 UPDATE + 落库回读
		if err := q.Exec("UPDATE gfc4_users SET name = ? WHERE id = ?", "alice2", "a"); err != nil {
			t.Fatalf("Exec UPDATE: %v", err)
		}
		var name string
		if err := q.Raw("SELECT name FROM gfc4_users WHERE id = ?", "a").Scan(&name); err != nil {
			t.Fatalf("回读 name: %v", err)
		}
		if name != "alice2" {
			t.Errorf("Exec UPDATE 应生效, 实际 %q", name)
		}
		// 带参 DELETE + 行数回退
		if err := q.Exec("DELETE FROM gfc4_users WHERE id = ?", "d"); err != nil {
			t.Fatalf("Exec DELETE: %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 3 {
			t.Errorf("Exec DELETE 后应 3 行, 实际 %d", n)
		}
	})

	t.Run("ExecResult_行数与IsZeroRow", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		// INSERT → 1 行
		r := q.ExecResult("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "d", "dora", int64(2), int64(40))
		if r.Error != nil || r.RowsAffected != 1 {
			t.Fatalf("INSERT RowsAffected 应 1, 实际 %d Error=%v", r.RowsAffected, r.Error)
		}
		// UPDATE 命中改值 → 1 行（差异固化：MySQL affected 计数默认 CLIENT_FOUND_ROWS
		// 关闭，未改值报 0；测试恒改新值保证两方言一致）
		r = q.ExecResult("UPDATE gfc4_users SET name = ? WHERE id = ?", "bob2", "b")
		if r.Error != nil || r.RowsAffected != 1 {
			t.Fatalf("UPDATE RowsAffected 应 1, 实际 %d Error=%v", r.RowsAffected, r.Error)
		}
		if got := gfc4FindByID(t, q, "b"); got.Name != "bob2" {
			t.Errorf("UPDATE 应落库 bob2, 实际 %q", got.Name)
		}
		// UPDATE 未命中 → 0 行无错，IsZeroRow 可判定
		r = q.ExecResult("UPDATE gfc4_users SET name = ? WHERE id = ?", "ghost", "zz")
		if r.Error != nil || !r.IsZeroRow() {
			t.Fatalf("未命中 UPDATE 期望 0 行无错, 实际 RowsAffected=%d Error=%v", r.RowsAffected, r.Error)
		}
		// DELETE 命中 → 1 行
		r = q.ExecResult("DELETE FROM gfc4_users WHERE id = ?", "a")
		if r.Error != nil || r.RowsAffected != 1 {
			t.Fatalf("DELETE RowsAffected 应 1, 实际 %d Error=%v", r.RowsAffected, r.Error)
		}
		if n := gfc4CountUsers(t, drv); n != 3 {
			t.Errorf("删除 a 后应剩 3 行(b/c/d), 实际 %d", n)
		}
	})

	t.Run("ExecResult_重复键错误映射", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		r := q.ExecResult("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "a", "dup", int64(1), int64(1))
		if r.Error == nil || !errors.Is(r.Error, contracts.ErrDuplicatedKey) {
			t.Errorf("重复键应映射 ErrDuplicatedKey, 实际 Error=%v", r.Error)
		}
		if r.RowsAffected != 0 {
			t.Errorf("错误路径 RowsAffected 应保持 0, 实际 %d", r.RowsAffected)
		}
	})

	t.Run("Raw_Row单行_未命中ErrNoRows", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		// Row 单行回收（'?' 占位符经 gorm Raw 正确绑定）
		row := q.Raw("SELECT name FROM gfc4_users WHERE id = ?", "a").Row()
		var name string
		if err := row.Scan(&name); err != nil {
			t.Fatalf("Row Scan: %v", err)
		}
		if name != "alice" {
			t.Errorf("Row 应回收 alice, 实际 %q", name)
		}
		// 未命中 → database/sql 层 sql.ErrNoRows（不经 wrapError，非框架哨兵）
		missRow := q.Raw("SELECT name FROM gfc4_users WHERE id = ?", "no-such").Row()
		var miss string
		err := missRow.Scan(&miss)
		if err == nil || !errors.Is(err, sql.ErrNoRows) {
			t.Errorf("Row 未命中应 sql.ErrNoRows, 实际 %v", err)
		}
	})

	t.Run("Raw_Rows游标迭代与关闭", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		// '?' 占位符绑定正确性由命中集合（仅 status=1 的 a,b）证明
		rows, err := q.Raw("SELECT id, name FROM gfc4_users WHERE status = ? ORDER BY id", int64(1)).Rows()
		if err != nil {
			t.Fatalf("Rows: %v", err)
		}
		// Columns 与 SELECT 列序一致
		cols, err := rows.Columns()
		if err != nil {
			t.Fatalf("Columns: %v", err)
		}
		if got := strings.Join(cols, ","); got != "id,name" {
			t.Errorf("Columns 应 id,name, 实际 %q", got)
		}
		// 迭代两行
		var got []string
		for rows.Next() {
			var id, name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Fatalf("迭代 Scan: %v", err)
			}
			got = append(got, id+":"+name)
		}
		if want := "a:alice,b:bob"; strings.Join(got, ",") != want {
			t.Errorf("Rows 迭代应 %q, 实际 %q", want, strings.Join(got, ","))
		}
		// Close 幂等无错
		if err := rows.Close(); err != nil {
			t.Errorf("Rows Close 不应报错: %v", err)
		}
	})

	t.Run("Transaction_提交可见_事务内Count会话复用", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		var inside int64
		err := q.Transaction(func(tx contracts.Query) error {
			if err := tx.Create(&gfc4User{ID: "tx1", Name: "in-tx-1", Status: 1, Cnt: 10}); err != nil {
				return err
			}
			if err := tx.Exec("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "tx2", "in-tx-2", int64(1), int64(20)); err != nil {
				return err
			}
			// 同一事务会话内应看到未提交的两行（session 复用）
			return tx.Model(&gfc4User{}).Count(&inside)
		})
		if err != nil {
			t.Fatalf("Transaction: %v", err)
		}
		if inside != 2 {
			t.Errorf("事务内 Count 应 2, 实际 %d", inside)
		}
		if n := gfc4CountUsers(t, drv); n != 2 {
			t.Fatalf("提交后应 2 行, 实际 %d", n)
		}
		rows := gfc4FindAll(t, drv.Query())
		gfc4AssertIDs(t, "提交后可见", rows, "tx1,tx2")
		if rows[0].Name != "in-tx-1" || rows[1].Name != "in-tx-2" {
			t.Errorf("提交后回读值错误: %+v", rows)
		}
	})

	t.Run("Transaction_业务错误回滚_原样透传", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		errBiz := errors.New("biz-fail-sentinel")
		err := q.Transaction(func(tx contracts.Query) error {
			if err := tx.Create(&gfc4User{ID: "rb1", Name: "rollback-me", Status: 1, Cnt: 1}); err != nil {
				return err
			}
			if err := tx.Exec("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "rb2", "rollback-me2", int64(1), int64(2)); err != nil {
				return err
			}
			return errBiz
		})
		if err == nil || !errors.Is(err, errBiz) {
			t.Fatalf("应返回注入的业务错误, 实际 %v", err)
		}
		if err != errBiz {
			t.Errorf("业务错误应原样透传不被包装, 实际 %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 0 {
			t.Errorf("整体回滚后应 0 行, 实际 %d", n)
		}
	})

	t.Run("Transaction_驱动错误同样回滚", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		err := q.Transaction(func(tx contracts.Query) error {
			if err := tx.Create(&gfc4User{ID: "dup", Name: "first", Status: 1, Cnt: 1}); err != nil {
				return err
			}
			// 同事务内二次插入同主键：ORM 错误沿 fn 返回 → 整体回滚
			return tx.Create(&gfc4User{ID: "dup", Name: "second", Status: 1, Cnt: 2})
		})
		if err == nil || !errors.Is(err, contracts.ErrDuplicatedKey) {
			t.Fatalf("应返回 ErrDuplicatedKey, 实际 %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 0 {
			t.Errorf("错误路径回滚后应 0 行, 实际 %d", n)
		}
	})

	t.Run("Transaction_事务内读写一致", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		tx := q.Begin()
		t.Cleanup(func() { _ = tx.Rollback() }) // 失败兜底：未终态的事务由 Cleanup 回收
		if err := tx.Create(&gfc4User{ID: "vis1", Name: "uncommitted", Status: 1, Cnt: 1}); err != nil {
			t.Fatalf("事务内 Create: %v", err)
		}
		// 事务内可见自己的未提交写（Find + Count 双路径）
		var rows []gfc4User
		if err := tx.Model(&gfc4User{}).Order("id").Find(&rows); err != nil {
			t.Fatalf("事务内 Find: %v", err)
		}
		if len(rows) != 1 || rows[0].ID != "vis1" || rows[0].Name != "uncommitted" {
			t.Errorf("事务内应读到未提交行, 实际 %+v", rows)
		}
		var inTx int64
		if err := tx.Model(&gfc4User{}).Count(&inTx); err != nil {
			t.Fatalf("事务内 Count: %v", err)
		}
		if inTx != 1 {
			t.Errorf("事务内 Count 应 1, 实际 %d", inTx)
		}
		// 外部连接不可见未提交数据（READ COMMITTED / REPEATABLE READ 一致）
		if n := gfc4CountUsers(t, drv); n != 0 {
			t.Errorf("外部不应看到未提交数据, 实际 %d 行", n)
		}
		// 回滚后外部仍不可见
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 0 {
			t.Errorf("回滚后外部应仍 0 行, 实际 %d", n)
		}
	})

	t.Run("Transaction_TxOption变体", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		// 差异固化：与 xorm 降级忽略不同，gorm 侧 parseTxOptions 把 StandardTxOptions
		// 真实转换为 sql.TxOptions 传给底层 Begin——隔离级别/只读均生效而非忽略。
		// TxReadCommitted：写入正常提交。
		err := q.Transaction(func(tx contracts.Query) error {
			return tx.Create(&gfc4User{ID: "opt1", Name: "read-committed", Status: 1, Cnt: 1})
		}, contracts.TxReadCommitted)
		if err != nil {
			t.Fatalf("TxReadCommitted 变体: %v", err)
		}
		// TxReadOnly：只读事务写入应被拒绝（PG "cannot execute INSERT in a read-only
		// transaction" SQLSTATE 25006；MySQL Error 1792 "Cannot execute statement in
		// a READ ONLY transaction"）——可观察行为，如实断言。
		errRO := q.Transaction(func(tx contracts.Query) error {
			return tx.Create(&gfc4User{ID: "opt2", Name: "read-only", Status: 1, Cnt: 2})
		}, contracts.TxReadOnly)
		if errRO == nil {
			t.Errorf("只读事务写入应被拒绝（sql.TxOptions ReadOnly 透传）")
		} else {
			t.Logf("只读事务写入错误（预期）: %v", errRO)
		}
		// 拒绝路径整体回滚：仅 opt1 可见
		if n := gfc4CountUsers(t, drv); n != 1 {
			t.Errorf("只读事务拒绝后应仅 1 行(opt1), 实际 %d", n)
		}
	})

	t.Run("Begin_提交与回滚可见性", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		// Begin + Create + Commit：提交后外部可见
		tx := q.Begin()
		t.Cleanup(func() { _ = tx.Rollback() }) // 失败兜底：未终态的事务由 Cleanup 回收
		if err := tx.Create(&gfc4User{ID: "bc1", Name: "commit-me", Status: 1, Cnt: 1}); err != nil {
			t.Fatalf("Begin/Create: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 1 {
			t.Errorf("提交后应 1 行, 实际 %d", n)
		}
		// Begin + Exec + Rollback：回滚后外部不可见
		tx2 := q.Begin()
		t.Cleanup(func() { _ = tx2.Rollback() })
		if err := tx2.Exec("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "br1", "rollback-me", int64(1), int64(9)); err != nil {
			t.Fatalf("Begin/Exec: %v", err)
		}
		if err := tx2.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 1 {
			t.Errorf("回滚后应仍 1 行, 实际 %d", n)
		}
	})

	t.Run("Begin_重复Commit与终态后误用", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		// TX-02 终态误用全矩阵。gorm 实测语义（finisher_api.go）：Commit/Rollback 后
		// ConnPool 仍持有事务句柄，二次终结透出 database/sql 的 sql.ErrTxDone，
		// errors.go 按同语义映射 contracts.ErrInvalidTransaction（gorm 自身
		// ErrInvalidTransaction 仅在 ConnPool 非事务时命中）。
		// 重复 Commit → ErrInvalidTransaction
		tx := q.Begin()
		t.Cleanup(func() { _ = tx.Rollback() })
		if err := tx.Create(&gfc4User{ID: "dc1", Name: "double-commit", Status: 1, Cnt: 1}); err != nil {
			t.Fatalf("Begin/Create: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("首次 Commit: %v", err)
		}
		if err := tx.Commit(); !errors.Is(err, contracts.ErrInvalidTransaction) {
			t.Errorf("重复 Commit 应 ErrInvalidTransaction, 实际 %v", err)
		}
		// Commit 后 Rollback → ErrInvalidTransaction
		if err := tx.Rollback(); !errors.Is(err, contracts.ErrInvalidTransaction) {
			t.Errorf("Commit 后 Rollback 应 ErrInvalidTransaction, 实际 %v", err)
		}
		// Rollback 后 Commit / 重复 Rollback → ErrInvalidTransaction
		tx2 := q.Begin()
		t.Cleanup(func() { _ = tx2.Rollback() })
		if err := tx2.Create(&gfc4User{ID: "dc2", Name: "double-rollback", Status: 1, Cnt: 2}); err != nil {
			t.Fatalf("第二次 Begin/Create: %v", err)
		}
		if err := tx2.Rollback(); err != nil {
			t.Fatalf("首次 Rollback: %v", err)
		}
		if err := tx2.Commit(); !errors.Is(err, contracts.ErrInvalidTransaction) {
			t.Errorf("Rollback 后 Commit 应 ErrInvalidTransaction, 实际 %v", err)
		}
		if err := tx2.Rollback(); !errors.Is(err, contracts.ErrInvalidTransaction) {
			t.Errorf("重复 Rollback 应 ErrInvalidTransaction, 实际 %v", err)
		}
		// 终态链上的插入不应落库（已提交/已回滚的事务句柄不再承载新写入）
		if n := gfc4CountUsers(t, drv); n != 1 {
			t.Errorf("仅首次事务的提交应可见, 实际 %d 行", n)
		}
	})

	t.Run("Begin_TxOption变体", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		// gorm 侧 TxOption 真实生效：Begin(变体) 不报错、事务正常终结
		//（TxRepeatableRead：PG BEGIN ISOLATION LEVEL REPEATABLE READ /
		// MySQL SET TRANSACTION ISOLATION LEVEL REPEATABLE-READ，单连接写均合法）
		tx := q.Begin(contracts.TxRepeatableRead)
		t.Cleanup(func() { _ = tx.Rollback() })
		if err := tx.Create(&gfc4User{ID: "bt1", Name: "repeatable-read", Status: 1, Cnt: 1}); err != nil {
			t.Fatalf("Begin(TxRepeatableRead)/Create: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Begin(TxRepeatableRead)/Commit: %v", err)
		}
		tx2 := q.Begin(contracts.TxSerializable)
		t.Cleanup(func() { _ = tx2.Rollback() })
		if err := tx2.Exec("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "bt2", "serializable", int64(1), int64(2)); err != nil {
			t.Fatalf("Begin(TxSerializable)/Exec: %v", err)
		}
		if err := tx2.Rollback(); err != nil {
			t.Fatalf("Begin(TxSerializable)/Rollback: %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 1 {
			t.Errorf("仅提交的 bt1 应可见, 实际 %d 行", n)
		}
	})

	t.Run("SavePoint_部分回滚", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		// 差异固化（1064 回归点）：gorm mysql dialector v1.6.0 生成未加引号的
		// "SAVEPOINT gfc4_sp_1"（postgres dialector 同为裸拼接）——普通
		// [A-Za-z0-9_] 标识符在 MySQL 8.0 / PG 均合法；gorm 核心还会临时解包
		// PreparedStmtTX 执行 SAVEPOINT（MySQL 预编译协议不支持 SAVEPOINT）。
		tx := q.Begin()
		t.Cleanup(func() { _ = tx.Rollback() })
		if err := tx.Create(&gfc4User{ID: "sp-a", Name: "keep", Status: 1, Cnt: 1}); err != nil {
			t.Fatalf("保存点前插入: %v", err)
		}
		if err := tx.SavePoint("gfc4_sp_1"); err != nil {
			t.Fatalf("SavePoint: %v", err)
		}
		if err := tx.Create(&gfc4User{ID: "sp-b", Name: "drop", Status: 1, Cnt: 2}); err != nil {
			t.Fatalf("保存点后插入: %v", err)
		}
		if err := tx.RollbackTo("gfc4_sp_1"); err != nil {
			t.Fatalf("RollbackTo: %v", err)
		}
		// 回滚到保存点后事务仍可用：继续插入并提交
		if err := tx.Create(&gfc4User{ID: "sp-c", Name: "after-rollback", Status: 1, Cnt: 3}); err != nil {
			t.Fatalf("回滚后继续插入: %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 2 {
			t.Fatalf("保存点回滚后应仅 2 行(sp-a/sp-c), 实际 %d", n)
		}
		rows := gfc4FindAll(t, drv.Query())
		gfc4AssertIDs(t, "保存点存活行", rows, "sp-a,sp-c")
		// 保存点前的行内容未被波及
		if rows[0].Name != "keep" {
			t.Errorf("保存点前行应保持原值, 实际 %+v", rows[0])
		}
	})

	t.Run("WithContext_已取消ctx读写拒绝", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		// 已取消 ctx 的查询应报错（database/sql 在取连接前短路 ctx.Err）
		var rows []gfc4User
		err := q.WithContext(ctx).Model(&gfc4User{}).Find(&rows)
		if err == nil {
			t.Error("已取消 ctx 的 Find 应报错")
		} else if !errors.Is(err, context.Canceled) {
			t.Errorf("已取消 ctx 的 Find 错误应匹配 context.Canceled, 实际 %v", err)
		}
		// 已取消 ctx 的写入应报错且不落库
		err = q.WithContext(ctx).Exec("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "cx1", "no-write", int64(1), int64(1))
		if err == nil {
			t.Error("已取消 ctx 的 Exec 应报错")
		} else if !errors.Is(err, context.Canceled) {
			t.Errorf("已取消 ctx 的 Exec 错误应匹配 context.Canceled, 实际 %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 3 {
			t.Errorf("已取消 ctx 的写入不应落库, 实际 %d 行", n)
		}
	})

	t.Run("WithContext_有效ctx正常执行", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		ctx := context.Background()
		var rows []gfc4User
		if err := q.WithContext(ctx).Model(&gfc4User{}).Order("id").Find(&rows); err != nil {
			t.Fatalf("有效 ctx Find: %v", err)
		}
		gfc4AssertIDs(t, "有效 ctx 查询", rows, "a,b,c")
		if err := q.WithContext(ctx).Exec("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "ctx1", "with-ctx", int64(2), int64(50)); err != nil {
			t.Fatalf("有效 ctx Exec: %v", err)
		}
		if got := gfc4FindByID(t, q, "ctx1"); got.Name != "with-ctx" {
			t.Errorf("有效 ctx 写入应落库, 实际 %q", got.Name)
		}
	})

	t.Run("Debug_NoOp链可正常执行", func(t *testing.T) {
		gfc4SeedBasic(t, drv)
		// Debug 文档化 no-op：置于链头与链中均不影响执行（每次终结使用全新 dest）
		var rows []gfc4User
		if err := q.Debug().Model(&gfc4User{}).Order("id").Find(&rows); err != nil {
			t.Fatalf("Debug 链 Find: %v", err)
		}
		gfc4AssertIDs(t, "Debug 链头", rows, "a,b,c")
		var rows2 []gfc4User
		if err := q.Model(&gfc4User{}).Debug().Where("status = ?", int64(1)).Order("id").Find(&rows2); err != nil {
			t.Fatalf("Debug 链中 Find: %v", err)
		}
		gfc4AssertIDs(t, "Debug 链中", rows2, "a,b")
		// Debug 后写终结同样可用
		if err := q.Debug().Exec("INSERT INTO gfc4_users (id, name, status, cnt) VALUES (?, ?, ?, ?)", "dbg1", "debug-write", int64(2), int64(1)); err != nil {
			t.Fatalf("Debug 链 Exec: %v", err)
		}
		if n := gfc4CountUsers(t, drv); n != 4 {
			t.Errorf("Debug 写后应 4 行, 实际 %d", n)
		}
	})

	t.Run("Scopes_多作用域依次应用", func(t *testing.T) {
		intgMigrate(t, drv, []string{"gfc4_users"}, &gfc4User{})
		q := drv.Query()
		for _, u := range []*gfc4User{
			{ID: "a", Name: "alpha", Status: 1, Cnt: 10},
			{ID: "b", Name: "bravo", Status: 1, Cnt: 20},
			{ID: "c", Name: "charlie", Status: 2, Cnt: 30},
			{ID: "d", Name: "delta", Status: 1, Cnt: 40},
			{ID: "e", Name: "echo", Status: 2, Cnt: 50},
			{ID: "f", Name: "foxtrot", Status: 3, Cnt: 60},
		} {
			if err := q.Create(u); err != nil {
				t.Fatalf("种子 %s: %v", u.ID, err)
			}
		}
		// 三个作用域依次应用：status=1 AND cnt>=20 并按 id 排序 → b,d
		rows := gfc4FindAll(t, q.Scopes(gfc4ScopeStatus(1), gfc4ScopeCntGE(20), gfc4ScopeOrderID()))
		gfc4AssertIDs(t, "作用域组合", rows, "b,d")
		// 原链不被污染：不带作用域的全表仍 6 行
		all := gfc4FindAll(t, drv.Query())
		gfc4AssertIDs(t, "原链未被污染", all, "a,b,c,d,e,f")
		// 作用域片段可复用：同一 builder 应用到全新查询
		rows2 := gfc4FindAll(t, q.Scopes(gfc4ScopeStatus(2), gfc4ScopeOrderID()))
		gfc4AssertIDs(t, "复用 status=2", rows2, "c,e")
		// 作用域链上仍可继续追加 Where（AND 组合）
		rows3 := gfc4FindAll(t, q.Scopes(gfc4ScopeStatus(1)).Where("cnt >= ?", int64(40)))
		gfc4AssertIDs(t, "作用域后追加 Where", rows3, "d")

		// 双驱动一致（已修复对齐）：nil 作用域跳过——契约 ADV-08。驱动层
		// GormQuery.Scopes 现已过滤 nil 函数（修复前 nil 会在 gorm 执行期
		// executeScopes 处 panic），行为与 xorm 一致。
		var scoped []gfc4User
		if err := q.Scopes(nil).Model(&gfc4User{}).Order("id").Find(&scoped); err != nil {
			t.Fatalf("nil 作用域应被跳过: %v", err)
		}
		gfc4AssertIDs(t, "nil 作用域跳过（无过滤）", scoped, "a,b,c,d,e,f")
	})

	t.Run("Paginate_非法页码尺寸归一", func(t *testing.T) {
		gfc4SeedPaged(t, drv)
		// page<=0 → 1、size<=0 → 20（utils.PageUtil.Normalize）
		// 若未归一，size=0 会退化为无 LIMIT 返回全部 23 行，可分辨。
		var rows []gfc4User
		if err := q.Model(&gfc4User{}).Paginate(0, 0).Order("id").Find(&rows); err != nil {
			t.Fatalf("Paginate(0,0): %v", err)
		}
		if len(rows) != 20 {
			t.Fatalf("Paginate(0,0) 归一 1/20 应返回 20 行, 实际 %d", len(rows))
		}
		if rows[0].ID != "u01" {
			t.Errorf("归一首页应从 u01 开始, 实际 %q", rows[0].ID)
		}
		// 负 page/size 同样归一 1/20
		var rows2 []gfc4User
		if err := q.Model(&gfc4User{}).Paginate(-2, -7).Order("id").Find(&rows2); err != nil {
			t.Fatalf("Paginate(-2,-7): %v", err)
		}
		if len(rows2) != 20 || rows2[0].ID != "u01" {
			t.Errorf("Paginate(-2,-7) 归一 1/20 应返回 u01 起始的 20 行, 实际 %d 行首 %q",
				len(rows2), func() string {
					if len(rows2) > 0 {
						return rows2[0].ID
					}
					return ""
				}())
		}
	})

	t.Run("Paginate_中间页_尾页不足_越界空结果", func(t *testing.T) {
		gfc4SeedPaged(t, drv)
		// 首页 1..10
		var p1 []gfc4User
		if err := q.Model(&gfc4User{}).Paginate(1, 10).Order("id").Find(&p1); err != nil {
			t.Fatalf("第 1 页: %v", err)
		}
		gfc4AssertIDs(t, "第 1 页", p1, "u01,u02,u03,u04,u05,u06,u07,u08,u09,u10")
		// 中间页 11..20
		var p2 []gfc4User
		if err := q.Model(&gfc4User{}).Paginate(2, 10).Order("id").Find(&p2); err != nil {
			t.Fatalf("第 2 页: %v", err)
		}
		gfc4AssertIDs(t, "第 2 页", p2, "u11,u12,u13,u14,u15,u16,u17,u18,u19,u20")
		// 尾页不足一页：3 行
		var p3 []gfc4User
		if err := q.Model(&gfc4User{}).Paginate(3, 10).Order("id").Find(&p3); err != nil {
			t.Fatalf("第 3 页: %v", err)
		}
		gfc4AssertIDs(t, "第 3 页尾页", p3, "u21,u22,u23")
		// 越界页：空结果无错
		var p4 []gfc4User
		if err := q.Model(&gfc4User{}).Paginate(99, 10).Order("id").Find(&p4); err != nil {
			t.Fatalf("越界页: %v", err)
		}
		if len(p4) != 0 {
			t.Errorf("越界页应空结果, 实际 %d 行", len(p4))
		}
	})

	t.Run("Paginate_Count两阶段总数与分页", func(t *testing.T) {
		gfc4SeedPaged(t, drv)
		// 两阶段用法：先 Count 总数（gorm Count 自动剥离并还原 ORDER BY），
		// 再同构链分页取数（Paginate 每轮覆盖式重设 Offset/Limit）
		base := q.Model(&gfc4User{}).Where("status = ?", int64(1)).Order("id")
		var total int64
		if err := base.Count(&total); err != nil {
			t.Fatalf("Count: %v", err)
		}
		if total != 13 {
			t.Fatalf("status=1 总数应 13, 实际 %d", total)
		}
		var pages [][]gfc4User
		for page := 1; page <= 4; page++ {
			var rows []gfc4User
			if err := base.Paginate(page, 5).Find(&rows); err != nil {
				t.Fatalf("第 %d 页: %v", page, err)
			}
			pages = append(pages, rows)
			for _, r := range rows {
				if r.Status != 1 {
					t.Errorf("分页结果混入 status=%d 行 %s", r.Status, r.ID)
				}
			}
		}
		gfc4AssertIDs(t, "两阶段第 1 页", pages[0], "u01,u02,u03,u04,u05")
		gfc4AssertIDs(t, "两阶段第 2 页", pages[1], "u06,u07,u08,u09,u10")
		gfc4AssertIDs(t, "两阶段第 3 页(尾页)", pages[2], "u11,u12,u13")
		if len(pages[3]) != 0 {
			t.Errorf("两阶段第 4 页应空, 实际 %d 行", len(pages[3]))
		}
		sum := len(pages[0]) + len(pages[1]) + len(pages[2]) + len(pages[3])
		if int64(sum) != total {
			t.Errorf("分页行数总和 %d 应等于 Count 总数 %d", sum, total)
		}
	})

	t.Run("PG专属_事务内DDL可回滚", func(t *testing.T) {
		if dialect != "postgres" {
			// 差异固化：MySQL DDL 隐式提交（CREATE TABLE 会提交整个事务），
			// 事务内 DDL 回滚语义不成立，分支跳过（PG DDL 事务性专属）
			t.Skip("PG 专属：MySQL 的 DDL 隐式提交语义不同，分支跳过")
		}
		gfc4DropSchemaTable(t, drv, "gfc4_tx_ddl")
		t.Cleanup(func() { gfc4DropSchemaTable(t, drv, "gfc4_tx_ddl") })
		// 未提交的 DDL 对其他连接不可见（MVCC），可见性断言必须在同一事务
		// 会话内进行（tx 链复用会话），终态存在性用外部连接检查。
		inTxExists := func(tx contracts.Query) int64 {
			var n int64
			if err := tx.Raw(`SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'gfc4_tx_ddl'`).Scan(&n); err != nil {
				t.Fatalf("事务内查 pg_tables: %v", err)
			}
			return n
		}
		exists := func() int64 {
			return gfc4CountRaw(t, drv, `SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'gfc4_tx_ddl'`)
		}
		// 事务内建表后回滚：事务会话内可见，提交前回滚则外部表不存在（PG DDL 事务性）
		tx := q.Begin()
		t.Cleanup(func() { _ = tx.Rollback() })
		if err := tx.Exec(`CREATE TABLE gfc4_tx_ddl (id varchar(16) PRIMARY KEY)`); err != nil {
			t.Fatalf("事务内建表: %v", err)
		}
		if n := inTxExists(tx); n != 1 {
			t.Fatalf("事务内应看到未提交的建表, 实际 %d", n)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
		if exists() != 0 {
			t.Errorf("回滚后表应不存在, 实际 %d", exists())
		}
		// 事务内建表后提交：外部可见表保留
		tx2 := q.Begin()
		t.Cleanup(func() { _ = tx2.Rollback() })
		if err := tx2.Exec(`CREATE TABLE gfc4_tx_ddl (id varchar(16) PRIMARY KEY)`); err != nil {
			t.Fatalf("第二次事务内建表: %v", err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if exists() != 1 {
			t.Errorf("提交后表应保留, 实际 %d", exists())
		}
	})

	t.Run("PG专属_GetSchema与Schema限定原生SQL", func(t *testing.T) {
		if dialect != "postgres" {
			// 差异固化：MySQL 的 Schema 概念对应 DATABASE 切换（跨库 DDL/权限
			// 语义不同），schema 限定原生 SQL 场景由 PG 分支验证，分支跳过
			t.Skip("PG 专属：MySQL 的 schema 语义为 DATABASE，分支跳过")
		}
		// 默认链无 schema 上下文
		if got := q.GetSchema(); got != "" {
			t.Errorf("默认链 GetSchema 应为空串, 实际 %q", got)
		}
		// 建租户 schema + 表 + 种子（原生 DDL/DML）
		intgMustExec(t, drv, `CREATE SCHEMA IF NOT EXISTS gfc4_tenant`)
		t.Cleanup(func() { _ = drv.Query().Exec(`DROP SCHEMA IF EXISTS gfc4_tenant CASCADE`) })
		intgMustExec(t, drv, `CREATE TABLE gfc4_tenant.gfc4_sch_users (id varchar(16) PRIMARY KEY, name varchar(64))`)
		intgMustExec(t, drv, `INSERT INTO gfc4_tenant.gfc4_sch_users (id, name) VALUES ('s1', 'sch-row')`)

		// Schema() 后 GetSchema 返回租户名，且原链不被污染（链不可变）
		sq := q.Schema("gfc4_tenant")
		if got := sq.GetSchema(); got != "gfc4_tenant" {
			t.Errorf("Schema() 后 GetSchema 应 gfc4_tenant, 实际 %q", got)
		}
		if got := q.GetSchema(); got != "" {
			t.Errorf("Schema() 不应污染原链, 原链 GetSchema 实际 %q", got)
		}
		// ADV-07/RAW-05 典型用法：GetSchema() 拼接 schema 限定的原生 SQL 表名
		var n int64
		if err := sq.Raw("SELECT count(*) FROM " + sq.GetSchema() + ".gfc4_sch_users").Scan(&n); err != nil {
			t.Fatalf("schema 限定原生 SQL: %v", err)
		}
		if n != 1 {
			t.Errorf("gfc4_tenant.gfc4_sch_users 应 1 行, 实际 %d", n)
		}
		// Schema 链上带参原生 SQL 依然正确（绑定 + schema 前缀并存）
		var name string
		if err := sq.Raw("SELECT name FROM "+sq.GetSchema()+".gfc4_sch_users WHERE id = ?", "s1").Scan(&name); err != nil {
			t.Fatalf("schema 限定带参查询: %v", err)
		}
		if name != "sch-row" {
			t.Errorf("schema 限定查询应回收 sch-row, 实际 %q", name)
		}
	})
}

// ── 作用域构造器（闭包复用查询片段）─────────────────────────────────

// gfc4ScopeStatus 构造 status 等值作用域。
func gfc4ScopeStatus(status int64) func(contracts.Query) contracts.Query {
	return func(qq contracts.Query) contracts.Query {
		return qq.Where("status = ?", status)
	}
}

// gfc4ScopeCntGE 构造 cnt 下界作用域。
func gfc4ScopeCntGE(min int64) func(contracts.Query) contracts.Query {
	return func(qq contracts.Query) contracts.Query {
		return qq.Where("cnt >= ?", min)
	}
}

// gfc4ScopeOrderID 构造主键升序作用域。
func gfc4ScopeOrderID() func(contracts.Query) contracts.Query {
	return func(qq contracts.Query) contracts.Query {
		return qq.Order("id")
	}
}

// gfc4DropSchemaTable 幂等删除 PG public schema 下的表（DDL 回滚用例专用）。
func gfc4DropSchemaTable(t *testing.T, drv *GormDriver, table string) {
	t.Helper()
	if err := drv.Query().Exec("DROP TABLE IF EXISTS " + table); err != nil {
		t.Fatalf("DROP TABLE %s: %v", table, err)
	}
}

// ── 入口（成对，各自调共享 runner；PG/MySQL 用真实 DSN）──────────────

func TestFullCovRawTx_PG(t *testing.T) {
	runFullCovRawTx(t, newIntgPG(t), "postgres")
}

func TestFullCovRawTx_MySQL(t *testing.T) {
	runFullCovRawTx(t, newIntgMySQL(t), "mysql")
}
