//go:build integration

package gormdriver

// mysql_integration_test.go：统一 orm tag 体系的 MySQL 集成测试（设计文档
// §11.15/§11.16/§11.18，U18）。依赖真实数据库，默认不运行。
// 运行方式：设置环境变量 GOFAST_TEST_MYSQL_DSN 后执行 `go test -tags integration`。
// 示例：
//
//	GOFAST_TEST_MYSQL_DSN="user:pass@tcp(host:3306)/db" \
//	  go test -tags integration ./database/drivers/gormdriver/
//
// 与 pg_integration_test.go 同包编译：Preload 全套 runner（runIntgPreloadMatrix）
// 与全套 orm tag 模型复用 pg 文件中的定义；本文件专注 MySQL 方言特有断言
// （SHOW CREATE TABLE / information_schema：类型映射、unsigned、comment、CHECK、
// 联合唯一/联合索引）与 §11.16/§11.14/§11.18 的 MySQL 侧行为。

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// newIntgMySQL 走 NewGormDriver 真实配置链路创建 mysql 驱动；未设置环境变量时跳过。
func newIntgMySQL(t *testing.T) *GormDriver {
	t.Helper()
	dsn := os.Getenv("GOFAST_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("未设置 GOFAST_TEST_MYSQL_DSN 环境变量，跳过 mysql 集成测试")
	}
	drv, err := NewGormDriver(contracts.ConnectionConfig{
		Driver: "gormdriver",
		Engine: "mysql",
		DSN:    dsn,
	}, intgNopLog{t: t})
	if err != nil {
		t.Fatalf("创建 gorm mysql 驱动失败: %v", err)
	}
	t.Cleanup(func() { _ = drv.Close() })
	return drv
}

// intgMySQLShowCreate 读 SHOW CREATE TABLE 原文（MySQL 方言 DDL 断言用）。
func intgMySQLShowCreate(t *testing.T, drv *GormDriver, table string) string {
	t.Helper()
	var tbl, ddl string
	if err := drv.Query().Raw("SHOW CREATE TABLE "+table).Row().Scan(&tbl, &ddl); err != nil {
		t.Fatalf("SHOW CREATE TABLE %s 失败: %v", table, err)
	}
	return ddl
}

// intgMySQLColumn 读 information_schema 单列元数据（MySQL 驱动返回大写键）。
func intgMySQLColumn(t *testing.T, drv *GormDriver, table, column string) map[string]any {
	t.Helper()
	var rows []map[string]any
	err := drv.Query().Raw(
		`SELECT column_type, is_nullable, column_default, column_comment
		 FROM information_schema.columns
		 WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		table, column).ScanMap(&rows)
	if err != nil || len(rows) != 1 {
		t.Fatalf("读取列 %s.%s 元数据失败 rows=%d err=%v", table, column, len(rows), err)
	}
	return rows[0]
}

// intgMySQLShowCreateNoTicks 读 SHOW CREATE TABLE 原文并去除反引号（断言用）。
func intgMySQLShowCreateNoTicks(t *testing.T, drv *GormDriver, table string) string {
	t.Helper()
	return strings.ReplaceAll(intgMySQLShowCreate(t, drv, table), "`", "")
}

// IntgUnsignedModel unsigned 数值列模型（仅 MySQL 迁移——PG 无无符号语义，
// "int unsigned" 类型透传在 PG 下是非法 DDL）。
type IntgUnsignedModel struct {
	ID    string `orm:"pk varchar(16) 'id'"`
	Score uint32 `orm:"unsigned int 'score' default(0)"`
}

func (IntgUnsignedModel) TableName() string { return "intg_unsigned" }

// TestMySQLOrmTag_DDLFullMatrix（§11.15/§11.18）MySQL 方言 DDL 等价：
// 类型映射（varchar/decimal/bigint/bool）、notnull/default、comment、CHECK、
// 联合唯一/联合索引、unsigned，以及 ext autoIncrementIncrement 差异说明。
func TestMySQLOrmTag_DDLFullMatrix(t *testing.T) {
	drv := newIntgMySQL(t)
	intgMigrate(t, drv, []string{"intg_ddl_models"}, &IntgDDLModel{})

	// varchar(100) → varchar(100)
	name := intgMySQLColumn(t, drv, "intg_ddl_models", "name")
	if fmt.Sprint(name["COLUMN_TYPE"]) != "varchar(100)" {
		t.Errorf("varchar(100) 应映射 varchar(100), 实际 %v", name["COLUMN_TYPE"])
	}
	if name["IS_NULLABLE"] != "NO" {
		t.Errorf("notnull 列应 NOT NULL, 实际 is_nullable=%v", name["IS_NULLABLE"])
	}
	intgAssertContains(t, "name 默认值", fmt.Sprint(name["COLUMN_DEFAULT"]), "anon")

	// decimal(10,2) 精度
	amount := intgMySQLColumn(t, drv, "intg_ddl_models", "amount")
	if fmt.Sprint(amount["COLUMN_TYPE"]) != "decimal(10,2)" {
		t.Errorf("decimal(10,2) 应映射 decimal(10,2), 实际 %v", amount["COLUMN_TYPE"])
	}
	if amount["IS_NULLABLE"] != "NO" {
		t.Errorf("amount notnull 应 NOT NULL, 实际 %v", amount["IS_NULLABLE"])
	}

	// bigint / bool（MySQL bool 即 tinyint(1)）类型映射
	if big := intgMySQLColumn(t, drv, "intg_ddl_models", "big_num"); big["COLUMN_TYPE"] != "bigint" {
		t.Errorf("bigint 应映射 bigint, 实际 %v", big["COLUMN_TYPE"])
	}
	if act := intgMySQLColumn(t, drv, "intg_ddl_models", "active"); act["COLUMN_TYPE"] != "tinyint(1)" {
		t.Errorf("bool 应映射 tinyint(1), 实际 %v", act["COLUMN_TYPE"])
	}

	// comment('备注列')：差异固化——gorm v1.31.1 核心迁移器 FullDataTypeOf 不渲染
	// 列注释，仅 postgres 驱动额外发 COMMENT ON COLUMN；MySQL 下 comment patch
	// 生效（schema.Field.Comment 已带值）但 DDL 落地缺失，注释为空。如需 MySQL
	// 列注释需外部 DDL 维护（该差异应随 gorm 升级复查）。
	if cmt := fmt.Sprint(intgMySQLColumn(t, drv, "intg_ddl_models", "remark")["COLUMN_COMMENT"]); cmt != "" {
		t.Errorf("MySQL 列注释当前应为空（gorm mysql 迁移器限制）, 实际 %q", cmt)
	}

	// CHECK 约束存在（MySQL 8：information_schema.check_constraints / SHOW CREATE TABLE）
	ddl := intgMySQLShowCreateNoTicks(t, drv, "intg_ddl_models")
	t.Logf("SHOW CREATE TABLE intg_ddl_models:\n%s", ddl)
	intgAssertContains(t, "CHECK", strings.ToUpper(ddl), "CHECK")
	intgAssertContains(t, "CHECK 表达式", ddl, "age >= 0")

	// 联合唯一 unique(uk_intg_tenant) 与联合索引 index(idx_intg_org) 落地
	intgAssertContains(t, "联合唯一", ddl, "uk_intg_tenant")
	intgAssertContains(t, "联合索引", ddl, "idx_intg_org")

	// unsigned：仅 MySQL 生效（差异固化：PG 迁移不含本模型）
	intgMigrate(t, drv, []string{"intg_unsigned"}, &IntgUnsignedModel{})
	score := intgMySQLColumn(t, drv, "intg_unsigned", "score")
	if !strings.Contains(fmt.Sprint(score["COLUMN_TYPE"]), "unsigned") {
		t.Errorf("unsigned int 应带 UNSIGNED, 实际 %v", score["COLUMN_TYPE"])
	}

	// ext autoIncrementIncrement:5 → MySQL AUTO_INCREMENT 步长为会话级变量
	//（auto_increment_increment，SHOW VARIABLES 作用域为会话且不影响建表 DDL），
	// 表选项之外的会话行为无法经表结构断言——此处仅固化"迁移与读写正常、
	// SHOW CREATE TABLE 无异常"（步长 patch 元数据已在单测 TestExt_PermAndAutoIncrementIncrement 断言）。
	var n int64
	if err := drv.Query().Raw("SELECT count(*) FROM intg_ddl_models").Scan(&n); err != nil {
		t.Errorf("autoIncrementIncrement 模型读写应正常: %v", err)
	}

	// 幂等：重复 AutoMigrate 不报错
	if err := drv.AutoMigrate(&IntgDDLModel{}); err != nil {
		t.Errorf("重复 AutoMigrate 应幂等不报错: %v", err)
	}
}

// TestMySQLOrmTag_DuplicateKeyMapping（§11.14/§11.18）MySQL Error 1062 映射
// contracts.ErrDuplicatedKey。
func TestMySQLOrmTag_DuplicateKeyMapping(t *testing.T) {
	drv := newIntgMySQL(t)
	intgMigrate(t, drv, []string{"intg_dups"}, &IntgDupModel{})

	q := drv.Query()
	if err := q.Create(&IntgDupModel{ID: "d1", Email: "dup@mysql.dev"}); err != nil {
		t.Fatalf("首次插入: %v", err)
	}
	err := q.Create(&IntgDupModel{ID: "d2", Email: "dup@mysql.dev"})
	if err == nil {
		t.Fatal("unique 列重复插入应报错（MySQL Error 1062）")
	}
	if !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("唯一冲突应映射 contracts.ErrDuplicatedKey, 实际: %v", err)
	}
}

// TestMySQLOrmTag_LockClauses（§11.10）FOR UPDATE / FOR SHARE 子句在 MySQL
// 下真实可用（并发互斥场景由 PG 用例覆盖，这里固化 SQL 生成与执行不报错）。
func TestMySQLOrmTag_LockClauses(t *testing.T) {
	drv := newIntgMySQL(t)
	intgMigrate(t, drv, []string{"intg_locks"}, &IntgLockModel{})
	intgMustExec(t, drv, "INSERT INTO intg_locks (id, cnt) VALUES ('m1', 5)")

	var rows []IntgLockModel
	if err := drv.Query().Model(&IntgLockModel{}).Lock(contracts.LockForUpdate).Find(&rows); err != nil {
		t.Fatalf("Lock(LockForUpdate) 真实执行不应报错: %v", err)
	}
	if len(rows) != 1 || rows[0].Cnt != 5 {
		t.Errorf("FOR UPDATE 查询结果异常: %+v", rows)
	}
	if err := drv.Query().Model(&IntgLockModel{}).Lock(contracts.LockShareMode).Find(&rows); err != nil {
		t.Fatalf("Lock(LockShareMode) 真实执行不应报错: %v", err)
	}
}

// TestMySQLOrmTag_ExtMatrix（§11.16）MySQL 侧：check 拒绝非法行 /
// migration:false 不建列但读写正常 / timePrecision:milli 填毫秒。
func TestMySQLOrmTag_ExtMatrix(t *testing.T) {
	drv := newIntgMySQL(t)

	// check：SHOW CREATE TABLE 有 CHECK + 非法行被数据库拒绝
	intgMigrate(t, drv, []string{"intg_checks"}, &IntgCheckModel{})
	intgAssertContains(t, "CHECK 约束", intgMySQLShowCreateNoTicks(t, drv, "intg_checks"), "age >= 0")
	if err := drv.Query().Create(&IntgCheckModel{ID: "ok1", Age: 5}); err != nil {
		t.Fatalf("合法行 Create 不应报错: %v", err)
	}
	if err := drv.Query().Create(&IntgCheckModel{ID: "bad1", Age: -1}); err == nil {
		t.Error("违反 CHECK 约束的行应被数据库拒绝")
	}

	// migration:false：不建列；外部 DDL 建列后读写正常
	intgMigrate(t, drv, []string{"intg_migs"}, &IntgMigModel{})
	var n int64
	if err := drv.Query().Raw(
		`SELECT count(*) FROM information_schema.columns
		 WHERE table_schema = DATABASE() AND table_name = 'intg_migs' AND column_name = 'remark'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("migration:false 不应建列 remark, count=%d err=%v", n, err)
	}
	intgMustExec(t, drv, "ALTER TABLE intg_migs ADD COLUMN remark varchar(100)")
	if err := drv.Query().Create(&IntgMigModel{ID: "m1", Remark: "kept"}); err != nil {
		t.Fatalf("Create（保留读写）: %v", err)
	}
	var got IntgMigModel
	if err := drv.Query().First(&got, "id = ?", "m1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Remark != "kept" {
		t.Errorf("migration:false 字段读写应正常, 实际 %q", got.Remark)
	}

	// timePrecision:milli：created 填 unix 毫秒（量级 1e12+）
	intgMigrate(t, drv, []string{"intg_times"}, &IntgTimeModel{})
	before := time.Now().UnixMilli()
	if err := drv.Query().Create(&IntgTimeModel{ID: "t1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	after := time.Now().UnixMilli()
	var tm IntgTimeModel
	if err := drv.Query().First(&tm, "id = ?", "t1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if tm.CreatedAt < 1e12 {
		t.Errorf("timePrecision:milli 的 created 应为毫秒量级（>=1e12）, 实际 %d", tm.CreatedAt)
	}
	if tm.CreatedAt < before || tm.CreatedAt > after {
		t.Errorf("created 应为 unix 毫秒（%d ∈ [%d,%d]）, 实际 %d", tm.CreatedAt, before, after, tm.CreatedAt)
	}
}

// TestMySQLOrmTag_OptimisticLock（§11.18）orm:"version" 模型 MySQL 真实方言冒烟。
func TestMySQLOrmTag_OptimisticLock(t *testing.T) {
	drv := newIntgMySQL(t)
	intgMigrate(t, drv, []string{"intg_vers"}, &IntgVerModel{})

	q := drv.Query()
	doc := &IntgVerModel{ID: "v1", Title: "t1"}
	if err := q.Create(doc); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if doc.Version != 1 {
		t.Fatalf("插入后 version 应置 1, 实际 %d", doc.Version)
	}

	got := &IntgVerModel{}
	if err := q.Model(&IntgVerModel{}).Where("id = ?", "v1").Take(got); err != nil {
		t.Fatalf("Take: %v", err)
	}
	got.Title = "t2"
	res := q.Model(&IntgVerModel{}).Where("id = ?", "v1").UpdatesResult(got)
	if res.Error != nil || res.RowsAffected != 1 {
		t.Fatalf("struct 更新期望 1 行, 实际 rows=%d err=%v", res.RowsAffected, res.Error)
	}
	if got.Version != 2 {
		t.Fatalf("struct 更新后 version 应回填 +1 为 2, 实际 %d", got.Version)
	}

	// 陈旧版本（库中已是 2）：0 行命中不报错，数据不变
	stale := &IntgVerModel{ID: "v1", Title: "stale", Version: 1}
	res = q.Model(&IntgVerModel{}).Where("id = ?", "v1").UpdatesResult(stale)
	if res.Error != nil {
		t.Fatalf("陈旧版本更新不应报错: %v", res.Error)
	}
	if res.RowsAffected != 0 {
		t.Fatalf("陈旧版本更新应命中 0 行, 实际 %d", res.RowsAffected)
	}
	var unchanged IntgVerModel
	if err := q.Model(&IntgVerModel{}).Where("id = ?", "v1").Take(&unchanged); err != nil {
		t.Fatal(err)
	}
	if unchanged.Version != 2 || unchanged.Title != "t2" {
		t.Errorf("陈旧版本更新不应改动数据: %+v", unchanged)
	}
}

// TestMySQLOrmTag_PreloadMatrix（§11.18）MySQL 上 has-many / many2many 全套
// 预加载（与 PG 共用 runner）。
func TestMySQLOrmTag_PreloadMatrix(t *testing.T) {
	drv := newIntgMySQL(t)
	runIntgPreloadMatrix(t, drv)
}
