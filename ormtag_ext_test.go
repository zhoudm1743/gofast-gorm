package gormdriver

// ormtag_ext_test.go：ext tag（gorm 专属能力）在 gormdriver 侧生效的测试（设计文档 4.3/11.16）。
// 覆盖：check 约束拒绝非法行、索引 where 选项建出部分索引、migration:false、
// timePrecision:milli、未知 ext key 不报错、perm:update、autoIncrementIncrement。

import (
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
	gormSchema "gorm.io/gorm/schema"
)

// patchedSchema 取模型 patch 后的 gorm schema（patch 证据断言用）。
func patchedSchema(t *testing.T, drv *GormDriver, model any) *gormSchema.Schema {
	t.Helper()
	if err := drv.ensurePatched(model, drv.db); err != nil {
		t.Fatalf("ensurePatched: %v", err)
	}
	stmt := &gorm.Statement{DB: drv.db}
	if err := stmt.Parse(model); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return stmt.Schema
}

// ── check 约束 ───────────────────────────────────────────────────────

type extCheckModel struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Age int    `orm:"'age' default(0)" ext:"check:age >= 0"`
}

func TestExt_CheckConstraint(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&extCheckModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	ddl := tableDDL(t, drv, "ext_check_models")
	assertContains(t, "DDL", ddl, "CHECK (age >= 0)")

	q := drv.Query()
	if err := q.Create(&extCheckModel{ID: "ok1", Age: 5}); err != nil {
		t.Fatalf("合法行 Create 不应报错: %v", err)
	}
	if err := q.Create(&extCheckModel{ID: "bad1", Age: -1}); err == nil {
		t.Fatal("违反 CHECK 约束的行应被数据库拒绝")
	}
}

// ── 索引完整选项（where 部分索引，覆盖 orm index 同维度） ──────────────

type extIndexModel struct {
	ID     string `orm:"pk varchar(16) 'id'"`
	Status int    `orm:"'status' index(idx_orm_should_override)" ext:"index:idx_adult,where:status > 0"`
}

func TestExt_IndexWhereOption(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&extIndexModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	idx := indexDDLs(t, drv, "ext_index_models")
	t.Logf("ext index DDL: %s", idx)
	assertContains(t, "index DDL", idx, "idx_adult")
	assertContains(t, "index DDL", idx, "WHERE status > 0")
	// ext 覆盖 orm 同维度：orm 注入的同名旧索引不应存在
	assertNotContains(t, "index DDL", idx, "idx_orm_should_override")
}

// ── migration:false：不建列但读写正常（表由外部 DDL 建好） ──────────────

type extMigrationModel struct {
	ID     string `orm:"pk varchar(16) 'id'"`
	Remark string `orm:"varchar(100) 'remark'" ext:"migration:false"`
}

func TestExt_MigrationFalse(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&extMigrationModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	ddl := tableDDL(t, drv, "ext_migration_models")
	assertNotContains(t, "DDL", ddl, "remark")

	// 表由外部 DDL 建好（模拟存量列）
	if err := drv.Query().Exec("ALTER TABLE ext_migration_models ADD COLUMN remark varchar(100)"); err != nil {
		t.Fatalf("外部 DDL: %v", err)
	}

	q := drv.Query()
	if err := q.Create(&extMigrationModel{ID: "m1", Remark: "kept"}); err != nil {
		t.Fatalf("Create（保留读写）: %v", err)
	}
	var got extMigrationModel
	if err := q.First(&got, "id = ?", "m1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Remark != "kept" {
		t.Errorf("migration:false 字段读写应正常, 实际 %q", got.Remark)
	}
}

// ── timePrecision:milli：created 填 unix 毫秒 ─────────────────────────

type extTimeModel struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	CreatedAt int64  `orm:"created 'created_at'" ext:"timePrecision:milli"`
	UpdatedAt int64  `orm:"updated 'updated_at'"`
}

func TestExt_TimePrecisionMilli(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&extTimeModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	q := drv.Query()
	before := time.Now().UnixMilli()
	if err := q.Create(&extTimeModel{ID: "t1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	after := time.Now().UnixMilli()

	var got extTimeModel
	if err := q.First(&got, "id = ?", "t1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.CreatedAt < before || got.CreatedAt > after {
		t.Errorf("timePrecision:milli 的 created 应为 unix 毫秒（%d ∈ [%d,%d]）, 实际 %d", got.CreatedAt, before, after, got.CreatedAt)
	}
	// 未声明精度的 updated 恒为 unix 秒
	nowSec := time.Now().Unix()
	if got.UpdatedAt > nowSec+1 || got.UpdatedAt < nowSec-5 {
		t.Errorf("updated 应为 unix 秒, 实际 %d（now=%d）", got.UpdatedAt, nowSec)
	}
}

// ── 未知 ext key 不报错（11.16） ─────────────────────────────────────

type extUnknownModel struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Val string `orm:"varchar(32) 'val'" ext:"someFutureKey:xyz,another:1"`
}

func TestExt_UnknownKeyNoError(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&extUnknownModel{}); err != nil {
		t.Fatalf("未知 ext key 不应报错: %v", err)
	}
	q := drv.Query()
	if err := q.Create(&extUnknownModel{ID: "u1", Val: "v"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	var got extUnknownModel
	if err := q.First(&got, "id = ?", "u1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Val != "v" {
		t.Errorf("未知 ext key 不影响读写, 实际 %+v", got)
	}
}

// ── perm:update / autoIncrementIncrement patch 元数据断言 ─────────────

type extPermModel struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Code string `orm:"varchar(32) 'code'" ext:"perm:update"`
	Note string `orm:"varchar(32) 'note'" ext:"perm:create"`
	Seq  int64  `orm:"'seq'" ext:"autoIncrementIncrement:5"`
}

func TestExt_PermAndAutoIncrementIncrement(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&extPermModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}

	sch := patchedSchema(t, drv, &extPermModel{})
	code := sch.FieldsByName["Code"]
	note := sch.FieldsByName["Note"]
	seq := sch.FieldsByName["Seq"]
	if code == nil || note == nil || seq == nil {
		t.Fatal("字段定位失败")
	}
	// perm:update：仅 Update 写入（Creatable=false, Updatable=true）
	if code.Creatable {
		t.Error("perm:create/update 组合: Code 应 Creatable=false")
	}
	if !code.Updatable {
		t.Error("perm:update: Code 应 Updatable=true")
	}
	// perm:create：仅 Create 写入
	if !note.Creatable || note.Updatable {
		t.Errorf("perm:create: Note 应 Creatable=true/Updatable=false, 实际 %v/%v", note.Creatable, note.Updatable)
	}
	// autoIncrementIncrement
	if seq.AutoIncrementIncrement != 5 {
		t.Errorf("autoIncrementIncrement 期望 5, 实际 %d", seq.AutoIncrementIncrement)
	}
}

// ── 数据库列名含 special 维度时 ext check 表达式按原文落地（含逗号场景由驱动切分）──

type extCheckNamed struct {
	ID    string `orm:"pk varchar(16) 'id'"`
	Score int    `orm:"'score' default(0)" ext:"check:score BETWEEN 0 AND 100"`
}

func TestExt_CheckExpression(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&extCheckNamed{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	ddl := tableDDL(t, drv, "ext_check_nameds")
	if !strings.Contains(ddl, "CHECK") || !strings.Contains(ddl, "score BETWEEN 0 AND 100") {
		t.Errorf("CHECK 表达式应按原文落地, 实际: %s", ddl)
	}
}
