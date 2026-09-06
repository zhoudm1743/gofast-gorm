package gormdriver

// ormtag_patch_test.go：统一 orm tag schema patch 的证据测试（设计文档 U10）。
// 覆盖：全维度 DDL 断言（sqlite_master）、DryRun SQL 断言、自定义列名查询、
// 忽略/只写/只读字段、extends('前缀')、禁用 token 报错、双 tag 兼容、
// FirstOrCreate/FindInBatches/Save upsert 回落在 orm tag 模型上可用。

import (
	"errors"
	"strings"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"gorm.io/gorm"
)

// ── 全维度 orm tag 模型（pk/类型/列名/notnull/unique/联合唯一/联合索引/
//    default/->/<-/-/created/updated/version 全覆盖） ─────────────────────

type ormAllModel struct {
	ID        string  `orm:"pk varchar(16) 'id'"`
	UserName  string  `orm:"varchar(100) 'user_name' notnull"`
	Email     string  `orm:"varchar(100) 'email' unique"`
	Status    int     `orm:"'status' default(0)"`
	Amount    float64 `orm:"decimal(10,2) 'amount' notnull"`
	OrgID     string  `orm:"varchar(16) 'org_id' index(idx_org_status)"`
	Phone     string  `orm:"varchar(20) 'phone' index(idx_org_status)"`
	TenantID  string  `orm:"varchar(16) 'tenant_id' unique(uk_tenant_phone)"`
	Mobile    string  `orm:"varchar(20) 'mobile' unique(uk_tenant_phone)"`
	Password  string  `orm:"varchar(100) 'password' ->"` // 只写（落库不读回）
	Token     string  `orm:"varchar(64) 'token' <-"`     // 只读（读回不写入）
	Internal  string  `orm:"-"`                          // 忽略（不建列/不读/不写）
	CreatedAt int64   `orm:"created 'created_at'"`
	UpdatedAt int64   `orm:"updated 'updated_at'"`
	Version   int64   `orm:"version 'version'"`
}

func (m *ormAllModel) AutoGenerateID() {}

func newOrmAllDriver(t *testing.T) *GormDriver {
	t.Helper()
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&ormAllModel{}); err != nil {
		t.Fatalf("orm tag 模型 AutoMigrate 失败: %v", err)
	}
	return drv
}

// tableDDL 读 sqlite_master 中建表 DDL。
func tableDDL(t *testing.T, drv *GormDriver, table string) string {
	t.Helper()
	var ddl string
	if err := drv.Query().Raw("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&ddl); err != nil {
		t.Fatalf("读取 %s 建表 DDL 失败: %v", table, err)
	}
	return ddl
}

// indexDDLs 读 sqlite_master 中某表全部索引 DDL。
func indexDDLs(t *testing.T, drv *GormDriver, table string) string {
	t.Helper()
	var ddls []string
	if err := drv.Query().Raw("SELECT coalesce(sql, '') FROM sqlite_master WHERE type = 'index' AND tbl_name = ?", table).Scan(&ddls); err != nil {
		t.Fatalf("读取 %s 索引 DDL 失败: %v", table, err)
	}
	return strings.Join(ddls, "\n")
}

func assertContains(t *testing.T, name, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("%s 应包含 %q\n实际: %s", name, needle, haystack)
	}
}

func assertNotContains(t *testing.T, name, haystack, needle string) {
	t.Helper()
	if strings.Contains(haystack, needle) {
		t.Errorf("%s 不应包含 %q\n实际: %s", name, needle, haystack)
	}
}

// 1. patch 证据：AutoMigrate 后逐项断言 DDL（列名/类型/非空/默认值/唯一/索引）。
// 说明：sqlite 方言标识符以反引号引用；列级 unique 经 ParseUniqueConstraints
// 以表级 CONSTRAINT ... UNIQUE (...) 落地（gorm v1.31.1 FullDataTypeOf 无内联 UNIQUE）。
func TestOrmTag_AutoMigrate_DDL(t *testing.T) {
	drv := newOrmAllDriver(t)
	ddl := tableDDL(t, drv, "orm_all_models")
	t.Logf("orm_all_models DDL: %s", ddl)

	// 列名 + 类型（自定义列名 'user_name' 命中，类型原文透传/映射）
	assertContains(t, "DDL", ddl, "`id` varchar(16)")
	assertContains(t, "DDL", ddl, "`user_name` varchar(100) NOT NULL")
	assertContains(t, "DDL", ddl, "`status` integer DEFAULT 0")
	assertContains(t, "DDL", ddl, "`amount` decimal(10,2) NOT NULL")
	// 只写/只读字段仍建列
	assertContains(t, "DDL", ddl, "`password` varchar(100)")
	assertContains(t, "DDL", ddl, "`token` varchar(64)")
	// created/updated 时间列
	assertContains(t, "DDL", ddl, "`created_at`")
	assertContains(t, "DDL", ddl, "`updated_at`")
	// unique：表级 CONSTRAINT UNIQUE（email / tenant_id+mobile 的索引形态见下方 UNIQUE INDEX）
	assertContains(t, "DDL", ddl, "UNIQUE (`email`)")
	// 忽略字段不建列
	assertNotContains(t, "DDL", ddl, "internal")
	// 主键（PRIMARY KEY 由 PrimaryFields 生成）
	assertContains(t, "DDL", ddl, `PRIMARY KEY`)

	// 索引双写 patch 证据：v1.2 关键修正——TagSettings 闸门 + Field.Tag 注入
	idx := indexDDLs(t, drv, "orm_all_models")
	t.Logf("indexes DDL:\n%s", idx)
	assertContains(t, "index DDL", idx, `idx_org_status`)
	assertContains(t, "index DDL", idx, "`org_id`")
	assertContains(t, "index DDL", idx, "`phone`")
	assertContains(t, "index DDL", idx, `uk_tenant_phone`)
	assertContains(t, "unique index DDL", strings.ToUpper(idx), `UNIQUE INDEX`)

	// version 字段：不做 schema patch，仅建普通列
	assertContains(t, "DDL", ddl, "`version`")
}

// 1b. patch 证据：DryRun SQL 断言（复用 query_regression_test.go 手法）。
func TestOrmTag_DryRunSQL(t *testing.T) {
	drv := newOrmAllDriver(t)
	q := drv.Query().Model(&ormAllModel{}).
		Where("user_name = ?", "alice").
		Order("user_name desc")
	stmt := q.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]ormAllModel{}).Statement
	sqlText := stmt.SQL.String()
	t.Logf("DryRun SQL: %s", sqlText)

	assertContains(t, "SQL", sqlText, "user_name")   // patch 后列名在 Where/Order 生效
	assertNotContains(t, "SQL", sqlText, "internal") // 忽略字段不参与查询
	assertNotContains(t, "SQL", sqlText, "UserName") // Go 字段名不得泄漏为列名

	// 显式 Select 投影同样命中 patch 后列名，且只写字段可投影（落库值可查）
	sel := drv.Query().Model(&ormAllModel{}).Select("user_name", "password")
	stmt2 := sel.(*GormQuery).db.Session(&gorm.Session{DryRun: true}).Find(&[]ormAllModel{}).Statement
	t.Logf("投影 SQL: %s", stmt2.SQL.String())
	assertContains(t, "投影 SQL", stmt2.SQL.String(), "user_name")
	assertContains(t, "投影 SQL", stmt2.SQL.String(), "password")
}

// 2. 自定义列名查询：Where/Order/Select/Pluck 命中 'user_name' 列。
func TestOrmTag_ColumnNameQuery(t *testing.T) {
	drv := newOrmAllDriver(t)
	q := drv.Query()
	if err := q.Create(&ormAllModel{ID: "u1", UserName: "alice", Email: "a@x.com", TenantID: "t1", Mobile: "m1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := q.Create(&ormAllModel{ID: "u2", UserName: "bob", Email: "b@x.com", TenantID: "t2", Mobile: "m2"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var rows []ormAllModel
	if err := q.Model(&ormAllModel{}).Where("user_name = ?", "alice").Find(&rows); err != nil {
		t.Fatalf("Where(user_name): %v", err)
	}
	if len(rows) != 1 || rows[0].UserName != "alice" {
		t.Errorf("Where('user_name') 期望命中 alice, 实际 %+v", rows)
	}

	rows = nil
	if err := q.Model(&ormAllModel{}).Order("user_name desc").Find(&rows); err != nil {
		t.Fatalf("Order(user_name): %v", err)
	}
	if len(rows) != 2 || rows[0].UserName != "bob" {
		t.Errorf("Order('user_name') 期望 [bob alice], 实际 %+v", rows)
	}

	rows = nil
	if err := q.Model(&ormAllModel{}).Select("user_name").Where("id = ?", "u1").Scan(&rows); err != nil {
		t.Fatalf("Select(user_name): %v", err)
	}
	if len(rows) != 1 || rows[0].UserName != "alice" || rows[0].ID != "" {
		t.Errorf("Select('user_name') 投影期望仅 user_name, 实际 %+v", rows)
	}

	var names []string
	if err := q.Model(&ormAllModel{}).Order("id").Pluck("user_name", &names); err != nil {
		t.Fatalf("Pluck(user_name): %v", err)
	}
	if len(names) != 2 || names[0] != "alice" {
		t.Errorf("Pluck('user_name') 期望 [alice bob], 实际 %v", names)
	}
}

// 3. 忽略字段 / 只写字段 / 只读字段。
func TestOrmTag_IgnoreWriteOnlyReadOnly(t *testing.T) {
	drv := newOrmAllDriver(t)
	q := drv.Query()
	if err := q.Create(&ormAllModel{ID: "rw1", UserName: "u", Password: "p@ss", Token: "will-not-land", Internal: "secret"}); err != nil {
		t.Fatalf("Create: %v", err)
	}

	var got ormAllModel
	if err := q.First(&got, "id = ?", "rw1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	// 忽略：不建列、写不落地、读零值
	if got.Internal != "" {
		t.Errorf("忽略字段读回应零值, 实际 %q", got.Internal)
	}
	// 只写（->）：落库但读不回
	if got.Password != "" {
		t.Errorf("只写字段读回应零值, 实际 %q", got.Password)
	}
	// 只读（<-）：写入被忽略
	if got.Token != "" {
		t.Errorf("只读字段 Create 不应写入, 实际 %q", got.Token)
	}
	// created/updated 自动填充、version 置 1
	if got.CreatedAt == 0 || got.UpdatedAt == 0 {
		t.Errorf("created/updated 应自动填充, 实际 %d/%d", got.CreatedAt, got.UpdatedAt)
	}
	if got.Version != 1 {
		t.Errorf("version 插入应置 1, 实际 %d", got.Version)
	}

	// 只写确实落库（绕过 gorm 语义用原生 SQL 验证）
	var pwd, token string
	if err := q.Raw("SELECT password, coalesce(token, '') FROM orm_all_models WHERE id = ?", "rw1").
		Row().Scan(&pwd, &token); err != nil {
		t.Fatalf("原生扫描失败: %v", err)
	}
	if pwd != "p@ss" {
		t.Errorf("只写字段应落库, 实际 %q", pwd)
	}
	if token != "" {
		t.Errorf("只读字段不应落库, 实际 %q", token)
	}
	// 忽略字段确实未建列（sqlite_master DDL 无 internal）
	ddl := tableDDL(t, drv, "orm_all_models")
	assertNotContains(t, "DDL", ddl, "internal")

	// 只读读回：原生写入后可读
	if err := q.Exec("UPDATE orm_all_models SET token = 'rt' WHERE id = ?", "rw1"); err != nil {
		t.Fatalf("原生写入 token: %v", err)
	}
	got = ormAllModel{}
	if err := q.First(&got, "id = ?", "rw1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Token != "rt" {
		t.Errorf("只读字段应读回填充, 实际 %q", got.Token)
	}
}

// ── extends('前缀') 嵌入列名前缀（嵌入类型必须导出——2.2/2.3 双驱动实测） ──

// OrmAuthorFields 嵌入字段载体（导出类型；非导出嵌入类型 gorm 静默跳过）
type OrmAuthorFields struct {
	Name string `orm:"varchar(50) 'name'"`
	Bio  string `orm:"varchar(100) 'bio' notnull"`
}

type ormEmbedModel struct {
	ID              string `orm:"pk varchar(16) 'id'"`
	OrmAuthorFields `orm:"extends('author_')"`
}

func TestOrmTag_ExtendsPrefix(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&ormEmbedModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	ddl := tableDDL(t, drv, "orm_embed_models")
	assertContains(t, "DDL", ddl, "`author_name` varchar(50)")
	assertContains(t, "DDL", ddl, "`author_bio` varchar(100) NOT NULL")
	assertNotContains(t, "DDL", ddl, "`name` varchar(50)")

	q := drv.Query()
	m := &ormEmbedModel{ID: "e1"}
	m.Name = "张三"
	m.Bio = "作者简介"
	if err := q.Create(m); err != nil {
		t.Fatalf("Create: %v", err)
	}
	var got ormEmbedModel
	if err := q.First(&got, "id = ?", "e1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Name != "张三" || got.Bio != "作者简介" {
		t.Errorf("嵌入前缀字段 CRUD 期望 张三/作者简介, 实际 %q/%q", got.Name, got.Bio)
	}
	// 前缀列确实落库
	var colName string
	if err := q.Raw("SELECT author_name FROM orm_embed_models WHERE id = ?", "e1").Row().Scan(&colName); err != nil {
		t.Fatalf("原生扫描 author_name: %v", err)
	}
	if colName != "张三" {
		t.Errorf("author_name 列期望 张三, 实际 %q", colName)
	}
	// 按嵌入列名查询
	rows := 0
	if err := q.Raw("SELECT count(*) FROM orm_embed_models WHERE author_bio = ?", "作者简介").Row().Scan(&rows); err != nil {
		t.Fatalf("按 author_bio 查询: %v", err)
	}
	if rows != 1 {
		t.Errorf("按 author_bio 查询应命中 1 行, 实际 %d", rows)
	}
}

// ── 禁用 token / 未知裸 token：AutoMigrate 启动期报错（4.4） ────────────

type ormBadDeletedModel struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Del int64  `orm:"deleted"`
}

type ormBadTokenModel struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Ext string `orm:"serializer:json"`
}

func TestOrmTag_DisabledAndUnknownTokens(t *testing.T) {
	drv := newTestDriver(t)

	err := drv.AutoMigrate(&ormBadDeletedModel{})
	if err == nil {
		t.Fatal("orm:\"deleted\" 禁用 token 应报错")
	}
	if !strings.Contains(err.Error(), "deleted") || !strings.Contains(err.Error(), "替代方案") {
		t.Errorf("deleted 报错信息应含 token 与替代方案指引, 实际: %v", err)
	}

	err = drv.AutoMigrate(&ormBadTokenModel{})
	if err == nil {
		t.Fatal("未知裸 token 应报错")
	}
	if !strings.Contains(err.Error(), "serializer:json") || !strings.Contains(err.Error(), "Ext") {
		t.Errorf("未知裸 token 报错信息应含 token 原文与字段名, 实际: %v", err)
	}
}

// ── orm + gorm 双 tag 同值模型：行为一致（兼容回归） ────────────────────

type ormDualTagModel struct {
	ID   string `orm:"pk varchar(16) 'id'" gorm:"primaryKey;size:16;column:id"`
	Name string `orm:"varchar(100) 'name'" gorm:"column:name;size:100"`
	Age  int    `orm:"'age' default(0)" gorm:"column:age;default:0"`
}

func TestOrmTag_DualTagCompat(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&ormDualTagModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	ddl := tableDDL(t, drv, "orm_dual_tag_models")
	assertContains(t, "DDL", ddl, "`id` varchar(16)")
	assertContains(t, "DDL", ddl, "`name` varchar(100)")
	assertContains(t, "DDL", ddl, "`age` integer DEFAULT 0")

	q := drv.Query()
	if err := q.Create(&ormDualTagModel{ID: "d1", Name: "n1"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	var got ormDualTagModel
	if err := q.First(&got, "id = ?", "d1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Name != "n1" || got.Age != 0 {
		t.Errorf("双 tag 同值模型行为应一致, 实际 %+v", got)
	}
}

// 纯 gorm tag 旧模型在 patch 链路下零回归。
func TestCompat_PureGormTagModel(t *testing.T) {
	drv := newTestDriverWithTable(t)
	q := drv.Query()
	if err := q.Create(&TestModel{ID: "g1", Name: "legacy"}); err != nil {
		t.Fatalf("Create: %v", err)
	}
	var got TestModel
	if err := q.First(&got, "id = ?", "g1"); err != nil {
		t.Fatalf("First: %v", err)
	}
	if got.Name != "legacy" {
		t.Errorf("旧 gorm tag 模型行为应与现状一致, 实际 %+v", got)
	}
	if err := q.Model(&TestModel{}).Where("id = ?", "g1").Update("name", "updated"); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := q.First(&got, "id = ?", "g1"); err != nil {
		t.Fatal(err)
	}
	if got.Name != "updated" {
		t.Errorf("Update 后期望 updated, 实际 %q", got.Name)
	}
}

// ── FirstOrCreate / FindInBatches / Save upsert 回落（orm tag 模型可用性） ──

type ormNoteModel struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Name string `orm:"varchar(50) 'name'"`
}

func (m *ormNoteModel) AutoGenerateID() {}

func TestOrmTag_FirstOrCreateAndBatches(t *testing.T) {
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(&ormNoteModel{}); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	q := drv.Query()

	// FirstOrCreate：不存在 → 插入（id 回调正常）；存在 → 返回已有行
	m := &ormNoteModel{ID: "n1", Name: "first"}
	if err := q.FirstOrCreate(m, "id = ?", "n1"); err != nil {
		t.Fatalf("FirstOrCreate: %v", err)
	}
	var count int64
	q.Model(&ormNoteModel{}).Count(&count)
	if count != 1 {
		t.Errorf("FirstOrCreate 后应 1 条, 实际 %d", count)
	}
	m2 := &ormNoteModel{ID: "n1", Name: "second"}
	if err := q.FirstOrCreate(m2, "id = ?", "n1"); err != nil {
		t.Fatalf("FirstOrCreate(已存在): %v", err)
	}
	q.Model(&ormNoteModel{}).Count(&count)
	if count != 1 {
		t.Errorf("重复 FirstOrCreate 仍应 1 条, 实际 %d", count)
	}

	// FindInBatches
	for i := 1; i <= 5; i++ {
		id := "b" + string(rune('0'+i))
		if err := q.Create(&ormNoteModel{ID: id, Name: "n"}); err != nil {
			t.Fatal(err)
		}
	}
	batches := 0
	var all []ormNoteModel
	if err := q.Model(&ormNoteModel{}).Order("id").FindInBatches(&all, 3, func(tx contracts.Query, batch int) error {
		batches++
		return nil
	}); err != nil {
		t.Fatalf("FindInBatches: %v", err)
	}
	if batches != 2 {
		t.Errorf("6 条按 3 分批应 2 批, 实际 %d", batches)
	}

	// Save upsert 回落：更新不存在的行 → 回落 INSERT
	up := &ormNoteModel{ID: "up1", Name: "upsert"}
	if err := q.Save(up); err != nil {
		t.Fatalf("Save(upsert 回落): %v", err)
	}
	var got ormNoteModel
	if err := q.First(&got, "id = ?", "up1"); err != nil {
		t.Fatalf("回落插入后 First: %v", err)
	}
	if got.Name != "upsert" {
		t.Errorf("回落插入内容期望 upsert, 实际 %q", got.Name)
	}
	// Save 更新存在的行
	got.Name = "updated"
	if err := q.Save(&got); err != nil {
		t.Fatalf("Save(更新): %v", err)
	}
	var again ormNoteModel
	if err := q.First(&again, "id = ?", "up1"); err != nil {
		t.Fatal(err)
	}
	if again.Name != "updated" {
		t.Errorf("Save 更新期望 updated, 实际 %q", again.Name)
	}
}

// patch 后模型上的唯一冲突错误映射仍生效。
func TestOrmTag_DuplicateKey(t *testing.T) {
	drv := newOrmAllDriver(t)
	q := drv.Query()
	if err := q.Create(&ormAllModel{ID: "d1", UserName: "u", Email: "dup@x.com"}); err != nil {
		t.Fatal(err)
	}
	err := q.Create(&ormAllModel{ID: "d2", UserName: "u2", Email: "dup@x.com"})
	if err == nil {
		t.Fatal("unique 列重复插入应报错")
	}
	if !errors.Is(err, contracts.ErrDuplicatedKey) {
		t.Errorf("唯一冲突应映射 ErrDuplicatedKey, 实际: %v", err)
	}
}
