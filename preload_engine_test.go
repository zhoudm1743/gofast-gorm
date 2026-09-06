package gormdriver

// preload_engine_test.go：Preload 双路径测试（设计文档 9.3 / 11.5）。
// 同一约定外键关联分别用「gorm 原生 Preload」与「gorm:"-" + rel + 共享引擎」
// 跑 Preload，回填逐项一致；另覆盖嵌套/conds/callback/many2many/多态/belongs-to。

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// ── 原生路径模型：约定外键（关联字段无 tag，gorm 原生解析） ────────────
// gorm has-many 约定外键：<父类型名><父主键名> → PlOrder.PlUserID

type PlUser struct {
	ID     string    `gorm:"primaryKey;size:16"`
	Name   string    `gorm:"size:64"`
	Orders []PlOrder // 约定外键，无 tag → gorm 原生 Preload
}

type PlOrder struct {
	ID       string `gorm:"primaryKey;size:16"`
	PlUserID string `gorm:"size:16"`
	Amount   int
}

// ── 引擎路径模型：gorm:"-" + rel ─────────────────────────────────────

type epUser struct {
	ID     string    `gorm:"primaryKey;size:16"`
	Name   string    `gorm:"size:64"`
	Key    string    `gorm:"size:16"`
	DeptID string    `gorm:"size:16"`
	Orders []epOrder `gorm:"-" rel:"foreignKey:UserID;references:ID"`
	Dept   *epDept   `gorm:"-" rel:"foreignKey:DeptID;references:ID"`
	Roles  []epRole  `gorm:"-" rel:"many2many:ep_user_roles;joinForeignKey:Key;joinReferences:Code"`
	Photos []epPhoto `gorm:"-" rel:"polymorphic:Owner;polymorphicValue:ep_users"`
}

type epOrder struct {
	ID     string `gorm:"primaryKey;size:16"`
	UserID string `gorm:"size:16"`
	Amount int
	Items  []epItem `gorm:"-" rel:"foreignKey:OrderID;references:ID"`
}

type epItem struct {
	ID      string `gorm:"primaryKey;size:16"`
	OrderID string `gorm:"size:16"`
	Name    string `gorm:"size:32"`
}

type epDept struct {
	ID   string `gorm:"primaryKey;size:16"`
	Name string `gorm:"size:32"`
}

type epRole struct {
	ID   string `gorm:"primaryKey;size:16"`
	Code string `gorm:"size:16"`
	Name string `gorm:"size:32"`
}

type epPhoto struct {
	ID        string `gorm:"primaryKey;size:16"`
	OwnerID   string `gorm:"size:16"`
	OwnerType string `gorm:"size:32"`
	URL       string `gorm:"size:64"`
}

// newPreloadDriver 建 6 张表（原生 2 + 引擎 4）与中间表，种子数据两套一致。
func newPreloadDriver(t *testing.T) (*GormDriver, []*PlUser, []*epUser) {
	t.Helper()
	drv := newTestDriver(t)
	if err := drv.AutoMigrate(
		&PlUser{}, &PlOrder{},
		&epUser{}, &epOrder{}, &epItem{}, &epDept{}, &epRole{}, &epPhoto{},
	); err != nil {
		t.Fatalf("AutoMigrate: %v", err)
	}
	// many2many 中间表（引擎直接表级查询，无需业务模型）
	for _, ddl := range []string{
		"CREATE TABLE ep_user_roles (`key` varchar(16), `code` varchar(16))",
		"INSERT INTO ep_user_roles VALUES ('k1','r1'), ('k1','r2'), ('k2','r2')",
	} {
		if err := drv.Query().Exec(ddl); err != nil {
			t.Fatalf("中间表初始化 %q: %v", ddl, err)
		}
	}

	q := drv.Query()
	// 原生套种子
	natives := []*PlUser{
		{ID: "u1", Name: "u1", Orders: nil},
		{ID: "u2", Name: "u2"},
		{ID: "u3", Name: "u3"}, // 无子行
	}
	orders := []*PlOrder{
		{ID: "o1", PlUserID: "u1", Amount: 100},
		{ID: "o2", PlUserID: "u1", Amount: 250},
		{ID: "o3", PlUserID: "u2", Amount: 50},
	}
	for _, o := range orders {
		if err := q.Create(o); err != nil {
			t.Fatalf("Create %s: %v", o.ID, err)
		}
	}
	for _, u := range natives {
		if err := q.Create(u); err != nil {
			t.Fatalf("Create %s: %v", u.ID, err)
		}
	}

	// 引擎套种子（同构数据）
	engineUsers := []*epUser{
		{ID: "u1", Name: "u1", Key: "k1", DeptID: "d1"},
		{ID: "u2", Name: "u2", Key: "k2", DeptID: "d2"},
		{ID: "u3", Name: "u3", Key: "k3", DeptID: ""}, // 无子行 + 零值外键
	}
	for i, u := range engineUsers {
		if err := q.Create(u); err != nil {
			t.Fatalf("Create ep %s: %v", u.ID, err)
		}
		_ = i
	}
	epOrders := []*epOrder{
		{ID: "o1", UserID: "u1", Amount: 100},
		{ID: "o2", UserID: "u1", Amount: 250},
		{ID: "o3", UserID: "u2", Amount: 50},
	}
	for _, o := range epOrders {
		if err := q.Create(o); err != nil {
			t.Fatalf("Create ep %s: %v", o.ID, err)
		}
	}
	items := []*epItem{
		{ID: "i1", OrderID: "o1", Name: "book"},
		{ID: "i2", OrderID: "o1", Name: "pen"},
		{ID: "i3", OrderID: "o3", Name: "bag"},
	}
	for _, it := range items {
		if err := q.Create(it); err != nil {
			t.Fatalf("Create ep item %s: %v", it.ID, err)
		}
	}
	depts := []*epDept{{ID: "d1", Name: "dev"}, {ID: "d2", Name: "ops"}}
	for _, d := range depts {
		if err := q.Create(d); err != nil {
			t.Fatal(err)
		}
	}
	roles := []*epRole{{ID: "r1", Code: "r1", Name: "admin"}, {ID: "r2", Code: "r2", Name: "dev"}}
	for _, r := range roles {
		if err := q.Create(r); err != nil {
			t.Fatal(err)
		}
	}
	photos := []*epPhoto{
		{ID: "p1", OwnerID: "u1", OwnerType: "ep_users", URL: "a.png"},
		{ID: "p2", OwnerID: "u1", OwnerType: "other_users", URL: "b.png"}, // 异类型父行隔离
		{ID: "p3", OwnerID: "u2", OwnerType: "ep_users", URL: "c.png"},
	}
	for _, p := range photos {
		if err := q.Create(p); err != nil {
			t.Fatal(err)
		}
	}
	return drv, natives, engineUsers
}

// summarizePld / summarizeEp 将回填结果序列化为可比较文本（引擎 vs 原生逐项一致）。
func summarizePld(users []PlUser) string {
	lines := make([]string, 0, len(users))
	for _, u := range users {
		ids := make([]string, 0, len(u.Orders))
		for _, o := range u.Orders {
			ids = append(ids, fmt.Sprintf("%s:%s:%d", o.ID, o.PlUserID, o.Amount))
		}
		sort.Strings(ids)
		lines = append(lines, u.ID+"|"+strings.Join(ids, ","))
	}
	return strings.Join(lines, "\n")
}

func summarizeEp(users []epUser) string {
	lines := make([]string, 0, len(users))
	for _, u := range users {
		ids := make([]string, 0, len(u.Orders))
		for _, o := range u.Orders {
			ids = append(ids, fmt.Sprintf("%s:%s:%d", o.ID, o.UserID, o.Amount))
		}
		sort.Strings(ids)
		lines = append(lines, u.ID+"|"+strings.Join(ids, ","))
	}
	return strings.Join(lines, "\n")
}

// 双路径一致：同一约定外键关联，「原生」与「gorm:"-" + rel + 共享引擎」逐项一致。
func TestPreload_DualPath_HasMany(t *testing.T) {
	drv, _, engineUsers := newPreloadDriver(t)
	q := drv.Query()
	_ = engineUsers // 引擎套种子随驱动建好，此处仅比对回填结果

	var nativeRows []PlUser
	if err := q.Model(&PlUser{}).Order("id").Preload("Orders").Find(&nativeRows); err != nil {
		t.Fatalf("原生 Preload: %v", err)
	}
	var engineRows []epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Orders").Find(&engineRows); err != nil {
		t.Fatalf("引擎 Preload: %v", err)
	}

	if len(engineRows) != 3 {
		t.Fatalf("引擎路径应 3 父行, 实际 %d", len(engineRows))
	}
	// 无子行 → 空切片非 nil（契约 5）
	if engineRows[2].Orders == nil || len(engineRows[2].Orders) != 0 {
		t.Errorf("无子行父行 Orders 应为非 nil 空切片, 实际 %#v", engineRows[2].Orders)
	}
	if nativeRows[2].Orders == nil || len(nativeRows[2].Orders) != 0 {
		t.Errorf("原生路径无子行父行 Orders 应为非 nil 空切片, 实际 %#v", nativeRows[2].Orders)
	}

	want, got := summarizePld(nativeRows), summarizeEp(engineRows)
	if want != got {
		t.Errorf("双路径回填应逐项一致:\n原生:\n%s\n引擎:\n%s", want, got)
	}
}

func TestPreload_Engine_FirstAndCondsAndCallback(t *testing.T) {
	drv, _, _ := newPreloadDriver(t)
	q := drv.Query()

	// First 单行回填
	var one epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Orders").First(&one); err != nil {
		t.Fatalf("First + 引擎 Preload: %v", err)
	}
	if len(one.Orders) != 2 || one.Orders[0].ID == "" {
		t.Errorf("First 引擎回填期望 2 子行, 实际 %+v", one.Orders)
	}

	// conds 条件过滤
	var rows []epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Orders", "amount > ?", 100).Find(&rows); err != nil {
		t.Fatalf("conds Preload: %v", err)
	}
	if len(rows[0].Orders) != 1 || rows[0].Orders[0].ID != "o2" {
		t.Errorf("conds 应仅回填 amount>100 的子行, 实际 %+v", rows[0].Orders)
	}
	if len(rows[1].Orders) != 0 {
		t.Errorf("u2 无命中条件的子行应为空切片, 实际 %+v", rows[1].Orders)
	}

	// callback：排序 + 限量
	rows = nil
	if err := q.Model(&epUser{}).Order("id").Preload("Orders", func(cq contracts.Query) contracts.Query {
		return cq.Order("amount DESC").Limit(1)
	}).Find(&rows); err != nil {
		t.Fatalf("callback Preload: %v", err)
	}
	if len(rows[0].Orders) != 1 || rows[0].Orders[0].ID != "o2" {
		t.Errorf("callback 排序限量期望 [o2], 实际 %+v", rows[0].Orders)
	}

	// 引擎路径回填对非 nil 元素类型（[]*epUser 形态）同样生效
	var ptrRows []*epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Orders").Find(&ptrRows); err != nil {
		t.Fatalf("[]*T 引擎 Preload: %v", err)
	}
	if len(ptrRows) != 3 || len(ptrRows[1].Orders) != 1 {
		t.Errorf("[]*T 形态回填期望 u2 有 1 子行, 实际 %+v", ptrRows[1].Orders)
	}
}

func TestPreload_Engine_NestedOrdersItems(t *testing.T) {
	drv, _, _ := newPreloadDriver(t)
	q := drv.Query()

	var rows []epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Orders.Items").Find(&rows); err != nil {
		t.Fatalf("嵌套 Preload: %v", err)
	}
	if len(rows[0].Orders) != 2 {
		t.Fatalf("u1 期望 2 子行, 实际 %+v", rows[0].Orders)
	}
	// 嵌套第二段回填：o1 有 2 items，o2 无 items（空切片）
	var o1, o2 *epOrder
	for i := range rows[0].Orders {
		switch rows[0].Orders[i].ID {
		case "o1":
			o1 = &rows[0].Orders[i]
		case "o2":
			o2 = &rows[0].Orders[i]
		}
	}
	if o1 == nil || len(o1.Items) != 2 {
		t.Errorf("o1.Items 期望 2, 实际 %+v", o1)
	}
	if o2 == nil || o2.Items == nil || len(o2.Items) != 0 {
		t.Errorf("o2.Items 期望非 nil 空切片, 实际 %+v", o2)
	}
}

func TestPreload_Engine_BelongsTo(t *testing.T) {
	drv, _, _ := newPreloadDriver(t)
	q := drv.Query()

	var rows []epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Dept").Find(&rows); err != nil {
		t.Fatalf("belongs-to Preload: %v", err)
	}
	if rows[0].Dept == nil || rows[0].Dept.Name != "dev" {
		t.Errorf("u1.Dept 期望 dev, 实际 %+v", rows[0].Dept)
	}
	// 外键零值 → 跳过该行，关联字段保持 nil，不报错（契约 4）
	if rows[2].Dept != nil {
		t.Errorf("u3 零值外键 Dept 应保持 nil, 实际 %+v", rows[2].Dept)
	}
}

func TestPreload_Engine_Many2Many(t *testing.T) {
	drv, _, _ := newPreloadDriver(t)
	q := drv.Query()

	var rows []epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Roles").Find(&rows); err != nil {
		t.Fatalf("many2many Preload: %v", err)
	}
	// 中间表: k1→r1,r2；k2→r2；k3 无映射 → 空切片
	if len(rows[0].Roles) != 2 {
		t.Errorf("u1.Roles 期望 2, 实际 %+v", rows[0].Roles)
	}
	if len(rows[1].Roles) != 1 || rows[1].Roles[0].Code != "r2" {
		t.Errorf("u2.Roles 期望 [r2], 实际 %+v", rows[1].Roles)
	}
	if rows[2].Roles == nil || len(rows[2].Roles) != 0 {
		t.Errorf("u3.Roles 期望非 nil 空切片, 实际 %+v", rows[2].Roles)
	}

	// conds/callback 作用于子表查询（第二跳）
	rows = nil
	if err := q.Model(&epUser{}).Order("id").Preload("Roles", "name = ?", "dev").Find(&rows); err != nil {
		t.Fatalf("many2many conds: %v", err)
	}
	if len(rows[0].Roles) != 1 || rows[0].Roles[0].Code != "r2" {
		t.Errorf("many2many conds 过滤期望 u1 仅 [r2], 实际 %+v", rows[0].Roles)
	}
}

func TestPreload_Engine_Polymorphic(t *testing.T) {
	drv, _, _ := newPreloadDriver(t)
	q := drv.Query()

	var rows []epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Photos").Find(&rows); err != nil {
		t.Fatalf("polymorphic Preload: %v", err)
	}
	// 类型列条件 owner_type='ep_users'：u1 仅 p1（p2 为异类型），u2 仅 p3
	if len(rows[0].Photos) != 1 || rows[0].Photos[0].URL != "a.png" {
		t.Errorf("u1.Photos 期望 [a.png], 实际 %+v", rows[0].Photos)
	}
	if len(rows[1].Photos) != 1 || rows[1].Photos[0].URL != "c.png" {
		t.Errorf("u2.Photos 期望 [c.png], 实际 %+v", rows[1].Photos)
	}
}

func TestPreload_Engine_FindInBatches(t *testing.T) {
	drv, _, _ := newPreloadDriver(t)
	q := drv.Query()

	batches := 0
	var all []epUser
	if err := q.Model(&epUser{}).Order("id").Preload("Orders").FindInBatches(&all, 2, func(tx contracts.Query, batch int) error {
		batches++
		// fc 回调内关联字段已就绪（逐批回填）
		for _, u := range all {
			if u.ID == "u1" && len(u.Orders) != 2 {
				t.Errorf("批内 u1.Orders 应已回填 2 行, 实际 %+v", u.Orders)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("FindInBatches + 引擎 Preload: %v", err)
	}
	if batches != 2 {
		t.Errorf("3 父行按 2 分批应 2 批, 实际 %d", batches)
	}
}
