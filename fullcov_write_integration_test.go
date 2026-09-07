//go:build integration

package gormdriver

// fullcov_write_integration_test.go —— 分组 3：contracts.Query 写终结 + Result
// 变体 + 高级创建 真实库全覆盖（PG/MySQL 双方言矩阵），对齐 gofast-xorm 侧
// fullcov_write_integration_test.go 的用例矩阵（fc3_ → gfc3_ 前缀）。
//
// 覆盖方法：Create / CreateInBatches / Save / Update / Updates / Delete /
// CreateResult / UpdateResult / UpdatesResult / DeleteResult / SaveResult /
// FirstOrCreate / FirstOrInit / FindInBatches / WithContext。
//
// 语义对齐契约（gorm 侧实测基准，全部经真库探针核实）：
//   - Update/Updates 一律带显式 Where 限定范围（Model(&bean) 主键并入更新条件
//     属 X-09，model_pk_integration_test.go 覆盖，此处不重复）；
//   - map 传参全键写入（含零值）、struct 传参跳零值；
//   - Update/Updates 列值支持 contracts.Expr（X-08，经 toGormValue 转 gorm.Expr）；
//   - Save：主键全零插入 / 非零主键全列更新（gorm Selects "*"，含零值覆盖）/
//     更新 0 行回落 INSERT ... ON CONFLICT|ODKU upsert；
//   - FirstOrCreate/FirstOrInit 命中回填、未命中按 dest 原值插入/保持原值；
//     conds 不回填 dest（差异固化，方案 §5.9 ADV-01）；
//   - Result 变体回填 RowsAffected + 错误（错误路径行数保持零值）。
//
// 差异固化条目（相对 xorm 侧同名矩阵，断言处均带注释）：
//   1. CreateInBatches 中途失败：gorm 分批包裹单事务 → 整体回滚 0 行落库
//      （xorm 逐块独立提交，前块已提交）；
//   2. CreateInBatches batchSize<=0：双驱动一致兜底为不分批整批插入
//      （gorm 原生无守卫：batch=0 报 ErrEmptySlice、负值 panic——驱动层已修）；
//   3. Save 值无变化：MySQL RowsAffected=0（ODKU 未变更计 0）、PG=1，均不报错不误插；
//   4. Save 显式条件未命中：gorm upsert 回落忽略链上条件 → 行被插入 rows=1
//      （xorm 回落被条件限定为 0 行、不落库）；
//   5. Save 切片 affected：PG=2、MySQL=3（ODKU 插入计 1、更新计 2）；
//   6. Delete 无条件保护：gorm ErrMissingWhereClause（xorm 原生空条件错误，
//      方案 §5.1 固化为"均报错且行数不变"）；
//   7. 时间戳 map 更新：gorm Model 链触碰 updated_at（原生 AutoUpdateTime）、
//      Table 链（无 schema 元数据）不触碰（xorm 侧 map 更新一律不触碰）；
//   8. FindInBatches：gorm dest 每批重置（累计行数 3,3,3,1、终态仅末批；
//      中断时 dest 仅含末批行），xorm 侧累积（3,6,9,10 / 中断含前两批）。
//
// 运行（docker-compose.yml 环境）：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	  go test -tags integration -run 'TestFullCovWrite' -v .
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -run 'TestFullCovWrite' -v .

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"gorm.io/gorm"
)

// ── 测试模型 ─────────────────────────────────────────────────────────
//
// 表名前缀 gfc3_（本分组专用，避免与其他分组/任务撞表）。gfc3User/gfc3DupUser
// 用 gorm tag（照 pg_integration_test.go AppUser 风格），gfc3TimeUser 用统一
// orm tag 的 created/updated（探针实测 AutoCreateTime/AutoUpdateTime 填充可靠）。

// gfc3User 单主键写测试模型：主键显式字符串 id，非自增——所有用例主键由调用方
// 显式给定，插入路径可控（零主键即空串，两个空串主键互斥即重复键）。
type gfc3User struct {
	ID   string `gorm:"column:id;primaryKey;size:16"`
	Name string `gorm:"column:name;size:64"`
	Cnt  int64  `gorm:"column:cnt"`
}

func (gfc3User) TableName() string { return "gfc3_users" }

// gfc3DupUser 带列级唯一约束的模型（CRUD-02 唯一列重复键映射）。
type gfc3DupUser struct {
	ID    string `gorm:"column:id;primaryKey;size:16"`
	Email string `gorm:"column:email;size:64;unique"`
}

func (gfc3DupUser) TableName() string { return "gfc3_dup_users" }

// gfc3TimeUser 时间戳模型：orm:"created/updated" → int64 unix 秒。
type gfc3TimeUser struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	Name      string `orm:"varchar(64) 'name'"`
	CreatedAt int64  `orm:"created 'created_at'"`
	UpdatedAt int64  `orm:"updated 'updated_at'"`
}

func (gfc3TimeUser) TableName() string { return "gfc3_time_users" }

// gfc3AutoIDUser 框架 ID 自动生成模型（contracts.IDAutoGenerator，指针接收者，
// 对齐框架 database.Model 形态）：Create 时驱动钩子/gorm 回调填充非空主键。
type gfc3AutoIDUser struct {
	ID   string `gorm:"column:id;primaryKey;size:40"`
	Name string `gorm:"column:name;size:64"`
}

func (gfc3AutoIDUser) TableName() string { return "gfc3_auto_users" }

// AutoGenerateID 实现 contracts.IDAutoGenerator。
func (u *gfc3AutoIDUser) AutoGenerateID() {
	if u.ID == "" {
		u.ID = fmt.Sprintf("auto-%d", time.Now().UnixNano())
	}
}

// ── 用例基建 ─────────────────────────────────────────────────────────

// gfc3Reset 用例开始前 DROP 重建 gfc3_users（intgDropTables→intgMigrate），
// t.Cleanup 清表（防用例中断残留影响他表）。
func gfc3Reset(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc3_users"}, &gfc3User{})
}

// gfc3ResetDup 重建 gfc3_dup_users（唯一列模型表）。
func gfc3ResetDup(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc3_dup_users"}, &gfc3DupUser{})
}

// gfc3ResetTime 重建 gfc3_time_users（时间戳模型表）。
func gfc3ResetTime(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc3_time_users"}, &gfc3TimeUser{})
}

// gfc3ResetAuto 重建 gfc3_auto_users（ID 自动生成模型表）。
func gfc3ResetAuto(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gfc3_auto_users"}, &gfc3AutoIDUser{})
}

// gfc3Seed 重置并灌入基线行（经 Create 写入，顺带覆盖 Create 多行形态）。
func gfc3Seed(t *testing.T, drv *GormDriver, rows ...gfc3User) {
	t.Helper()
	gfc3Reset(t, drv)
	for i := range rows {
		if err := drv.Query().Create(&rows[i]); err != nil {
			t.Fatalf("种子 %+v: %v", rows[i], err)
		}
	}
}

// gfc3Read 按主键回读单行（驱动读链路验证落库真实数据）。
func gfc3Read(t *testing.T, drv *GormDriver, id string) gfc3User {
	t.Helper()
	var row gfc3User
	if err := drv.Query().Model(&gfc3User{}).Where("id = ?", id).First(&row); err != nil {
		t.Fatalf("回读 id=%q: %v", id, err)
	}
	return row
}

// gfc3Expect 断言某行存在且字段值完全一致。
func gfc3Expect(t *testing.T, drv *GormDriver, id, wantName string, wantCnt int64) {
	t.Helper()
	row := gfc3Read(t, drv, id)
	if row.Name != wantName || row.Cnt != wantCnt {
		t.Errorf("行 %q 期望 (name=%q, cnt=%d), 实际 %+v", id, wantName, wantCnt, row)
	}
}

// gfc3Count 原生 SQL 行数（写操作真实落库量）。
func gfc3Count(t *testing.T, drv *GormDriver) int64 {
	t.Helper()
	var n int64
	if err := drv.Query().Raw("SELECT count(*) FROM gfc3_users").Scan(&n); err != nil {
		t.Fatalf("count gfc3_users: %v", err)
	}
	return n
}

// gfc3BatchItems 构造 n 个显式主键元素（id 前缀 + 两位序号，名称/计数按序号派生）。
func gfc3BatchItems(prefix string, n int) []*gfc3User {
	items := make([]*gfc3User, 0, n)
	for i := 0; i < n; i++ {
		items = append(items, &gfc3User{
			ID:   fmt.Sprintf("%s%02d", prefix, i),
			Name: fmt.Sprintf("n-%s-%d", prefix, i),
			Cnt:  int64(i + 1),
		})
	}
	return items
}

// ── 共享 runner（PG/MySQL 双入口）─────────────────────────────────────

// runFullCovWrite 写终结全覆盖矩阵。dialect 传 "pg"/"mysql"，仅在受影响行数
// 方言语义（MySQL affected=已变更行/ODKU 计数、PG=命中行）与错误文本断言处分支。
func runFullCovWrite(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	// 顶层兜底清理（子用例各自 gfc3Reset*，此处防中断残留）
	t.Cleanup(func() {
		intgDropTables(t, drv, "gfc3_users", "gfc3_dup_users", "gfc3_time_users", "gfc3_auto_users")
	})

	t.Run("Create_单条与显式主键不覆盖_自动ID", func(t *testing.T) {
		gfc3Reset(t, drv)
		gfc3ResetAuto(t, drv)
		q := drv.Query()

		// CRUD-01：显式主键不覆盖 + 回读逐字段
		u := gfc3User{ID: "u1", Name: "alice", Cnt: 10}
		if err := q.Create(&u); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if u.ID != "u1" {
			t.Errorf("显式主键不应被驱动覆盖, 实际 %q", u.ID)
		}
		gfc3Expect(t, drv, "u1", "alice", 10)

		// 第二条显式主键同样原样落库（互不覆盖）
		if err := q.Create(&gfc3User{ID: "u2", Name: "bob", Cnt: 20}); err != nil {
			t.Fatalf("Create u2: %v", err)
		}
		gfc3Expect(t, drv, "u2", "bob", 20)
		if n := gfc3Count(t, drv); n != 2 {
			t.Errorf("期望 2 行, 实际 %d", n)
		}

		// AutoGenerateID 模型：空主键自动生成（驱动钩子 + gorm create 前回调双路径）
		au := &gfc3AutoIDUser{Name: "auto"}
		if err := q.Create(au); err != nil {
			t.Fatalf("Create(自动ID): %v", err)
		}
		if au.ID == "" {
			t.Fatal("AutoGenerateID 模型 Create 后 ID 不应为空")
		}
		var got gfc3AutoIDUser
		if err := q.First(&got, "id = ?", au.ID); err != nil {
			t.Fatalf("回读自动 ID 行: %v", err)
		}
		if got.Name != "auto" {
			t.Errorf("自动 ID 行落库内容不符: %+v", got)
		}

		// 预置主键不被 AutoGenerateID 覆盖
		explicit := &gfc3AutoIDUser{ID: "a-explicit", Name: "preset"}
		if err := q.Create(explicit); err != nil {
			t.Fatalf("Create(预置ID): %v", err)
		}
		if explicit.ID != "a-explicit" {
			t.Errorf("预置主键不应被覆盖, 实际 %q", explicit.ID)
		}
	})

	t.Run("Create_重复主键与唯一列错误映射", func(t *testing.T) {
		gfc3Reset(t, drv)
		gfc3ResetDup(t, drv)
		q := drv.Query()

		// CRUD-02：重复主键
		if err := q.Create(&gfc3User{ID: "d1", Name: "first", Cnt: 1}); err != nil {
			t.Fatalf("首次 Create: %v", err)
		}
		err := q.Create(&gfc3User{ID: "d1", Name: "dup", Cnt: 2})
		if err == nil {
			t.Fatal("重复主键 Create 应报错")
		}
		if !errors.Is(err, contracts.ErrDuplicatedKey) {
			t.Errorf("应映射 contracts.ErrDuplicatedKey, 实际: %v", err)
		}
		// 方言错误文本兜底核对（wrapError 文本匹配映射来源）
		if dialect == "mysql" {
			if !strings.Contains(err.Error(), "Duplicate entry") {
				t.Errorf("MySQL 应透出 Duplicate entry, 实际: %v", err)
			}
		} else if !strings.Contains(err.Error(), "duplicate key") {
			t.Errorf("PG 应透出 duplicate key, 实际: %v", err)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Errorf("失败插入不应落库, 期望 1 行, 实际 %d", n)
		}

		// 唯一列重复（主键不同、唯一列相同）
		if err := q.Create(&gfc3DupUser{ID: "e1", Email: "dup@gfc3.dev"}); err != nil {
			t.Fatalf("唯一表首次 Create: %v", err)
		}
		err = q.Create(&gfc3DupUser{ID: "e2", Email: "dup@gfc3.dev"})
		if err == nil || !errors.Is(err, contracts.ErrDuplicatedKey) {
			t.Fatalf("唯一列冲突应映射 ErrDuplicatedKey, 实际: %v", err)
		}
	})

	t.Run("CreateInBatches_分批与尾批", func(t *testing.T) {
		gfc3Reset(t, drv)
		q := drv.Query()

		// CRUD-03：5 条、批大小 2 → 2+2+1（尾批不足同样完整落库）
		items := gfc3BatchItems("b", 5)
		if err := q.CreateInBatches(&items, 2); err != nil {
			t.Fatalf("CreateInBatches: %v", err)
		}
		if n := gfc3Count(t, drv); n != 5 {
			t.Fatalf("期望 5 行, 实际 %d", n)
		}
		for i, it := range items {
			gfc3Expect(t, drv, it.ID, fmt.Sprintf("n-b-%d", i), int64(i+1))
		}
	})

	t.Run("CreateInBatches_中途失败全回滚", func(t *testing.T) {
		gfc3Reset(t, drv)
		q := drv.Query()

		// CRUD-03：4 条、批大小 2：chunk1=[k1,k2]；chunk2=[k1 重复, k3] 报错即停
		items := []*gfc3User{
			{ID: "k1", Name: "one", Cnt: 1},
			{ID: "k2", Name: "two", Cnt: 2},
			{ID: "k1", Name: "dup", Cnt: 3},
			{ID: "k3", Name: "three", Cnt: 4},
		}
		err := q.CreateInBatches(&items, 2)
		if err == nil {
			t.Fatal("块 2 含重复主键应报错")
		}
		if !errors.Is(err, contracts.ErrDuplicatedKey) {
			t.Errorf("应映射 ErrDuplicatedKey, 实际: %v", err)
		}
		// 差异固化：gorm 对 len>batchSize 的分批插入包裹单事务，任一批失败
		// 整体回滚（0 行落库）；xorm 逐块独立提交（前块 2 行已提交）。
		if n := gfc3Count(t, drv); n != 0 {
			t.Errorf("gorm 分批单事务应整体回滚, 期望 0 行, 实际 %d", n)
		}
	})

	t.Run("CreateInBatches_非正批量与回落", func(t *testing.T) {
		gfc3Reset(t, drv)
		q := drv.Query()

		// 非切片回落单条 Create（gorm finisher default 分支）
		if err := q.CreateInBatches(&gfc3User{ID: "s1", Name: "single", Cnt: 9}, 5); err != nil {
			t.Fatalf("非切片 CreateInBatches: %v", err)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Fatalf("期望回落单条 Create 落库 1 行, 实际 %d", n)
		}
		gfc3Expect(t, drv, "s1", "single", 9)
		err := q.CreateInBatches(&gfc3User{ID: "s1", Name: "again", Cnt: 1}, 5)
		if err == nil || !errors.Is(err, contracts.ErrDuplicatedKey) {
			t.Errorf("回落 Create 重复主键应映射 ErrDuplicatedKey, 实际: %v", err)
		}

		// 双驱动一致（已修复对齐）：batchSize<=0 兜底为不分批整批插入。
		// gorm 原生对非正 batch 无守卫（batch=0 报 ErrEmptySlice、负值 panic），
		// 驱动层已加守卫，语义与 xorm 的「非正 batch 兜底整批」一致。
		items := gfc3BatchItems("p", 3)
		if err := q.CreateInBatches(&items, 0); err != nil {
			t.Errorf("batch=0 应兜底整批落库, 实际: %v", err)
		}
		if n := gfc3Count(t, drv); n != 4 {
			t.Errorf("batch=0 整批落库后期望 1+3=4 行, 实际 %d", n)
		}

		// batch=-1 同款兜底（修复前 gorm 内部 reflect.Slice 越界 panic）
		bad := gfc3BatchItems("m", 2)
		if err := q.CreateInBatches(&bad, -1); err != nil {
			t.Errorf("batch=-1 应兜底整批落库, 实际: %v", err)
		}
		if n := gfc3Count(t, drv); n != 6 {
			t.Errorf("batch=-1 整批落库后期望 4+2=6 行, 实际 %d", n)
		}
	})

	t.Run("Save_三分支", func(t *testing.T) {
		gfc3Reset(t, drv)
		q := drv.Query()

		// CRUD-04a：零主键走插入（空串主键行落库）
		u := gfc3User{Name: "inserted", Cnt: 7}
		if err := q.Save(&u); err != nil {
			t.Fatalf("Save(零主键): %v", err)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Fatalf("期望插入 1 行, 实际 %d", n)
		}
		row := gfc3Read(t, drv, "")
		if row.Name != "inserted" || row.Cnt != 7 {
			t.Errorf("插入行内容不符: %+v", row)
		}

		// CRUD-04b：非零主键全列更新（gorm Save Selects "*"，含零值覆盖）
		gfc3Seed(t, drv, gfc3User{ID: "a", Name: "alice", Cnt: 10})
		if err := q.Save(&gfc3User{ID: "a", Name: "", Cnt: 0}); err != nil {
			t.Fatalf("Save(全列覆盖): %v", err)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Fatalf("Save 更新不应新增行, 期望 1 行, 实际 %d", n)
		}
		gfc3Expect(t, drv, "a", "", 0)

		// CRUD-04c：更新 0 行回落插入 upsert（先物理删除 → Save 重建该行）
		gfc3Seed(t, drv, gfc3User{ID: "gone", Name: "x", Cnt: 1})
		if err := q.Delete(&gfc3User{ID: "gone"}); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if n := gfc3Count(t, drv); n != 0 {
			t.Fatalf("删除后应 0 行, 实际 %d", n)
		}
		if err := q.Save(&gfc3User{ID: "gone", Name: "reborn", Cnt: 5}); err != nil {
			t.Fatalf("Save(0 行回落): %v", err)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Fatalf("回落插入后应 1 行, 实际 %d", n)
		}
		gfc3Expect(t, drv, "gone", "reborn", 5)
	})

	t.Run("Save_值无变化affected差异", func(t *testing.T) {
		gfc3Seed(t, drv, gfc3User{ID: "s1", Name: "staged", Cnt: 1})
		q := drv.Query()

		// CRUD-05 差异固化：值无变化的已存在行 Save 不报错、不误插（不得触发
		// 主键重复错误）。MySQL affected=0（已变更行计数 + ODKU 未变更计 0，
		// 真库实测）、PG=1（命中行计数）。
		cur := gfc3Read(t, drv, "s1")
		res := q.SaveResult(&gfc3User{ID: "s1", Name: cur.Name, Cnt: cur.Cnt})
		if res.Error != nil {
			t.Fatalf("值无变化 Save 不应报错: %v", res.Error)
		}
		if dialect == "mysql" {
			if !res.IsZeroRow() {
				t.Errorf("MySQL 值无变化 Save 期望 RowsAffected=0, 实际 %d", res.RowsAffected)
			}
		} else if res.RowsAffected != 1 {
			t.Errorf("PG 值无变化 Save 期望 RowsAffected=1, 实际 %d", res.RowsAffected)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Errorf("值无变化 Save 不应误插, 期望 1 行, 实际 %d", n)
		}
		gfc3Expect(t, drv, "s1", cur.Name, cur.Cnt)
	})

	t.Run("Update_单列UpdatesMap与Struct", func(t *testing.T) {
		gfc3Seed(t, drv,
			gfc3User{ID: "a", Name: "alice", Cnt: 10},
			gfc3User{ID: "b", Name: "bob", Cnt: 20},
			gfc3User{ID: "c", Name: "carol", Cnt: 30},
		)
		q := drv.Query()

		// CRUD-06a：单列 Update 带显式 Where
		if err := q.Model(&gfc3User{}).Where("id = ?", "a").Update("name", "alice-v2"); err != nil {
			t.Fatalf("Update(name): %v", err)
		}
		gfc3Expect(t, drv, "a", "alice-v2", 10)

		// 单列零值写入（Table 链形态：cnt → 0 无条件写键）
		if err := q.Table("gfc3_users").Where("id = ?", "b").Update("cnt", int64(0)); err != nil {
			t.Fatalf("Update(cnt=0): %v", err)
		}
		gfc3Expect(t, drv, "b", "bob", 0)

		// CRUD-06b：Updates map 全键写入含零值
		if err := q.Model(&gfc3User{}).Where("id = ?", "a").
			Updates(map[string]any{"name": "", "cnt": int64(0)}); err != nil {
			t.Fatalf("Updates(map 零值): %v", err)
		}
		gfc3Expect(t, drv, "a", "", 0)

		// map 常规键
		if err := q.Model(&gfc3User{}).Where("id = ?", "b").
			Updates(map[string]any{"name": "bob-v2", "cnt": int64(21)}); err != nil {
			t.Fatalf("Updates(map): %v", err)
		}
		gfc3Expect(t, drv, "b", "bob-v2", 21)

		// CRUD-06c：Updates struct 跳零值（仅非零字段写入）
		if err := q.Model(&gfc3User{}).Where("id = ?", "c").Updates(gfc3User{Cnt: 99}); err != nil {
			t.Fatalf("Updates(struct 部分): %v", err)
		}
		gfc3Expect(t, drv, "c", "carol", 99)
		if err := q.Model(&gfc3User{}).Where("id = ?", "c").Updates(&gfc3User{Name: "carol-v3"}); err != nil {
			t.Fatalf("Updates(&struct): %v", err)
		}
		gfc3Expect(t, drv, "c", "carol-v3", 99)

		// 未命中行不报错、非目标行不被波及
		if err := q.Model(&gfc3User{}).Where("id = ?", "zz").Update("name", "x"); err != nil {
			t.Fatalf("未命中 Update 不应报错: %v", err)
		}
		gfc3Expect(t, drv, "b", "bob-v2", 21)
	})

	t.Run("UpdateExpr_单列混合Map与条件组合", func(t *testing.T) {
		gfc3Seed(t, drv,
			gfc3User{ID: "a", Name: "alice", Cnt: 10},
			gfc3User{ID: "b", Name: "bob", Cnt: 20},
			gfc3User{ID: "c", Name: "carol", Cnt: 30},
		)
		q := drv.Query()

		// CRUD-10（X-08）：单列表达式 SET cnt = cnt + ? 数据库端原子更新
		if err := q.Model(&gfc3User{}).Where("id = ?", "a").
			Update("cnt", contracts.Expr("cnt + ?", 5)); err != nil {
			t.Fatalf("Update 表达式: %v", err)
		}
		gfc3Expect(t, drv, "a", "alice", 15)

		// 混合 map：表达式键 + 普通键同语句生效
		if err := q.Model(&gfc3User{}).Where("id = ?", "b").Updates(map[string]any{
			"cnt":  contracts.Expr("cnt * ?", 2),
			"name": "bob-x2",
		}); err != nil {
			t.Fatalf("Updates 混合表达式: %v", err)
		}
		gfc3Expect(t, drv, "b", "bob-x2", 40)

		// 表达式与 OrWhere 组合：id=a OR id=c 命中
		if err := q.Model(&gfc3User{}).Where("id = ?", "a").OrWhere("id = ?", "c").
			Update("name", "or-hit"); err != nil {
			t.Fatalf("OrWhere 组合: %v", err)
		}
		gfc3Expect(t, drv, "a", "or-hit", 15)
		gfc3Expect(t, drv, "c", "or-hit", 30)
		gfc3Expect(t, drv, "b", "bob-x2", 40)

		// 表达式与 Not 组合：cnt > 15 且非 id=c → 仅 b
		if err := q.Model(&gfc3User{}).Where("cnt > ?", int64(15)).Not("id = ?", "c").
			Update("name", "not-hit"); err != nil {
			t.Fatalf("Not 组合: %v", err)
		}
		gfc3Expect(t, drv, "b", "not-hit", 40)
		gfc3Expect(t, drv, "c", "or-hit", 30)

		// 未命中行表达式 0 影响不报错
		if err := q.Model(&gfc3User{}).Where("id = ?", "zz").
			Update("cnt", contracts.Expr("cnt + ?", 1)); err != nil {
			t.Fatalf("未命中表达式 Update 不应报错: %v", err)
		}
		gfc3Expect(t, drv, "c", "or-hit", 30)
	})

	t.Run("Delete_主键交集与无条件保护", func(t *testing.T) {
		gfc3Seed(t, drv,
			gfc3User{ID: "a", Name: "alice", Cnt: 10},
			gfc3User{ID: "b", Name: "bob", Cnt: 20},
			gfc3User{ID: "c", Name: "carol", Cnt: 30},
		)
		q := drv.Query()

		// CRUD-08：bean 主键删除
		if err := q.Delete(&gfc3User{ID: "b"}); err != nil {
			t.Fatalf("Delete(bean): %v", err)
		}
		if n := gfc3Count(t, drv); n != 2 {
			t.Fatalf("期望删除 1 行剩 2 行, 实际 %d", n)
		}
		gfc3Expect(t, drv, "a", "alice", 10)

		// 未命中：0 行无错
		if err := q.Delete(&gfc3User{ID: "nope"}); err != nil {
			t.Fatalf("未命中 Delete 不应报错: %v", err)
		}
		if n := gfc3Count(t, drv); n != 2 {
			t.Errorf("未命中删除后应仍 2 行, 实际 %d", n)
		}

		// bean 主键与变参 conds 取 AND：id=a AND name=bob 无交集 → 0 行
		res := q.DeleteResult(&gfc3User{ID: "a"}, "name = ?", "bob")
		if res.Error != nil {
			t.Fatalf("DeleteResult(交集为空): %v", res.Error)
		}
		if !res.IsZeroRow() {
			t.Errorf("交集为空期望 0 行, 实际 RowsAffected=%d", res.RowsAffected)
		}
		gfc3Expect(t, drv, "a", "alice", 10)

		// id=a AND name=alice 交集命中 → 仅删该行
		if err := q.Delete(&gfc3User{ID: "a"}, "name = ?", "alice"); err != nil {
			t.Fatalf("Delete(bean+conds): %v", err)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Fatalf("期望剩 1 行, 实际 %d", n)
		}

		// 无条件全零 bean：gorm 拒绝（防误全表删除）。差异固化：gorm 报
		// ErrMissingWhereClause（wrapError 不映射原样透出），xorm 报原生空条件
		// 错误——方案 §5.1 固化为"均报错且行数不变"。
		err := q.Delete(&gfc3User{})
		if err == nil {
			t.Fatal("无任何条件的 Delete 应被拒绝")
		}
		if !errors.Is(err, gorm.ErrMissingWhereClause) {
			t.Errorf("gorm 应透出 ErrMissingWhereClause, 实际: %v", err)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Errorf("拒绝后行数不应变化, 实际 %d", n)
		}
	})

	t.Run("Result变体_RowsAffected与IsZeroRow", func(t *testing.T) {
		gfc3Seed(t, drv,
			gfc3User{ID: "a", Name: "alice", Cnt: 10},
			gfc3User{ID: "b", Name: "bob", Cnt: 20},
		)
		q := drv.Query()

		// CRUD-09：CreateResult 单条恒 1；切片为总行数
		res := q.CreateResult(&gfc3User{ID: "r1", Name: "new", Cnt: 1})
		if res.Error != nil || res.RowsAffected != 1 || res.IsZeroRow() {
			t.Errorf("CreateResult 单条期望 RowsAffected=1, 实际 %+v", res)
		}
		res = q.CreateResult([]*gfc3User{
			{ID: "r2", Name: "n2", Cnt: 2},
			{ID: "r3", Name: "n3", Cnt: 3},
			{ID: "r4", Name: "n4", Cnt: 4},
		})
		if res.Error != nil || res.RowsAffected != 3 {
			t.Errorf("CreateResult 切片期望 RowsAffected=3, 实际 %+v", res)
		}
		if n := gfc3Count(t, drv); n != 6 {
			t.Fatalf("CreateResult 后期望 6 行（2 种子+1 单条+3 切片）, 实际 %d", n)
		}

		// 错误路径：重复主键 → ErrDuplicatedKey，行数保持零值
		res = q.CreateResult(&gfc3User{ID: "r2", Name: "dup", Cnt: 9})
		if res.Error == nil || !errors.Is(res.Error, contracts.ErrDuplicatedKey) {
			t.Errorf("CreateResult 错误路径应映射 ErrDuplicatedKey, 实际: %v", res.Error)
		}
		// IsZeroRow 契约语义为「成功且 0 行」（Error==nil && RowsAffected==0），
		// 错误路径下恒为 false——此处只断言行数保持零值。
		if res.RowsAffected != 0 {
			t.Errorf("错误路径行数应保持零值, 实际 %+v", res)
		}

		// UpdateResult：命中 1 / 未命中 0 无错（IsZeroRow）
		res = q.Model(&gfc3User{}).Where("id = ?", "a").UpdateResult("cnt", int64(11))
		if res.Error != nil || res.RowsAffected != 1 || res.IsZeroRow() {
			t.Errorf("UpdateResult 命中期望 1 行, 实际 %+v", res)
		}
		gfc3Expect(t, drv, "a", "alice", 11)
		res = q.Model(&gfc3User{}).Where("id = ?", "zz").UpdateResult("name", "x")
		if res.Error != nil || !res.IsZeroRow() {
			t.Errorf("UpdateResult 未命中期望 IsZeroRow, 实际 %+v", res)
		}

		// UpdatesResult：命中 / 未命中
		res = q.Model(&gfc3User{}).Where("id = ?", "b").
			UpdatesResult(map[string]any{"name": "bob-v2", "cnt": int64(21)})
		if res.Error != nil || res.RowsAffected != 1 || res.IsZeroRow() {
			t.Errorf("UpdatesResult 命中期望 1 行, 实际 %+v", res)
		}
		gfc3Expect(t, drv, "b", "bob-v2", 21)
		res = q.Model(&gfc3User{}).Where("id = ?", "zz").UpdatesResult(map[string]any{"name": "x"})
		if res.Error != nil || !res.IsZeroRow() {
			t.Errorf("UpdatesResult 未命中期望 IsZeroRow, 实际 %+v", res)
		}

		// DeleteResult：命中 1 / 未命中 IsZeroRow
		res = q.DeleteResult(&gfc3User{ID: "r2"})
		if res.Error != nil || res.RowsAffected != 1 || res.IsZeroRow() {
			t.Errorf("DeleteResult 命中期望 1 行, 实际 %+v", res)
		}
		res = q.DeleteResult(&gfc3User{ID: "r2"}) // 再删同主键 → 未命中
		if res.Error != nil || !res.IsZeroRow() {
			t.Errorf("DeleteResult 未命中期望 IsZeroRow, 实际 %+v", res)
		}
		if n := gfc3Count(t, drv); n != 5 {
			t.Errorf("DeleteResult 后期望 5 行, 实际 %d", n)
		}
	})

	t.Run("SaveResult_插入更新upsert与显式条件差异", func(t *testing.T) {
		gfc3Seed(t, drv, gfc3User{ID: "s1", Name: "staged", Cnt: 1})
		q := drv.Query()

		// 更新路径命中（主键非零且行存在）：RowsAffected = 1
		res := q.SaveResult(&gfc3User{ID: "s1", Name: "updated", Cnt: 2})
		if res.Error != nil || res.RowsAffected != 1 || res.IsZeroRow() {
			t.Fatalf("SaveResult 更新命中期望 1 行, 实际 %+v", res)
		}
		gfc3Expect(t, drv, "s1", "updated", 2)

		// 插入路径（主键全零）：RowsAffected = 1
		res = q.SaveResult(&gfc3User{Name: "inserted", Cnt: 3})
		if res.Error != nil || res.RowsAffected != 1 || res.IsZeroRow() {
			t.Fatalf("SaveResult 插入期望 1 行, 实际 %+v", res)
		}
		gfc3Expect(t, drv, "", "inserted", 3)

		// 0 行更新回落插入（upsert）：缺失主键行被重建（两方言均 rows=1，
		// gorm 回落 INSERT ... ON CONFLICT|ODKU，真库实测）
		res = q.SaveResult(&gfc3User{ID: "ghost", Name: "reborn", Cnt: 4})
		if res.Error != nil || res.RowsAffected != 1 {
			t.Fatalf("SaveResult upsert 期望 1 行, 实际 %+v", res)
		}
		gfc3Expect(t, drv, "ghost", "reborn", 4)

		// 差异固化（疑似缺陷，只记录不修改）：链上显式条件 + 行缺失时，gorm
		// 原生 Save 的 upsert 回落忽略链上 Where，行被真实插入（rows=1）；
		// xorm 同参数回落被显式条件限定为 0 行、不落库。
		base := gfc3Count(t, drv)
		res = q.Where("id = ?", "no-such2").SaveResult(&gfc3User{ID: "no-such2", Name: "x", Cnt: 1})
		if res.Error != nil {
			t.Fatalf("显式条件未命中 SaveResult 不应报错: %v", res.Error)
		}
		if res.RowsAffected != 1 || res.IsZeroRow() {
			t.Errorf("gorm upsert 回落应插入该行（rows=1）, 实际 %+v", res)
		}
		if n := gfc3Count(t, drv); n != base+1 {
			t.Errorf("显式条件未命中 Save 应落库 1 行（gorm 差异固化）, 期望 %d, 实际 %d", base+1, n)
		}
		gfc3Expect(t, drv, "no-such2", "x", 1)
	})

	t.Run("时间戳列_插入填充与更新触碰", func(t *testing.T) {
		gfc3ResetTime(t, drv)
		q := drv.Query()

		// CRUD-11：orm:"created/updated" 非零预置不被覆盖（探针实测）
		if err := q.Create(&gfc3TimeUser{ID: "t1", Name: "preset", CreatedAt: 12345, UpdatedAt: 67890}); err != nil {
			t.Fatalf("Create(预置时间戳): %v", err)
		}
		var row gfc3TimeUser
		if err := q.First(&row, "id = ?", "t1"); err != nil {
			t.Fatal(err)
		}
		if row.CreatedAt != 12345 || row.UpdatedAt != 67890 {
			t.Errorf("非零预置时间戳不应被覆盖, 实际 created=%d updated=%d", row.CreatedAt, row.UpdatedAt)
		}

		// 空值插入自动填充 unix 秒
		if err := q.Create(&gfc3TimeUser{ID: "t2", Name: "auto"}); err != nil {
			t.Fatalf("Create(自动时间戳): %v", err)
		}
		var row2 gfc3TimeUser
		if err := q.First(&row2, "id = ?", "t2"); err != nil {
			t.Fatal(err)
		}
		if row2.CreatedAt < 1e9 || row2.UpdatedAt < 1e9 {
			t.Errorf("created/updated 应自动填充 unix 秒, 实际 %d/%d", row2.CreatedAt, row2.UpdatedAt)
		}

		// Updates(map) 经 Model 链触碰 updated_at（gorm 原生 AutoUpdateTime，
		// 差异固化：xorm 侧 map 更新一律不触碰 updated）
		before := row2.UpdatedAt
		time.Sleep(1100 * time.Millisecond) // 秒级精度，跨秒观察触碰
		if err := q.Model(&gfc3TimeUser{}).Where("id = ?", "t2").Updates(map[string]any{"name": "m1"}); err != nil {
			t.Fatalf("Updates(map): %v", err)
		}
		var row3 gfc3TimeUser
		if err := q.First(&row3, "id = ?", "t2"); err != nil {
			t.Fatal(err)
		}
		if row3.UpdatedAt <= before {
			t.Errorf("Model 链 Updates(map) 应触碰 updated_at: %d → %d", before, row3.UpdatedAt)
		}
		if row3.CreatedAt != row2.CreatedAt {
			t.Errorf("更新不应触碰 created_at: %d → %d", row2.CreatedAt, row3.CreatedAt)
		}

		// 差异固化：Table 链（无链上 schema 元数据）Updates(map) 不触碰 updated_at
		time.Sleep(1100 * time.Millisecond)
		if err := q.Table("gfc3_time_users").Where("id = ?", "t2").Updates(map[string]any{"name": "m2"}); err != nil {
			t.Fatalf("Updates(map) via Table: %v", err)
		}
		var row4 gfc3TimeUser
		if err := q.First(&row4, "id = ?", "t2"); err != nil {
			t.Fatal(err)
		}
		if row4.UpdatedAt != row3.UpdatedAt {
			t.Errorf("Table 链 Updates(map) 不应触碰 updated_at: %d → %d", row3.UpdatedAt, row4.UpdatedAt)
		}

		// Update 单列经 Model 链同样触碰 updated_at
		time.Sleep(1100 * time.Millisecond)
		if err := q.Model(&gfc3TimeUser{}).Where("id = ?", "t2").Update("name", "m3"); err != nil {
			t.Fatalf("Update(col): %v", err)
		}
		var row5 gfc3TimeUser
		if err := q.First(&row5, "id = ?", "t2"); err != nil {
			t.Fatal(err)
		}
		if row5.UpdatedAt <= row4.UpdatedAt {
			t.Errorf("Model 链 Update 单列应触碰 updated_at: %d → %d", row4.UpdatedAt, row5.UpdatedAt)
		}
	})

	t.Run("FirstOrCreate_未命中创建与命中回填", func(t *testing.T) {
		gfc3Reset(t, drv)
		q := drv.Query()

		// ADV-01：未命中 → dest 原值插入（主键显式给定 → 落库即该主键行）
		dest := &gfc3User{ID: "nc1", Name: "new-carol", Cnt: 5}
		if err := q.FirstOrCreate(dest, "id = ?", "nc1"); err != nil {
			t.Fatalf("FirstOrCreate(未命中): %v", err)
		}
		if dest.ID != "nc1" || dest.Name != "new-carol" || dest.Cnt != 5 {
			t.Errorf("未命中后 dest 应保留待插入原值, 实际 %+v", dest)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Fatalf("未命中应插入 1 行, 实际 %d", n)
		}
		gfc3Expect(t, drv, "nc1", "new-carol", 5)

		// 命中：返回已有行（回填 dest），不新增；dest 预置值不收窄查询条件
		//（name 与库中不一致仍按 conds 命中）
		dest2 := &gfc3User{ID: "nc1", Name: "mismatch", Cnt: 999}
		if err := q.FirstOrCreate(dest2, "id = ?", "nc1"); err != nil {
			t.Fatalf("FirstOrCreate(命中): %v", err)
		}
		if dest2.ID != "nc1" || dest2.Name != "new-carol" || dest2.Cnt != 5 {
			t.Errorf("命中应按库中行回填 dest, 实际 %+v", dest2)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Errorf("命中后不应新增行, 期望 1 行, 实际 %d", n)
		}

		// 差异固化（方案 ADV-01"conds 不回填 dest"）：空 dest + string conds
		// 未命中时 dest 保持全零（string conds 生成 clause.Expr 不可回填），
		// 落库为全零行（空串主键）
		dest3 := &gfc3User{}
		if err := q.FirstOrCreate(dest3, "id = ?", "nc-empty"); err != nil {
			t.Fatalf("FirstOrCreate(空 dest): %v", err)
		}
		if dest3.ID != "" || dest3.Name != "" {
			t.Errorf("string conds 不回填 dest（差异固化）, 实际 %+v", dest3)
		}
		if n := gfc3Count(t, drv); n != 2 {
			t.Errorf("空 dest 未命中应插入 1 行, 期望 2 行, 实际 %d", n)
		}
		zeroRow := gfc3Read(t, drv, "")
		if zeroRow.Name != "" || zeroRow.Cnt != 0 {
			t.Errorf("空 dest 应按全零值插入, 实际 %+v", zeroRow)
		}
	})

	t.Run("FirstOrInit_命中填充未命中不落库", func(t *testing.T) {
		gfc3Seed(t, drv, gfc3User{ID: "w1", Name: "alice", Cnt: 10})
		q := drv.Query()

		// ADV-02：命中 → 查询结果回填 dest
		dest := &gfc3User{}
		if err := q.FirstOrInit(dest, "id = ?", "w1"); err != nil {
			t.Fatalf("FirstOrInit(命中): %v", err)
		}
		if dest.ID != "w1" || dest.Name != "alice" || dest.Cnt != 10 {
			t.Errorf("命中应回填库中行, 实际 %+v", dest)
		}

		// 未命中 → dest 保持调用方原值、不落库
		miss := &gfc3User{ID: "no1", Name: "preset"}
		if err := q.FirstOrInit(miss, "id = ?", "no1"); err != nil {
			t.Fatalf("FirstOrInit(未命中): %v", err)
		}
		if miss.ID != "no1" || miss.Name != "preset" || miss.Cnt != 0 {
			t.Errorf("未命中 dest 应保持原值, 实际 %+v", miss)
		}
		if n := gfc3Count(t, drv); n != 1 {
			t.Errorf("FirstOrInit 不应落库, 期望 1 行, 实际 %d", n)
		}
	})

	t.Run("FindInBatches_分批回调与尾批", func(t *testing.T) {
		rows := make([]gfc3User, 0, 10)
		for i := 0; i < 10; i++ {
			rows = append(rows, gfc3User{ID: fmt.Sprintf("f%02d", i), Name: fmt.Sprintf("fn%d", i), Cnt: int64(i)})
		}
		gfc3Seed(t, drv, rows...)
		q := drv.Query()

		var users []gfc3User
		var batches []int // 回调序号
		var cums []int    // 回调时 dest 行数
		var seen []string
		err := q.Model(&gfc3User{}).Order("id").FindInBatches(&users, 3, func(tx contracts.Query, batch int) error {
			batches = append(batches, batch)
			cums = append(cums, len(users))
			for _, u := range users {
				seen = append(seen, u.ID)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("FindInBatches: %v", err)
		}
		// 10 行 / 批 3 → 4 次回调（3+3+3+1 尾批），序号 1..4
		if len(batches) != 4 {
			t.Fatalf("期望 4 次回调, 实际 %d", len(batches))
		}
		for i, b := range batches {
			if b != i+1 {
				t.Errorf("回调序号期望 %d, 实际 %d", i+1, b)
			}
		}
		// 差异固化：gorm dest 每批重置（每批 Find 覆盖 dest），累计行数 3,3,3,1；
		// xorm 侧累积为 3,6,9,10。
		wantCum := []int{3, 3, 3, 1}
		for i := range cums {
			if cums[i] != wantCum[i] {
				t.Errorf("第 %d 批 dest 行数期望 %d, 实际 %d", i+1, wantCum[i], cums[i])
			}
		}
		// 全量行无重复/遗漏（跨批收集）
		if len(seen) != 10 {
			t.Fatalf("应遍历全部 10 行, 实际 %d", len(seen))
		}
		distinct := map[string]bool{}
		for _, id := range seen {
			distinct[id] = true
		}
		if len(distinct) != 10 {
			t.Errorf("应无重复/遗漏批次行, 实际去重 %d", len(distinct))
		}
		// 终态 dest 仅含末批 1 行（gorm 差异固化）
		if len(users) != 1 || users[0].ID != "f09" {
			t.Errorf("终态 dest 应仅含末批 f09, 实际 %+v", users)
		}
	})

	t.Run("FindInBatches_空表与回调错误中断", func(t *testing.T) {
		q := drv.Query()

		// ADV-03：空表无回调、dest 为空、不报错
		gfc3Reset(t, drv)
		var users []gfc3User
		callbacks := 0
		err := q.Model(&gfc3User{}).FindInBatches(&users, 2, func(tx contracts.Query, batch int) error {
			callbacks++
			return nil
		})
		if err != nil {
			t.Fatalf("空表 FindInBatches: %v", err)
		}
		if callbacks != 0 {
			t.Errorf("空表不应触发回调, 实际 %d 次", callbacks)
		}
		if len(users) != 0 {
			t.Errorf("空表 dest 应为空, 实际 %d 行", len(users))
		}

		// 回调错误中断：错误原样透出、终态 dest 仅含中断批（差异固化：gorm
		// dest 每批重置，中断时仅第 2 批 3 行；xorm 累积前两批 6 行）、库中数据不受影响
		rows := make([]gfc3User, 0, 10)
		for i := 0; i < 10; i++ {
			rows = append(rows, gfc3User{ID: fmt.Sprintf("e%02d", i), Name: fmt.Sprintf("en%d", i), Cnt: int64(i)})
		}
		gfc3Seed(t, drv, rows...)

		interrupt := errors.New("gfc3: 回调中断")
		var users2 []gfc3User
		var batchNos []int
		callbacks = 0
		err = q.Model(&gfc3User{}).Order("id").FindInBatches(&users2, 3, func(tx contracts.Query, batch int) error {
			callbacks++
			batchNos = append(batchNos, batch)
			if batch == 2 {
				return interrupt
			}
			return nil
		})
		if err == nil || !errors.Is(err, interrupt) {
			t.Fatalf("回调错误应原样透出, 实际: %v", err)
		}
		if callbacks != 2 {
			t.Errorf("期望第 2 批后中断（共 2 次回调）, 实际 %d 次", callbacks)
		}
		if len(batchNos) != 2 || batchNos[1] != 2 {
			t.Errorf("回调序号期望 [1 2], 实际 %v", batchNos)
		}
		if len(users2) != 3 || users2[0].ID != "e03" {
			t.Errorf("中断时 dest 应仅含第 2 批 3 行（e03..e05）, 实际 %+v", users2)
		}
		if n := gfc3Count(t, drv); n != 10 {
			t.Errorf("库中行数不应受中断影响, 期望 10, 实际 %d", n)
		}
	})

	t.Run("WithContext_取消拒绝执行", func(t *testing.T) {
		gfc3Seed(t, drv, gfc3User{ID: "a", Name: "alice", Cnt: 10})
		q := drv.Query()

		// ADV-05：已取消 context 拒绝执行（Find 读路径 + Exec 写路径）
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var rows []gfc3User
		err := q.WithContext(ctx).Model(&gfc3User{}).Where("id = ?", "a").Find(&rows)
		if err == nil {
			t.Error("已取消 context 的 Find 应拒绝执行")
		}
		err = q.WithContext(ctx).Exec("UPDATE gfc3_users SET cnt = 99 WHERE id = 'a'")
		if err == nil {
			t.Error("已取消 context 的 Exec 应拒绝执行")
		}
		// 拒绝执行 → 数据不被改动
		gfc3Expect(t, drv, "a", "alice", 10)
	})
}

// TestFullCovWrite_PG PG 入口。
func TestFullCovWrite_PG(t *testing.T) {
	runFullCovWrite(t, newIntgPG(t), "pg")
}

// TestFullCovWrite_MySQL MySQL 入口。
func TestFullCovWrite_MySQL(t *testing.T) {
	runFullCovWrite(t, newIntgMySQL(t), "mysql")
}
