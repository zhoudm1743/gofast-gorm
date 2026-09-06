package gormdriver

import (
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/zhoudm1743/go-fast-framework/database"
	"gorm.io/gorm"
)

// testUser 嵌入框架基础模型的测试模型。
type testUser struct {
	database.Model
	Name string
}

// newTestGormDB 创建内存 sqlite 的 gorm 实例并迁移模型。
func newTestGormDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("创建测试数据库失败: %v", err)
	}
	// 与生产驱动一致：注册 ID 自动生成回调
	if err := registerAutoGenerateIDCallback(db); err != nil {
		t.Fatalf("注册 ID 回调失败: %v", err)
	}
	if err := db.AutoMigrate(&testUser{}); err != nil {
		t.Fatalf("自动迁移失败: %v", err)
	}
	return db
}

// TestModel_CreateGeneratesID 验证普通 Create 自动生成主键 ID。
func TestModel_CreateGeneratesID(t *testing.T) {
	db := newTestGormDB(t)

	u := &testUser{Name: "alice"}
	if err := db.Create(u).Error; err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	if u.ID == "" {
		t.Fatal("Create 后 ID 不应为空")
	}
}

// TestModel_FirstOrCreateGeneratesID 验证 FirstOrCreate 内部创建路径也生成主键 ID
// （该路径不经过框架驱动层的 invokeBeforeCreate，依赖 Create 前回调）。
func TestModel_FirstOrCreateGeneratesID(t *testing.T) {
	db := newTestGormDB(t)

	// 未命中：应创建并生成 ID
	u1 := &testUser{Name: "bob"}
	if err := db.Where("name = ?", "bob").FirstOrCreate(u1).Error; err != nil {
		t.Fatalf("FirstOrCreate 失败: %v", err)
	}
	if u1.ID == "" {
		t.Fatal("FirstOrCreate 创建后 ID 不应为空")
	}

	// 命中已有记录：应返回已有记录（ID 不变），不重复创建
	u2 := &testUser{Name: "bob"}
	if err := db.Where("name = ?", "bob").FirstOrCreate(u2).Error; err != nil {
		t.Fatalf("二次 FirstOrCreate 失败: %v", err)
	}
	if u2.ID != u1.ID {
		t.Fatalf("命中已有记录应返回同一 ID: 期望 %s 实际 %s", u1.ID, u2.ID)
	}
}

// TestModel_FirstOrInitNoCreate 验证 FirstOrInit 不触发创建、不生成 ID
// （该方法只在内存中初始化，不写数据库）。
func TestModel_FirstOrInitNoCreate(t *testing.T) {
	db := newTestGormDB(t)

	u := &testUser{Name: "carol"}
	if err := db.Where("name = ?", "carol").FirstOrInit(u).Error; err != nil {
		t.Fatalf("FirstOrInit 失败: %v", err)
	}
	if u.ID != "" {
		t.Fatal("FirstOrInit 未创建记录，ID 应为空")
	}
}
