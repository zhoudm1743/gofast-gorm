//go:build integration

package gormdriver

// fullcov_rel_integration_test.go —— 关联功能域真实库补强包（双驱动测试方案
// §六 第 5 项 / §5.6 REL 矩阵：has-one / 复合外键 / polymorphic 缺省值 /
// references 非主键 / 自引用树 / many2many conds / 错误路径 无 P/M 覆盖）。
//
// 运行方式（docker-compose.yml 环境）：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	  go test -tags integration -run 'TestFullCovRel_PG$' -v .
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -run 'TestFullCovRel_MySQL$' -v .
//
// 关联域 gorm 侧已有 belongs-to / has-many / many2many 的 S/P/M 覆盖
//（pg_integration_test.go runIntgPreloadMatrix 等），本文件补缺口，8 组：
//
//	REL-H1_has-one基础    一对一回填/无子行零值/conds 过滤（原生 + 引擎双路径）
//	REL-H1_has-one嵌套    用户→档案→头像两层回填（双路径）
//	REL-C1_复合外键       联合外键两列元组 IN（双路径，含 conds）
//	REL-B2_references非主键 belongs-to 自定义引用列指向父表非主键唯一列（双路径）
//	REL-P1_polymorphic缺省值 显式值 + 缺省值回退父表名（原生 + 引擎各一对）
//	REL-S1_自引用树       parent_id 自引用 + 嵌套预加载两层（双路径）
//	REL-M2_many2many_conds 中间表无模型、conds 作用于子表第二跳（双路径）
//	REL-E1_错误路径       未知字段/非关联字段/两方向不成立（差异固化）
//
// 双路径组织（对齐 preload_engine_test.go / fullcov_ext PL-02 口径）：
// 同一关联形态各建一对模型——gorm 原生路径（orm:"-" gorm:"foreignKey:..."，
// Preload 由 gorm 原生回调执行）与共享引擎路径（orm:"-" gorm:"-" rel:"..."，
// Preload 分流到共享 Preload 引擎），共享同一组物理表与种子数据，回填逐项
// 断言一致。每组用 intgMigrate 独立建表灌数（DROP 后重建，失败重跑无残留）。
//
// polymorphic 缺省值实测规则（REL-P1，源码推导 + 真实库固化）：
//   - gorm 原生：polymorphicValue 缺省 = schema.Table（父模型解析后的表名），
//     见 gorm/schema/relationship.go buildPolymorphicRelation（Value: schema.Table）；
//   - 共享引擎：polymorphicValue 缺省 = MetaAdapter.TableName(父模型)（契约 8
//     "缺省值取父表名"），见 preload/relation.go resolvePolymorphic；
//   - 两者在本文件实测均回退父表裸名（gfcr_p1_users），显式 polymorphicValue
//     均可覆盖；类型列隔离负例（noise）两路径均不回填。
//
// 本文件模型/辅助全部 gfcr 前缀，表前缀 gfcr_，不与既有 fullcov_*/intg_*
// 模型冲突；复用同包 intgMigrate/intgMustExec/newIntgPG/newIntgMySQL。

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"gorm.io/gorm"
)

// ── 测试模型（前缀 gfcr_，全部显式 TableName；N=原生路径 / E=共享引擎路径）──

// ── REL-H1：has-one（一对一）──

// gfcrH1Avatar 头像叶子模型（两层嵌套第三表，双路径共用叶子）。
type gfcrH1Avatar struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	ProfileID string `orm:"varchar(16) 'profile_id'"`
	URL       string `orm:"varchar(120) 'url'"`
}

func (gfcrH1Avatar) TableName() string { return "gfcr_h1_avatars" }

// gfcrH1ProfileN 原生路径档案（Avatar 为 gorm 原生 has-one）。
type gfcrH1ProfileN struct {
	ID     string        `orm:"pk varchar(16) 'id'"`
	UserID string        `orm:"varchar(16) 'user_id'"`
	Bio    string        `orm:"varchar(64) 'bio'"`
	Avatar *gfcrH1Avatar `orm:"-" gorm:"foreignKey:ProfileID;references:ID"`
}

func (gfcrH1ProfileN) TableName() string { return "gfcr_h1_profiles" }

// gfcrH1ProfileE 引擎路径档案（Avatar 为 gorm:"-" + rel）。
type gfcrH1ProfileE struct {
	ID     string        `orm:"pk varchar(16) 'id'"`
	UserID string        `orm:"varchar(16) 'user_id'"`
	Bio    string        `orm:"varchar(64) 'bio'"`
	Avatar *gfcrH1Avatar `orm:"-" gorm:"-" rel:"foreignKey:ProfileID;references:ID"`
}

func (gfcrH1ProfileE) TableName() string { return "gfcr_h1_profiles" }

// gfcrH1UserN 原生路径用户（Profile 为 gorm 原生 has-one）。
type gfcrH1UserN struct {
	ID      string          `orm:"pk varchar(16) 'id'"`
	Name    string          `orm:"varchar(64) 'name'"`
	Profile *gfcrH1ProfileN `orm:"-" gorm:"foreignKey:UserID;references:ID"`
}

func (gfcrH1UserN) TableName() string { return "gfcr_h1_users" }

// gfcrH1UserE 引擎路径用户（Profile 为 gorm:"-" + rel）。
type gfcrH1UserE struct {
	ID      string          `orm:"pk varchar(16) 'id'"`
	Name    string          `orm:"varchar(64) 'name'"`
	Profile *gfcrH1ProfileE `orm:"-" gorm:"-" rel:"foreignKey:UserID;references:ID"`
}

func (gfcrH1UserE) TableName() string { return "gfcr_h1_users" }

// ── REL-C1：复合外键（两列联合外键）──

// gfcrC1Record 记录叶子（联合外键 user_site_id + user_locale，双路径共用叶子）。
type gfcrC1Record struct {
	ID         string `orm:"pk varchar(16) 'id'"`
	UserSiteID string `orm:"varchar(16) 'user_site_id'"`
	UserLocale string `orm:"varchar(16) 'user_locale'"`
	Note       string `orm:"varchar(32) 'note'"`
}

func (gfcrC1Record) TableName() string { return "gfcr_c1_records" }

// gfcrC1UserN 原生路径复合外键父表。
type gfcrC1UserN struct {
	ID      string         `orm:"pk varchar(16) 'id'"`
	SiteID  string         `orm:"varchar(16) 'site_id'"`
	Locale  string         `orm:"varchar(16) 'locale'"`
	Records []gfcrC1Record `orm:"-" gorm:"foreignKey:UserSiteID,UserLocale;references:SiteID,Locale"`
}

func (gfcrC1UserN) TableName() string { return "gfcr_c1_users" }

// gfcrC1UserE 引擎路径复合外键父表（ormtag splitFieldList 解析逗号复合键）。
type gfcrC1UserE struct {
	ID      string         `orm:"pk varchar(16) 'id'"`
	SiteID  string         `orm:"varchar(16) 'site_id'"`
	Locale  string         `orm:"varchar(16) 'locale'"`
	Records []gfcrC1Record `orm:"-" gorm:"-" rel:"foreignKey:UserSiteID,UserLocale;references:SiteID,Locale"`
}

func (gfcrC1UserE) TableName() string { return "gfcr_c1_users" }

// ── REL-B2：belongs-to references 非主键唯一列 ──

// gfcrB2User 用户（code 为非主键唯一列，被引用列）。
type gfcrB2User struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Code string `orm:"varchar(16) 'code' unique"`
	Name string `orm:"varchar(64) 'name'"`
}

func (gfcrB2User) TableName() string { return "gfcr_b2_users" }

// gfcrB2CardN 原生路径卡片（Owner belongs-to：user_code → users.code 非主键）。
type gfcrB2CardN struct {
	ID       string      `orm:"pk varchar(16) 'id'"`
	UserCode string      `orm:"varchar(16) 'user_code'"`
	Label    string      `orm:"varchar(32) 'label'"`
	Owner    *gfcrB2User `orm:"-" gorm:"foreignKey:UserCode;references:Code"`
}

func (gfcrB2CardN) TableName() string { return "gfcr_b2_cards" }

// gfcrB2CardE 引擎路径卡片（同形态 rel tag）。
type gfcrB2CardE struct {
	ID       string      `orm:"pk varchar(16) 'id'"`
	UserCode string      `orm:"varchar(16) 'user_code'"`
	Label    string      `orm:"varchar(32) 'label'"`
	Owner    *gfcrB2User `orm:"-" gorm:"-" rel:"foreignKey:UserCode;references:Code"`
}

func (gfcrB2CardE) TableName() string { return "gfcr_b2_cards" }

// ── REL-P1：polymorphic（显式值 + 缺省值回退父表名）──

// gfcrP1Photo 多态照片叶子（owner_id + owner_type，四父模型共用叶子）。
type gfcrP1Photo struct {
	ID        string `orm:"pk varchar(16) 'id'"`
	OwnerID   string `orm:"varchar(16) 'owner_id'"`
	OwnerType string `orm:"varchar(32) 'owner_type'"`
	URL       string `orm:"varchar(64) 'url'"`
}

func (gfcrP1Photo) TableName() string { return "gfcr_p1_photos" }

// gfcrP1UserN 原生路径·缺省 polymorphicValue（实测回退 = 表名 gfcr_p1_users）。
type gfcrP1UserN struct {
	ID     string        `orm:"pk varchar(16) 'id'"`
	Name   string        `orm:"varchar(64) 'name'"`
	Photos []gfcrP1Photo `orm:"-" gorm:"polymorphic:Owner"`
}

func (gfcrP1UserN) TableName() string { return "gfcr_p1_users" }

// gfcrP1UserX 原生路径·显式 polymorphicValue:custom_type。
type gfcrP1UserX struct {
	ID     string        `orm:"pk varchar(16) 'id'"`
	Name   string        `orm:"varchar(64) 'name'"`
	Photos []gfcrP1Photo `orm:"-" gorm:"polymorphic:Owner;polymorphicValue:custom_type"`
}

func (gfcrP1UserX) TableName() string { return "gfcr_p1_users" }

// gfcrP1UserE 引擎路径·显式 polymorphicValue:custom_type。
type gfcrP1UserE struct {
	ID     string        `orm:"pk varchar(16) 'id'"`
	Name   string        `orm:"varchar(64) 'name'"`
	Photos []gfcrP1Photo `orm:"-" gorm:"-" rel:"polymorphic:Owner;polymorphicValue:custom_type"`
}

func (gfcrP1UserE) TableName() string { return "gfcr_p1_users" }

// gfcrP1UserD 引擎路径·缺省 polymorphicValue（契约 8：缺省回退父表名）。
type gfcrP1UserD struct {
	ID     string        `orm:"pk varchar(16) 'id'"`
	Name   string        `orm:"varchar(64) 'name'"`
	Photos []gfcrP1Photo `orm:"-" gorm:"-" rel:"polymorphic:Owner"`
}

func (gfcrP1UserD) TableName() string { return "gfcr_p1_users" }

// ── REL-S1：自引用树 ──

// gfcrS1NodeN 原生路径自引用节点（parent_id → id，同表自关联）。
type gfcrS1NodeN struct {
	ID       string        `orm:"pk varchar(16) 'id'"`
	ParentID string        `orm:"varchar(16) 'parent_id' null"`
	Name     string        `orm:"varchar(64) 'name'"`
	Children []gfcrS1NodeN `orm:"-" gorm:"foreignKey:ParentID;references:ID"`
}

func (gfcrS1NodeN) TableName() string { return "gfcr_s1_nodes" }

// gfcrS1NodeE 引擎路径自引用节点。
type gfcrS1NodeE struct {
	ID       string        `orm:"pk varchar(16) 'id'"`
	ParentID string        `orm:"varchar(16) 'parent_id' null"`
	Name     string        `orm:"varchar(64) 'name'"`
	Children []gfcrS1NodeE `orm:"-" gorm:"-" rel:"foreignKey:ParentID;references:ID"`
}

func (gfcrS1NodeE) TableName() string { return "gfcr_s1_nodes" }

// ── REL-M2：many2many + conds（中间表无业务模型）──

// gfcrM2Role 角色叶子（many2many 子表，双路径共用叶子）。
type gfcrM2Role struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Code string `orm:"varchar(16) 'code'"`
	Name string `orm:"varchar(64) 'name'"`
}

func (gfcrM2Role) TableName() string { return "gfcr_m2_roles" }

// gfcrM2UserN 原生路径 many2many 父表。
// 差异固化（tag 语义）：gorm 原生 joinForeignKey/joinReferences 是【中间表列名】，
// 缺省绑定父/子模型主键；引用非主键字段必须显式补 foreignKey/references 指向
// 父/子模型字段。引擎 rel tag 的 joinForeignKey/joinReferences 则直接是父/子
// 模型【Go 字段名】（缺省回退主键）——本组原生侧用全显式形态与引擎侧对齐到
// 同一中间表列 (s_key, code) 与同一套种子数据。
type gfcrM2UserN struct {
	ID    string       `orm:"pk varchar(16) 'id'"`
	SKey  string       `orm:"varchar(16) 's_key'"`
	Roles []gfcrM2Role `orm:"-" gorm:"many2many:gfcr_m2_user_roles;foreignKey:SKey;joinForeignKey:SKey;references:Code;joinReferences:Code"`
}

func (gfcrM2UserN) TableName() string { return "gfcr_m2_users" }

// gfcrM2UserE 引擎路径 many2many 父表（中间表 gfcr_m2_user_roles 无模型）。
type gfcrM2UserE struct {
	ID    string       `orm:"pk varchar(16) 'id'"`
	SKey  string       `orm:"varchar(16) 's_key'"`
	Roles []gfcrM2Role `orm:"-" gorm:"-" rel:"many2many:gfcr_m2_user_roles;joinForeignKey:SKey;joinReferences:Code"`
}

func (gfcrM2UserE) TableName() string { return "gfcr_m2_users" }

// ── REL-E1：错误路径（同一模型覆盖原生/引擎两类错误族）──

// gfcrE1Leaf 无关叶子模型（仅作为"非关联 struct 字段"的类型载体，无需建表：
// 关联解析在查询发起前即报错）。
type gfcrE1Leaf struct {
	ID string `orm:"pk varchar(16) 'id'"`
}

// gfcrE1Model 错误路径模型：Extra（非关联标量字段）/ Other（无 rel tag 的
// struct 字段）均带 gorm:"-" 走引擎路径；Name 为普通列字段走原生路径。
type gfcrE1Model struct {
	ID    string      `orm:"pk varchar(16) 'id'"`
	Name  string      `orm:"varchar(64) 'name'"`
	Extra string      `orm:"-" gorm:"-"`
	Other *gfcrE1Leaf `orm:"-" gorm:"-"`
}

func (gfcrE1Model) TableName() string { return "gfcr_e1_models" }

// ── 断言辅助 ─────────────────────────────────────────────────────────────

// gfcrSortedIDs 子行 id 集合排序（方言/引擎返回序不敏感比较）。
func gfcrSortedIDs(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

// gfcrSeed 逐条灌种子（失败即 Fatal，行内不遗留半量数据歧义）。
func gfcrSeed(t *testing.T, drv *GormDriver, seeds ...any) {
	t.Helper()
	for _, s := range seeds {
		if err := drv.Query().Create(s); err != nil {
			t.Fatalf("种子 %T %+v: %v", s, s, err)
		}
	}
}

// ── 分组入口 ───────────────────────────────────────────────────────────

// TestFullCovRel_PG PostgreSQL 关联域补强入口。
func TestFullCovRel_PG(t *testing.T) {
	runFullCovRel(t, newIntgPG(t), "pg")
}

// TestFullCovRel_MySQL MySQL 关联域补强入口。
func TestFullCovRel_MySQL(t *testing.T) {
	runFullCovRel(t, newIntgMySQL(t), "mysql")
}

// runFullCovRel 共享 runner：8 组关联用例，dialect 区分方言分支（"pg"/"mysql"）。
func runFullCovRel(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	t.Run("REL-H1_has-one基础", func(t *testing.T) { runGfcrHasOne(t, drv, dialect) })
	t.Run("REL-H1_has-one嵌套", func(t *testing.T) { runGfcrHasOneNested(t, drv, dialect) })
	t.Run("REL-C1_复合外键", func(t *testing.T) { runGfcrCompositeFK(t, drv, dialect) })
	t.Run("REL-B2_references非主键", func(t *testing.T) { runGfcrRefNonPK(t, drv, dialect) })
	t.Run("REL-P1_polymorphic缺省值", func(t *testing.T) { runGfcrPolyDefault(t, drv, dialect) })
	t.Run("REL-S1_自引用树", func(t *testing.T) { runGfcrSelfRef(t, drv, dialect) })
	t.Run("REL-M2_many2many_conds", func(t *testing.T) { runGfcrM2MConds(t, drv, dialect) })
	t.Run("REL-E1_错误路径", func(t *testing.T) { runGfcrErrPath(t, drv, dialect) })
}

// ── REL-H1：has-one 基础（回填 / 无子行零值 / conds 过滤）──────────────

func runGfcrHasOne(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect // 关联回填语义两方言一致，无方言分支
	intgMigrate(t, drv, []string{"gfcr_h1_users", "gfcr_h1_profiles", "gfcr_h1_avatars"},
		&gfcrH1UserN{}, &gfcrH1ProfileN{}, &gfcrH1Avatar{}, &gfcrH1UserE{}, &gfcrH1ProfileE{})
	gfcrSeed(t, drv,
		&gfcrH1UserN{ID: "h1u1", Name: "alice"},
		&gfcrH1UserN{ID: "h1u2", Name: "bob"},   // 无档案
		&gfcrH1UserN{ID: "h1u3", Name: "carol"}, // 有档案
		&gfcrH1ProfileN{ID: "h1p1", UserID: "h1u1", Bio: "bio-1"},
		&gfcrH1ProfileN{ID: "h1p3", UserID: "h1u3", Bio: "bio-3"},
	)

	// 原生路径：有子行回填 / 无子行保持 nil
	var nRows []gfcrH1UserN
	if err := drv.Query().Model(&gfcrH1UserN{}).Order("id").Preload("Profile").Find(&nRows); err != nil {
		t.Fatalf("原生 has-one Preload: %v", err)
	}
	if len(nRows) != 3 {
		t.Fatalf("原生路径应 3 用户, 实际 %d", len(nRows))
	}
	if nRows[0].Profile == nil || nRows[0].Profile.Bio != "bio-1" {
		t.Errorf("原生 u1.Profile 应回填 bio-1, 实际 %+v", nRows[0].Profile)
	}
	if nRows[1].Profile != nil {
		t.Errorf("原生 u2 无档案应保持 nil, 实际 %+v", nRows[1].Profile)
	}
	if nRows[2].Profile == nil || nRows[2].Profile.Bio != "bio-3" {
		t.Errorf("原生 u3.Profile 应回填 bio-3, 实际 %+v", nRows[2].Profile)
	}

	// 引擎路径：逐项一致（has-one 回填单个、无子行零值，契约 5）
	var eRows []gfcrH1UserE
	if err := drv.Query().Model(&gfcrH1UserE{}).Order("id").Preload("Profile").Find(&eRows); err != nil {
		t.Fatalf("引擎 has-one Preload: %v", err)
	}
	if len(eRows) != 3 {
		t.Fatalf("引擎路径应 3 用户, 实际 %d", len(eRows))
	}
	if eRows[0].Profile == nil || eRows[0].Profile.Bio != "bio-1" {
		t.Errorf("引擎 u1.Profile 应回填 bio-1, 实际 %+v", eRows[0].Profile)
	}
	if eRows[1].Profile != nil {
		t.Errorf("引擎 u2 无档案应保持 nil（契约 5）, 实际 %+v", eRows[1].Profile)
	}
	if eRows[2].Profile == nil || eRows[2].Profile.Bio != "bio-3" {
		t.Errorf("引擎 u3.Profile 应回填 bio-3, 实际 %+v", eRows[2].Profile)
	}

	// conds 过滤子行：仅 bio-1 命中 → u1 保留、u3 被过滤为 nil（双路径一致）
	var nCond []gfcrH1UserN
	if err := drv.Query().Model(&gfcrH1UserN{}).Order("id").
		Preload("Profile", "bio = ?", "bio-1").Find(&nCond); err != nil {
		t.Fatalf("原生 conds Preload: %v", err)
	}
	if nCond[0].Profile == nil || nCond[0].Profile.Bio != "bio-1" || nCond[2].Profile != nil {
		t.Errorf("原生 conds 应仅回填 u1（u3 被过滤为 nil）, 实际 u1=%+v u3=%+v", nCond[0].Profile, nCond[2].Profile)
	}
	var eCond []gfcrH1UserE
	if err := drv.Query().Model(&gfcrH1UserE{}).Order("id").
		Preload("Profile", "bio = ?", "bio-1").Find(&eCond); err != nil {
		t.Fatalf("引擎 conds Preload: %v", err)
	}
	if eCond[0].Profile == nil || eCond[0].Profile.Bio != "bio-1" || eCond[2].Profile != nil {
		t.Errorf("引擎 conds 应仅回填 u1（u3 被过滤为 nil）, 实际 u1=%+v u3=%+v", eCond[0].Profile, eCond[2].Profile)
	}
}

// ── REL-H1：has-one 嵌套（用户→档案→头像两层）─────────────────────────

func runGfcrHasOneNested(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	intgMigrate(t, drv, []string{"gfcr_h1_users", "gfcr_h1_profiles", "gfcr_h1_avatars"},
		&gfcrH1UserN{}, &gfcrH1ProfileN{}, &gfcrH1Avatar{}, &gfcrH1UserE{}, &gfcrH1ProfileE{})
	gfcrSeed(t, drv,
		&gfcrH1UserN{ID: "h1u1", Name: "alice"},
		&gfcrH1UserN{ID: "h1u2", Name: "bob"},   // 无档案（第二层无父行）
		&gfcrH1UserN{ID: "h1u3", Name: "carol"}, // 档案无头像
		&gfcrH1ProfileN{ID: "h1p1", UserID: "h1u1", Bio: "bio-1"},
		&gfcrH1ProfileN{ID: "h1p3", UserID: "h1u3", Bio: "bio-3"},
		&gfcrH1Avatar{ID: "h1a1", ProfileID: "h1p1", URL: "avatar-1"},
	)

	// 原生路径：两层回填 u1→p1→av1；u3 档案有、头像 nil；u2 档案 nil
	var nRows []gfcrH1UserN
	if err := drv.Query().Model(&gfcrH1UserN{}).Order("id").
		Preload("Profile.Avatar").Find(&nRows); err != nil {
		t.Fatalf("原生嵌套 has-one Preload: %v", err)
	}
	if len(nRows) != 3 {
		t.Fatalf("原生路径应 3 用户, 实际 %d", len(nRows))
	}
	if nRows[0].Profile == nil || nRows[0].Profile.Avatar == nil || nRows[0].Profile.Avatar.URL != "avatar-1" {
		t.Errorf("原生 u1 应两层回填 avatar-1, 实际 %+v", nRows[0].Profile)
	}
	if nRows[1].Profile != nil {
		t.Errorf("原生 u2 无档案应 nil, 实际 %+v", nRows[1].Profile)
	}
	if nRows[2].Profile == nil || nRows[2].Profile.Avatar != nil {
		t.Errorf("原生 u3 档案应回填且头像保持 nil, 实际 %+v", nRows[2].Profile)
	}

	// 引擎路径：两层递归（契约 2：以本层回填后的子行作为下层父行）
	var eRows []gfcrH1UserE
	if err := drv.Query().Model(&gfcrH1UserE{}).Order("id").
		Preload("Profile.Avatar").Find(&eRows); err != nil {
		t.Fatalf("引擎嵌套 has-one Preload: %v", err)
	}
	if len(eRows) != 3 {
		t.Fatalf("引擎路径应 3 用户, 实际 %d", len(eRows))
	}
	if eRows[0].Profile == nil || eRows[0].Profile.Avatar == nil || eRows[0].Profile.Avatar.URL != "avatar-1" {
		t.Errorf("引擎 u1 应两层回填 avatar-1, 实际 %+v", eRows[0].Profile)
	}
	if eRows[1].Profile != nil {
		t.Errorf("引擎 u2 无档案应 nil, 实际 %+v", eRows[1].Profile)
	}
	if eRows[2].Profile == nil || eRows[2].Profile.Avatar != nil {
		t.Errorf("引擎 u3 档案应回填且头像保持 nil, 实际 %+v", eRows[2].Profile)
	}
}

// ── REL-C1：复合外键（两列联合外键）────────────────────────────────────

func runGfcrCompositeFK(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	intgMigrate(t, drv, []string{"gfcr_c1_users", "gfcr_c1_records"},
		&gfcrC1UserN{}, &gfcrC1UserE{}, &gfcrC1Record{})
	gfcrSeed(t, drv,
		&gfcrC1UserN{ID: "c1u1", SiteID: "s1", Locale: "zh"},
		&gfcrC1UserN{ID: "c1u2", SiteID: "s1", Locale: "en"},
		&gfcrC1UserN{ID: "c1u3", SiteID: "s2", Locale: "zh"},
		&gfcrC1Record{ID: "c1r1", UserSiteID: "s1", UserLocale: "zh", Note: "n1"},
		&gfcrC1Record{ID: "c1r2", UserSiteID: "s1", UserLocale: "en", Note: "n2"},
		&gfcrC1Record{ID: "c1r3", UserSiteID: "s2", UserLocale: "zh", Note: "n3"},
		&gfcrC1Record{ID: "c1r4", UserSiteID: "s1", UserLocale: "zh", Note: "n4"},
	)
	recIDs := func(rs []gfcrC1Record) []string {
		out := make([]string, 0, len(rs))
		for _, r := range rs {
			out = append(out, r.ID)
		}
		return gfcrSortedIDs(out)
	}

	// 原生路径：复合外键 (site_id, locale) 精确配对，不错位
	var nRows []gfcrC1UserN
	if err := drv.Query().Model(&gfcrC1UserN{}).Order("id").Preload("Records").Find(&nRows); err != nil {
		t.Fatalf("原生复合外键 Preload: %v", err)
	}
	if len(nRows) != 3 {
		t.Fatalf("原生路径应 3 父行, 实际 %d", len(nRows))
	}
	if got := recIDs(nRows[0].Records); fmt.Sprint(got) != "[c1r1 c1r4]" {
		t.Errorf("原生 u1 复合键应命中 [c1r1 c1r4], 实际 %v", got)
	}
	if got := recIDs(nRows[1].Records); fmt.Sprint(got) != "[c1r2]" {
		t.Errorf("原生 u2 (s1,en) 不应错位命中 zh 行, 实际 %v", got)
	}
	if got := recIDs(nRows[2].Records); fmt.Sprint(got) != "[c1r3]" {
		t.Errorf("原生 u3 (s2,zh) 应命中 [c1r3], 实际 %v", got)
	}

	// 引擎路径：复合外键手写元组 IN（契约 1），逐项一致
	var eRows []gfcrC1UserE
	if err := drv.Query().Model(&gfcrC1UserE{}).Order("id").Preload("Records").Find(&eRows); err != nil {
		t.Fatalf("引擎复合外键 Preload: %v", err)
	}
	if len(eRows) != 3 {
		t.Fatalf("引擎路径应 3 父行, 实际 %d", len(eRows))
	}
	if got := recIDs(eRows[0].Records); fmt.Sprint(got) != "[c1r1 c1r4]" {
		t.Errorf("引擎 u1 复合键应命中 [c1r1 c1r4], 实际 %v", got)
	}
	if got := recIDs(eRows[1].Records); fmt.Sprint(got) != "[c1r2]" {
		t.Errorf("引擎 u2 (s1,en) 不应错位命中 zh 行, 实际 %v", got)
	}
	if got := recIDs(eRows[2].Records); fmt.Sprint(got) != "[c1r3]" {
		t.Errorf("引擎 u3 (s2,zh) 应命中 [c1r3], 实际 %v", got)
	}

	// conds 过滤子表（双路径一致）：note = n4 → u1 仅 [c1r4]
	var nCond []gfcrC1UserN
	if err := drv.Query().Model(&gfcrC1UserN{}).Order("id").
		Preload("Records", "note = ?", "n4").Find(&nCond); err != nil {
		t.Fatalf("原生复合外键 conds: %v", err)
	}
	if got := recIDs(nCond[0].Records); fmt.Sprint(got) != "[c1r4]" {
		t.Errorf("原生 conds 应仅回填 [c1r4], 实际 %v", got)
	}
	var eCond []gfcrC1UserE
	if err := drv.Query().Model(&gfcrC1UserE{}).Order("id").
		Preload("Records", "note = ?", "n4").Find(&eCond); err != nil {
		t.Fatalf("引擎复合外键 conds: %v", err)
	}
	if got := recIDs(eCond[0].Records); fmt.Sprint(got) != "[c1r4]" {
		t.Errorf("引擎 conds 应仅回填 [c1r4], 实际 %v", got)
	}
}

// ── REL-B2：belongs-to references 指向父表非主键唯一列 ─────────────────

func runGfcrRefNonPK(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	intgMigrate(t, drv, []string{"gfcr_b2_users", "gfcr_b2_cards"},
		&gfcrB2User{}, &gfcrB2CardN{}, &gfcrB2CardE{})
	gfcrSeed(t, drv,
		&gfcrB2User{ID: "b2u1", Code: "C001", Name: "owner-1"},
		&gfcrB2User{ID: "b2u2", Code: "C002", Name: "owner-2"},
		&gfcrB2CardN{ID: "b2d1", UserCode: "C001", Label: "card-1"},
		&gfcrB2CardN{ID: "b2d2", UserCode: "C002", Label: "card-2"},
		&gfcrB2CardN{ID: "b2d3", UserCode: "", Label: "card-zero-fk"},   // 零值外键
		&gfcrB2CardN{ID: "b2d4", UserCode: "MISS", Label: "card-orphan"}, // 无匹配
	)

	// 原生路径：按非主键 code 关联；零值/无匹配 → Owner 保持 nil
	var nRows []gfcrB2CardN
	if err := drv.Query().Model(&gfcrB2CardN{}).Order("id").Preload("Owner").Find(&nRows); err != nil {
		t.Fatalf("原生 references 非主键 Preload: %v", err)
	}
	if len(nRows) != 4 {
		t.Fatalf("原生路径应 4 卡片, 实际 %d", len(nRows))
	}
	if nRows[0].Owner == nil || nRows[0].Owner.Name != "owner-1" {
		t.Errorf("原生 d1 应按 code=C001 回填 owner-1, 实际 %+v", nRows[0].Owner)
	}
	if nRows[1].Owner == nil || nRows[1].Owner.Name != "owner-2" {
		t.Errorf("原生 d2 应按 code=C002 回填 owner-2, 实际 %+v", nRows[1].Owner)
	}
	if nRows[2].Owner != nil {
		t.Errorf("原生 d3 零值外键应保持 nil, 实际 %+v", nRows[2].Owner)
	}
	if nRows[3].Owner != nil {
		t.Errorf("原生 d4 无匹配引用值应保持 nil, 实际 %+v", nRows[3].Owner)
	}

	// 引擎路径：belongs-to 方向判定（外键在父行/卡片侧），逐项一致
	var eRows []gfcrB2CardE
	if err := drv.Query().Model(&gfcrB2CardE{}).Order("id").Preload("Owner").Find(&eRows); err != nil {
		t.Fatalf("引擎 references 非主键 Preload: %v", err)
	}
	if len(eRows) != 4 {
		t.Fatalf("引擎路径应 4 卡片, 实际 %d", len(eRows))
	}
	if eRows[0].Owner == nil || eRows[0].Owner.Name != "owner-1" {
		t.Errorf("引擎 d1 应按 code=C001 回填 owner-1, 实际 %+v", eRows[0].Owner)
	}
	if eRows[1].Owner == nil || eRows[1].Owner.Name != "owner-2" {
		t.Errorf("引擎 d2 应按 code=C002 回填 owner-2, 实际 %+v", eRows[1].Owner)
	}
	if eRows[2].Owner != nil {
		t.Errorf("引擎 d3 零值外键应保持 nil（契约 4 跳过）, 实际 %+v", eRows[2].Owner)
	}
	if eRows[3].Owner != nil {
		t.Errorf("引擎 d4 无匹配引用值应保持 nil, 实际 %+v", eRows[3].Owner)
	}

	// conds 过滤被引用表查询：name = owner-2 → d1 被过滤、d2 保留（双路径一致）
	var nCond []gfcrB2CardN
	if err := drv.Query().Model(&gfcrB2CardN{}).Order("id").
		Preload("Owner", "name = ?", "owner-2").Find(&nCond); err != nil {
		t.Fatalf("原生 conds: %v", err)
	}
	if nCond[0].Owner != nil || nCond[1].Owner == nil || nCond[1].Owner.Name != "owner-2" {
		t.Errorf("原生 conds 应仅 d2 保留 owner-2, 实际 d1=%+v d2=%+v", nCond[0].Owner, nCond[1].Owner)
	}
	var eCond []gfcrB2CardE
	if err := drv.Query().Model(&gfcrB2CardE{}).Order("id").
		Preload("Owner", "name = ?", "owner-2").Find(&eCond); err != nil {
		t.Fatalf("引擎 conds: %v", err)
	}
	if eCond[0].Owner != nil || eCond[1].Owner == nil || eCond[1].Owner.Name != "owner-2" {
		t.Errorf("引擎 conds 应仅 d2 保留 owner-2, 实际 d1=%+v d2=%+v", eCond[0].Owner, eCond[1].Owner)
	}
}

// ── REL-P1：polymorphic 显式值 + 缺省值回退父表名 ──────────────────────

// runGfcrPolyDefault 多态缺省值实测（同一行父数据 + 三类类型列种子）：
// 缺省回退值（原生 schema.Table 与引擎 Meta.TableName）实测均为父表裸名
// gfcr_p1_users；显式 polymorphicValue 均覆盖为 custom_type；异类型行隔离。
func runGfcrPolyDefault(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	intgMigrate(t, drv, []string{"gfcr_p1_users", "gfcr_p1_photos"},
		&gfcrP1UserN{}, &gfcrP1UserX{}, &gfcrP1UserE{}, &gfcrP1UserD{}, &gfcrP1Photo{})
	gfcrSeed(t, drv,
		&gfcrP1UserN{ID: "p1u1", Name: "poly-user"},
		&gfcrP1Photo{ID: "p1h1", OwnerID: "p1u1", OwnerType: "custom_type", URL: "explicit-x"},
		&gfcrP1Photo{ID: "p1h2", OwnerID: "p1u1", OwnerType: "gfcr_p1_users", URL: "default-table-name"},
		&gfcrP1Photo{ID: "p1h3", OwnerID: "p1u1", OwnerType: "noise", URL: "isolated"},
	)
	// 原生·缺省：gorm polymorphicValue 缺省 = schema.Table = gfcr_p1_users
	var nDef []gfcrP1UserN
	if err := drv.Query().Model(&gfcrP1UserN{}).Where("id = ?", "p1u1").Preload("Photos").Find(&nDef); err != nil {
		t.Fatalf("原生缺省多态: %v", err)
	}
	if len(nDef) != 1 || len(nDef[0].Photos) != 1 || nDef[0].Photos[0].URL != "default-table-name" {
		t.Errorf("原生缺省 polymorphicValue 应回退父表名（仅命中 p1h2）, 实际 %+v", nDef)
	}

	// 原生·显式：polymorphicValue:custom_type 覆盖
	var nExp []gfcrP1UserX
	if err := drv.Query().Model(&gfcrP1UserX{}).Where("id = ?", "p1u1").Preload("Photos").Find(&nExp); err != nil {
		t.Fatalf("原生显式多态: %v", err)
	}
	if len(nExp) != 1 || len(nExp[0].Photos) != 1 || nExp[0].Photos[0].URL != "explicit-x" {
		t.Errorf("原生显式 custom_type 应仅命中 p1h1, 实际 %+v", nExp)
	}

	// 引擎·显式：rel polymorphicValue:custom_type
	var eExp []gfcrP1UserE
	if err := drv.Query().Model(&gfcrP1UserE{}).Where("id = ?", "p1u1").Preload("Photos").Find(&eExp); err != nil {
		t.Fatalf("引擎显式多态: %v", err)
	}
	if len(eExp) != 1 || len(eExp[0].Photos) != 1 || eExp[0].Photos[0].URL != "explicit-x" {
		t.Errorf("引擎显式 custom_type 应仅命中 p1h1, 实际 %+v", eExp)
	}

	// 引擎·缺省：契约 8 缺省回退父表名，与原生缺省值一致
	var eDef []gfcrP1UserD
	if err := drv.Query().Model(&gfcrP1UserD{}).Where("id = ?", "p1u1").Preload("Photos").Find(&eDef); err != nil {
		t.Fatalf("引擎缺省多态: %v", err)
	}
	if len(eDef) != 1 || len(eDef[0].Photos) != 1 || eDef[0].Photos[0].URL != "default-table-name" {
		t.Errorf("引擎缺省 polymorphicValue 应回退父表名（仅命中 p1h2）, 实际 %+v", eDef)
	}

	// 类型列隔离负例：noise 行任何路径都不回填（上面 4 组 len==1 已含隔离断言）
	// 差异固化：缺省值两实现规则一致（父表名），显式值两实现均可覆盖，
	// 不存在方言差异——无方言分支。
}

// ── REL-S1：自引用树 + 嵌套预加载两层 ─────────────────────────────────

func runGfcrSelfRef(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	intgMigrate(t, drv, []string{"gfcr_s1_nodes"}, &gfcrS1NodeN{}, &gfcrS1NodeE{})
	gfcrSeed(t, drv,
		&gfcrS1NodeN{ID: "s1n1", Name: "root"},
		&gfcrS1NodeN{ID: "s1n2", ParentID: "s1n1", Name: "l2-a"},
		&gfcrS1NodeN{ID: "s1n3", ParentID: "s1n1", Name: "l2-b"},
		&gfcrS1NodeN{ID: "s1n4", ParentID: "s1n2", Name: "l3"},
	)
	nodeIDs := func(ns []gfcrS1NodeN) []string {
		out := make([]string, 0, len(ns))
		for _, n := range ns {
			out = append(out, n.ID)
		}
		return gfcrSortedIDs(out)
	}
	engNodeIDs := func(ns []gfcrS1NodeE) []string {
		out := make([]string, 0, len(ns))
		for _, n := range ns {
			out = append(out, n.ID)
		}
		return gfcrSortedIDs(out)
	}

	// 原生路径：两层嵌套 n1 → [n2,n3]；n2 → [n4]；n3 → 空
	var nRoots []gfcrS1NodeN
	if err := drv.Query().Model(&gfcrS1NodeN{}).Where("id = ?", "s1n1").
		Preload("Children.Children").Find(&nRoots); err != nil {
		t.Fatalf("原生自引用嵌套 Preload: %v", err)
	}
	if len(nRoots) != 1 {
		t.Fatalf("原生路径应 1 根节点, 实际 %d", len(nRoots))
	}
	if got := nodeIDs(nRoots[0].Children); fmt.Sprint(got) != "[s1n2 s1n3]" {
		t.Errorf("原生根节点子层应 [s1n2 s1n3], 实际 %v", got)
	}
	var n2, n3 *gfcrS1NodeN
	for i := range nRoots[0].Children {
		switch nRoots[0].Children[i].ID {
		case "s1n2":
			n2 = &nRoots[0].Children[i]
		case "s1n3":
			n3 = &nRoots[0].Children[i]
		}
	}
	if n2 == nil || fmt.Sprint(nodeIDs(n2.Children)) != "[s1n4]" {
		t.Errorf("原生 s1n2 子层应 [s1n4], 实际 %+v", n2)
	}
	if n3 == nil || len(n3.Children) != 0 {
		t.Errorf("原生 s1n3 子层应为空, 实际 %+v", n3)
	}

	// 引擎路径：自引用递归，逐项一致；无子行回填非 nil 空切片（契约 5）
	var eRoots []gfcrS1NodeE
	if err := drv.Query().Model(&gfcrS1NodeE{}).Where("id = ?", "s1n1").
		Preload("Children.Children").Find(&eRoots); err != nil {
		t.Fatalf("引擎自引用嵌套 Preload: %v", err)
	}
	if len(eRoots) != 1 {
		t.Fatalf("引擎路径应 1 根节点, 实际 %d", len(eRoots))
	}
	if got := engNodeIDs(eRoots[0].Children); fmt.Sprint(got) != "[s1n2 s1n3]" {
		t.Errorf("引擎根节点子层应 [s1n2 s1n3], 实际 %v", got)
	}
	var e2, e3 *gfcrS1NodeE
	for i := range eRoots[0].Children {
		switch eRoots[0].Children[i].ID {
		case "s1n2":
			e2 = &eRoots[0].Children[i]
		case "s1n3":
			e3 = &eRoots[0].Children[i]
		}
	}
	if e2 == nil || fmt.Sprint(engNodeIDs(e2.Children)) != "[s1n4]" {
		t.Errorf("引擎 s1n2 子层应 [s1n4], 实际 %+v", e2)
	}
	if e3 == nil || e3.Children == nil || len(e3.Children) != 0 {
		t.Errorf("引擎 s1n3 子层应为非 nil 空切片（契约 5）, 实际 %#v", e3)
	}

	// 单层 conds 过滤（双路径）：name = l2-a → 根仅 [s1n2]
	var nCond []gfcrS1NodeN
	if err := drv.Query().Model(&gfcrS1NodeN{}).Where("id = ?", "s1n1").
		Preload("Children", "name = ?", "l2-a").Find(&nCond); err != nil {
		t.Fatalf("原生自引用 conds: %v", err)
	}
	if got := nodeIDs(nCond[0].Children); fmt.Sprint(got) != "[s1n2]" {
		t.Errorf("原生 conds 应仅回填 [s1n2], 实际 %v", got)
	}
	var eCond []gfcrS1NodeE
	if err := drv.Query().Model(&gfcrS1NodeE{}).Where("id = ?", "s1n1").
		Preload("Children", "name = ?", "l2-a").Find(&eCond); err != nil {
		t.Fatalf("引擎自引用 conds: %v", err)
	}
	if got := engNodeIDs(eCond[0].Children); fmt.Sprint(got) != "[s1n2]" {
		t.Errorf("引擎 conds 应仅回填 [s1n2], 实际 %v", got)
	}
}

// ── REL-M2：many2many + conds（中间表无模型，conds 作用于子表）────────

func runGfcrM2MConds(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	intgMigrate(t, drv, []string{"gfcr_m2_users", "gfcr_m2_roles", "gfcr_m2_user_roles"},
		&gfcrM2UserN{}, &gfcrM2UserE{}, &gfcrM2Role{})
	// 中间表无业务模型：因本组含 gorm 原生 many2many 模型（gfcrM2UserN），
	// gorm AutoMigrate 已自动建出 gfcr_m2_user_roles（(s_key, code) 联合主键），
	// 引擎路径经 ORMTagResolver 直接表级只读访问该表（契约 7/9），无需手工 DDL。
	gfcrSeed(t, drv,
		&gfcrM2UserN{ID: "m2u1", SKey: "k1"},
		&gfcrM2UserN{ID: "m2u2", SKey: "k2"},
		&gfcrM2UserN{ID: "m2u3", SKey: "k3"}, // 无角色映射
		&gfcrM2Role{ID: "m2r1", Code: "c1", Name: "管理员"},
		&gfcrM2Role{ID: "m2r2", Code: "c2", Name: "审计"},
		&gfcrM2Role{ID: "m2r3", Code: "c3", Name: "访客"},
	)
	intgMustExec(t, drv,
		`INSERT INTO gfcr_m2_user_roles (s_key, code) VALUES ('k1','c1'), ('k1','c2'), ('k1','c3'), ('k2','c2')`)
	roleCodes := func(rs []gfcrM2Role) []string {
		out := make([]string, 0, len(rs))
		for _, r := range rs {
			out = append(out, r.Code)
		}
		return gfcrSortedIDs(out)
	}

	// 原生路径基线：u1 三角色、u2 单角色、u3 空（映射不错位）
	var nRows []gfcrM2UserN
	if err := drv.Query().Model(&gfcrM2UserN{}).Order("id").Preload("Roles").Find(&nRows); err != nil {
		t.Fatalf("原生 many2many Preload: %v", err)
	}
	if got := roleCodes(nRows[0].Roles); fmt.Sprint(got) != "[c1 c2 c3]" {
		t.Errorf("原生 u1 应 3 角色, 实际 %v", got)
	}
	if got := roleCodes(nRows[1].Roles); fmt.Sprint(got) != "[c2]" {
		t.Errorf("原生 u2 应仅 [c2], 实际 %v", got)
	}
	if len(nRows[2].Roles) != 0 {
		t.Errorf("原生 u3 无映射应为空, 实际 %v", nRows[2].Roles)
	}

	// 引擎路径基线：两跳回填逐项一致；u3 非 nil 空切片（契约 5）
	var eRows []gfcrM2UserE
	if err := drv.Query().Model(&gfcrM2UserE{}).Order("id").Preload("Roles").Find(&eRows); err != nil {
		t.Fatalf("引擎 many2many Preload: %v", err)
	}
	if got := roleCodes(eRows[0].Roles); fmt.Sprint(got) != "[c1 c2 c3]" {
		t.Errorf("引擎 u1 应 3 角色, 实际 %v", got)
	}
	if got := roleCodes(eRows[1].Roles); fmt.Sprint(got) != "[c2]" {
		t.Errorf("引擎 u2 应仅 [c2], 实际 %v", got)
	}
	if eRows[2].Roles == nil || len(eRows[2].Roles) != 0 {
		t.Errorf("引擎 u3 应为非 nil 空切片（契约 5）, 实际 %#v", eRows[2].Roles)
	}

	// conds 作用于子表查询（第二跳）：name = 审计 → u1 仅 [c2]（中间表映射不变）
	var nCond []gfcrM2UserN
	if err := drv.Query().Model(&gfcrM2UserN{}).Order("id").
		Preload("Roles", "name = ?", "审计").Find(&nCond); err != nil {
		t.Fatalf("原生 many2many conds: %v", err)
	}
	if got := roleCodes(nCond[0].Roles); fmt.Sprint(got) != "[c2]" {
		t.Errorf("原生 conds 应过滤 u1 至 [c2], 实际 %v", got)
	}
	if got := roleCodes(nCond[1].Roles); fmt.Sprint(got) != "[c2]" {
		t.Errorf("原生 conds 下 u2 仍应 [c2], 实际 %v", got)
	}
	var eCond []gfcrM2UserE
	if err := drv.Query().Model(&gfcrM2UserE{}).Order("id").
		Preload("Roles", "name = ?", "审计").Find(&eCond); err != nil {
		t.Fatalf("引擎 many2many conds: %v", err)
	}
	if got := roleCodes(eCond[0].Roles); fmt.Sprint(got) != "[c2]" {
		t.Errorf("引擎 conds 应过滤 u1 至 [c2], 实际 %v", got)
	}

	// 反向 conds：name = 访客 → u1 仅 [c3]、u2 空（证明 conds 在子表而非中间表生效）
	var nCond2 []gfcrM2UserN
	if err := drv.Query().Model(&gfcrM2UserN{}).Order("id").
		Preload("Roles", "name = ?", "访客").Find(&nCond2); err != nil {
		t.Fatalf("原生 many2many 反向 conds: %v", err)
	}
	if got := roleCodes(nCond2[0].Roles); fmt.Sprint(got) != "[c3]" {
		t.Errorf("原生反向 conds 应过滤 u1 至 [c3], 实际 %v", got)
	}
	var eCond2 []gfcrM2UserE
	if err := drv.Query().Model(&gfcrM2UserE{}).Order("id").
		Preload("Roles", "name = ?", "访客").Find(&eCond2); err != nil {
		t.Fatalf("引擎 many2many 反向 conds: %v", err)
	}
	if got := roleCodes(eCond2[0].Roles); fmt.Sprint(got) != "[c3]" {
		t.Errorf("引擎反向 conds 应过滤 u1 至 [c3], 实际 %v", got)
	}
	if len(eCond2[1].Roles) != 0 {
		t.Errorf("引擎反向 conds 下 u2 应为空, 实际 %v", eCond2[1].Roles)
	}
}

// ── REL-E1：错误路径（未知字段 / 非关联字段 / 两方向不成立）────────────

// runGfcrErrPath 差异固化：
//   - 未知关联字段：gorm 原生 Preload 在 Find 期报 gorm.ErrUnsupportedRelation
//     包装错误（"unsupported relations for schema"），wrapError 不将其映射为
//     contracts.ErrUnsupported——与 xorm 侧直接 ErrUnsupported 不同；
//   - 非关联字段（gorm:"-" 标量）：分流共享引擎 → 契约 6/类型校验报
//     contracts.ErrUnsupported；
//   - 无 rel/gorm 关联 tag 且两侧约定外键均不成立的 struct 字段：引擎方向探测
//     双向失败 → contracts.ErrUnsupported（信息引导显式 rel tag）。
func runGfcrErrPath(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	_ = dialect
	intgMigrate(t, drv, []string{"gfcr_e1_models"}, &gfcrE1Model{})
	gfcrSeed(t, drv, &gfcrE1Model{ID: "e1a", Name: "e1-row"})

	// ① 未知关联字段（N 与 E 形态字段均不存在 → 均走原生）→ gorm.ErrUnsupportedRelation
	var e1Rows []gfcrE1Model
	err := drv.Query().Model(&gfcrE1Model{}).Preload("NoSuchRel").Find(&e1Rows)
	if err == nil || !errors.Is(err, gorm.ErrUnsupportedRelation) {
		t.Errorf("未知字段应报 gorm.ErrUnsupportedRelation（差异固化：不映射 ErrUnsupported）, 实际: %v", err)
	}

	// ② 原生路径非关联列字段（Name 为普通列）→ 同为 gorm.ErrUnsupportedRelation
	err = drv.Query().Model(&gfcrE1Model{}).Preload("Name").Find(&e1Rows)
	if err == nil || !errors.Is(err, gorm.ErrUnsupportedRelation) {
		t.Errorf("原生非关联列字段应报 gorm.ErrUnsupportedRelation, 实际: %v", err)
	}

	// ③ 引擎路径非关联标量字段（gorm:"-" 的 Extra string）→ contracts.ErrUnsupported
	err = drv.Query().Model(&gfcrE1Model{}).Preload("Extra").Find(&e1Rows)
	if err == nil || !errors.Is(err, contracts.ErrUnsupported) {
		t.Errorf("引擎非关联标量字段应报 contracts.ErrUnsupported（契约：类型非关联形态）, 实际: %v", err)
	}

	// ④ 引擎路径无 rel tag 的 struct 字段（gorm:"-" 的 Other）→ 两方向约定外键
	// 均不成立 → contracts.ErrUnsupported
	err = drv.Query().Model(&gfcrE1Model{}).Preload("Other").Find(&e1Rows)
	if err == nil || !errors.Is(err, contracts.ErrUnsupported) {
		t.Errorf("引擎两方向不成立应报 contracts.ErrUnsupported, 实际: %v", err)
	}
}
