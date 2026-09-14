package gormdriver

// soft_delete.go 框架托管软删除（sd tag，文档 §4.5/§11.9）的 gorm 侧实现。
//
// sd 标记的模型（database.SoftDelete/SoftDeleteMilli/SoftDeleteNano/
// SoftDeleteFlag/SoftDeleteTime 或自定义 sd tag 字段）由框架驱动托管：
//   - Delete/DeleteResult 自动改写为置位 UPDATE（写 SdMeta.DeletedValue(now)），
//     不再物理删除；
//   - 默认查询（First/Find/Take/Count/Pluck/… 与 Update/Updates/Save）经
//     Query/Update 回调自动附加 SdMeta.AliveCond 存活过滤（Unscoped 绕过）；
//   - OnlyTrashed/Restore 类型感知（SdMeta.TrashedCond/AliveValue）。
//
// 未打 sd 标记的 deleted_at 列保持旧版业务级手动语义（查询需显式
// Where("deleted_at = ?", 0)），注册表不收录、回调不生效，完全兼容。

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/zhoudm1743/go-fast-framework/contracts/ormtag"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormSchema "gorm.io/gorm/schema"
)

// sdFieldMeta 软删字段注册表条目（reflect.Type → 元数据，patch 期写入）。
type sdFieldMeta struct {
	Column string        // 软删列名（patch 后的 DBName）
	Sd     ormtag.SdMeta // 模式/类型感知条件与值
}

// registerSdFields 将 meta.Sd 中的软删字段登记进注册表（列名取 gorm schema
// patch 后的 DBName，嵌入展开字段经 FieldsByName 提升可查）。sd 字段必须映射
// 为数据列：被 orm/gorm 标记忽略（无列）时 patch 期即报错，fail-fast。
// 一模型多个 sd 字段时按字段名排序取第一个（确定性）。
func registerSdFields(s *gormSchema.Schema, meta *ormtag.ModelMeta, registry *sync.Map) error {
	if registry == nil || len(meta.Sd) == 0 {
		return nil
	}
	names := make([]string, 0, len(meta.Sd))
	for name := range meta.Sd {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		gf := s.FieldsByName[name]
		if gf == nil || gf.DBName == "" {
			return fmt.Errorf("gormdriver: 字段 %q 带 sd 软删标记但不是映射的数据列（被忽略或缺列名），请移除 sd 标记或补充列映射", name)
		}
		registry.Store(s.ModelType, &sdFieldMeta{Column: gf.DBName, Sd: meta.Sd[name]})
		return nil
	}
	return nil
}

// registerSoftDeleteCallbacks 在连接上注册软删查询/更新过滤回调（各一次，
// gorm 回调随连接隔离）。过滤条件在回调中按 Statement.Schema.ModelType
// 查注册表——Execute 在回调前已完成 Parse，Schema 必定可用。
func (d *GormDriver) registerSoftDeleteCallbacks() error {
	filter := func(tx *gorm.DB) {
		if tx.Statement.Unscoped || tx.Statement.Schema == nil {
			return
		}
		info, ok := d.sdFields.Load(tx.Statement.Schema.ModelType)
		if !ok {
			return
		}
		sd := info.(*sdFieldMeta)
		cond, args := sd.Sd.AliveCond(sd.Column)
		tx.Statement.AddClause(clause.Where{Exprs: []clause.Expression{
			clause.Expr{SQL: cond, Vars: args},
		}})
	}
	if err := d.db.Callback().Query().Before("gorm:query").Register("gofast:soft_delete_filter", filter); err != nil {
		return fmt.Errorf("[GoFast] gormdriver driver: register soft delete query callback failed: %w", err)
	}
	if err := d.db.Callback().Update().Before("gorm:update").Register("gofast:soft_delete_filter", filter); err != nil {
		return fmt.Errorf("[GoFast] gormdriver driver: register soft delete update callback failed: %w", err)
	}
	return nil
}

// sdOfType 按模型类型查软删元数据（指针/切片/数组解引用到 struct）。
func (d *GormDriver) sdOfType(t reflect.Type) (*sdFieldMeta, bool) {
	if d == nil || t == nil {
		return nil, false
	}
	for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, false
	}
	v, ok := d.sdFields.Load(t)
	if !ok {
		return nil, false
	}
	return v.(*sdFieldMeta), true
}

// sdOfModel 从模型值解析软删元数据（Delete 的 dest）。
func (d *GormDriver) sdOfModel(model any) (*sdFieldMeta, bool) {
	return d.sdOfType(indirectStructType(model))
}

// sdOfChain 从链上 Model/Dest 解析软删元数据（OnlyTrashed/Restore）；
// 解析前对目标懒加载 patch，保证注册表已写入。无模型（裸 Table 链）返回 false。
func (q *GormQuery) sdOfChain() (*sdFieldMeta, reflect.Type, bool) {
	if q.driver == nil {
		return nil, nil, false
	}
	target := q.db.Statement.Model
	if target == nil {
		target = q.db.Statement.Dest
	}
	t := indirectStructType(target)
	if t == nil {
		return nil, nil, false
	}
	if err := q.driver.ensurePatched(target, q.db); err != nil {
		return nil, nil, false
	}
	if sd, ok := q.driver.sdOfType(t); ok {
		return sd, t, true
	}
	return nil, nil, false
}

// softDelete 将 Delete 改写为软删除置位 UPDATE：SET deleted_at = DeletedValue，
// WHERE 沿用原 Delete 条件（含 dest 主键），存活过滤由 Update 回调统一附加
// （与 gorm.DeletedAt 的 DeleteClauses 语义一致：不重复软删已删行）。
func (q *GormQuery) softDelete(db *gorm.DB, value any, sd *sdFieldMeta, conds ...any) *gorm.DB {
	tx := db.Model(value)
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	return tx.Update(sd.Column, sd.Sd.DeletedValue(time.Now()))
}
