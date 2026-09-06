package gormdriver

// optimistic_lock.go 实现 orm:"version" 乐观锁的 gorm 侧写路径仿真（设计文档 7.4）。
//
// 与 xorm version 语义逐条对齐（2.2 源码 + 运行双重核实）：
//
//	路径                          | 仿真行为
//	------------------------------|------------------------------------------------------
//	Create / CreateInBatches      | 插入前 version 强制置 1（反射写 dest，覆盖零值）
//	Save 空主键（插入分支）        | version 置 1 后走原生插入
//	Save 非空主键（更新分支）      | WHERE version=旧值 + SET version=version+1，成功回填 +1；
//	                              | 更新 0 行回落 INSERT（upsert），回落插入 version 置 1
//	Updates(struct)/UpdatesResult | WHERE version=旧值 + SET version=version+1，成功回填 +1
//	Updates(map) / Update 单列    | 不参与版本控制（对齐 xorm isStruct 判定）
//	Delete                        | 不参与（xorm 删除不带版本条件）
//	RowsAffected=0（陈旧版本）    | 不报错，正常返回 0 行；业务需感知自行断言 RowsAffected
//
// 版本字段元数据来自 ensurePatched 阶段登记的 versionFields 注册表；
// 仿真使用 gorm 公开 API（2.3 原型实测可行），不引入要求字段改型的官方插件。

import (
	"context"
	"reflect"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormSchema "gorm.io/gorm/schema"
)

// versionMetaOf 取模型类型登记的乐观锁字段元数据；无 version 字段返回 nil。
func (d *GormDriver) versionMetaOf(model any) *versionFieldMeta {
	if d == nil || model == nil {
		return nil
	}
	t := indirectStructType(model)
	if t == nil {
		return nil
	}
	if v, ok := d.versionFields.Load(t); ok {
		return v.(*versionFieldMeta)
	}
	return nil
}

// hasVersionLock values 是否为带 orm:"version" 的 struct 更新值。
func (d *GormDriver) hasVersionLock(values any) bool {
	if d == nil {
		return false
	}
	rv := reflect.Indirect(reflect.ValueOf(values))
	if rv.Kind() != reflect.Struct {
		return false // map/切片等不参与（对齐 xorm isStruct 判定）
	}
	return d.versionMetaOf(values) != nil
}

// setVersionOnCreate Create/CreateInBatches/CreateResult 插入前把 version 字段
// 强制置 1（覆盖零值，反射写 dest；对齐 xorm 插入 args 追加 1）。
func (d *GormDriver) setVersionOnCreate(value any) {
	vm := d.versionMetaOf(value)
	if vm == nil {
		return
	}
	eachStructRow(value, func(rv reflect.Value) {
		if fv := versionFieldOf(rv, vm); fv.IsValid() {
			_ = setVersionOne(fv)
		}
	})
}

// saveWithLock Save 的乐观锁仿真（7.4）：
//   - 空主键（插入分支）：version 置 1 后走原生 Save；
//   - 非空主键（更新分支）：全字段 UPDATE 追加 WHERE version=旧值、SET 以
//     Expr(version+1) 自增，成功后回填内存字段 +1；更新 0 行回落 INSERT
//     （upsert 回落保持可用），回落插入时 version 置 1。
//
// 无 version 字段的模型走原生 Save（含 gorm 内建 0 行回落行为），零行为变化。
func (d *GormDriver) saveWithLock(db *gorm.DB, value any) (int64, error) {
	vm := d.versionMetaOf(value)
	if vm == nil {
		tx := db.Save(value)
		return tx.RowsAffected, wrapError(tx.Error)
	}
	stmt := &gorm.Statement{DB: d.db}
	if err := stmt.Parse(value); err != nil {
		return 0, wrapError(err)
	}
	sch := stmt.Schema
	rv := reflect.Indirect(reflect.ValueOf(value))
	if rv.Kind() != reflect.Struct || !rv.CanSet() {
		tx := db.Save(value)
		return tx.RowsAffected, wrapError(tx.Error)
	}

	// 插入分支：主键零值（对齐 gorm Save 的判定）
	for _, pf := range sch.PrimaryFields {
		if _, isZero := pf.ValueOf(context.Background(), rv); isZero {
			if fv := versionFieldOf(rv, vm); fv.IsValid() {
				_ = setVersionOne(fv)
			}
			tx := db.Save(value)
			return tx.RowsAffected, wrapError(tx.Error)
		}
	}

	// 更新分支
	fv := versionFieldOf(rv, vm)
	old, ok := versionIntValue(fv)
	if !ok {
		// 非整型 version 字段：无法仿真，退回原生 Save
		tx := db.Save(value)
		return tx.RowsAffected, wrapError(tx.Error)
	}
	m := structToUpdateMap(sch, rv, true, db)
	if m == nil {
		tx := db.Save(value)
		return tx.RowsAffected, wrapError(tx.Error)
	}
	m[vm.Column] = gorm.Expr(vm.Column+" + ?", 1)
	tx := db.Model(value).
		Where(clause.Eq{Column: clause.Column{Name: vm.Column}, Value: old}).
		Updates(m)
	if tx.Error != nil {
		return tx.RowsAffected, wrapError(tx.Error)
	}
	if tx.RowsAffected == 0 {
		// upsert 回落：更新 0 行回落 INSERT，回落插入时 version 置 1（7.4）
		_ = setVersionOne(fv)
		createTx := db.Create(value)
		return createTx.RowsAffected, wrapError(createTx.Error)
	}
	// 成功后回填内存字段 +1（对齐 xorm incrVersionFieldValue）
	_ = setVersionInt(fv, old+1)
	return tx.RowsAffected, nil
}

// updatesWithLock Updates(struct)/UpdatesResult(struct) 的乐观锁仿真（7.4）：
// 追加 WHERE version=旧值，SET 以 Expr(version+1) 自增；成功后回填内存字段 +1；
// 冲突（RowsAffected=0）不报错正常返回。返回 handled=false 表示 values 不参与
// 版本控制（非 version 模型 / 非 struct / 非整型 version），调用方走原生路径。
func (d *GormDriver) updatesWithLock(db *gorm.DB, values any) (tx *gorm.DB, handled bool) {
	if !d.hasVersionLock(values) {
		return nil, false
	}
	rv := reflect.Indirect(reflect.ValueOf(values))
	stmt := &gorm.Statement{DB: d.db}
	if err := stmt.Parse(values); err != nil {
		return nil, false
	}
	fv := versionFieldOf(rv, d.versionMetaOf(values))
	old, ok := versionIntValue(fv)
	if !ok {
		return nil, false
	}
	m := structToUpdateMap(stmt.Schema, rv, false, db)
	if m == nil {
		return nil, false
	}
	vm := d.versionMetaOf(values)
	m[vm.Column] = gorm.Expr(vm.Column+" + ?", 1)

	// 保留链上已设置的 Model（表名/PK 条件来源）；未设置时以 values 兜底
	model := db.Statement.Model
	if model == nil {
		model = values
	}
	tx = db.Model(model).
		Where(clause.Eq{Column: clause.Column{Name: vm.Column}, Value: old}).
		Updates(m)
	if tx.Error == nil && tx.RowsAffected > 0 && fv.IsValid() && fv.CanSet() {
		_ = setVersionInt(fv, old+1)
	}
	return tx, true
}

// ── struct → 更新 map（复刻 gorm Save 全字段 / Updates 非零值语义） ────

// structToUpdateMap 将 struct 按列展开为 Updates 的 map 值：
//   - includeZero=true：Save 语义——全字段（含零值）写入；
//   - includeZero=false：Updates(struct) 语义——仅非零字段写入；
//   - AutoUpdateTime 字段对齐 gorm：始终以当前时间按精度写入；
//   - 跳过主键（留在 WHERE）与不可更新字段。
func structToUpdateMap(sch *gormSchema.Schema, rv reflect.Value, includeZero bool, db *gorm.DB) map[string]any {
	if sch == nil {
		return nil
	}
	ctx := context.Background()
	nowFn := db.NowFunc
	if nowFn == nil {
		nowFn = time.Now
	}
	m := make(map[string]any, len(sch.DBNames))
	for _, dbName := range sch.DBNames {
		f := sch.LookUpField(dbName)
		if f == nil || f.DBName == "" || f.PrimaryKey || !f.Updatable {
			continue
		}
		if f.AutoUpdateTime > 0 {
			switch f.AutoUpdateTime {
			case gormSchema.UnixNanosecond:
				m[dbName] = nowFn().UnixNano()
			case gormSchema.UnixMillisecond:
				m[dbName] = nowFn().UnixMilli()
			case gormSchema.UnixSecond:
				m[dbName] = nowFn().Unix()
			default:
				m[dbName] = nowFn()
			}
			continue
		}
		v, isZero := f.ValueOf(ctx, rv)
		if !includeZero && isZero {
			continue
		}
		m[dbName] = v
	}
	return m
}

// ── 反射辅助 ─────────────────────────────────────────────────────────

// eachStructRow 展开 value（*T / T / []T / []*T / 各级指针包装）为可写 struct 行。
func eachStructRow(value any, fn func(rv reflect.Value)) {
	rv := reflect.ValueOf(value)
	for rv.IsValid() && rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			e := rv.Index(i)
			for e.IsValid() && e.Kind() == reflect.Ptr {
				if e.IsNil() {
					e = reflect.Value{}
					break
				}
				e = e.Elem()
			}
			if e.IsValid() && e.Kind() == reflect.Struct && e.CanSet() {
				fn(e)
			}
		}
	case reflect.Struct:
		if rv.CanSet() {
			fn(rv)
		}
	}
}

// versionFieldOf 沿注册表索引路径定位 struct 行上的 version 字段（指针安全）。
func versionFieldOf(rv reflect.Value, vm *versionFieldMeta) reflect.Value {
	if vm == nil {
		return reflect.Value{}
	}
	cur := rv
	for _, idx := range vm.Index {
		if cur.Kind() == reflect.Ptr {
			if cur.IsNil() {
				return reflect.Value{}
			}
			cur = cur.Elem()
		}
		if cur.Kind() != reflect.Struct {
			return reflect.Value{}
		}
		cur = cur.Field(idx)
	}
	return cur
}

// versionIntValue 读取 version 字段整型值（非整型返回 ok=false）。
func versionIntValue(fv reflect.Value) (int64, bool) {
	if !fv.IsValid() {
		return 0, false
	}
	switch fv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fv.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(fv.Uint()), true
	}
	return 0, false
}

// setVersionOne version 置 1（int/uint/float 形态）。
func setVersionOne(fv reflect.Value) bool {
	return setVersionInt(fv, 1)
}

// setVersionInt version 置整型值。
func setVersionInt(fv reflect.Value, v int64) bool {
	if !fv.IsValid() || !fv.CanSet() {
		return false
	}
	switch fv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fv.SetInt(v)
		return true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		fv.SetUint(uint64(v))
		return true
	case reflect.Float32, reflect.Float64:
		fv.SetFloat(float64(v))
		return true
	}
	return false
}
