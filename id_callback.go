package gormdriver

import (
	"reflect"

	"github.com/zhoudm1743/go-fast-framework/contracts"
	"gorm.io/gorm"
)

// registerAutoGenerateIDCallback 在 gorm:create 之前为实现 IDAutoGenerator 的模型生成主键。
// 承接原框架 database.Model.BeforeCreate(*gorm.DB)，覆盖 FirstOrCreate 等 GORM 内部创建路径。
func registerAutoGenerateIDCallback(db *gorm.DB) error {
	return db.Callback().Create().Before("gorm:create").Register("gofast:auto_generate_id", autoGenerateIDCallback)
}

func autoGenerateIDCallback(tx *gorm.DB) {
	if tx.Statement == nil || tx.Statement.Dest == nil {
		return
	}
	callAutoGenerateID(tx.Statement.Dest)
}

func callAutoGenerateID(dest any) {
	if dest == nil {
		return
	}
	if gen, ok := dest.(contracts.IDAutoGenerator); ok {
		gen.AutoGenerateID()
		return
	}

	rv := reflect.ValueOf(dest)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < rv.Len(); i++ {
			elem := rv.Index(i)
			invokeAutoGenerateID(elem)
		}
	case reflect.Struct:
		invokeAutoGenerateID(rv)
	}
}

func invokeAutoGenerateID(v reflect.Value) {
	if !v.IsValid() {
		return
	}
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return
		}
		if gen, ok := v.Interface().(contracts.IDAutoGenerator); ok {
			gen.AutoGenerateID()
		}
		return
	}
	if v.CanAddr() {
		if gen, ok := v.Addr().Interface().(contracts.IDAutoGenerator); ok {
			gen.AutoGenerateID()
		}
	}
}
