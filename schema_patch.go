package gormdriver

// schema_patch.go 实现统一 orm tag（xorm 语法）到 gorm schema 的 patch 通道。
//
// 设计文档：docs/md/orm-tag-design.md（v1.2）第七章。
// 核心机制：stmt.Parse 命中 gorm cacheStore 后返回的 *schema.Schema 为全连接
// 共享实例，patch 一次终身生效；orm tag 优先于 gorm tag 的同类语义（4.6），
// ext tag 优先于 orm tag 的同维度语义。
//
// 全部 patch 动作均为设计文档 2.3 运行级实验验证过的结论，严禁"跳步"简化：
//   - 列名/pk/类型/约束等为 Field 导出字段直改 + 重建索引 map；
//   - 索引/唯一索引必须"TagSettings 闸门 + Field.Tag 注入 gorm 语法片段"双写
//     （v1.2 关键修正：parseFieldIndexes 重读原始 field.Tag，仅改 TagSettings 无效，
//     schema/index.go 是全库唯一运行期读 Field.Tag 的位置，注入无侧漏）；
//   - version 不做 schema patch，登记注册表供写路径乐观锁仿真（7.4）。

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/zhoudm1743/go-fast-framework/contracts/ormtag"
	"github.com/zhoudm1743/go-fast-framework/database/preload"

	"gorm.io/gorm"
	gormSchema "gorm.io/gorm/schema"
)

// versionFieldMeta 乐观锁字段注册表条目（reflect.Type → 元数据，供 7.4 写路径仿真）。
type versionFieldMeta struct {
	FieldName string // Go 字段名
	Column    string // 版本列名（patch 后的 DBName）
	Index     []int  // struct 内反射索引路径（支持嵌入展开）
}

// ensurePatched 保证 model 对应的 gorm schema 已按 orm tag patch（文档 7.1）。
// 双重检查锁 + 驱动级互斥；Parse 失败（含禁用 token / 未知裸 token / 非约定
// 外键关联字段）原样向上透出。
//
// db 为首次 stmt.Parse 使用的连接（决定 gorm 按 reflect.Type 缓存 schema 时
// 钉住的 NamingStrategy 表名前缀）：查询链调用方必须传链上 db（动态 Schema()
// 场景为租户前缀克隆），连接级路径传 d.db——gorm schema 缓存以类型为键、
// 首次解析钉住表名，传错 db 会导致租户查询被钉到 public（U18 集成回归）。
func (d *GormDriver) ensurePatched(model any, db *gorm.DB) error {
	if d == nil || d.db == nil || model == nil {
		return nil
	}
	if db == nil {
		db = d.db
	}
	t := indirectStructType(model)
	if t == nil {
		return nil // 非 struct（map/标量投影等）无需 patch
	}
	if _, ok := d.patched.Load(t); ok {
		return nil
	}
	d.patchMu.Lock()
	defer d.patchMu.Unlock()
	if _, ok := d.patched.Load(t); ok {
		return nil
	}
	return patchModel(db, t, &d.versionFields)
}

// PatchSchema 将 models 的 orm/ext 统一标签 patch 进 db 连接的 gorm schema 缓存（7.1），
// 供自带迁移/DDL 工具链的业务在自定义 gorm 连接上复用统一标签事实源（典型用法：
// 自建连接先 PatchSchema(db, models...) 再 db.AutoMigrate(models...)，DDL 即由 orm 标签驱动）。
//
// db 语义与 ensurePatched 一致：gorm schema 缓存以类型为键、首次解析钉住 NamingStrategy
// 表名前缀，Parse 与后续 AutoMigrate 必须使用同一条连接。
// 幂等性以「每函数调用」为界（索引双写片段不可重复注入）：同一连接请合并 models
// 一次性传入；每连接一次 PatchSchema 的迁移场景天然满足。
func PatchSchema(db *gorm.DB, models ...any) error {
	if db == nil {
		return nil
	}
	var memo sync.Map // reflect.Type → struct{}{}（本调用内去重）
	for _, model := range models {
		if model == nil {
			continue
		}
		t := indirectStructType(model)
		if t == nil {
			continue // 非 struct（map/标量投影等）无需 patch
		}
		if _, ok := memo.LoadOrStore(t, struct{}{}); ok {
			continue
		}
		if err := patchModel(db, t, nil); err != nil {
			return err
		}
	}
	return nil
}

// patchModel 对单个模型类型执行 stmt.Parse + orm/ext patch（无去重，调用方负责幂等）。
// versionRegistry 为 orm:"version" 乐观锁字段注册表（7.4 写路径仿真用）；nil 时跳过
// 登记（PatchSchema 迁移场景无写路径，无需注册）。
func patchModel(db *gorm.DB, t reflect.Type, versionRegistry *sync.Map) error {
	normalized := reflect.New(t).Interface()
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(normalized); err != nil {
		return fmt.Errorf(
			"gormdriver: 解析模型 %s 失败: %w（若为非约定外键的关联字段，请为该字段补充 gorm:\"-\" 并配置 rel tag，或改用 gorm 原生关联 tag）",
			t.Name(), err)
	}
	meta, err := ormtag.Parse(normalized)
	if err != nil {
		return err
	}
	// orm patch（返回索引双写片段），随后 ext patch 可覆盖同维度片段，
	// 最后统一收口注入 Field.Tag（4.6 优先级：ext > orm）
	indexSpecs, err := applyFieldMeta(db, stmt.Schema, meta, versionRegistry)
	if err != nil {
		return err
	}
	applyExtMeta(db, stmt.Schema, meta, indexSpecs)
	for gf, fragments := range indexSpecs {
		for _, frag := range fragments {
			gf.Tag = injectGormTag(gf.Tag, frag.fragment)
		}
	}
	return nil
}

// ── applyFieldMeta：orm tag 逐字段 patch（文档 7.1 第一张表） ────────────

// indexFragment 双写 patch 的 gorm tag 注入片段（闸门在 TagSettings，片段注入 Field.Tag）。
type indexFragment struct {
	gateKey  string // TagSettings 闸门键：INDEX / UNIQUEINDEX
	fragment string // 注入到 Field.Tag gorm 值尾部的片段，如 "index:idx_x"、"uniqueIndex:uk_x"
}

// setIndexFragment 追加一条双写片段（同字段可同时位于联合唯一组与普通索引组）。
func setIndexFragment(specs map[*gormSchema.Field][]indexFragment, gf *gormSchema.Field, frag indexFragment) {
	specs[gf] = append(specs[gf], frag)
}

func applyFieldMeta(db *gorm.DB, s *gormSchema.Schema, meta *ormtag.ModelMeta, versionRegistry *sync.Map) (map[*gormSchema.Field][]indexFragment, error) {
	ignored := make(map[*gormSchema.Field]bool)
	ormPKFields := make(map[string]bool)
	anyPK := false
	indexSpecs := make(map[*gormSchema.Field][]indexFragment)
	var versionField *gormSchema.Field

	byIndex := gormFieldsByReflectedIndex(s)

	for i := range meta.Fields {
		fm := meta.Fields[i]
		if fm.Extends {
			continue // 嵌入标记条目：前缀 patch 在下方统一对叶子字段处理
		}
		gf := locateGormField(s, byIndex, fm)
		if gf == nil {
			warnf(db, "[gormdriver] 模型 %s 的 orm tag 字段 %q 未能在 gorm schema 中定位，已跳过 patch", s.Name, fm.FieldName)
			continue
		}

		// 关联字段（gorm 已按约定/原生 tag 解析为关系）：orm:"-" 不做忽略 patch，
		// 保留 gorm 原生关联语义——不建列、原生 Preload 正常，效果等价（9.1.1）。
		if _, isRel := s.Relationships.Relations[fm.FieldName]; isRel {
			continue
		}

		if fm.Ignore {
			// "-"：忽略五联（不建列/不读/不写），对齐 gorm "-:all" 语义（field.go:344）
			ignoreGormField(s, gf)
			ignored[gf] = true
			continue
		}

		// pk：PrimaryKey + 重建 PrimaryFields（rebuildFieldMaps 统一做）
		if fm.PrimaryKey {
			gf.PrimaryKey = true
			ormPKFields[gf.Name] = true
			anyPK = true
		}
		if fm.AutoIncrement {
			gf.AutoIncrement = true
		}
		// 列名：改 DBName，索引 map 由 rebuildFieldMaps 重建
		if fm.ColumnName != "" && gf.DBName != fm.ColumnName {
			warnGormConflict(db, gf, "column", fm.ColumnName, "COLUMN")
			gf.DBName = fm.ColumnName
		}
		// 类型 token：泛型映射写 Size/Precision/Scale，DataType 原文透传（含 unsigned）
		if fm.Type != "" {
			if v, ok := gf.TagSettings["TYPE"]; ok && v != "" {
				warnGormConflict(db, gf, "type", fm.Type, "TYPE")
			}
			applyTypeToken(gf, fm)
		}
		if fm.NotNull {
			gf.NotNull = true
		}
		if fm.Null {
			gf.NotNull = false
		}
		// unique：列级唯一（ParseUniqueConstraints 直读 Field.Unique；DDL 另有内联 UNIQUE）。
		// unique(name) 联合唯一组不置 Field.Unique——否则 gorm 会为每个成员列额外
		// 生成单列 UNIQUE 约束（uni_<table>_<col>），语义错（组内多列应为联合唯一
		// 而非各自唯一），且 MySQL 重复 AutoMigrate 时 MigrateColumnUnique 经
		// HasConstraint（仅查 referential_constraints）误判缺失、反复 ADD CONSTRAINT
		// 报 Error 1061（U18 集成回归发现）；xorm 原生 unique(name) 语义同为联合
		// 唯一索引、不含单列约束，两驱动对齐。
		if fm.Unique && fm.UniqueName == "" {
			gf.Unique = true
		}
		// unique(name)：双写——TagSettings["UNIQUEINDEX"] 闸门 + Field.Tag 注入 uniqueIndex:name
		if fm.UniqueName != "" {
			gf.TagSettings["UNIQUEINDEX"] = fm.UniqueName
			setIndexFragment(indexSpecs, gf, indexFragment{gateKey: "UNIQUEINDEX", fragment: "uniqueIndex:" + fm.UniqueName})
		}
		// index / index(name)：双写——TagSettings["INDEX"] 闸门 + Field.Tag 注入 index[:name]
		//（v1.2 关键修正：仅 patch TagSettings 无效，parseFieldIndexes 重读原始 Field.Tag）
		if fm.Index || fm.IndexName != "" {
			if fm.IndexName != "" {
				gf.TagSettings["INDEX"] = fm.IndexName
				setIndexFragment(indexSpecs, gf, indexFragment{gateKey: "INDEX", fragment: "index:" + fm.IndexName})
			} else {
				gf.TagSettings["INDEX"] = "INDEX"
				setIndexFragment(indexSpecs, gf, indexFragment{gateKey: "INDEX", fragment: "index"})
			}
		}
		// default：DefaultValue + HasDefaultValue + DefaultValueInterface
		if fm.HasDefault {
			if v, ok := gf.TagSettings["DEFAULT"]; ok && v != "" {
				warnGormConflict(db, gf, "default", fm.Default, "DEFAULT")
			}
			applyDefaultValue(gf, fm)
		}
		if fm.Comment != "" {
			gf.Comment = fm.Comment
		}
		// created/updated：int64 字段填 unix 秒（time.Time 字段填 UnixTime），
		// 精度扩展由 ext timePrecision 调整
		if fm.Created {
			gf.AutoCreateTime = autoTimeType(gf)
		}
		if fm.Updated {
			gf.AutoUpdateTime = autoTimeType(gf)
		}
		// ->（xorm 只写）/ <-（xorm 只读）：翻译为 gorm 权限布尔组合（方向见 4.2 注）
		if fm.WriteOnly {
			gf.Readable = false
		}
		if fm.ReadOnly {
			gf.Creatable = false
			gf.Updatable = false
		}
		// version：不做 schema patch，登记注册表供写路径仿真（7.4）
		if fm.Version {
			if versionField == nil {
				versionField = gf
			} else {
				warnf(db, "[gormdriver] 模型 %s 存在多个 orm:\"version\" 字段，仅首个 %q 生效", s.Name, versionField.Name)
			}
		}
		// utc/local/collate/json：不 patch（gorm 无列级能力 / serializer 不可 patch），
		// 日志引导（4.5）：json 推荐字段类型实现 Scanner/Valuer
		if fm.UTC || fm.Local {
			warnf(db, "[gormdriver] 模型 %s 字段 %q 的 orm tag utc/local 在 gorm 侧不支持（时区由 DSN loc/NowFunc 全局控制），已忽略", s.Name, fm.FieldName)
		}
		if fm.Collate != "" {
			warnf(db, "[gormdriver] 模型 %s 字段 %q 的 orm tag collate(%s) 在 gorm 侧不支持，已忽略；必须时可用 gorm:\"type:... collate ...\" 逃生口", s.Name, fm.FieldName, fm.Collate)
		}
		if fm.JSON {
			warnf(db, "[gormdriver] 模型 %s 字段 %q 的 orm tag json 在 gorm 侧不可 patch（serializer 在 Parse 内包装）；推荐字段类型实现 sql.Scanner/driver.Valuer，或用 gorm:\"serializer:json\" 逃生口", s.Name, fm.FieldName)
		}
	}

	// extends('前缀')：定位嵌入来源字段（BindNames[0] 匹配嵌入字段 Go 名），
	// 对嵌入叶子字段的约定列名加前缀；显式列名的前缀已由 ormtag 解析器应用，
	// 此处 HasPrefix 防止双写前缀（2.3 实测 CRUD 正常）。
	for i := range meta.Fields {
		fm := meta.Fields[i]
		if !fm.Extends || fm.ExtendsPrefix == "" {
			continue
		}
		for _, gf := range s.Fields {
			if len(gf.BindNames) > 1 && gf.BindNames[0] == fm.FieldName &&
				gf.DBName != "" && !strings.HasPrefix(gf.DBName, fm.ExtendsPrefix) {
				gf.DBName = fm.ExtendsPrefix + gf.DBName
			}
		}
	}

	// orm pk 声明与 gorm 自动提升主键（字段名 id/ID 无显式 pk tag）冲突时，
	// 取消自动提升（TagSettings 无 PRIMARYKEY 即为自动提升产物），避免复合主键
	if anyPK {
		for _, gf := range s.Fields {
			if !gf.PrimaryKey || ormPKFields[gf.Name] {
				continue
			}
			if _, ok := gf.TagSettings["PRIMARYKEY"]; ok {
				continue
			}
			if _, ok := gf.TagSettings["PRIMARY_KEY"]; ok {
				continue
			}
			gf.PrimaryKey = false
		}
	}

	rebuildFieldMaps(s)

	// version 注册表（rebuild 后 DBName 已为最终列名）
	if versionField != nil && versionRegistry != nil {
		versionRegistry.Store(s.ModelType, &versionFieldMeta{
			FieldName: versionField.Name,
			Column:    versionField.DBName,
			Index:     normalizeReflectedIndex(versionField.StructField.Index),
		})
	}

	// patch 后自检（7.3 缓解 3）：异常返回 error 而非静默继续。
	// 索引双写片段由调用方在 applyExtMeta（可覆盖同维度）之后统一注入。
	return indexSpecs, verifyPatchedSchema(s, ignored)
}

// applyExtMeta：ext tag 逐字段 patch（gorm 专属能力，文档 7.1 第二张表 / 4.3）。
// indexSpecs 为 orm patch 产物的索引双写片段，ext index 在此覆盖 orm index 同维度。
func applyExtMeta(db *gorm.DB, s *gormSchema.Schema, meta *ormtag.ModelMeta, indexSpecs map[*gormSchema.Field][]indexFragment) {
	for name, em := range meta.Exts {
		gf := s.FieldsByName[name]
		if gf == nil {
			continue
		}
		// check → TagSettings["CHECK"]（ParseCheckConstraints 直读，2.3 运行实测生效）
		if em.Check != "" {
			gf.TagSettings["CHECK"] = em.Check
		}
		// index 完整选项：双写（覆盖 orm index token 的同维度语义，4.6 优先级 2；
		// 仅替换 INDEX 闸门维度，unique(name) 组保持不动）
		if em.IndexOptions != "" {
			gf.TagSettings["INDEX"] = em.IndexOptions
			fragments := indexSpecs[gf]
			kept := fragments[:0:0]
			for _, frag := range fragments {
				if frag.gateKey != "INDEX" {
					kept = append(kept, frag)
				}
			}
			indexSpecs[gf] = append(kept, indexFragment{gateKey: "INDEX", fragment: "index:" + em.IndexOptions})
		}
		// autoIncrementIncrement → 导出字段直接赋值（2.3 已验证）
		if em.AutoIncrementIncrement > 0 {
			gf.AutoIncrementIncrement = em.AutoIncrementIncrement
		}
		// migration:false → IgnoreMigration=true（保留读写）
		if em.IgnoreMigration {
			gf.IgnoreMigration = true
		}
		// timePrecision → AutoCreateTime/AutoUpdateTime 精度枚举
		if em.TimePrecision != "" {
			var tt gormSchema.TimeType
			switch strings.ToLower(em.TimePrecision) {
			case "milli":
				tt = gormSchema.UnixMillisecond
			case "nano":
				tt = gormSchema.UnixNanosecond
			default:
				warnf(db, "[gormdriver] 模型 %s 字段 %q 的 ext timePrecision %q 无法识别（支持 milli|nano），已忽略", s.Name, name, em.TimePrecision)
			}
			if tt != 0 {
				if gf.AutoCreateTime > 0 {
					gf.AutoCreateTime = tt
				}
				if gf.AutoUpdateTime > 0 {
					gf.AutoUpdateTime = tt
				}
			}
		}
		// perm → Creatable/Updatable 组合（gorm <-:create / <-:update 语义）
		if em.PermCreateOnly && !em.PermUpdateOnly {
			gf.Updatable = false
		}
		if em.PermUpdateOnly && !em.PermCreateOnly {
			gf.Creatable = false
		}
		// 未知 ext key：本驱动不识别，未来第三驱动可能消费（文档 11.16）
		for _, k := range em.UnknownKeys {
			warnf(db, "[gormdriver] 模型 %s 字段 %q 的 ext tag 含未知 key %q，已忽略（不报错）", s.Name, name, k)
		}
	}
}

// ── 单维度 patch 辅助 ────────────────────────────────────────────────

// ignoreGormField "-" 忽略五联：Creatable/Updatable/Readable=false、
// IgnoreMigration=true、清 DataType/DBName，并移出 FieldsByName/FieldsByBindName
// （FieldsByDBName/DBNames 由 rebuildFieldMaps 重建时排除）。
func ignoreGormField(s *gormSchema.Schema, gf *gormSchema.Field) {
	gf.Creatable = false
	gf.Updatable = false
	gf.Readable = false
	gf.IgnoreMigration = true
	gf.DataType = ""
	gf.DBName = ""
	delete(s.FieldsByName, gf.Name)
	delete(s.FieldsByBindName, gf.BindName())
}

// applyTypeToken 类型 token patch：varchar/char 系 → Size；decimal/numeric →
// Precision/Scale；其余 DataType 原文透传（schema/field.go TYPE 机制，2.3 实测
// decimal(10,2)/int unsigned 原文进 DDL）。unsigned 并入类型透传。
func applyTypeToken(gf *gormSchema.Field, fm ormtag.FieldMeta) {
	name, params := splitTypeToken(fm.Type)
	base := fm.Type
	switch strings.ToLower(name) {
	case "varchar", "char", "nchar", "nvarchar", "varchar2", "character":
		if n, ok := singleIntParam(params); ok {
			gf.Size = n
			base = strings.ToLower(name) + "(" + strconv.Itoa(n) + ")"
		}
	case "decimal", "numeric":
		if len(params) >= 1 {
			if p, err := strconv.Atoi(params[0]); err == nil {
				gf.Precision = p
				scale := 0
				if len(params) >= 2 {
					if sc, err := strconv.Atoi(params[1]); err == nil {
						scale = sc
					}
				}
				gf.Scale = scale
				base = strings.ToLower(name) + "(" + strconv.Itoa(p) + "," + strconv.Itoa(scale) + ")"
			}
		}
	}
	if fm.Unsigned {
		base += " unsigned"
	}
	gf.DataType = gormSchema.DataType(base)
}

// splitTypeToken 拆分类型 token 原文为类型名与括号参数（"varchar(100)" → "varchar", ["100"]；
// "decimal(10,2)" → "decimal", ["10","2"]）。
func splitTypeToken(raw string) (name string, params []string) {
	idx := strings.IndexByte(raw, '(')
	if idx < 0 {
		return raw, nil
	}
	name = raw[:idx]
	inner := strings.TrimSuffix(raw[idx+1:], ")")
	for _, p := range strings.Split(inner, ",") {
		params = append(params, strings.TrimSpace(p))
	}
	return name, params
}

// singleIntParam 单整型参数提取（varchar(100)）。
func singleIntParam(params []string) (int, bool) {
	if len(params) != 1 {
		return 0, false
	}
	n, err := strconv.Atoi(params[0])
	if err != nil {
		return 0, false
	}
	return n, true
}

// applyDefaultValue default patch：DefaultValue + HasDefaultValue + DefaultValueInterface
// （按字段类型解析默认值，对齐 gorm ParseField 行为；DefaultValueInterface 非空时
// migrator 走 bind-var 路径，字符串默认值在 DDL 中正确加引号）。
func applyDefaultValue(gf *gormSchema.Field, fm ormtag.FieldMeta) {
	gf.HasDefaultValue = true
	gf.DefaultValue = fm.Default
	switch gf.IndirectFieldType.Kind() {
	case reflect.Bool:
		if b, err := strconv.ParseBool(fm.Default); err == nil {
			gf.DefaultValueInterface = b
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n, err := strconv.ParseInt(fm.Default, 0, 64); err == nil {
			gf.DefaultValueInterface = n
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if n, err := strconv.ParseUint(fm.Default, 0, 64); err == nil {
			gf.DefaultValueInterface = n
		}
	case reflect.Float32, reflect.Float64:
		if f, err := strconv.ParseFloat(fm.Default, 64); err == nil {
			gf.DefaultValueInterface = f
		}
	case reflect.String:
		gf.DefaultValueInterface = fm.Default
	}
}

// autoTimeType created/updated 的 gorm 时间类型：time.Time 字段 → UnixTime，
// 其余（int64 等）→ UnixSecond（与 gorm autoCreateTime 语义一致，2.3 实测）。
func autoTimeType(gf *gormSchema.Field) gormSchema.TimeType {
	if gf.IndirectFieldType == gormSchema.TimeReflectType {
		return gormSchema.UnixTime
	}
	return gormSchema.UnixSecond
}

// ── schema 结构重建与自检 ────────────────────────────────────────────

// rebuildFieldMaps 依据 patch 后的 Field.DBName/PrimaryKey 重建
// FieldsByDBName/DBNames/PrimaryFields/PrioritizedPrimaryField/PrimaryFieldDBNames
// （对齐 gorm schema.Parse 的首见优先语义）。
func rebuildFieldMaps(s *gormSchema.Schema) {
	s.FieldsByDBName = make(map[string]*gormSchema.Field, len(s.Fields))
	s.DBNames = make([]string, 0, len(s.Fields))
	s.PrimaryFields = nil
	for _, f := range s.Fields {
		if f.DBName == "" {
			continue
		}
		if _, ok := s.FieldsByDBName[f.DBName]; !ok {
			s.FieldsByDBName[f.DBName] = f
			s.DBNames = append(s.DBNames, f.DBName)
		}
		if f.PrimaryKey {
			s.PrimaryFields = append(s.PrimaryFields, f)
		}
	}
	s.PrimaryFieldDBNames = nil
	for _, f := range s.PrimaryFields {
		s.PrimaryFieldDBNames = append(s.PrimaryFieldDBNames, f.DBName)
	}
	s.PrioritizedPrimaryField = nil
	switch len(s.PrimaryFields) {
	case 1:
		s.PrioritizedPrimaryField = s.PrimaryFields[0]
	default:
		if len(s.PrimaryFields) > 1 {
			// 复合主键：AUTOINCREMENT 字段优先（对齐 gorm），否则保持 nil
			for _, f := range s.PrimaryFields {
				if f.AutoIncrement {
					s.PrioritizedPrimaryField = f
					break
				}
			}
		}
	}
}

// verifyPatchedSchema patch 后自检（7.3 缓解 3）：
//  1. FieldsByDBName 键集必须与"patch 期望产物"完全一致（忽略字段被移出、
//     改名列以新名收录、无字段静默丢失/冲突覆盖）；
//  2. DBNames 与 FieldsByDBName 键集一致；
//  3. PrimaryFields 与 PrioritizedPrimaryField 状态自洽。
func verifyPatchedSchema(s *gormSchema.Schema, ignored map[*gormSchema.Field]bool) error {
	expected := make(map[string]bool, len(s.Fields))
	for _, f := range s.Fields {
		if ignored[f] || f.DBName == "" {
			continue
		}
		expected[f.DBName] = true
	}
	if len(s.DBNames) != len(s.FieldsByDBName) {
		return fmt.Errorf("gormdriver: 模型 %s schema patch 自检失败：DBNames(%d) 与 FieldsByDBName(%d) 数量不一致",
			s.Name, len(s.DBNames), len(s.FieldsByDBName))
	}
	actual := make(map[string]bool, len(s.FieldsByDBName))
	for k, f := range s.FieldsByDBName {
		if f == nil || f.DBName != k {
			return fmt.Errorf("gormdriver: 模型 %s schema patch 自检失败：FieldsByDBName 键 %q 与字段 DBName 不一致", s.Name, k)
		}
		actual[k] = true
	}
	var missing, unexpected []string
	for k := range expected {
		if !actual[k] {
			missing = append(missing, k)
		}
	}
	for k := range actual {
		if !expected[k] {
			unexpected = append(unexpected, k)
		}
	}
	if len(missing) > 0 || len(unexpected) > 0 {
		return fmt.Errorf("gormdriver: 模型 %s schema patch 自检失败：patch 后 FieldsByDBName 键集异常（丢失 %v，多余 %v）",
			s.Name, missing, unexpected)
	}
	for _, f := range s.PrimaryFields {
		if !f.PrimaryKey {
			return fmt.Errorf("gormdriver: 模型 %s schema patch 自检失败：PrimaryFields 含非主键字段 %q", s.Name, f.Name)
		}
	}
	if s.PrioritizedPrimaryField != nil && !s.PrioritizedPrimaryField.PrimaryKey {
		return fmt.Errorf("gormdriver: 模型 %s schema patch 自检失败：PrioritizedPrimaryField %q 非主键", s.Name, s.PrioritizedPrimaryField.Name)
	}
	return nil
}

// ── gorm 字段定位 ────────────────────────────────────────────────────

// gormFieldsByReflectedIndex 建立"归一化反射索引路径 → Field"映射。
// gorm 对指针匿名嵌入的叶子字段以负数编码首级索引（-wrapperIdx-1），
// 统一归一化为正数以对齐 ormtag 的 FieldIndex 路径。
func gormFieldsByReflectedIndex(s *gormSchema.Schema) map[string]*gormSchema.Field {
	out := make(map[string]*gormSchema.Field, len(s.Fields))
	for _, f := range s.Fields {
		out[indexKey(normalizeReflectedIndex(f.StructField.Index))] = f
	}
	return out
}

// normalizeReflectedIndex 归一化反射索引路径（负数首级还原为嵌入包装字段下标）。
func normalizeReflectedIndex(index []int) []int {
	out := make([]int, len(index))
	for i, v := range index {
		if i == 0 && v < 0 {
			out[i] = -v - 1
		} else {
			out[i] = v
		}
	}
	return out
}

func indexKey(index []int) string {
	var b strings.Builder
	for i, v := range index {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(v))
	}
	return b.String()
}

// locateGormField 定位 orm tag 字段对应的 gorm Field：优先按 FieldIndex 路径
// 精确匹配（嵌入展开安全），回退按字段名（FieldsByName）。
func locateGormField(s *gormSchema.Schema, byIndex map[string]*gormSchema.Field, fm ormtag.FieldMeta) *gormSchema.Field {
	if len(fm.FieldIndex) > 0 {
		if gf, ok := byIndex[indexKey(fm.FieldIndex)]; ok {
			return gf
		}
	}
	if gf, ok := s.FieldsByName[fm.FieldName]; ok && len(fm.FieldIndex) <= 1 {
		return gf
	}
	return nil
}

// ── Field.Tag 注入（索引双写 patch 第二写） ──────────────────────────

// injectGormTag 将 gormVal 追加到 Field.Tag 的 gorm tag 值尾部（保留其他 tag 键）。
// 全库运行期仅 schema/index.go parseFieldIndexes 读 Field.Tag，注入无侧漏（2.3）。
func injectGormTag(tag reflect.StructTag, gormVal string) reflect.StructTag {
	raw := string(tag)
	if old := tag.Get("gorm"); old != "" {
		raw = strings.Replace(raw, `gorm:"`+old+`"`, "", 1)
	}
	raw = strings.TrimSpace(raw)
	if raw != "" {
		raw += " "
	}
	return reflect.StructTag(raw + `gorm:"` + gormVal + `"`)
}

// ── 日志 ─────────────────────────────────────────────────────────────

// warnf 输出 Warn 日志（10.3 陷阱表：双 tag 冲突必须告警）；logger 不可用时静默。
func warnf(db *gorm.DB, format string, args ...any) {
	if db == nil || db.Logger == nil {
		return
	}
	db.Logger.Warn(context.Background(), fmt.Sprintf(format, args...))
}

// warnGormConflict orm tag 覆盖 gorm tag 同维度声明时的冲突告警（10.3）。
func warnGormConflict(db *gorm.DB, gf *gormSchema.Field, dim, ormVal, gormKey string) {
	if gormVal := strings.TrimSpace(gf.TagSettings[gormKey]); gormVal != "" && !strings.EqualFold(gormVal, ormVal) {
		modelName := ""
		if gf.Schema != nil {
			modelName = gf.Schema.Name
		}
		warnf(db, "[gormdriver] 字段 %s.%s 的 orm tag %s=%q 覆盖了 gorm tag 同维度声明 %s=%q（orm 优先，文档 4.6）",
			modelName, gf.Name, dim, ormVal, strings.ToLower(gormKey), gormVal)
	}
}

// ── preload.MetaAdapter 适配器（gorm 侧实现，供共享 Preload 引擎） ─────

// gormMetaAdapter 将 gorm schema 元数据适配为共享 Preload 引擎的 MetaAdapter。
// TableName 返回 stmt.Parse 后 schema.Table——命名策略解析后的最终表名
// （含 TablePrefix / 租户 schema 前缀，动态 Schema() 场景由查询链 db 的
// NamingStrategy 承载）；裸列名经 ColumnOfField/HasColumn 提供。
type gormMetaAdapter struct {
	driver *GormDriver
	db     *gorm.DB
}

var _ preload.MetaAdapter = (*gormMetaAdapter)(nil)

// schemaOf 解析（并懒加载 patch）模型类型对应的 gorm schema。
func (a *gormMetaAdapter) schemaOf(t reflect.Type) (*gormSchema.Schema, error) {
	model := reflect.New(t).Interface()
	if a.driver != nil {
		if err := a.driver.ensurePatched(model, a.db); err != nil {
			return nil, err
		}
	}
	stmt := &gorm.Statement{DB: a.db}
	if err := stmt.Parse(model); err != nil {
		return nil, err
	}
	return stmt.Schema, nil
}

// TableName 模型类型对应的表名（含命名策略前缀）。
func (a *gormMetaAdapter) TableName(modelType reflect.Type) (string, error) {
	s, err := a.schemaOf(modelType)
	if err != nil {
		return "", err
	}
	return s.Table, nil
}

// PrimaryKeys 模型主键列名列表。
func (a *gormMetaAdapter) PrimaryKeys(modelType reflect.Type) ([]string, error) {
	s, err := a.schemaOf(modelType)
	if err != nil {
		return nil, err
	}
	return append([]string(nil), s.PrimaryFieldDBNames...), nil
}

// ColumnOfField Go 字段名 → 列名（忽略字段/关联字段无列名时 ok=false）。
func (a *gormMetaAdapter) ColumnOfField(modelType reflect.Type, fieldName string) (string, bool) {
	s, err := a.schemaOf(modelType)
	if err != nil {
		return "", false
	}
	if f, ok := s.FieldsByName[fieldName]; ok && f.DBName != "" {
		return f.DBName, true
	}
	return "", false
}

// HasColumn 列名是否存在于模型表中。
func (a *gormMetaAdapter) HasColumn(modelType reflect.Type, columnName string) bool {
	s, err := a.schemaOf(modelType)
	if err != nil {
		return false
	}
	return s.FieldsByDBName[columnName] != nil
}

// ── 通用反射辅助 ─────────────────────────────────────────────────────

// indirectStructType 解引用指针/切片/数组得到 struct 类型；非 struct 返回 nil。
func indirectStructType(v any) reflect.Type {
	if v == nil {
		return nil
	}
	t := reflect.TypeOf(v)
	for t != nil && (t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array) {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return nil
	}
	return t
}
