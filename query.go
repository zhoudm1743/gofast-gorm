package gormdriver

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/zhoudm1743/go-fast-framework/contracts"
	"github.com/zhoudm1743/go-fast-framework/database/preload"
	"github.com/zhoudm1743/go-fast-framework/utils"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// GormQuery 将 contracts.Query 的每个方法代理到 *gorm.DB。
// 所有链式方法返回新的 GormQuery 实例，保持不可变。
type GormQuery struct {
	db     *gorm.DB
	schema string // 动态 schema 前缀，主要用于 PostgreSQL 多 schema 场景
	driver *GormDriver

	// enginePreloads 共享 Preload 引擎待执行项（9.3 分流：gorm:"-" 关联字段
	// 由共享引擎接管；gorm 原生 Preload 无法回填被忽略的关联字段，故延迟到
	// 终结方法行装载完成后执行）。
	enginePreloads []enginePreloadSpec
}

// enginePreloadSpec 单个共享引擎 Preload 的定制项。
type enginePreloadSpec struct {
	path      string
	conds     []any
	callbacks []func(contracts.Query) contracts.Query
}

var _ contracts.Query = (*GormQuery)(nil)

// wrap 创建新的 GormQuery，传入新的 *gorm.DB，并保留当前 schema、driver 与
// 待执行的引擎 Preload 列表。
func (q *GormQuery) wrap(db *gorm.DB) *GormQuery {
	return &GormQuery{db: db, schema: q.schema, driver: q.driver, enginePreloads: q.enginePreloads}
}

// schemaTable 在 schema 非空且 name 中不含 "." 时自动加上 "schema." 前缀。
func (q *GormQuery) schemaTable(name string) string {
	if q.schema != "" && !strings.Contains(name, ".") {
		return q.schema + "." + name
	}
	return name
}

// ── 调试／Schema ─────────────────────────────────────────────────────

// Schema 在当前查询链上设置动态 schema（主要用于 PostgreSQL）。
// 后续的 Model()/Table() 调用将自动在表名前加上 "schema." 前缀。
// 示例：facades.DB().Connection("pg").Schema("analytics").Model(&Event{}).Find(&events)
func (q *GormQuery) Schema(name string) contracts.Query {
	if name == "" {
		return q
	}
	// 克隆 NamingStrategy 并替换 TablePrefix 为租户 schema 前缀。
	// 这样主表、Preload 子查询、Joins 关联表统一由 NamingStrategy 生成租户 schema 前缀，
	// 而不是沿用连接配置里固定的默认 schema（如 "public."），否则关联表会查错 schema。
	// 必须克隆，避免修改共享的 NamingStrategy 影响其他连接上的查询。
	// 使用 Session(&gorm.Session{})（clone=2）以保留已构建的 Statement 状态，
	// 确保 Schema() 无论在 Model/Where 之前或之后调用都不丢失查询条件。
	db := q.db.Session(&gorm.Session{})
	switch ns := db.NamingStrategy.(type) {
	case schema.NamingStrategy:
		ns.TablePrefix = name + "."
		db.NamingStrategy = ns
	case *schema.NamingStrategy:
		clone := *ns
		clone.TablePrefix = name + "."
		db.NamingStrategy = &clone
	}
	// 保留 driver 与 enginePreloads（与 wrap 同语义）：Schema() 后的链仍需
	// driver 做 orm tag 懒加载 patch（applySchema → ensurePatched）与共享
	// Preload 引擎回填；丢失会导致 Schema 链上的写操作按未 patch 的约定
	// 列名拼 SQL（U18 集成测试回归发现）。
	return &GormQuery{db: db, schema: name, driver: q.driver, enginePreloads: q.enginePreloads}
}

// GetSchema 返回当前查询上下文的 schema 名称（PostgreSQL 多 schema 场景）。
// 无 schema 上下文时返回空字符串。
// 业务代码需要在原生 SQL 中拼接 schema 限定的表名时使用此方法。
func (q *GormQuery) GetSchema() string {
	return q.schema
}

// Cache 对当前查询链开启结果缓存。
// 通过 context 注入缓存配置，gormCacher（caches.Cacher 实现）据此决定是否读写缓存。
// 未启用缓存插件的连接上调用本方法无副作用。
// 注意：Row()/Rows() 等游标类终结方法会内部剥离缓存标记（见 withoutCache），
// 避免命中缓存时返回空游标导致 panic。
func (q *GormQuery) Cache(opts ...contracts.CacheOption) contracts.Query {
	cfg := contracts.NewCacheConfig(opts...)
	ctx := q.db.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, cacheCtxKey{}, cfg)
	return q.wrap(q.db.WithContext(ctx))
}

// withoutCache 返回已剥离缓存标记的查询链。
// 用于 Row()/Rows() 等返回游标、不适合缓存（无法序列化恢复）的终结方法：
// 若命中缓存会短路查询回调，游标无法构造，导致 panic 或 "unsupported data type"。
func (q *GormQuery) withoutCache() *GormQuery {
	ctx := q.db.Statement.Context
	if ctx != nil {
		if _, ok := ctx.Value(cacheCtxKey{}).(contracts.CacheConfig); ok {
			ctx = context.WithValue(ctx, cacheCtxKey{}, nil)
			return q.wrap(q.db.WithContext(ctx))
		}
	}
	return q
}

// ensurePatched 懒加载兜底：终结方法执行前保证 dest/value 所在模型已按 orm
// tag patch（懒加载兜底，文档 7.2）。错误向上透出。
// 传链上 q.db 作为首次解析连接：动态 Schema() 链的租户前缀 NS 决定 gorm
// schema 缓存钉住的表名（见 ensurePatched 注释）。
func (q *GormQuery) ensurePatched(value any) error {
	if q.driver == nil || value == nil {
		return nil
	}
	return q.driver.ensurePatched(value, q.db)
}

// ensurePatchedWrite 写路径盲区补齐：Update/Updates 系方法头部对 values 与
// 链上 Model 所在模型 ensurePatched（乐观锁与 patch 正确性必需）。
func (q *GormQuery) ensurePatchedWrite(values any) error {
	if q.driver == nil {
		return nil
	}
	if values != nil {
		if err := q.driver.ensurePatched(values, q.db); err != nil {
			return err
		}
	}
	if m := q.db.Statement.Model; m != nil {
		return q.driver.ensurePatched(m, q.db)
	}
	return nil
}

// ensurePatchedChainModel 对链上 Model（无 dest 的聚合/单列路径）ensurePatched。
func (q *GormQuery) ensurePatchedChainModel() error {
	if q.driver == nil {
		return nil
	}
	if m := q.db.Statement.Model; m != nil {
		return q.driver.ensurePatched(m, q.db)
	}
	if d := q.db.Statement.Dest; d != nil {
		return q.driver.ensurePatched(d, q.db)
	}
	return nil
}

// applySchema 在终结方法（First/Find/Create 等）执行前处理租户 schema，
// 并对 dest 执行 orm tag schema patch（懒加载兜底，文档 7.2）。
// 多租户 schema 已通过 Schema() 动态设置 NamingStrategy.TablePrefix，主表与关联表
// （Preload/Joins）由 GORM 统一生成租户 schema 前缀，此处仅保留兜底逻辑：
// 若 NamingStrategy 未生效（如模型实现 TableName() 或自定义 Namer）——解析 dest 得到
// 无 schema 前缀的裸表名——且调用方未显式指定表名，则显式加上 schema 前缀。
// 注意：不再使用 SET search_path，因为该语句作用于连接 session，会污染连接池
// （被其他租户复用连接时串 schema），且 SET 与后续查询可能落在不同连接上不可靠。
func (q *GormQuery) applySchema(dest any) (*gorm.DB, error) {
	if err := q.ensurePatched(dest); err != nil {
		return nil, err
	}
	if q.schema == "" || dest == nil {
		return q.db, nil
	}
	// 调用方已显式 Table()/Model()（schema 模式下驱动已拼好前缀）：
	// 尊重显式表名，不得按 dest 推导覆盖，否则投影结构体、分表等
	// dest 推导表名与实际表名不一致的场景会静默查错表。
	if q.db.Statement.Table != "" || q.db.Statement.TableExpr != nil {
		return q.db, nil
	}
	stmt := &gorm.Statement{DB: q.db}
	// Statement.Parse 会把带 "schema." 前缀的表名拆分：前缀留在 TableExpr、
	// Table 裁成裸名，故用 TableExpr == nil 判定 NamingStrategy 未生效（无前缀），
	// 而非判断 Table 是否含 "."（前缀生效时 Table 恒为裸名，该判定永真）。
	if err := stmt.Parse(dest); err == nil && stmt.Table != "" && stmt.TableExpr == nil && !strings.Contains(stmt.Table, ".") {
		return q.db.Table(q.schema + "." + stmt.Table), nil
	}
	return q.db, nil
}

// ── 构建条件 ─────────────────────────────────────────────────────────

func (q *GormQuery) Table(name string) contracts.Query {
	return q.wrap(q.db.Table(q.schemaTable(name)))
}

func (q *GormQuery) Model(value any) contracts.Query {
	if q.schema != "" {
		// 通过 gorm.Statement.Parse 解析 model 对应的裸表名，再加上 schema 前缀。
		stmt := &gorm.Statement{DB: q.db}
		if err := stmt.Parse(value); err == nil && stmt.Table != "" && !strings.Contains(stmt.Table, ".") {
			return q.wrap(q.db.Table(q.schema + "." + stmt.Table).Model(value))
		}
	}
	return q.wrap(q.db.Model(value))
}

func (q *GormQuery) Select(query any, args ...any) contracts.Query {
	// Select("*") 等价于"选择所有字段"，即 GORM 的默认行为。
	// 若直接透传 "*"，GORM 会设置 Selects=["*"]，从而引发两个边界问题：
	//  1. Pluck 依据 len(Selects)==1 判定"已显式指定单列"而跳过 pluck 列，
	//     最终 SELECT * 扫描到单列 dest 时 panic；
	//  2. buildSelect 中 len(Selects)>0 优先，导致 Omit 失效。
	// 这里统一清空 Selects，回归 GORM 默认语义，以上问题随之消失。
	if s, ok := query.(string); ok && len(args) == 0 && strings.TrimSpace(s) == "*" {
		return q.wrap(q.db.Select([]string{}))
	}
	return q.wrap(q.db.Select(query, args...))
}

func (q *GormQuery) Omit(columns ...string) contracts.Query {
	return q.wrap(q.db.Omit(columns...))
}

func (q *GormQuery) Where(query any, args ...any) contracts.Query {
	return q.wrap(q.db.Where(query, args...))
}

func (q *GormQuery) OrWhere(query any, args ...any) contracts.Query {
	return q.wrap(q.db.Or(query, args...))
}

func (q *GormQuery) Not(query any, args ...any) contracts.Query {
	return q.wrap(q.db.Not(query, args...))
}

func (q *GormQuery) Order(value any) contracts.Query {
	return q.wrap(q.db.Order(value))
}

func (q *GormQuery) Limit(limit int) contracts.Query {
	return q.wrap(q.db.Limit(limit))
}

func (q *GormQuery) Offset(offset int) contracts.Query {
	return q.wrap(q.db.Offset(offset))
}

func (q *GormQuery) Group(name string) contracts.Query {
	return q.wrap(q.db.Group(name))
}

func (q *GormQuery) Having(query any, args ...any) contracts.Query {
	return q.wrap(q.db.Having(query, args...))
}

func (q *GormQuery) Distinct(args ...any) contracts.Query {
	return q.wrap(q.db.Distinct(args...))
}

// ── 关联 ─────────────────────────────────────────────────────────────

func (q *GormQuery) Joins(query string, args ...any) contracts.Query {
	return q.wrap(q.db.Joins(query, args...))
}

// useEnginePreload 判定 Preload 首段字段是否走共享 Preload 引擎（9.3 分流）：
// 字段 gorm tag 为 "-"（gorm 关联解析被跳过，rel tag 元数据由共享引擎接管）→ true；
// 有 gorm 关联 tag 或无任何 tag（约定外键）→ false（gorm 原生 Preload）。
// 嵌套路径仅按首段判定，后续段由引擎自行解析 rel/约定。
func (q *GormQuery) useEnginePreload(path string) bool {
	if q.driver == nil {
		return false
	}
	model := q.db.Statement.Model
	if model == nil {
		model = q.db.Statement.Dest
	}
	if model == nil {
		return false
	}
	t := indirectStructType(model)
	if t == nil {
		return false
	}
	seg := path
	if idx := strings.IndexByte(seg, '.'); idx >= 0 {
		seg = seg[:idx]
	}
	if sf, ok := t.FieldByName(seg); ok && strings.TrimSpace(sf.Tag.Get("gorm")) == "-" {
		return true
	}
	return false
}

// Preload 关联预加载（9.3 分流）：
//  1. 字段有 gorm 关联 tag（foreignKey/many2many/polymorphic 等）→ gorm 原生 Preload；
//  2. 字段有 gorm:"-" → 共享 Preload 引擎（rel tag 元数据，含 many2many/polymorphic），
//     回填在终结方法行装载完成后执行；
//  3. 字段无任何 tag → gorm 原生 Preload（约定外键）。
func (q *GormQuery) Preload(query string, args ...any) contracts.Query {
	if q.useEnginePreload(query) {
		spec := enginePreloadSpec{path: query}
		for _, a := range args {
			if cb, ok := a.(func(contracts.Query) contracts.Query); ok {
				spec.callbacks = append(spec.callbacks, cb)
			} else {
				spec.conds = append(spec.conds, a)
			}
		}
		next := q.wrap(q.db)
		next.enginePreloads = append(append([]enginePreloadSpec{}, q.enginePreloads...), spec)
		return next
	}
	if q.schema != "" {
		schema := q.schema
		// Preload 生成的子查询不会继承 Tenant() 设置的 schema，
		// 需要在回调中手动设置 Table(schema.table)。
		// 注意：NamingStrategy 可能已给表名加上默认 schema 前缀（如 "public."），
		// 这里需先剥离已有前缀再拼接租户 schema，否则会命中 "public.xxx" 而非租户表。
		schemaCb := func(db *gorm.DB) *gorm.DB {
			table := db.Statement.Table
			if table == "" && db.Statement.Model != nil {
				// Preload 子查询回调时机下 Statement.Table 尚未解析（仍为空），
				// 此时 NamingStrategy 对实现 TableName() 的模型不生效，
				// 需从 Statement.Model 重新解析出裸表名，再手动拼租户 schema 前缀。
				stmt := &gorm.Statement{DB: db}
				if err := stmt.Parse(db.Statement.Model); err == nil {
					table = stmt.Table
				}
			}
			if table == "" {
				return db
			}
			if idx := strings.LastIndex(table, "."); idx >= 0 {
				table = table[idx+1:]
			}
			return db.Table(schema + "." + table)
		}
		hasCallback := false
		for i, a := range args {
			if existing, ok := a.(func(*gorm.DB) *gorm.DB); ok {
				args[i] = func(db *gorm.DB) *gorm.DB {
					return existing(schemaCb(db))
				}
				hasCallback = true
				break
			}
		}
		if !hasCallback {
			args = append(args, schemaCb)
		}
	}
	return q.wrap(q.db.Preload(query, args...))
}

// runEnginePreloads 终结方法行装载完成后执行共享 Preload 引擎回填（9.3）。
// 子查询由 NewQuery 闭包创建：从当前 q.db 构造全新会话（继承 schema 前缀、
// ctx 与缓存标记，不继承父链条件）；表名/列名经 gormMetaAdapter 以命名策略
// 解析后的最终名提供。
func (q *GormQuery) runEnginePreloads(dest any) error {
	if len(q.enginePreloads) == 0 || dest == nil || q.driver == nil {
		return nil
	}
	meta := &gormMetaAdapter{driver: q.driver, db: q.db}
	engine := &preload.Engine{
		Meta: meta,
		NewQuery: func() contracts.Query {
			return &GormQuery{
				db:     q.db.Session(&gorm.Session{NewDB: true}),
				schema: q.schema,
				driver: q.driver,
			}
		},
		Resolver: &preload.ORMTagResolver{Meta: meta},
	}
	for _, spec := range q.enginePreloads {
		if err := engine.Preload(dest, spec.path, spec.conds, spec.callbacks); err != nil {
			return wrapError(err)
		}
	}
	return nil
}

// ── 终结方法 ─────────────────────────────────────────────────────────

func (q *GormQuery) Find(dest any, conds ...any) error {
	db, err := q.applySchema(dest)
	if err != nil {
		return err
	}
	if err := wrapError(db.Find(dest, conds...).Error); err != nil {
		return err
	}
	if err := q.runEnginePreloads(dest); err != nil {
		return err
	}
	return invokeAfterFind(q, dest)
}

func (q *GormQuery) First(dest any, conds ...any) error {
	db, err := q.applySchema(dest)
	if err != nil {
		return err
	}
	if err := wrapError(db.First(dest, conds...).Error); err != nil {
		return err
	}
	if err := q.runEnginePreloads(dest); err != nil {
		return err
	}
	return invokeAfterFind(q, dest)
}

func (q *GormQuery) Last(dest any, conds ...any) error {
	db, err := q.applySchema(dest)
	if err != nil {
		return err
	}
	if err := wrapError(db.Last(dest, conds...).Error); err != nil {
		return err
	}
	if err := q.runEnginePreloads(dest); err != nil {
		return err
	}
	return invokeAfterFind(q, dest)
}

func (q *GormQuery) Take(dest any, conds ...any) error {
	db, err := q.applySchema(dest)
	if err != nil {
		return err
	}
	if err := wrapError(db.Take(dest, conds...).Error); err != nil {
		return err
	}
	if err := q.runEnginePreloads(dest); err != nil {
		return err
	}
	return invokeAfterFind(q, dest)
}

func (q *GormQuery) Create(value any) error {
	if err := invokeBeforeCreate(q, value); err != nil {
		return err
	}
	if err := q.ensurePatched(value); err != nil {
		return err
	}
	if q.driver != nil {
		q.driver.setVersionOnCreate(value) // 乐观锁：插入 version 置 1（7.4）
	}
	db, err := q.applySchema(value)
	if err != nil {
		return err
	}
	if err := wrapError(db.Create(value).Error); err != nil {
		return err
	}
	return invokeAfterCreate(q, value)
}

func (q *GormQuery) CreateInBatches(value any, batchSize int) error {
	if err := invokeBeforeCreate(q, value); err != nil {
		return err
	}
	if err := q.ensurePatched(value); err != nil {
		return err
	}
	if q.driver != nil {
		q.driver.setVersionOnCreate(value) // 乐观锁：插入 version 置 1（7.4）
	}
	db, err := q.applySchema(value)
	if err != nil {
		return err
	}
	if err := wrapError(db.CreateInBatches(value, batchSize).Error); err != nil {
		return err
	}
	return invokeAfterCreate(q, value)
}

func (q *GormQuery) Save(value any) error {
	if err := invokeBeforeUpdate(q, value); err != nil {
		return err
	}
	if err := q.ensurePatched(value); err != nil {
		return err
	}
	db, err := q.applySchema(value)
	if err != nil {
		return err
	}
	if q.driver != nil {
		// 乐观锁仿真（7.4）：无 version 字段时内部走原生 Save（含 upsert 回落）
		if _, err := q.driver.saveWithLock(db, value); err != nil {
			return err
		}
	} else if err := wrapError(db.Save(value).Error); err != nil {
		return err
	}
	return invokeAfterUpdate(q, value)
}

// toGormValue 将列值中的 SQL 表达式（contracts.Expr 的返回值，X-08）转换为
// gorm.Expr，由数据库端执行（如 "count + ?" 实现原子自增）；其余值原样返回。
func toGormValue(v any) any {
	if expr, ok := v.(contracts.SQLExpression); ok {
		query, args := expr.ExprSQL()
		return gorm.Expr(query, args...)
	}
	return v
}

// convertUpdateValues 处理 Updates 的批量更新值（X-08）：
// values 为 map[string]any 且含 SQLExpression 时，拷贝一份新 map 逐值转换后传入
// （不得修改调用方传入的 map）；struct 与其他类型原样返回——GORM 的 struct 更新
// 不支持表达式字段，需要表达式时请改用 map 形式。
func convertUpdateValues(values any) any {
	m, ok := values.(map[string]any)
	if !ok {
		return values
	}
	hasExpr := false
	for _, v := range m {
		if _, ok := v.(contracts.SQLExpression); ok {
			hasExpr = true
			break
		}
	}
	if !hasExpr {
		return values
	}
	converted := make(map[string]any, len(m))
	for k, v := range m {
		converted[k] = toGormValue(v)
	}
	return converted
}

func (q *GormQuery) Update(column string, value any) error {
	// 盲区补齐：Update 单列不经 applySchema，链上模型需懒加载 patch。
	// Update 单列不参与乐观锁（7.4，对齐 xorm 指定列更新无版本条件）。
	if err := q.ensurePatchedChainModel(); err != nil {
		return err
	}
	// 值为 SQL 表达式（contracts.Expr）时转换为 gorm.Expr，实现数据库端原子更新（X-08）。
	return wrapError(q.db.Update(column, toGormValue(value)).Error)
}

func (q *GormQuery) Updates(values any) error {
	if err := q.ensurePatchedWrite(values); err != nil {
		return err
	}
	if q.driver != nil && q.driver.hasVersionLock(values) {
		// 乐观锁仿真（7.4）：struct 更新 WHERE version=旧值 + 自增 + 回填
		if _, handled := q.driver.updatesWithLock(q.db, values); handled {
			return nil
		}
	}
	return wrapError(q.db.Updates(convertUpdateValues(values)).Error)
}

func (q *GormQuery) Delete(value any, conds ...any) error {
	if err := invokeBeforeDelete(q, value); err != nil {
		return err
	}
	db, err := q.applySchema(value)
	if err != nil {
		return err
	}
	if err := wrapError(db.Delete(value, conds...).Error); err != nil {
		return err
	}
	return invokeAfterDelete(q, value)
}

func (q *GormQuery) Count(count *int64) error {
	if err := q.ensurePatchedChainModel(); err != nil {
		return err
	}
	return wrapError(q.db.Count(count).Error)
}

func (q *GormQuery) Scan(dest any) error {
	if err := q.ensurePatched(dest); err != nil {
		return err
	}
	return wrapError(q.db.Scan(dest).Error)
}

func (q *GormQuery) Pluck(column string, dest any) error {
	if err := q.ensurePatchedChainModel(); err != nil {
		return err
	}
	return wrapError(q.db.Pluck(column, dest).Error)
}

func (q *GormQuery) Row() contracts.Row {
	return q.withoutCache().db.Row()
}

func (q *GormQuery) Rows() (contracts.Rows, error) {
	rows, err := q.withoutCache().db.Rows()
	if err != nil {
		return nil, wrapError(err)
	}
	return rows, nil
}

// ── 写操作 Result 变体 ──────────────────────────────────────────────

func (q *GormQuery) CreateResult(value any) contracts.Result {
	if err := invokeBeforeCreate(q, value); err != nil {
		return contracts.Result{Error: err}
	}
	if err := q.ensurePatched(value); err != nil {
		return contracts.Result{Error: err}
	}
	if q.driver != nil {
		q.driver.setVersionOnCreate(value) // 乐观锁：插入 version 置 1（7.4）
	}
	db, err := q.applySchema(value)
	if err != nil {
		return contracts.Result{Error: err}
	}
	tx := db.Create(value)
	if tx.Error != nil {
		return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
	}
	if err := invokeAfterCreate(q, value); err != nil {
		return contracts.Result{Error: err}
	}
	return contracts.Result{RowsAffected: tx.RowsAffected}
}

func (q *GormQuery) UpdateResult(column string, value any) contracts.Result {
	// 盲区补齐：UpdateResult 单列不经 applySchema；不参与乐观锁（7.4）。
	if err := q.ensurePatchedChainModel(); err != nil {
		return contracts.Result{Error: err}
	}
	tx := q.db.Update(column, toGormValue(value))
	return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
}

func (q *GormQuery) UpdatesResult(values any) contracts.Result {
	if err := q.ensurePatchedWrite(values); err != nil {
		return contracts.Result{Error: err}
	}
	if q.driver != nil && q.driver.hasVersionLock(values) {
		// 乐观锁仿真（7.4）：struct 更新 WHERE version=旧值 + 自增 + 回填
		if tx, handled := q.driver.updatesWithLock(q.db, values); handled {
			return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
		}
	}
	tx := q.db.Updates(convertUpdateValues(values))
	return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
}

func (q *GormQuery) DeleteResult(value any, conds ...any) contracts.Result {
	if err := invokeBeforeDelete(q, value); err != nil {
		return contracts.Result{Error: err}
	}
	db, err := q.applySchema(value)
	if err != nil {
		return contracts.Result{Error: err}
	}
	tx := db.Delete(value, conds...)
	if tx.Error != nil {
		return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
	}
	if err := invokeAfterDelete(q, value); err != nil {
		return contracts.Result{Error: err}
	}
	return contracts.Result{RowsAffected: tx.RowsAffected}
}

func (q *GormQuery) SaveResult(value any) contracts.Result {
	if err := invokeBeforeUpdate(q, value); err != nil {
		return contracts.Result{Error: err}
	}
	if err := q.ensurePatched(value); err != nil {
		return contracts.Result{Error: err}
	}
	db, err := q.applySchema(value)
	if err != nil {
		return contracts.Result{Error: err}
	}
	if q.driver != nil {
		// 乐观锁仿真（7.4）：无 version 字段时内部走原生 Save（含 upsert 回落）
		rows, err := q.driver.saveWithLock(db, value)
		if err != nil {
			return contracts.Result{RowsAffected: rows, Error: err}
		}
		if err := invokeAfterUpdate(q, value); err != nil {
			return contracts.Result{Error: err}
		}
		return contracts.Result{RowsAffected: rows}
	}
	tx := db.Save(value)
	if tx.Error != nil {
		return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
	}
	if err := invokeAfterUpdate(q, value); err != nil {
		return contracts.Result{Error: err}
	}
	return contracts.Result{RowsAffected: tx.RowsAffected}
}

// ── 原生 SQL ─────────────────────────────────────────────────────────

func (q *GormQuery) Raw(sql string, values ...any) contracts.Query {
	return q.wrap(q.db.Raw(sql, values...))
}

func (q *GormQuery) Exec(sql string, values ...any) error {
	return wrapError(q.db.Exec(sql, values...).Error)
}

// ExecResult 执行原生 SQL 写操作并返回受影响行数（X-08）。
// 与 Exec 的差异：Exec 只返回错误，ExecResult 额外携带 RowsAffected，
// 适用于需要依据行数做业务判定的原生 SQL 场景（如配额原子扣减、存在性更新）。
func (q *GormQuery) ExecResult(sql string, values ...any) contracts.Result {
	tx := q.db.Exec(sql, values...)
	return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
}

// ── 事务 ─────────────────────────────────────────────────────────────

func (q *GormQuery) Transaction(fc func(tx contracts.Query) error, opts ...contracts.TxOption) error {
	txOpts := parseTxOptions(opts...)
	return wrapError(q.db.Transaction(func(tx *gorm.DB) error {
		return fc(q.wrap(tx))
	}, txOpts))
}

func (q *GormQuery) Begin(opts ...contracts.TxOption) contracts.Query {
	txOpts := parseTxOptions(opts...)
	var tx *gorm.DB
	if txOpts != nil {
		tx = q.db.Begin(txOpts)
	} else {
		tx = q.db.Begin()
	}
	return q.wrap(tx)
}

func (q *GormQuery) Commit() error {
	return wrapError(q.db.Commit().Error)
}

func (q *GormQuery) Rollback() error {
	return wrapError(q.db.Rollback().Error)
}

func (q *GormQuery) SavePoint(name string) error {
	return wrapError(q.db.SavePoint(name).Error)
}

func (q *GormQuery) RollbackTo(name string) error {
	return wrapError(q.db.RollbackTo(name).Error)
}

// ── 分页 ─────────────────────────────────────────────────────────────

func (q *GormQuery) Paginate(page, size int) contracts.Query {
	page, size = utils.PageUtil.Normalize(page, size)
	return q.wrap(q.db.Offset(utils.PageUtil.Offset(page, size)).Limit(size))
}

// ── 作用域 ───────────────────────────────────────────────────────────

func (q *GormQuery) Scopes(funcs ...func(contracts.Query) contracts.Query) contracts.Query {
	gormScopes := make([]func(*gorm.DB) *gorm.DB, 0, len(funcs))
	for _, fn := range funcs {
		fn := fn
		gormScopes = append(gormScopes, func(db *gorm.DB) *gorm.DB {
			result := fn(q.wrap(db))
			if gq, ok := result.(*GormQuery); ok {
				return gq.db
			}
			return db
		})
	}
	return q.wrap(q.db.Scopes(gormScopes...))
}

// ── 上下文 ───────────────────────────────────────────────────────────

func (q *GormQuery) WithContext(ctx context.Context) contracts.Query {
	return q.wrap(q.db.WithContext(ctx))
}

// ── 调试 ─────────────────────────────────────────────────────

func (q *GormQuery) Debug() contracts.Query {
	return q.wrap(q.db.Debug())
}

// ── 悲观锁 ───────────────────────────────────────────────────────────

func (q *GormQuery) Lock(mode contracts.LockMode) contracts.Query {
	switch mode {
	case contracts.LockForUpdate:
		// 悲观锁查询不能被缓存：命中缓存会跳过 DB 执行，导致锁不生效。
		// 自动剥离缓存标记，保证 FOR UPDATE/SHARE 语义。
		base := q.withoutCache()
		return base.wrap(base.db.Clauses(clause.Locking{Strength: "UPDATE"}))
	case contracts.LockShareMode:
		base := q.withoutCache()
		return base.wrap(base.db.Clauses(clause.Locking{Strength: "SHARE"}))
	default:
		return q
	}
}

// ── 软删除扩展 ───────────────────────────────────────────────────

func (q *GormQuery) Unscoped() contracts.Query {
	return q.wrap(q.db.Unscoped())
}

// OnlyTrashed 仅查询已软删除的记录（deleted_at != 0）。
// 注意：列名 "deleted_at" 与 database.SoftDelete.DeletedAt 字段绑定，
// 若自定义软删除列名需自行实现此逻辑。
func (q *GormQuery) OnlyTrashed() contracts.Query {
	return q.wrap(q.db.Unscoped().Where("deleted_at != 0"))
}

func (q *GormQuery) Restore() error {
	return wrapError(q.db.Unscoped().Update("deleted_at", 0).Error)
}

func (q *GormQuery) ForceDelete(value any, conds ...any) error {
	return wrapError(q.db.Unscoped().Delete(value, conds...).Error)
}

// ── 高级查询 ─────────────────────────────────────────────────────────

func (q *GormQuery) FirstOrCreate(dest any, conds ...any) error {
	db, err := q.applySchema(dest)
	if err != nil {
		return err
	}
	return wrapError(db.FirstOrCreate(dest, conds...).Error)
}

func (q *GormQuery) FirstOrInit(dest any, conds ...any) error {
	db, err := q.applySchema(dest)
	if err != nil {
		return err
	}
	return wrapError(db.FirstOrInit(dest, conds...).Error)
}

func (q *GormQuery) FindInBatches(dest any, batchSize int, fc func(tx contracts.Query, batch int) error) error {
	// 非法 batchSize 防护：gorm 内部对非正 batchSize 会以 reflect 切片越界 panic
	//（§11.6"非法 batchSize 不 panic"），此处提前拦截返回明确错误（xorm 侧同
	// 参数不 panic，行为差异已在套件注释固化）。
	if batchSize <= 0 {
		return fmt.Errorf("%w: FindInBatches batchSize 必须为正整数，收到 %d", contracts.ErrUnsupported, batchSize)
	}
	db, err := q.applySchema(dest)
	if err != nil {
		return err
	}
	return wrapError(db.FindInBatches(dest, batchSize, func(tx *gorm.DB, batch int) error {
		gq := q.wrap(tx)
		// 共享引擎 Preload 逐批回填（与 gorm 原生批内 Preload 时机一致）
		if err := gq.runEnginePreloads(dest); err != nil {
			return err
		}
		return fc(gq, batch)
	}).Error)
}

func (q *GormQuery) ScanMap(dest *[]map[string]any) error {
	rows, err := q.db.Rows()
	if err != nil {
		return wrapError(err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return wrapError(err)
	}

	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return wrapError(err)
		}
		row := make(map[string]any, len(columns))
		for i, col := range columns {
			val := values[i]
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		*dest = append(*dest, row)
	}
	if err := rows.Err(); err != nil {
		return wrapError(err)
	}
	return nil
}

func (q *GormQuery) Exists(dest any, conds ...any) (bool, error) {
	db, err := q.applySchema(dest)
	if err != nil {
		return false, err
	}
	tx := db.Model(dest)
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
	var count int64
	if err := tx.Limit(1).Count(&count).Error; err != nil {
		return false, wrapError(err)
	}
	return count > 0, nil
}

// ── 辅助 ─────────────────────────────────────────────────────────────

func parseTxOptions(opts ...contracts.TxOption) *sql.TxOptions {
	for _, opt := range opts {
		if std, ok := opt.(*contracts.StandardTxOptions); ok {
			return &sql.TxOptions{
				Isolation: std.Isolation,
				ReadOnly:  std.ReadOnly,
			}
		}
	}
	return nil
}

// ── 模型钩子调用框架 ─────────────────────────────────────────────────

// walkValues 将 value 展开为可寻址对象指针，对每个元素调用 fn。
// 支持 *T、T、[]T、*[]T、[]*T、*[]*T；切片逐元素执行，非切片单次执行。
// 若 fn 返回错误，立即终止遍历并返回该错误。
func walkValues(value any, fn func(iface any) error) error {
	rv := reflect.ValueOf(value)
	// 解引用外层指针
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.Slice:
		for i := 0; i < rv.Len(); i++ {
			elem := rv.Index(i)
			var iface any
			if elem.Kind() == reflect.Ptr {
				if elem.IsNil() {
					continue
				}
				iface = elem.Interface()
			} else if elem.CanAddr() {
				iface = elem.Addr().Interface()
			} else {
				continue
			}
			if err := fn(iface); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		if !rv.CanAddr() {
			return nil
		}
		return fn(rv.Addr().Interface())
	default:
		return nil
	}
}

// invokeBeforeCreate 对 value 依次调用 IDAutoGenerator 和 BeforeCreator。
func invokeBeforeCreate(q contracts.Query, value any) error {
	return walkValues(value, func(iface any) error {
		if ag, ok := iface.(contracts.IDAutoGenerator); ok {
			ag.AutoGenerateID()
		}
		if bc, ok := iface.(contracts.BeforeCreator); ok {
			return bc.OnBeforeCreate(q)
		}
		return nil
	})
}

// invokeAfterCreate 对 value 调用 AfterCreator。
func invokeAfterCreate(q contracts.Query, value any) error {
	return walkValues(value, func(iface any) error {
		if ac, ok := iface.(contracts.AfterCreator); ok {
			return ac.OnAfterCreate(q)
		}
		return nil
	})
}

// invokeBeforeUpdate 对 value 调用 BeforeUpdater。
func invokeBeforeUpdate(q contracts.Query, value any) error {
	return walkValues(value, func(iface any) error {
		if bu, ok := iface.(contracts.BeforeUpdater); ok {
			return bu.OnBeforeUpdate(q)
		}
		return nil
	})
}

// invokeAfterUpdate 对 value 调用 AfterUpdater。
func invokeAfterUpdate(q contracts.Query, value any) error {
	return walkValues(value, func(iface any) error {
		if au, ok := iface.(contracts.AfterUpdater); ok {
			return au.OnAfterUpdate(q)
		}
		return nil
	})
}

// invokeBeforeDelete 对 value 调用 BeforeDeleter。
func invokeBeforeDelete(q contracts.Query, value any) error {
	return walkValues(value, func(iface any) error {
		if bd, ok := iface.(contracts.BeforeDeleter); ok {
			return bd.OnBeforeDelete(q)
		}
		return nil
	})
}

// invokeAfterDelete 对 value 调用 AfterDeleter。
func invokeAfterDelete(q contracts.Query, value any) error {
	return walkValues(value, func(iface any) error {
		if ad, ok := iface.(contracts.AfterDeleter); ok {
			return ad.OnAfterDelete(q)
		}
		return nil
	})
}

// invokeAfterFind 对 value 调用 AfterFinder。
func invokeAfterFind(q contracts.Query, value any) error {
	return walkValues(value, func(iface any) error {
		if af, ok := iface.(contracts.AfterFinder); ok {
			return af.OnAfterFind(q)
		}
		return nil
	})
}
