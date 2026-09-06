package gormdriver

import (
	"context"
	"database/sql"
	"reflect"
	"strings"

	"github.com/zhoudm1743/go-fast-framework/contracts"
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
}

var _ contracts.Query = (*GormQuery)(nil)

// wrap 创建新的 GormQuery，传入新的 *gorm.DB，并保留当前 schema。
func (q *GormQuery) wrap(db *gorm.DB) *GormQuery {
	return &GormQuery{db: db, schema: q.schema}
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
	return &GormQuery{db: db, schema: name}
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

// applySchema 在终结方法（First/Find/Create 等）执行前处理租户 schema。
// 多租户 schema 已通过 Schema() 动态设置 NamingStrategy.TablePrefix，主表与关联表
// （Preload/Joins）由 GORM 统一生成租户 schema 前缀，此处仅保留兜底逻辑：
// 若 NamingStrategy 未生效（如模型实现 TableName() 或自定义 Namer）——解析 dest 得到
// 无 schema 前缀的裸表名——且调用方未显式指定表名，则显式加上 schema 前缀。
// 注意：不再使用 SET search_path，因为该语句作用于连接 session，会污染连接池
// （被其他租户复用连接时串 schema），且 SET 与后续查询可能落在不同连接上不可靠。
func (q *GormQuery) applySchema(dest any) *gorm.DB {
	if q.schema == "" || dest == nil {
		return q.db
	}
	// 调用方已显式 Table()/Model()（schema 模式下驱动已拼好前缀）：
	// 尊重显式表名，不得按 dest 推导覆盖，否则投影结构体、分表等
	// dest 推导表名与实际表名不一致的场景会静默查错表。
	if q.db.Statement.Table != "" || q.db.Statement.TableExpr != nil {
		return q.db
	}
	stmt := &gorm.Statement{DB: q.db}
	// Statement.Parse 会把带 "schema." 前缀的表名拆分：前缀留在 TableExpr、
	// Table 裁成裸名，故用 TableExpr == nil 判定 NamingStrategy 未生效（无前缀），
	// 而非判断 Table 是否含 "."（前缀生效时 Table 恒为裸名，该判定永真）。
	if err := stmt.Parse(dest); err == nil && stmt.Table != "" && stmt.TableExpr == nil && !strings.Contains(stmt.Table, ".") {
		return q.db.Table(q.schema + "." + stmt.Table)
	}
	return q.db
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

func (q *GormQuery) Preload(query string, args ...any) contracts.Query {
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

// ── 终结方法 ─────────────────────────────────────────────────────────

func (q *GormQuery) Find(dest any, conds ...any) error {
	if err := wrapError(q.applySchema(dest).Find(dest, conds...).Error); err != nil {
		return err
	}
	return invokeAfterFind(q, dest)
}

func (q *GormQuery) First(dest any, conds ...any) error {
	if err := wrapError(q.applySchema(dest).First(dest, conds...).Error); err != nil {
		return err
	}
	return invokeAfterFind(q, dest)
}

func (q *GormQuery) Last(dest any, conds ...any) error {
	if err := wrapError(q.applySchema(dest).Last(dest, conds...).Error); err != nil {
		return err
	}
	return invokeAfterFind(q, dest)
}

func (q *GormQuery) Take(dest any, conds ...any) error {
	if err := wrapError(q.applySchema(dest).Take(dest, conds...).Error); err != nil {
		return err
	}
	return invokeAfterFind(q, dest)
}

func (q *GormQuery) Create(value any) error {
	if err := invokeBeforeCreate(q, value); err != nil {
		return err
	}
	if err := wrapError(q.applySchema(value).Create(value).Error); err != nil {
		return err
	}
	return invokeAfterCreate(q, value)
}

func (q *GormQuery) CreateInBatches(value any, batchSize int) error {
	if err := invokeBeforeCreate(q, value); err != nil {
		return err
	}
	if err := wrapError(q.applySchema(value).CreateInBatches(value, batchSize).Error); err != nil {
		return err
	}
	return invokeAfterCreate(q, value)
}

func (q *GormQuery) Save(value any) error {
	if err := invokeBeforeUpdate(q, value); err != nil {
		return err
	}
	if err := wrapError(q.applySchema(value).Save(value).Error); err != nil {
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
	// 值为 SQL 表达式（contracts.Expr）时转换为 gorm.Expr，实现数据库端原子更新（X-08）。
	return wrapError(q.db.Update(column, toGormValue(value)).Error)
}

func (q *GormQuery) Updates(values any) error {
	return wrapError(q.db.Updates(convertUpdateValues(values)).Error)
}

func (q *GormQuery) Delete(value any, conds ...any) error {
	if err := invokeBeforeDelete(q, value); err != nil {
		return err
	}
	if err := wrapError(q.applySchema(value).Delete(value, conds...).Error); err != nil {
		return err
	}
	return invokeAfterDelete(q, value)
}

func (q *GormQuery) Count(count *int64) error {
	return wrapError(q.db.Count(count).Error)
}

func (q *GormQuery) Scan(dest any) error {
	return wrapError(q.db.Scan(dest).Error)
}

func (q *GormQuery) Pluck(column string, dest any) error {
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
	tx := q.applySchema(value).Create(value)
	if tx.Error != nil {
		return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
	}
	if err := invokeAfterCreate(q, value); err != nil {
		return contracts.Result{Error: err}
	}
	return contracts.Result{RowsAffected: tx.RowsAffected}
}

func (q *GormQuery) UpdateResult(column string, value any) contracts.Result {
	tx := q.db.Update(column, toGormValue(value))
	return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
}

func (q *GormQuery) UpdatesResult(values any) contracts.Result {
	tx := q.db.Updates(convertUpdateValues(values))
	return contracts.Result{RowsAffected: tx.RowsAffected, Error: wrapError(tx.Error)}
}

func (q *GormQuery) DeleteResult(value any, conds ...any) contracts.Result {
	if err := invokeBeforeDelete(q, value); err != nil {
		return contracts.Result{Error: err}
	}
	tx := q.applySchema(value).Delete(value, conds...)
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
	tx := q.applySchema(value).Save(value)
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
	return wrapError(q.applySchema(dest).FirstOrCreate(dest, conds...).Error)
}

func (q *GormQuery) FirstOrInit(dest any, conds ...any) error {
	return wrapError(q.applySchema(dest).FirstOrInit(dest, conds...).Error)
}

func (q *GormQuery) FindInBatches(dest any, batchSize int, fc func(tx contracts.Query, batch int) error) error {
	return wrapError(q.applySchema(dest).FindInBatches(dest, batchSize, func(tx *gorm.DB, batch int) error {
		return fc(q.wrap(tx), batch)
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
	var count int64
	tx := q.applySchema(dest).Model(dest)
	if len(conds) > 0 {
		tx = tx.Where(conds[0], conds[1:]...)
	}
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
