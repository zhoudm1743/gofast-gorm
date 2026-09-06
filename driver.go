package gormdriver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/zhoudm1743/go-fast-framework/contracts"

	"github.com/glebarez/sqlite"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	gormLogger "gorm.io/gorm/logger"
	gormSchema "gorm.io/gorm/schema"
)

// GormDriver 实现 contracts.Driver
type GormDriver struct {
	db            *gorm.DB
	schema        string // PostgreSQL schema（用于 AutoMigrate 时显式 SET search_path）
	cachesEnabled bool   // 查询缓存插件是否已启用（避免重复注册）

	// ── 统一 orm tag 体系（schema patch，文档 7.1/7.4）──
	patched       sync.Map   // reflect.Type → struct{}：已 patch 模型类型
	patchMu       sync.Mutex // patch 双重检查锁
	versionFields sync.Map   // reflect.Type → *versionFieldMeta：乐观锁字段注册表
}

var _ contracts.Driver = (*GormDriver)(nil)

// NewGormDriver 根据连接配置创建 GORM 驱动实例。
func NewGormDriver(cfg contracts.ConnectionConfig, log contracts.Log) (*GormDriver, error) {
	cfg.ApplyDefaults()

	dsn := cfg.BuildDSN()
	if dsn == "" {
		return nil, fmt.Errorf("[GoFast] gormdriver driver: unsupported engine %q", cfg.Engine)
	}

	gormCfg := buildGormConfig(cfg, log)

	// 当配置了 Schema（主要用于 PostgreSQL）时，设置 GORM NamingStrategy，
	// 使 AutoMigrate 和所有 GORM 生成的 SQL 都自动携带 "schema." 前缀。
	// search_path 已在 DSN 中设置，两者并行，互不冲突：
	//   - search_path：保证原始 SQL（Exec/Raw）及 PostgreSQL 内部（外键等）正确路由
	//   - NamingStrategy.TablePrefix：保证 GORM 结构化查询和 AutoMigrate 在正确 schema 建表
	if cfg.Schema != "" {
		tablePrefix := cfg.Schema + "."
		if cfg.TablePrefix != "" {
			tablePrefix = cfg.Schema + "." + cfg.TablePrefix
		}
		gormCfg.NamingStrategy = gormSchema.NamingStrategy{
			TablePrefix: tablePrefix,
		}
	} else if cfg.TablePrefix != "" {
		gormCfg.NamingStrategy = gormSchema.NamingStrategy{
			TablePrefix: cfg.TablePrefix,
		}
	}

	var db *gorm.DB
	var err error

	switch cfg.Engine {
	case "postgres":
		db, err = gorm.Open(postgres.Open(dsn), gormCfg)
	case "sqlite", "sqlite3":
		// 自动创建数据库文件的父目录，避免 "unable to open database file"。
		// DSN 格式为 "file:<path>?..."，提取文件路径部分。
		dbPath := cfg.Database
		if dbPath == "" {
			// 从 DSN 中解析，根据 "file:" 前缀和 "?" 分隔符提取
			raw := strings.TrimPrefix(dsn, "file:")
			if idx := strings.Index(raw, "?"); idx != -1 {
				dbPath = raw[:idx]
			} else {
				dbPath = raw
			}
		}
		if dir := filepath.Dir(dbPath); dir != "." && dir != "" {
			if mkErr := os.MkdirAll(dir, 0o755); mkErr != nil {
				return nil, fmt.Errorf("[GoFast] sqlite: cannot create dir %q: %w", dir, mkErr)
			}
		}
		db, err = gorm.Open(sqlite.Open(dsn), gormCfg)
	case "mysql":
		db, err = gorm.Open(mysql.Open(dsn), gormCfg)
	case "mssql":
		db, err = gorm.Open(sqlserver.Open(dsn), gormCfg)
	default:
		return nil, fmt.Errorf("[GoFast] gormdriver driver: unsupported engine %q", cfg.Engine)
	}

	if err != nil {
		return nil, fmt.Errorf("[GoFast] gormdriver driver: connection failed: %w", err)
	}

	// 配置连接池
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("[GoFast] gormdriver driver: get sql.DB failed: %w", err)
	}
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(time.Duration(cfg.ConnMaxLifetime) * time.Minute)
	sqlDB.SetConnMaxIdleTime(time.Duration(cfg.ConnMaxIdleTime) * time.Minute)

	// 验证数据库连通性
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("[GoFast] gormdriver driver: database ping failed: %w", err)
	}

	// 注册主键自动生成回调（覆盖 FirstOrCreate 等不经框架钩子的创建路径）
	if err := registerAutoGenerateIDCallback(db); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("[GoFast] gormdriver driver: register id callback failed: %w", err)
	}

	return &GormDriver{db: db, schema: cfg.Schema}, nil
}

func (d *GormDriver) Query(ctx ...context.Context) contracts.Query {
	db := d.db.Session(&gorm.Session{NewDB: true})
	if len(ctx) > 0 && ctx[0] != nil {
		db = db.WithContext(ctx[0])
	}
	return &GormQuery{db: db, driver: d}
}

func (d *GormDriver) DriverName() string { return "gormdriver" }

func (d *GormDriver) Ping() error {
	sqlDB, err := d.db.DB()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return sqlDB.PingContext(ctx)
}

func (d *GormDriver) Close() error {
	sqlDB, err := d.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (d *GormDriver) AutoMigrate(models ...any) error {
	// patch 触发时机（7.2）：AutoMigrate 前对全部 models 先 ensurePatched，
	// 保证 DDL 使用 orm tag patch 后的元数据；orm tag 禁用 token/未知裸 token
	// 在此启动期即报错（4.4）
	for _, model := range models {
		if err := d.ensurePatched(model, d.db); err != nil {
			return err
		}
	}
	if d.schema == "" {
		return d.db.AutoMigrate(models...)
	}
	// PostgreSQL 多租户：在事务内显式 SET LOCAL search_path，
	// 确保 AutoMigrate 的 DDL 在正确的 schema 执行，不依赖连接池的 DSN 初始化值。
	return d.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(fmt.Sprintf(`SET LOCAL search_path TO "%s"`, d.schema)).Error; err != nil {
			return fmt.Errorf("set search_path failed: %w", err)
		}
		return tx.AutoMigrate(models...)
	})
}

// RawDB 逃生口：允许高级用户直接获取 *gorm.DB（不推荐常规使用）。
func (d *GormDriver) RawDB() *gorm.DB { return d.db }

// ── 内部：构建 GORM 配置 ────────────────────────────────────────────

type logWriter struct {
	log contracts.Log
}

func (w *logWriter) Printf(format string, args ...interface{}) {
	w.log.Infof(format, args...)
}

func buildGormConfig(cfg contracts.ConnectionConfig, log contracts.Log) *gorm.Config {
	var level gormLogger.LogLevel
	switch cfg.LogLevel {
	case "error":
		level = gormLogger.Error
	case "warn":
		level = gormLogger.Warn
	case "info":
		level = gormLogger.Info
	case "silent":
		level = gormLogger.Silent
	default:
		level = gormLogger.Info
	}

	customLogger := gormLogger.New(
		&logWriter{log: log},
		gormLogger.Config{
			SlowThreshold:             time.Duration(cfg.SlowThreshold) * time.Millisecond,
			LogLevel:                  level,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	return &gorm.Config{
		Logger: customLogger,
		NowFunc: func() time.Time {
			return time.Now().Local()
		},
		DisableForeignKeyConstraintWhenMigrating: true,
		PrepareStmt:                              true,
	}
}
