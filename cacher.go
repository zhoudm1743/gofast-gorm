package gormdriver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/go-gorm/caches/v4"
	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// 查询缓存相关常量。
const (
	// queryCacheTag 所有查询缓存共用的失效标签。
	// 写操作（Create/Update/Delete/Save）触发 Invalidate 时按此标签整体清除，
	// 不干扰其他业务缓存（框架 Tags 机制仅删除关联 key）。
	queryCacheTag = "gorm:query:cache"
)

// EnableCaches 为连接启用查询缓存插件（go-gorm/caches）。
// 底层使用框架 Cache 服务存储，只有显式调用 Query().Cache() 的查询才会读写缓存。
// 写操作（Create/Update/Delete/Save）会自动失效全部查询缓存。
// 重复调用安全（仅首次注册插件）。
func (d *GormDriver) EnableCaches(cache contracts.Cache) error {
	if cache == nil {
		return fmt.Errorf("[GoFast] 启用查询缓存失败: cache 服务为空")
	}
	if d.cachesEnabled {
		return nil
	}
	if err := d.db.Use(&caches.Caches{Conf: &caches.Config{
		Cacher: newGormCacher(cache),
	}}); err != nil {
		return fmt.Errorf("[GoFast] 启用 gorm 查询缓存失败: %w", err)
	}
	d.cachesEnabled = true
	return nil
}

// cacheCtxKey 缓存配置写入 context 的键。
type cacheCtxKey struct{}

// cacheConfigFromCtx 从 context 提取查询缓存配置。
// 未通过 Query().Cache() 开启缓存的查询返回 ok=false。
func cacheConfigFromCtx(ctx context.Context) (contracts.CacheConfig, bool) {
	if ctx == nil {
		return contracts.CacheConfig{}, false
	}
	cfg, ok := ctx.Value(cacheCtxKey{}).(contracts.CacheConfig)
	return cfg, ok
}

// gormCacher 实现 caches.Cacher，底层使用框架的 Cache 服务存储查询结果。
// 通过 context 标记实现"按需缓存"：只有显式调用 Query().Cache() 的查询
// 才会读写缓存，其余查询零副作用。
type gormCacher struct {
	cache contracts.Cache

	mu     sync.Mutex
	stores map[string]struct{} // 已使用的缓存存储名（"" 表示框架默认存储）
}

func newGormCacher(cache contracts.Cache) *gormCacher {
	return &gormCacher{cache: cache, stores: make(map[string]struct{})}
}

// Get 命中缓存则反序列化到 q 并返回；未开启缓存或未命中返回 nil。
func (c *gormCacher) Get(ctx context.Context, key string, q *caches.Query[any]) (*caches.Query[any], error) {
	cfg, ok := cacheConfigFromCtx(ctx)
	if !ok {
		return nil, nil
	}
	key = cacheKey(key, q.Dest)
	data := c.cache.Store(cfg.Store).Get(key)
	if data == nil {
		return nil, nil
	}
	bytes, ok := toBytes(data)
	if !ok {
		return nil, nil
	}
	if err := q.Unmarshal(bytes); err != nil {
		return nil, fmt.Errorf("[GoFast] 查询缓存反序列化失败: %w", err)
	}
	return q, nil
}

// Store 将查询结果序列化后写入缓存；未开启缓存则跳过。
func (c *gormCacher) Store(ctx context.Context, key string, val *caches.Query[any]) error {
	cfg, ok := cacheConfigFromCtx(ctx)
	if !ok {
		return nil
	}
	bytes, err := val.Marshal()
	if err != nil {
		return fmt.Errorf("[GoFast] 查询缓存序列化失败: %w", err)
	}
	c.recordStore(cfg.Store)
	tags := append([]string{queryCacheTag}, cfg.Tags...)
	return c.cache.Store(cfg.Store).Tags(tags...).Put(cacheKey(key, val.Dest), bytes, cfg.TTL)
}

// Invalidate 清除所有已使用的缓存存储上的查询缓存。
// 由 go-gorm/caches 插件在 INSERT/UPDATE/DELETE 时自动调用。
func (c *gormCacher) Invalidate(ctx context.Context) error {
	c.mu.Lock()
	storeNames := make([]string, 0, len(c.stores))
	for name := range c.stores {
		storeNames = append(storeNames, name)
	}
	c.mu.Unlock()

	var errs []error
	for _, name := range storeNames {
		if err := c.cache.Store(name).Tags(queryCacheTag).Flush(); err != nil {
			errs = append(errs, fmt.Errorf("缓存存储 %q 失效失败: %w", name, err))
		}
	}
	return errors.Join(errs...)
}

func (c *gormCacher) recordStore(name string) {
	c.mu.Lock()
	c.stores[name] = struct{}{}
	c.mu.Unlock()
}

// cacheKey 将插件生成的标识符与 dest 类型绑定，生成最终缓存 key。
// 原因：go-gorm/caches 的 key 仅由 SQL + 参数组成，同一 SQL 若被不同 dest 类型复用，
// 命中时会把缓存的 JSON 反序列化进当前 dest 类型（json 复用指针类型），导致返回错误数据。
// 追加 dest 类型后，不同结构的查询天然隔离，且不影响按 tag 的失效逻辑。
func cacheKey(key string, dest any) string {
	if dest == nil {
		return key
	}
	return key + "#t=" + reflect.TypeOf(dest).String()
}

// toBytes 将缓存值还原为字节切片。
// 框架 memory/redis store 对 []byte 均原样返回（redis store 经 "b:" 前缀实现字节保真，
// 避免 json.Marshal 将 []byte 编码为 base64 导致双重编码）。
func toBytes(v any) ([]byte, bool) {
	switch b := v.(type) {
	case []byte:
		return b, true
	case string:
		return []byte(b), true
	default:
		return nil, false
	}
}
