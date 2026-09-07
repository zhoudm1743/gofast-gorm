# gofast-gorm

GoFast 框架的 [GORM](https://gorm.io) 数据库驱动插件。

框架核心 `go-fast-framework` 不再内置 ORM 驱动，需显式安装并注册本插件。

## 安装

```bash
go get github.com/zhoudm1743/gofast-gorm@latest
```

## 接入

```go
import (
    "github.com/zhoudm1743/go-fast-framework/database"
    "github.com/zhoudm1743/go-fast-framework/foundation"
    gormdriver "github.com/zhoudm1743/gofast-gorm"
)

app.SetProviders([]foundation.ServiceProvider{
    // ...
    &database.ServiceProvider{},
    &gormdriver.ServiceProvider{},
})
```

```yaml
database:
  connections:
    main:
      driver: gormdriver
      engine: mysql   # mysql | postgres | sqlite | mssql
      # ...
```

## 语义差异固化说明

与 gofast-xorm 驱动的双驱动一致性语义（差异 = 有意保留的 gorm 原生行为，断言锁定于测试）：

| 差异点 | gorm 侧行为 | 锁定测试 |
|--------|------------|---------|
| Save 值无变化 affected | MySQL=0 / PG=1（均不报错不误插） | fullcov_write `Save_值无变化` |
| Save 0 行回落 upsert 忽略链上 Where | 回落插入不受显式条件限定（xorm 0 行不落库） | fullcov_write `SaveResult` |
| CreateInBatches 中途失败 | 分批包裹单事务整体回滚（xorm 前块已提交） | fullcov_write `CreateInBatches_中途失败` |
| FirstOrCreate 已填充 dest | 主键内联收窄未命中回落 Create（xorm 按 conds 命中回填） | fullcov_chain `链式不可变与dest条件` |
| Having 带参占位符 | 支持（xorm 链上拒绝 ErrUnsupported） | fullcov_chain `Having过滤` |
| Joins 非法串 | 数据库原始错误（xorm 报 ErrUnsupported） | fullcov_ext `Joins` |
| Preload contracts 回调 | 仅共享引擎路径（gorm:"-"）支持，原生路径报错 | fullcov_ext `PL-02` |
| 软删业务级 deleted_at 列 | gorm 不识别（Delete 为物理删，需业务 Update 软删；OnlyTrashed/Restore 已兼容 int64 与 gorm.DeletedAt 双形态） | fullcov_ext `SoftDelete_*` |
| 事务终态误用 | sql.ErrTxDone → ErrInvalidTransaction（errors.go 映射） | fullcov_rawtx `Begin_重复Commit与终态后误用` |
| 超时/连接失败哨兵 | statement_timeout(57014)/max_execution_time(3024) → ErrQueryTimeout；连接失败 → ErrConnFailed | fault_integration_test |

双驱动联合测试方案与执行报告：`../docs/md/dual-driver-test-plan.md`。

## 依赖

- `github.com/zhoudm1743/go-fast-framework` >= v0.8.2
- `gorm.io/gorm` 及对应方言驱动

## License

Apache-2.0
