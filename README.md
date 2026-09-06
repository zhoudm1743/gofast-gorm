# gofast-gorm

GoFast 框架的 [GORM](https://gorm.io) 数据库驱动插件。

框架核心 `go-fast-framework` 不再内置 ORM 驱动，需显式安装并注册本插件。

## 安装

```bash
go get github.com/zhoudm1743/gofast-gorm@v0.8.2
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

## 依赖

- `github.com/zhoudm1743/go-fast-framework` >= v0.8.2
- `gorm.io/gorm` 及对应方言驱动

## License

Apache-2.0
