package gormdriver

import (
	"github.com/zhoudm1743/go-fast-framework/contracts"
	"github.com/zhoudm1743/go-fast-framework/database"
	"github.com/zhoudm1743/go-fast-framework/foundation"
)

// ServiceProvider gorm 驱动接入点（可选驱动，需显式加入应用 providers）。
// 将本 Provider 加入应用 providers 后，配置 driver: "gormdriver" 即可使用：
//
//	app.SetProviders(append(providers, &gormdriver.ServiceProvider{}))
type ServiceProvider struct{}

func (sp *ServiceProvider) Register(app foundation.Application) {
	database.RegisterDriver("gormdriver", func(cfg database.ConnectionConfig, log contracts.Log) (contracts.Driver, error) {
		return NewGormDriver(cfg, log)
	})
}

func (sp *ServiceProvider) Boot(app foundation.Application) error {
	return nil
}
