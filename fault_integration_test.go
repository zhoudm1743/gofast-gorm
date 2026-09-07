//go:build integration

package gormdriver

// fault_integration_test.go：L4 并发/故障注入集成测试（方案 §5.15/§5.16、§六 第 10/12 项）。
// 依赖真实数据库，默认不运行。
//
// 运行方式：
//
//	GOFAST_TEST_PG_DSN="postgres://gofast:gofast123@127.0.0.1:5432/gofast_test?sslmode=disable" \
//	GOFAST_TEST_MYSQL_DSN="gofast:gofast123@tcp(127.0.0.1:3306)/gofast_test" \
//	  go test -tags integration -count=1 -run 'TestFault' -v .
//
// 用例矩阵（PG/MySQL 双跑，共享 runFaultMatrix）：
//
//	CON-01 并发唯一键竞态：10 goroutine 抢插同一唯一键 → 恰 1 成功、9 个 ErrDuplicatedKey
//	CON-02 并发乐观锁：同一 version 行 5 方并发 Save → 恰 1 成功、其余回落插入撞主键 ErrDuplicatedKey
//	LK-05  死锁映射：两事务交叉锁行（PG deadlock_timeout=200ms / MySQL InnoDB 自动检测）→ 一方 ErrDeadlock
//	ERR-05 查询超时：PG statement_timeout / MySQL max_execution_time → ErrQueryTimeout
//	CON-03 隔离级别可见性：REPEATABLE READ 快照固定 vs READ COMMITTED 读新值（默认隔离差异固化）
//	CON-04 锁等待秩序冒烟：三事务顺序等同一 FOR UPDATE 行锁，串行自增终值=3 且无死锁
//	CON-05 连接池压力：MaxOpenConns=2 驱动并发 20 查询全部成功（游标无泄漏）
//	ERR    哨兵对照（PG/MySQL 侧）：与 errors_matrix_test.go 的 SQLite 侧合成三方言错误矩阵
//	ERR-06 断连映射（manual 门控）：不可达端口 NewGormDriver → ErrConnFailed
//
// 防悬挂约定：并发/锁用例全部走 select + time.After(30s) 通道等待
// （goroutine 内不 t.Fatalf，错误经通道回传主 goroutine）。

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhoudm1743/go-fast-framework/contracts"
)

// ── 测试模型（前缀 gflt_，全部显式 TableName）──────────────────────────

// gfltFaultRaceRow CON-01 并发唯一键竞态模型（固定主键即唯一键）。
type gfltFaultRaceRow struct {
	ID   string `orm:"pk varchar(32) 'id'"`
	Code string `orm:"varchar(64) 'code' notnull"`
}

func (gfltFaultRaceRow) TableName() string { return "gflt_fault_race_rows" }

// gfltFaultVerRow CON-02 并发乐观锁模型（orm:"version" 走 saveWithLock 仿真）。
type gfltFaultVerRow struct {
	ID      string `orm:"pk varchar(32) 'id'"`
	Name    string `orm:"varchar(64) 'name'"`
	Version int64  `orm:"version 'version'"`
}

func (gfltFaultVerRow) TableName() string { return "gflt_fault_ver_rows" }

// gfltFaultDLRow LK-05 死锁映射模型。
type gfltFaultDLRow struct {
	ID   string `orm:"pk varchar(16) 'id'"`
	Name string `orm:"varchar(32) 'name'"`
}

func (gfltFaultDLRow) TableName() string { return "gflt_fault_dl_rows" }

// gfltFaultIsoRow CON-03 隔离级别可见性模型。
type gfltFaultIsoRow struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Val int64  `orm:"'val' default(0)"`
}

func (gfltFaultIsoRow) TableName() string { return "gflt_fault_iso_rows" }

// gfltFaultLockRow CON-04 锁等待秩序模型。
type gfltFaultLockRow struct {
	ID  string `orm:"pk varchar(16) 'id'"`
	Cnt int64  `orm:"'cnt' default(0)"`
}

func (gfltFaultLockRow) TableName() string { return "gflt_fault_lock_rows" }

// gfltFaultMatrixRow ERR 哨兵对照（PG/MySQL 侧）模型（与 SQLite 侧 gfltMatrixRow 分表）。
type gfltFaultMatrixRow struct {
	ID   string `orm:"pk varchar(32) 'id'"`
	Name string `orm:"varchar(64) 'name'"`
}

func (gfltFaultMatrixRow) TableName() string { return "gflt_fault_matrix_rows" }

// gfltFaultTimeoutProbe ERR-05（MySQL）重型 SELECT 探针表模型（即建即清）。
type gfltFaultTimeoutProbe struct {
	ID  int64  `orm:"pk bigint 'id'"`
	Pad string `orm:"varchar(8) 'pad'"`
}

func (gfltFaultTimeoutProbe) TableName() string { return "gflt_fault_timeout_probe" }

// ── 防悬挂辅助（模板：gofast-xorm/fullcov_ext_integration_test.go fc5WaitCh）──

// gfltWaitErrCh 带超时的错误通道接收（goroutine 内不 t.Fatalf，错误经通道回传；
// select + time.After(30s) 防悬挂）。
func gfltWaitErrCh(t *testing.T, ch <-chan error, name string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(30 * time.Second):
		t.Fatalf("等待 %s 超时（30s 防悬挂）", name)
		return nil
	}
}

// ── 入口 ───────────────────────────────────────────────────────────────

// TestFault_PG L4 并发/故障注入矩阵（PostgreSQL 侧）。
func TestFault_PG(t *testing.T) {
	drv := newIntgPG(t)
	runFaultMatrix(t, drv, "postgres")
}

// TestFault_MySQL L4 并发/故障注入矩阵（MySQL 侧）。
func TestFault_MySQL(t *testing.T) {
	drv := newIntgMySQL(t)
	runFaultMatrix(t, drv, "mysql")
}

// runFaultMatrix PG/MySQL 共享用例矩阵（dialect ∈ "postgres" | "mysql"）。
func runFaultMatrix(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	t.Run("CON-01_并发唯一键竞态", func(t *testing.T) { gfltCON01UniqueRace(t, drv) })
	t.Run("CON-02_并发乐观锁", func(t *testing.T) { gfltCON02OptimisticRace(t, drv) })
	t.Run("LK-05_死锁映射", func(t *testing.T) { gfltCONLK05Deadlock(t, drv, dialect) })
	t.Run("ERR-05_查询超时", func(t *testing.T) { gfltCONERR05QueryTimeout(t, drv, dialect) })
	t.Run("CON-03_隔离级别可见性", func(t *testing.T) { gfltCON03IsolationVisibility(t, drv, dialect) })
	t.Run("CON-04_锁等待秩序冒烟", func(t *testing.T) { gfltCON04LockWaitOrder(t, drv) })
	t.Run("CON-05_连接池压力", func(t *testing.T) { gfltCON05PoolPressure(t, dialect) })
	t.Run("ERR_哨兵对照_PG_MySQL侧", func(t *testing.T) { gfltFAULTERRMatrixDialect(t, drv) })
}

// ── CON-01 并发唯一键竞态 ──────────────────────────────────────────────

// gfltCON01UniqueRace 10 goroutine 抢插同一唯一键（并发 sync.WaitGroup）：
// 恰 1 方成功、其余 9 方 ErrDuplicatedKey、表内行数=1。
func gfltCON01UniqueRace(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gflt_fault_race_rows"}, &gfltFaultRaceRow{})

	const workers = 10
	results := make(chan contracts.Result, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- drv.Query().CreateResult(&gfltFaultRaceRow{ID: "race-key", Code: "same"})
		}()
	}
	wg.Wait()
	close(results)

	ok, dup := 0, 0
	for res := range results {
		switch {
		case res.Error == nil && res.RowsAffected == 1:
			ok++
		case errors.Is(res.Error, contracts.ErrDuplicatedKey):
			dup++
		default:
			t.Errorf("并发抢插出现未预期结果: rows=%d err=%v", res.RowsAffected, res.Error)
		}
	}
	if ok != 1 || dup != workers-1 {
		t.Errorf("并发抢插唯一键应恰 1 成功、%d 个 ErrDuplicatedKey, 实际 ok=%d dup=%d", workers-1, ok, dup)
	}

	var count int64
	if err := drv.Query().Model(&gfltFaultRaceRow{}).Count(&count); err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 1 {
		t.Errorf("并发抢插后行数应为 1, 实际 %d", count)
	}
}

// ── CON-02 并发乐观锁 ──────────────────────────────────────────────────

// gfltCON02OptimisticRace 同一 version 行 5 方并发 Save（均基于 version=1 陈旧快照）。
// 仿真语义（optimistic_lock.go saveWithLock 更新分支）：UPDATE ... WHERE version=旧值
// + SET version=version+1，恰 1 方命中；其余 RowsAffected=0 回落 INSERT——同主键
// 回落插入撞主键唯一约束 → ErrDuplicatedKey。终态 version=2（仅一次自增）。
func gfltCON02OptimisticRace(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gflt_fault_ver_rows"}, &gfltFaultVerRow{})
	if err := drv.Query().Create(&gfltFaultVerRow{ID: "ver-1", Name: "init"}); err != nil {
		t.Fatalf("Create 初值: %v", err)
	}

	const workers = 5
	results := make(chan contracts.Result, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			row := &gfltFaultVerRow{ID: "ver-1", Name: fmt.Sprintf("writer-%d", n), Version: 1}
			results <- drv.Query().SaveResult(row)
		}(i)
	}
	wg.Wait()
	close(results)

	ok, lost := 0, 0
	for res := range results {
		switch {
		case res.Error == nil && res.RowsAffected == 1:
			ok++
		case errors.Is(res.Error, contracts.ErrDuplicatedKey):
			lost++
		default:
			t.Errorf("并发 Save 出现未预期结果: rows=%d err=%v", res.RowsAffected, res.Error)
		}
	}
	if ok != 1 || lost != workers-1 {
		t.Errorf("并发乐观锁应恰 1 方成功、%d 方 ErrDuplicatedKey, 实际 ok=%d lost=%d", workers-1, ok, lost)
	}

	var final gfltFaultVerRow
	if err := drv.Query().First(&final, "id = ?", "ver-1"); err != nil {
		t.Fatalf("读终态: %v", err)
	}
	if final.Version != 2 {
		t.Errorf("终态 version 应为 2（1→2 仅一次自增）, 实际 %d", final.Version)
	}
	if !strings.HasPrefix(final.Name, "writer-") {
		t.Errorf("获胜方内容异常: %q", final.Name)
	}
}

// ── LK-05 死锁映射 ─────────────────────────────────────────────────────

// gfltCONLK05Deadlock 两事务交叉锁行（T1 锁行1 等行2、T2 锁行2 等行1）：
//   - PG：事务内 SET LOCAL deadlock_timeout='200ms'（SET LOCAL 事务级，不污染连接池），
//     ~200ms 内检测到死锁；受害方 ErrDeadlock（"deadlock detected"）。
//   - MySQL：InnoDB 默认自动死锁检测（innodb_deadlock_detect=ON），环形成立即报 1213。
//
// 受害方不确定（引擎自选），断言恰一方 ErrDeadlock、幸存方阻塞解除后成功。
func gfltCONLK05Deadlock(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	intgMigrate(t, drv, []string{"gflt_fault_dl_rows"}, &gfltFaultDLRow{})
	if err := drv.Query().Create(&gfltFaultDLRow{ID: "dl-1", Name: "a"}); err != nil {
		t.Fatalf("Create dl-1: %v", err)
	}
	if err := drv.Query().Create(&gfltFaultDLRow{ID: "dl-2", Name: "b"}); err != nil {
		t.Fatalf("Create dl-2: %v", err)
	}

	t1 := drv.Query().Begin()
	t2 := drv.Query().Begin()
	cleanup := func() {
		_ = t1.Rollback() // 受害方事务已被引擎中止，Rollback 可能报 ErrTxDone，忽略
		_ = t2.Rollback()
	}

	if dialect == "postgres" {
		// PG：调低死锁检测阈值（事务级参数），防悬挂
		if err := t1.Exec("SET LOCAL deadlock_timeout = '200ms'"); err != nil {
			cleanup()
			t.Fatalf("T1 SET LOCAL deadlock_timeout: %v", err)
		}
		if err := t2.Exec("SET LOCAL deadlock_timeout = '200ms'"); err != nil {
			cleanup()
			t.Fatalf("T2 SET LOCAL deadlock_timeout: %v", err)
		}
	}

	// 阶段一：各自持有不同行锁
	if err := t1.Exec("UPDATE gflt_fault_dl_rows SET name = 't1' WHERE id = ?", "dl-1"); err != nil {
		cleanup()
		t.Fatalf("T1 锁行1: %v", err)
	}
	if err := t2.Exec("UPDATE gflt_fault_dl_rows SET name = 't2' WHERE id = ?", "dl-2"); err != nil {
		cleanup()
		t.Fatalf("T2 锁行2: %v", err)
	}

	// 阶段二：T2 先反向等行1（阻塞），再让 T1 反向等行2 → 环形等待
	errT2 := make(chan error, 1)
	go func() { errT2 <- t2.Exec("UPDATE gflt_fault_dl_rows SET name = 't2x' WHERE id = ?", "dl-1") }()
	time.Sleep(400 * time.Millisecond) // 确保 T2 已阻塞在行1上（无 channel 可观测锁等待，宽松兜底）
	errT1 := make(chan error, 1)
	go func() { errT1 <- t1.Exec("UPDATE gflt_fault_dl_rows SET name = 't1x' WHERE id = ?", "dl-2") }()

	e2 := gfltWaitErrCh(t, errT2, "T2 反向更新（死锁解除）")
	e1 := gfltWaitErrCh(t, errT1, "T1 反向更新（死锁解除）")
	cleanup()

	// 受害方由引擎自选（nondeterministic），恰一方 ErrDeadlock；幸存方在对方
	// 回滚释放行锁后解除阻塞，应成功完成
	dl1, dl2 := errors.Is(e1, contracts.ErrDeadlock), errors.Is(e2, contracts.ErrDeadlock)
	if dl1 == dl2 {
		t.Fatalf("两事务应恰一方 ErrDeadlock, 实际 T1 err=%v (deadlock=%v) / T2 err=%v (deadlock=%v)", e1, dl1, e2, dl2)
	}
	if dl1 && e2 != nil {
		t.Errorf("幸存方 T2 反向更新应成功（受害方回滚释放行锁）, 实际: %v", e2)
	}
	if dl2 && e1 != nil {
		t.Errorf("幸存方 T1 反向更新应成功（受害方回滚释放行锁）, 实际: %v", e1)
	}
}

// ── ERR-05 查询超时 ────────────────────────────────────────────────────

// gfltCONERR05QueryTimeout 服务器端查询超时映射 ErrQueryTimeout：
//   - PG：事务内 SET LOCAL statement_timeout=200 + pg_sleep(0.5) → 57014
//     "canceling statement due to statement timeout"；
//   - MySQL：SET SESSION max_execution_time=200 + 重型 SELECT 笛卡尔积 → 3024
//     "maximum statement execution time exceeded"（注意只对 SELECT 生效；
//     SELECT SLEEP() 实测不被 max_execution_time 中断，属已知文档化限制，
//     故用真实表三表连接强制执行时长超过阈值）。
//
// 均在事务内执行（database/sql 事务绑定单连接 → SET 与慢查询同连接生效；
// SET LOCAL 事务结束自动还原；MySQL 会话参数在返回错误前尽力还原，不污染回池连接）。
func gfltCONERR05QueryTimeout(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	if dialect == "postgres" {
		err := drv.Query().Transaction(func(tx contracts.Query) error {
			if err := tx.Exec("SET LOCAL statement_timeout = 200"); err != nil {
				return err
			}
			// pg_sleep 返回 void，走 Rows() 游标承接（超时错误在查询执行期即返回）
			rows, qerr := tx.Raw("SELECT pg_sleep(0.5)").Rows()
			if qerr == nil {
				_ = rows.Close()
			}
			return qerr
		})
		if !errors.Is(err, contracts.ErrQueryTimeout) {
			t.Fatalf("PG statement_timeout 应映射 ErrQueryTimeout, 实际: %v", err)
		}
		return
	}

	// MySQL：重型 SELECT（2000³ 组合、WHERE 阻断统计短路）执行远超 200ms，
	// 被 max_execution_time=200 以 3024 中断。探针表即建即清，不干扰其他用例。
	intgDropTables(t, drv, "gflt_fault_timeout_probe")
	intgMustExec(t, drv, `CREATE TABLE gflt_fault_timeout_probe (id BIGINT PRIMARY KEY, pad VARCHAR(8))`)
	t.Cleanup(func() { intgDropTables(t, drv, "gflt_fault_timeout_probe") })
	batch := make([]*gfltFaultTimeoutProbe, 0, 2000)
	for i := 1; i <= 2000; i++ {
		batch = append(batch, &gfltFaultTimeoutProbe{ID: int64(i), Pad: "pad"})
	}
	if err := drv.Query().CreateInBatches(batch, 500); err != nil {
		t.Fatalf("填充探针表: %v", err)
	}

	err := drv.Query().Transaction(func(tx contracts.Query) error {
		if err := tx.Exec("SET SESSION max_execution_time = 200"); err != nil {
			return err
		}
		var out int64
		qerr := tx.Raw(`SELECT COUNT(*) FROM gflt_fault_timeout_probe a
			JOIN gflt_fault_timeout_probe b ON b.id > 0
			JOIN gflt_fault_timeout_probe c ON c.id > 0
			WHERE a.pad = 'pad'`).Scan(&out)
		_ = tx.Exec("SET SESSION max_execution_time = 0") // 尽力还原会话参数（语句级错误后事务仍可用）
		return qerr
	})
	if !errors.Is(err, contracts.ErrQueryTimeout) {
		t.Fatalf("MySQL max_execution_time 应映射 ErrQueryTimeout, 实际: %v", err)
	}
}

// ── CON-03 隔离级别可见性 ──────────────────────────────────────────────

// gfltCON03IsolationVisibility 双事务可见性对照：
// 事务 A 写未提交 → 事务 B（REPEATABLE READ）快照固定先后两读一致；
// 事务 C（READ COMMITTED）在 A 提交后读到新值。
//
// 差异固化：PG 默认 READ COMMITTED——事务 B 需显式 TxRepeatableRead 才快照固定；
// MySQL 默认 REPEATABLE READ——事务 B 用默认 Begin() 即快照固定。
func gfltCON03IsolationVisibility(t *testing.T, drv *GormDriver, dialect string) {
	t.Helper()
	intgMigrate(t, drv, []string{"gflt_fault_iso_rows"}, &gfltFaultIsoRow{})
	if err := drv.Query().Create(&gfltFaultIsoRow{ID: "iso-1", Val: 0}); err != nil {
		t.Fatalf("Create 初值: %v", err)
	}

	// 事务 A：写未提交
	txA := drv.Query().Begin()
	if err := txA.Exec("UPDATE gflt_fault_iso_rows SET val = 100 WHERE id = ?", "iso-1"); err != nil {
		_ = txA.Rollback()
		t.Fatalf("A 更新: %v", err)
	}

	// 事务 B：快照固定（方言差异见函数注释）
	var txB contracts.Query
	if dialect == "postgres" {
		txB = drv.Query().Begin(contracts.TxRepeatableRead)
	} else {
		txB = drv.Query().Begin() // MySQL 默认 REPEATABLE READ（差异固化）
	}
	readVal := func(q contracts.Query) int64 {
		var v int64
		if err := q.Raw("SELECT val FROM gflt_fault_iso_rows WHERE id = ?", "iso-1").Scan(&v); err != nil {
			t.Fatalf("读 val: %v", err)
		}
		return v
	}
	if got := readVal(txB); got != 0 {
		_ = txA.Rollback()
		_ = txB.Rollback()
		t.Errorf("RR 首读应见未提交前快照 0, 实际 %d", got)
	}
	if err := txA.Commit(); err != nil {
		_ = txB.Rollback()
		t.Fatalf("A 提交: %v", err)
	}
	if got := readVal(txB); got != 0 {
		_ = txB.Rollback()
		t.Errorf("RR 二读快照应不变（仍 0，A 已提交 100）, 实际 %d", got)
	}
	_ = txB.Rollback()

	// 对照组 C：READ COMMITTED 读到 A 的已提交新值（两方言一致）
	txC := drv.Query().Begin(contracts.TxReadCommitted)
	if got := readVal(txC); got != 100 {
		_ = txC.Rollback()
		t.Errorf("READ COMMITTED 应读到已提交新值 100, 实际 %d", got)
	}
	_ = txC.Rollback()
}

// ── CON-04 锁等待秩序冒烟 ──────────────────────────────────────────────

// gfltCON04LockWaitOrder 三事务顺序等同一 FOR UPDATE 行锁：T1 持锁 → T2/T3 先后
// 排队阻塞 → T1 提交后锁按序传递，各事务以相对表达式（cnt = cnt + 1）串行自增。
// 宽松断言：T2/T3 均无死锁成功、终值 = 3（不校验严格获锁次序）。
func gfltCON04LockWaitOrder(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gflt_fault_lock_rows"}, &gfltFaultLockRow{})
	if err := drv.Query().Create(&gfltFaultLockRow{ID: "lock-1", Cnt: 0}); err != nil {
		t.Fatalf("Create 初值: %v", err)
	}

	// T1 先获锁并持锁
	t1 := drv.Query().Begin()
	var held gfltFaultLockRow
	if err := t1.Model(&gfltFaultLockRow{}).Where("id = ?", "lock-1").Lock(contracts.LockForUpdate).First(&held); err != nil {
		_ = t1.Rollback()
		t.Fatalf("T1 FOR UPDATE: %v", err)
	}

	// 排队者：FOR UPDATE 阻塞等锁 → 获锁后相对自增并提交
	acquire := func(tag string, done chan<- error) {
		tx := drv.Query().Begin()
		var r gfltFaultLockRow
		if err := tx.Model(&gfltFaultLockRow{}).Where("id = ?", "lock-1").Lock(contracts.LockForUpdate).First(&r); err != nil {
			_ = tx.Rollback()
			done <- fmt.Errorf("%s FOR UPDATE: %w", tag, err)
			return
		}
		if err := tx.Exec("UPDATE gflt_fault_lock_rows SET cnt = cnt + 1 WHERE id = ?", "lock-1"); err != nil {
			_ = tx.Rollback()
			done <- fmt.Errorf("%s 自增: %w", tag, err)
			return
		}
		if err := tx.Commit(); err != nil {
			done <- fmt.Errorf("%s 提交: %w", tag, err)
			return
		}
		done <- nil
	}

	t2Done := make(chan error, 1)
	t3Done := make(chan error, 1)
	go acquire("T2", t2Done)
	time.Sleep(300 * time.Millisecond) // T2 先进入锁等待队列
	go acquire("T3", t3Done)
	time.Sleep(300 * time.Millisecond) // T3 随后排队

	// T1 持锁更新后提交 → 锁按 T2、T3 顺序传递
	if err := t1.Exec("UPDATE gflt_fault_lock_rows SET cnt = cnt + 1 WHERE id = ?", "lock-1"); err != nil {
		_ = t1.Rollback()
		t.Fatalf("T1 自增: %v", err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatalf("T1 提交: %v", err)
	}

	if err := gfltWaitErrCh(t, t2Done, "T2 结果"); err != nil {
		t.Errorf("T2 应无死锁成功, 实际: %v", err)
	}
	if err := gfltWaitErrCh(t, t3Done, "T3 结果"); err != nil {
		t.Errorf("T3 应无死锁成功, 实际: %v", err)
	}
	var final gfltFaultLockRow
	if err := drv.Query().First(&final, "id = ?", "lock-1"); err != nil {
		t.Fatalf("读终值: %v", err)
	}
	if final.Cnt != 3 {
		t.Errorf("三事务串行自增终值应为 3, 实际 %d", final.Cnt)
	}
}

// ── CON-05 连接池压力 ──────────────────────────────────────────────────

// gfltCON05PoolPressure MaxOpenConns=2 的小连接池驱动并发 20 查询全部成功。
// 驱动经 NewGormDriver 配置链路即可构造小连接池（driver.go 尊重 cfg.MaxOpenConns），
// 无需 database/sql 直连兜底；游标 Close 无泄漏以"池不耗尽、全部返回"间接固化——
// 若游标泄漏占用连接，池容量 2 下后续查询必然阻塞（由 15s ctx 截止兜底报错）。
func gfltCON05PoolPressure(t *testing.T, dialect string) {
	t.Helper()
	dsnEnv := "GOFAST_TEST_PG_DSN"
	if dialect == "mysql" {
		dsnEnv = "GOFAST_TEST_MYSQL_DSN"
	}
	small, err := NewGormDriver(contracts.ConnectionConfig{
		Driver:       "gormdriver",
		Engine:       dialect,
		DSN:          os.Getenv(dsnEnv),
		MaxOpenConns: 2,
		MaxIdleConns: 2,
		LogLevel:     "silent",
	}, intgNopLog{t: t})
	if err != nil {
		t.Fatalf("创建 MaxOpenConns=2 驱动: %v", err)
	}
	t.Cleanup(func() { _ = small.Close() })

	const workers = 20
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var one int64
			if err := small.Query(ctx).Raw("SELECT 1").Scan(&one); err != nil {
				errCh <- fmt.Errorf("worker-%d: %w", n, err)
				return
			}
			errCh <- nil
		}(i)
	}
	wg.Wait()
	close(errCh)

	failed := 0
	for err := range errCh {
		if err != nil {
			failed++
			t.Errorf("小连接池并发查询失败: %v", err)
		}
	}
	if failed > 0 {
		t.Errorf("MaxOpenConns=2 下 20 并发查询应全部成功, 失败 %d 个", failed)
	}
}

// ── ERR 哨兵对照（PG/MySQL 侧，与 errors_matrix_test.go 合成三方言矩阵）──

// gfltFAULTERRMatrixDialect 当前方言下重复键与未命中两哨兵的映射对照：
//
//	哨兵               | SQLite                      | MySQL                  | PostgreSQL
//	ErrRecordNotFound  | First 未命中（sql.ErrNoRows）| 同左                    | 同左
//	ErrDuplicatedKey   | UNIQUE constraint failed     | Error 1062 Duplicate   | duplicate key violates
func gfltFAULTERRMatrixDialect(t *testing.T, drv *GormDriver) {
	t.Helper()
	intgMigrate(t, drv, []string{"gflt_fault_matrix_rows"}, &gfltFaultMatrixRow{})
	q := drv.Query()

	cases := []struct {
		name string
		want error
		run  func() error
	}{
		{
			name: "First 未命中 → ErrRecordNotFound",
			want: contracts.ErrRecordNotFound,
			run: func() error {
				var row gfltFaultMatrixRow
				return q.Where("id = ?", "no-such-row").First(&row)
			},
		},
		{
			name: "重复主键 → ErrDuplicatedKey",
			want: contracts.ErrDuplicatedKey,
			run: func() error {
				if err := q.Create(&gfltFaultMatrixRow{ID: "dup-1", Name: "first"}); err != nil {
					return fmt.Errorf("首次插入不应失败: %w", err)
				}
				return q.Create(&gfltFaultMatrixRow{ID: "dup-1", Name: "second"})
			},
		},
	}
	for _, tc := range cases {
		if err := tc.run(); !errors.Is(err, tc.want) {
			t.Errorf("%s: 期望 %v, 实际: %v", tc.name, tc.want, err)
		}
	}
}

// ── ERR-06 断连失败映射（manual 门控，本文件内）────────────────────────

// TestFault_ManualConnFailure ERR-06 连接失败映射。默认跳过；设
// GOFAST_TEST_MANUAL_CONNFAIL=1 启用。不需要真的停容器——用不可达端口
// （127.0.0.1:1，环回上必无监听，立即 connection refused）人为制造连接失败：
// ① 正常 DSN 驱动 Ping 成功（连通基线）；② 同引擎不可达端口 DSN
// NewGormDriver 必败且错误映射 contracts.ErrConnFailed。
func TestFault_ManualConnFailure(t *testing.T) {
	if os.Getenv("GOFAST_TEST_MANUAL_CONNFAIL") == "" {
		t.Skip("manual：停容器注入断连，设 GOFAST_TEST_MANUAL_CONNFAIL=1 启用")
	}

	type engineCase struct {
		engine string
		dsnEnv string
		badDSN string
	}
	candidates := []engineCase{
		{"postgres", "GOFAST_TEST_PG_DSN", "postgres://gofast:gofast123@127.0.0.1:1/gofast_test?sslmode=disable&connect_timeout=2"},
		{"mysql", "GOFAST_TEST_MYSQL_DSN", "gofast:gofast123@tcp(127.0.0.1:1)/gofast_test?timeout=2s"},
	}
	var picked *engineCase
	for i := range candidates {
		if os.Getenv(candidates[i].dsnEnv) != "" {
			picked = &candidates[i]
			break
		}
	}
	if picked == nil {
		t.Skip("未设置 GOFAST_TEST_PG_DSN / GOFAST_TEST_MYSQL_DSN，跳过")
	}

	// ① 连通基线：正常 DSN 驱动 Ping 成功
	base, err := NewGormDriver(contracts.ConnectionConfig{
		Driver: "gormdriver", Engine: picked.engine, DSN: os.Getenv(picked.dsnEnv), LogLevel: "silent",
	}, intgNopLog{t: t})
	if err != nil {
		t.Fatalf("基线驱动创建失败: %v", err)
	}
	defer base.Close()
	if err := base.Ping(); err != nil {
		t.Fatalf("基线 Ping 应成功: %v", err)
	}

	// ② 不可达端口 → NewGormDriver 必败 → ErrConnFailed（30s 兜底防悬挂）
	errCh := make(chan error, 1)
	go func() {
		_, cerr := NewGormDriver(contracts.ConnectionConfig{
			Driver: "gormdriver", Engine: picked.engine, DSN: picked.badDSN, LogLevel: "silent",
		}, intgNopLog{t: t})
		errCh <- cerr
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("不可达端口 NewGormDriver 应失败, 实际成功")
		}
		if !errors.Is(err, contracts.ErrConnFailed) {
			t.Fatalf("连接失败应映射 ErrConnFailed, 实际: %v", err)
		}
		t.Logf("连接失败按 ErrConnFailed 映射: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("不可达端口连接 30s 未返回（防悬挂触发）")
	}
}
