# BUG-03: `zh->state` 在 IO 线程与用户线程间无同步访问

## 概要

`zhandle_t.state` 字段被 IO 线程（`do_io`）在 `zookeeper_interest` 中写入，
同时被用户线程通过公共 API `zoo_state()` 无锁读取。`state` 是普通 `int`，
既非 `_Atomic` 也未用互斥锁保护，构成数据竞争。

## 复现

```sh
mise run e2e-tsan
```

TSan 报告：

```
WARNING: ThreadSanitizer: data race
  Write of size 4 by thread T1 (IO 线程):
    #0 zookeeper_interest   zookeeper.c:2596   zh->state = ZOO_CONNECTING_STATE
    #1 do_io                mt_adaptor.c:381

  Previous read of size 4 by main thread:
    #0 zoo_state            zookeeper.c:3533   return zh->state
    #1 wait_for_connection  tests/e2e.c:101
    #2 main                 tests/e2e.c:239
```

## 根因分析

### 写入端：IO 线程（zookeeper.c:2590–2601）

```c
if (errno == EWOULDBLOCK || errno == EINPROGRESS) {
    if (zh->fd->cert != NULL)
        zh->state = ZOO_SSL_CONNECTING_STATE;   // ← T1 写入，无锁
    else
        zh->state = ZOO_CONNECTING_STATE;        // ← T1 写入，无锁
}
```

`zookeeper_interest` 在 IO 线程（`do_io`）中轮询调用，在连接建立过程中
修改 `zh->state`。

### 读取端：用户线程（zookeeper.c:3533–3536）

```c
int zoo_state(zhandle_t *zh)
{
    if (zh != 0)
        return zh->state;   // ← 主线程读取，无锁
    return 0;
}
```

`zoo_state()` 是公共 API，用户线程调用它来轮询连接状态。函数直接返回
`zh->state`，不获取任何锁。测试代码（`tests/e2e.c:101`）在
`wait_for_connection` 循环中反复调用 `zoo_state()`。

### 为什么这是 bug

在 C11 内存模型下，对同一变量的并发读写如果没有同步关系（happens-before），
行为是未定义的。在实践中：

- 在弱内存序架构（ARM、POWER）上可能读到撕裂或过期的状态值
- 编译器可能将 `zh->state` 的读取优化为寄存器缓存，导致用户线程永远
  看不到状态更新（轮询死循环）
- TSan 正确地将此标记为数据竞争

## 影响范围

- **缺陷位置**：`vendor/zookeeper-client-c/src/zookeeper.c:2596`（写）
  和 `zookeeper.c:3533`（读）
- **触发条件**：threaded 模式下，用户线程在连接建立期间调用 `zoo_state()`
- **潜在后果**：
  - 用户线程在 `wait_for_connection` 类循环中看不到状态更新，导致超时
  - 弱内存序架构上读到垃圾值，进入未预期的分支

## 建议修复

将 `zhandle_t.state` 改为 `_Atomic int`（或 `volatile` + 内存屏障），
并在 `zoo_state()` 中使用原子读取：

```c
// zk_adaptor.h
struct _zhandle {
    _Atomic int state;
    ...
};

// zookeeper.c
int zoo_state(zhandle_t *zh) {
    if (zh != 0)
        return atomic_load_explicit(&zh->state, memory_order_acquire);
    return 0;
}
```

IO 线程中的写入也需要改为 `atomic_store_explicit(&zh->state, ..., memory_order_release)`。
