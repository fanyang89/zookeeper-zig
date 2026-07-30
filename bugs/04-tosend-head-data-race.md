# BUG-04: `zookeeper_interest` 无锁读取 `to_send.head`，与 `queue_buffer` 竞争

## 概要

IO 线程在 `zookeeper_interest` 中直接读取发送队列头 `zh->to_send.head`
判断是否有待发数据，但不持有缓冲区链表锁。与此同时，用户线程通过
`zoo_acreate` 等异步 API 调用 `queue_buffer` 在锁保护下修改同一链表。
两个线程对 `list->last->next`（即 `to_send` 链表节点）的并发访问构成
数据竞争。

## 复现

```sh
mise run e2e-tsan
```

TSan 报告：

```
WARNING: ThreadSanitizer: data race
  Write of size 8 by main thread (mutexes: write M0, write M1):
    #0 queue_buffer        zookeeper.c:1740   list->last->next = b
    #1 queue_buffer_bytes  zookeeper.c:1751
    #2 zoo_acreate_ttl     zookeeper.c:4257
    #3 zoo_acreate         zookeeper.c:4207
    #4 main                tests/e2e.c:242

  Previous read of size 8 by thread T1 (IO 线程):
    #0 zookeeper_interest  zookeeper.c:2707   zh->to_send.head && ...
    #1 do_io               mt_adaptor.c:381
```

## 根因分析

### 写入端：用户线程（zookeeper.c:1726–1745）

```c
static void queue_buffer(buffer_head_t *list, buffer_list_t *b, int add_to_front)
{
    b->next = 0;
    lock_buffer_list(list);          // ← 获取锁
    if (list->head) {
        assert(list->last);
        if (add_to_front) {
            b->next = list->head;
            list->head = b;
        } else {
            list->last->next = b;    // ← 行 1740，TSan 报告的写入点
            list->last = b;
        }
    } else {
        list->head = b;
        list->last = b;
    }
    unlock_buffer_list(list);
}
```

用户线程调用 `zoo_acreate` → `queue_buffer_bytes` → `queue_buffer`，
在锁保护下将新的请求缓冲区追加到 `zh->to_send` 链表。

### 读取端：IO 线程（zookeeper.c:2704–2712）

```c
*interest = ZOOKEEPER_READ;
if ((zh->to_send.head && (is_connected(zh) || is_sasl_auth_in_progress(zh)))
    || zh->state == ZOO_CONNECTING_STATE
    || zh->state == ZOO_SSL_CONNECTING_STATE) {
    *interest |= ZOOKEEPER_WRITE;    // 有数据要发，关注可写事件
}
```

IO 线程在 `do_io` 循环中调用 `zookeeper_interest`，直接读取
`zh->to_send.head` **判断是否有待发数据**。此处**不获取**
`to_send` 的缓冲区链表锁。

### 竞争窗口

```
T1 (IO):   zookeeper_interest → 读 zh->to_send.head   ← 无锁
Main:      zoo_acreate → queue_buffer → 写 list->last->next   ← 有锁
```

虽然 `queue_buffer` 持有锁，但 `zookeeper_interest` 读取链表时不持有同一
锁，因此构成竞争。TSan 报告 `zookeeper_interest` 的 `to_send.head` 读取
与 `queue_buffer` 的 `list->last->next` 写入发生在一块连续的堆内存上
（同属 `zhandle_t` 结构体内部的 `to_send` 链表）。

## 影响范围

- **缺陷位置**：`vendor/zookeeper-client-c/src/zookeeper.c:2707`（无锁读取）
- **触发条件**：threaded 模式下，用户线程提交异步请求（acreate/aset/adelete
  等）的瞬间，IO 线程恰好正在 `zookeeper_interest` 中检查发送队列
- **潜在后果**：
  - IO 线程短暂地看不到刚入队的请求（丢失一次 `select`/`poll` 的可写事件），
    导致请求延迟一个 IO 周期才发出
  - 在弱内存序架构上可能读到链表节点的中间状态，导致空指针解引用

## 建议修复

`zookeeper_interest` 中读取 `to_send.head` 时应先获取缓冲区链表锁：

```c
lock_buffer_list(&zh->to_send);
int has_pending = (zh->to_send.head != NULL);
unlock_buffer_list(&zh->to_send);

if ((has_pending && (is_connected(zh) || is_sasl_auth_in_progress(zh)))
    || ...) {
    *interest |= ZOOKEEPER_WRITE;
}
```

或者将 `to_send.head` 改为原子指针，用 `atomic_load` 读取。
