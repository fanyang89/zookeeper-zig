# BUG-02: `ia_deserialize_buffer` 缺少负长度校验，导致巨型分配中止

## 概要

`ia_deserialize_buffer`（`recordio.c:237`）在调用 `malloc` 之前只检查
`b->len == -1`（NULL 哨兵），但不拒绝其他负数值。当 `b->len` 为负且不为 -1
时，`int32_t` 隐式转换为 `size_t` 产生极大值（~2⁶⁴），触发 ASan
`allocation-size-too-big` 中止，或造成资源耗尽。

同文件中的 `ia_deserialize_string` 有正确的 `if (len < 0) return -EINVAL`
校验，说明这是一处遗漏而非设计意图。

## 影响范围

- **缺陷位置**：`vendor/zookeeper-client-c/src/recordio.c:237–259`
  （`ia_deserialize_buffer` 函数）
- **间接影响**：所有 jute 结构体中含有 `buffer` 字段且从网络流反序列化的
  消息类型，包括 `ConnectResponse.passwd`、`SetDataRequest.data`、
  `SetSASLResponse.token`、`GetSASLRequest.token`、`SetSASLRequest.token`
  等。

## 根因分析

### 缺陷代码（recordio.c:237–259）

```c
int ia_deserialize_buffer(struct iarchive *ia, const char *name,
        struct buffer *b)
{
    struct buff_struct *priv = ia->priv;
    int rc = ia_deserialize_int(ia, "len", &b->len);  // ① 读取 len（int32_t）
    if (rc < 0)
        return rc;
    if ((priv->len - priv->off) < b->len) {            // ② 有符号比较——负值逃逸
        return -E2BIG;
    }
    if (b->len == -1) {                                 // ③ 仅检查 -1 哨兵
       b->buff = NULL;
       return rc;
    }
    b->buff = malloc(b->len);                           // ④ 负 len → 极大 size_t
    if (!b->buff) {
        return -ENOMEM;
    }
    memcpy(b->buff, priv->buffer+priv->off, b->len);
    priv->off += b->len;
    return 0;
}
```

逐步分析：

| 步骤 | 问题 |
|------|------|
| ① | `ia_deserialize_int` 读取 4 字节并经 `ntohl` 转换，不校验符号。 |
| ② | `(priv->len - priv->off)` 是小的非负 `int32_t`，与负的 `b->len` 比较：`0 < -56353` 为 **false**——边界检查被绕过。 |
| ③ | 只排除了 `-1`（Jute 协议的 NULL 约定），其他所有负值继续往下走。 |
| ④ | `malloc(b->len)`：`b->len` 是 `int32_t = -56353`，隐式提升为 `size_t = 0xFFFFFFFFFFFF23DF`（约 18 EB）。ASan 判定 `allocation-size-too-big`，中止进程。 |

### 对比：`ia_deserialize_string` 正确处理了负长度（recordio.c:260–281）

```c
int ia_deserialize_string(struct iarchive *ia, const char *name, char **s)
{
    ...
    if ((priv->len - priv->off) < len) {
        return -E2BIG;
    }
    if (len < 0) {           // ← buffer 版本缺少这行
        return -EINVAL;
    }
    *s = malloc(len+1);
    ...
}
```

## 复现

### 复现输入

| 文件                                        | 原始字节           | 选择器                         | len 字段         |
|---------------------------------------------|--------------------|--------------------------------|------------------|
| `tests/.runtime/fuzz/asan/crash-1285...`    | `23 ff ff 23 df`   | `0x23` → 11 (SetSASLResponse)  | `0xffff23df` = -56353 |

解码：
- 字节 0 = `0x23`，`35 % 12 = 11` → `case 11`：`deserialize_SetSASLResponse`
- 字节 1–4 = `ff ff 23 df`（big-endian）→ `ntohl` → `b->len = 0xffff23df = -56353`

### ASan 输出

```
==3965121==ERROR: AddressSanitizer: requested allocation size
  0xffffffffffff23df exceeds maximum supported size
  of 0x10000000000 (thread T0)
    #0 malloc  (libclang_rt.asan.so)
    #1 ia_deserialize_buffer  recordio.c:252
    #2 deserialize_SetSASLResponse  zookeeper.jute.c:509
    #3 LLVMFuzzerTestOneInput  tests/fuzz/jute.c:116
```

### 复现命令

```sh
runtime_dir=$(clang --print-runtime-dir)
printf '\x23\xff\xff\x23\xdf' > /tmp/poc-bug02
zig build fuzz -Dfuzz=true -Dsanitize=address \
    -Dclang-runtime-dir="$runtime_dir"
./zig-cache/o/*/fuzz_jute /tmp/poc-bug02
```

## 安全影响

与 BUG-01 类似，恶意服务器可在包含 `buffer` 字段的响应中嵌入负长度值，
导致客户端进程因 ASan 中止或内存分配失败而崩溃。属于**拒绝服务**漏洞。

## 建议修复

在 `b->len == -1` 哨兵检查之后、`malloc` 之前加入负值校验，与
`ia_deserialize_string` 保持一致：

```c
if (b->len == -1) {
   b->buff = NULL;
   return rc;
}
if (b->len < 0) {              // ← 新增
    return -EINVAL;
}
b->buff = malloc((size_t)b->len);
```

该修复在 `recordio.c` 中一次性完成，不需要改动 jute 生成代码。
