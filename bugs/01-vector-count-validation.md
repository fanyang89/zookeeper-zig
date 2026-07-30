# BUG-01: vector 反序列化缺少 count 校验，导致 calloc 溢出 / OOM

## 概要

所有 jute 生成的 `deserialize_*_vector` 函数在调用 `calloc` 前不校验从网络
流读入的 `count` 值。当 `count` 为负数时触发 `calloc` 参数溢出（UB / ASan
中止）；当 `count` 为极大正数时触发内存耗尽（OOM）。

## 影响范围

以下 5 个函数均有此缺陷（`generated/zookeeper.jute.c`）：

| 行号  | 函数                          |
|-------|-------------------------------|
| 239   | `deserialize_String_vector`   |
| 554   | `deserialize_ACL_vector`      |
| 1180  | `deserialize_ClientInfo_vector`|
| 1262  | `deserialize_Id_vector`       |
| 1685  | `deserialize_Txn_vector`      |

所有调用这些函数的上层反序列化路径均受影响，包括但不限于：
`GetChildrenResponse`、`GetChildren2Response`、`GetACLResponse`、
`GetEphemeralsResponse`、`WhoAmIResponse`、`ConnectResponse` 等。

## 根因分析

以 `deserialize_String_vector` 为例（第 234–247 行）：

```c
int deserialize_String_vector(struct iarchive *in, const char *tag, struct String_vector *v)
{
    int rc = 0;
    int32_t i;
    rc = in->start_vector(in, tag, &v->count);     // 从流中读取 count（int32_t）
    v->data = calloc(v->count, sizeof(*v->data));  // ← 直接使用，无任何校验
    for(i=0;i<v->count;i++) {
        rc = rc ? rc : in->deserialize_String(in, "value", &v->data[i]);
    }
    rc = in->end_vector(in, tag);
    return rc;
}
```

`start_vector` 内部调用 `ia_deserialize_int`，仅做缓冲区边界检查（4 字节是否
可读），不校验语义合法性。`calloc` 的两个参数均为 `size_t`（无符号），当
`v->count` 为负 `int32_t` 时隐式转换为极大的 `size_t`。

### 负 count 路径（calloc 参数溢出）

`v->count = -842150451`（`0xCDcdcdcd`）时：

```
calloc(-842150451, 8)  →  参数溢出
```

ASan 直接中止：

```
ERROR: AddressSanitizer: calloc parameters overflow:
  count * size (-842150451 * 8) cannot be represented in type size_t
```

### 大正数 count 路径（OOM）

`v->count = 553648128`（`0x21000000`）时：

```
calloc(553648128, 8)  →  尝试分配 ~4.4 GB  →  OOM
```

libFuzzer 报告：

```
ERROR: libFuzzer: out-of-memory (malloc(4429185024))
```

### calloc 返回 NULL 后的空指针解引用

若设置 `allocator_may_return_null=1`，`calloc` 对大正数 count 返回 NULL，
随后循环体 `&v->data[i]` 对 NULL 解引用，导致 segfault：

```c
v->data = calloc(v->count, sizeof(*v->data));  // 返回 NULL
for(i=0;i<v->count;i++) {                       // count > 0，进入循环
    rc = rc ? rc : deserialize_ACL(in, "value", &v->data[i]);
    //                                         ^^^^^^^^^^^ NULL 解引用
}
```

## 复现

### 复现输入

fuzz harness 使用第一个字节作为选择器（`byte % 12`），其余字节为序列化
载荷。以下 4 个输入均可触发本 bug：

| 文件                                       | 选择器 | 目标函数                    | count 值          | 表现      |
|--------------------------------------------|--------|-----------------------------|-------------------|-----------|
| `tests/.runtime/fuzz/plain/oom-a0b4...`    | `0x40` → 4 (GetACLResponse)    | `deserialize_ACL_vector`    | `0x5cfbdbfb` = 1558148091 | OOM (31 GB) |
| `tests/.runtime/fuzz/ubsan/oom-7a97...`    | `0x34` → 4 (GetACLResponse)    | `deserialize_ACL_vector`    | `0x34464646` = 877039682  | OOM (17 GB) |
| `tests/.runtime/fuzz/asan2/oom-e865...`    | `0x3a` → 10 (GetChildrenResp)  | `deserialize_String_vector` | `0x21000000` = 553648128  | OOM (4.4 GB) |
| 早期样本 `\x11\xcd\xcd\xcd\xcd\x23`        | `0x11` → 5 (GetChildren2Resp)  | `deserialize_String_vector` | `0xcdcdcdcd` = -842150451 | calloc 溢出 |

### 复现命令

```sh
runtime_dir=$(clang --print-runtime-dir)

# 触发 calloc 参数溢出（负 count）
printf '\x11\xcd\xcd\xcd\xcd\x23' > /tmp/poc-bug01
zig build fuzz -Dfuzz=true -Dsanitize=address \
    -Dclang-runtime-dir="$runtime_dir"
./zig-cache/o/*/fuzz_jute /tmp/poc-bug01

# 触发 OOM（大正数 count）
printf '\x3a\x21\x00\x00\x00\x00' > /tmp/poc-bug01
zig build fuzz -Dfuzz=true \
    -Dclang-runtime-dir="$runtime_dir"
./zig-cache/o/*/fuzz_jute /tmp/poc-bug01
```

## 安全影响

 ZooKeeper C client 在连接到恶意/被攻陷的服务器时，服务器可在响应中嵌入
 crafted 的 vector count，导致客户端进程崩溃（calloc 溢出 / OOM-killer 杀死
 / NULL deref segfault）。属于**拒绝服务**漏洞。

## 建议修复

在 `calloc` 之前加入 count 校验。可选择上限阈值以防 OOM：

```c
rc = in->start_vector(in, tag, &v->count);
if (rc < 0) return rc;
if (v->count < 0 || v->count > MAX_VECTOR_COUNT) return -EINVAL;
v->data = calloc(v->count, sizeof(*v->data));
if (v->count > 0 && !v->data) return -ENOMEM;
```

由于该代码由 jute 代码生成器产生，**根本修复应在代码生成器模板中**加入校验
逻辑，使所有生成的 `deserialize_*_vector` 函数统一受益。
