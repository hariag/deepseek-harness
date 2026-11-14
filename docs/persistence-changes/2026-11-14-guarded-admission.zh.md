---
description: "记录持久化类型更改及其兼容性确认。"
kind: persistence-change
---

# 2026-11-14-guarded-admission

[English](2026-11-14-guarded-admission.md) | 中文

## 概述

新增四种必需的受保护派发事件，分别记录保留、释放、取消和对账，Session 头版本不变。

## 目录

- [声明](#declaration)
- [兼容性](#compatibility)
- [验证](#verification)
- [开发备注](#dev-note)

<a id="declaration"></a>
## 声明

```yaml persistence-change
schemaVersion: 1
id: 2026-11-14-guarded-admission
baseline: false
changes:
  - root: "event:agent/admission/cancel"
    previous: null
    after: "beddf08f4c251fe00bd33763b31d2db7eea3fff7fdc3ff59a72fa293f11cb187"
    decision: same-version
  - root: "event:agent/admission/hold"
    previous: null
    after: "6c28dac83cdfa5cfdb14fa218ca362312c8bc883cc6b70ac161c01f8d2590a80"
    decision: same-version
  - root: "event:agent/admission/reconciled"
    previous: null
    after: "6c447256e6ed7acef60b37695d7dbb80d77ee068354d8eb6f97bec6faab22dc3"
    decision: same-version
  - root: "event:agent/admission/release"
    previous: null
    after: "ab7095e8f3e6ada5c995179895eebb2033575a80990dd1880a1e7d793eb51a23"
    decision: same-version
```

<a id="compatibility"></a>
## 兼容性

不含这些事件的日志保留原有解释。旧 reader 不能安全地忽略这些事件所承载的派发约束：实测先前目标 writer 基线的 JSONL reader 在读及写打开时，对未标记可忽略的未知类型直接拒绝，发生尾部修复前即停止，日志内容保持不变。含受保护投影的 reader 可重放新记录；此结论不代表所有旧版本兼容，也不保证外部副作用恰好一次。

<a id="verification"></a>
## 验证

隔离的旧 reader 夹具在先前目标 writer 基线上通过 5 项定点：四类未知事件均被拒绝且文件字节与 inode 不变；不含新事件的旧日志可读取与追加。夹具 packages/session/session-persistence-jsonl/tests/fixtures/guarded-old-reader.spec.fixture 在该基线工作区以只读覆盖方式运行。组合 guarded/writer 候选的定点套件通过 47 项；真实 Host 恢复尚未验证。

<a id="dev-note"></a>
## 开发备注

无。
