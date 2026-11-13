---
description: "记录持久化类型更改及其兼容性确认。"
kind: persistence-change
---

# 2026-11-13-guarded-admission

[English](2026-11-13-guarded-admission.md) | 中文

## 概述

为受保护的 Agent 收件输入新增必需的准入保持、释放、取消和恢复确认事件。

## 目录

- [声明](#declaration)
- [兼容性](#compatibility)
- [验证](#verification)
- [开发备注](#dev-note)

<a id="declaration"></a>
## 声明

```yaml persistence-change
schemaVersion: 1
id: 2026-11-13-guarded-admission
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

不认识新必需事件的读取器必须拒绝会话，不能修改文件或通过恢复截断；这些事件不可忽略。恢复后的受保护输入保持阻塞，直至获得精确的活动写入回执并由可信任务决议同步授权领取。回执本身不能证明执行或授权重试。

旧版 Agent 工厂仅能在持久化后端实现 `withWriteOpenValidation` 时恢复普通会话。后端必须先取得独占写入租约，在迁移、修复或其他写入前验证事件快照，并由返回的写入句柄持续持有该租约。无法执行该作用域约束时，必须在打开写入句柄前拒绝。缺少此能力的后端不能通过旧版工厂恢复会话，包括历史记录尚未知晓的会话；只读访问仍可用。无锁预读后再打开写入句柄并不足够，因为其他写者可能在两者之间修改日志。JSONL 已实现该能力；第三方后端必须实现并测试相同契约，才能支持旧版恢复。

每次 Agent-loop 恢复均要求后端支持有作用域的写入前验证。在独占写入租约下，迁移、修复或补写中断回合结束事件之前，先验证所有持久 admission 状态迁移；无法保证这一点的后端必须拒绝恢复。历史有效时，即使尚未注册 provider，也可以持久补写中断回合的结束事件。该修复不会领取受保护输入：输入保持待处理，直到 provider 恢复且宿主完成对账。

可信任务宿主在取得日志租约并冻结先前结果未明的认领后，注册唯一的 `ctx.agents.registerGuardedClaimProvider`。注册会在维护屏障内准备已有空闲 Agent，全部绑定提交前受保护工作保持等待；新 Agent 发布也等待同一屏障。宿主查询日志时必须覆盖历史目标会话，仅当不存在待处理日志任务且会话没有受保护历史时才能跳过 Agent。协调器在每次领取时重新核验权限。卸载前先停止派发并等待在途领取完成，再释放 provider；权限缺失或结果不明时继续阻塞受保护工作。此门不能保证外部副作用恰好一次。

受保护发送保留精确的收件队列目标；若已中止的活动会使目标发生重定向，则拒绝发送。每条受保护输入各占一个独立回合；`agent/turn-stopping` 监听器可以继续该回合，而不会把无关排队输入带入其中。绑定 provider 时会唤醒已排队的受保护输入，不会插入新消息。普通 Agent 取消会保留受保护输入；宿主必须持久记录取消决定，并用 `cancelGuardedAdmission` 移除每条精确匹配的待处理输入，之后被其阻塞的工作才能继续。

<a id="verification"></a>
## 验证

针对待部署版本验证 Agent 准入、JSONL 回执、旧版兼容性、崩溃恢复及构建后的运行时。

<a id="dev-note"></a>
## 开发备注

无。
