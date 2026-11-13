---
description: "Records a persistence type transition and its compatibility acknowledgement."
kind: persistence-change
---

# 2026-11-13-guarded-admission

English | [中文](2026-11-13-guarded-admission.zh.md)

## Summary

Adds required admission hold, release, cancellation, and recovery confirmation events for guarded Agent inbox input.

## Table of Contents

- [Declaration](#declaration)
- [Compatibility](#compatibility)
- [Verification](#verification)
- [Dev Note](#dev-note)

<a id="declaration"></a>
## Declaration

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
## Compatibility

Readers that do not recognize the new required events must refuse the Session without mutation or recovery truncation; the events are not ignorable. A recovered guarded input remains blocked until an exact live-writer receipt and a synchronous trusted task decision allow its claim. The receipt alone does not prove execution or authorize a retry.

Legacy Agent factories can resume an ordinary session only when their persistence backend implements `withWriteOpenValidation`. A backend must acquire its exclusive writer lease, validate the event snapshot before any migration, repair, or other write, and retain that lease through the returned write handle. It must reject before opening a writer when it cannot enforce the scope. Backends without this capability cannot safely resume through a legacy factory, including sessions whose history is not yet known; read-only access remains available. An unlocked preliminary read is insufficient because another writer could change the log before the write open. JSONL implements this capability; third-party backends must implement and test the same contract before legacy resume is available.

Every Agent-loop resume requires a backend with scoped write-open validation. Under the exclusive writer lease it validates all durable admission transitions before migration, repair or interrupted-turn closers; a backend that cannot enforce this rejects the resume. For valid history, interrupted-turn closers may be persisted even when no provider is registered. That repair does not claim a guarded input: the input remains pending until its provider returns and the host reconciles it.

The trusted task host registers a single `ctx.agents.registerGuardedClaimProvider` after obtaining its journal lease and holding unresolved prior claims. Registration prepares existing idle Agents under maintenance and holds guarded work until all bindings commit; new Agent publication waits for the same barrier. The host must include historical target sessions in its journal lookup and may skip an Agent only when no pending journal work or guarded Session history applies. Coordinators recheck authority at each claim. On unload, stop dispatch and drain in-flight claims before disposing the provider; missing or uncertain authority keeps guarded work blocked. This gate cannot make external side effects exactly once.

A guarded send preserves its exact inbox target and rejects if an aborted activity would redirect it. Each guarded input owns a separate turn; an `agent/turn-stopping` listener may continue that turn without admitting unrelated queued input. Binding a provider wakes already-pending guarded inputs without inserting a new message. A normal Agent cancellation leaves guarded inputs parked; the host must persist its cancellation decision and use `cancelGuardedAdmission` to remove each exact pending input before later work blocked by it can proceed.

<a id="verification"></a>
## Verification

Verify Agent admission, JSONL receipts, legacy compatibility, crash recovery, and the built runtime against the version being deployed.

<a id="dev-note"></a>
## Dev Note

None.
