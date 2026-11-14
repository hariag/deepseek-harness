---
description: "Records a persistence type transition and its compatibility acknowledgement."
kind: persistence-change
---

# 2026-11-14-guarded-admission

English | [中文](2026-11-14-guarded-admission.zh.md)

## Summary

Adds four required guarded-admission events for hold, release, cancel, and reconciliation without changing the Session header version.

## Table of Contents

- [Declaration](#declaration)
- [Compatibility](#compatibility)
- [Verification](#verification)
- [Dev Note](#dev-note)

<a id="declaration"></a>
## Declaration

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
## Compatibility

Logs without these events retain their previous interpretation. A reader predating the events cannot safely ignore their admission authority: the tested preceding target-writer baseline JSONL reader refuses their unmarked unknown types on both read and write open before repairing a torn tail. Its refusal leaves the log unchanged. Readers with the guarded projection replay the new records; this decision does not establish compatibility with every older release or exactly-once external side effects.

<a id="verification"></a>
## Verification

The isolated old-reader fixture against that baseline passed 5 targeted tests: each of the four unknown events was refused without changing file bytes or inode, and an old log without the events was read and appended. The fixture is packages/session/session-persistence-jsonl/tests/fixtures/guarded-old-reader.spec.fixture; it was overlaid on that base checkout for this run. The combined guarded/writer targeted candidate suite passed 47 tests; full real Host recovery remains unverified.

<a id="dev-note"></a>
## Dev Note

None.
