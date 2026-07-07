# 31. Example Flows

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

## 31.1 Launch Local stdio Agent

```txt
client appends agent.launch.requested
launch operator claims request
provider starts local stdio process
stdio adapter initializes protocol
session adapter creates session
runtime appends session.created
runtime appends launch.ready
client observes session handle
```

## 31.2 Prompt Through an ACP Adapter

```txt
client appends prompt.requested
prompt operator claims request
runtime verifies live session ownership
ACP adapter sends session/prompt
agent sends session/update notifications
runtime appends prompt.chunk records
agent returns terminal response
runtime appends prompt.completed
client observes result
```

## 31.3 Human Approval

```txt
agent requests permission
runtime appends permission.requested
approval UI observes projection
human approves
UI appends permission.resolved
runtime resumes waiting turn
runtime appends prompt.completed or continuing chunks
```

## 31.4 Timer

```txt
agent schedules sleep
runtime appends timer.scheduled
timer operator reconstructs schedule from log
after time, timer operator appends timer.fired
awaitable resolves
agent continuation proceeds
```

## 31.5 Restart With Stale Session

```txt
session.created exists in durable log
runtime crashes
new runtime starts
projection rebuilds session row
client repeats idempotent launch
runtime checks live session ownership
no live session exists
runtime either reattaches or appends launch.not_live
client receives typed failure
client does not append prompt to stale session
```

---

---

# 34. Minimal Conforming Example

A minimal system can implement only:

```txt
durable log
session intent records
prompt intent records
projection rows
single in-process fake agent adapter
```

Even then, it should preserve:

```txt
append intent
claim work
execute side effect
append terminal state
observe projection
```

That minimal system is useful for testing architecture even if it has no network agents.

---
