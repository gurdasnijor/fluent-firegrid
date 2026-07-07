# 30. Reference Architecture

Doc-Class: RFC
Status: draft
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

A full implementation might look like:

```txt
Application Client
  append intents
  observe projections

Durable Log
  ordered records

Projection Engine
  materialized read models

Operators
  LaunchOperator
  PromptOperator
  ApprovalOperator
  TimerOperator
  ResourceOperator

Runtime Services
  SessionService
  ProviderService
  ResourceService
  ProtocolAdapterRegistry

Agent Adapters
  ACP Adapter
  Stdio Adapter
  HTTP Adapter
  Vendor API Adapter
  InProcess Adapter

Live Resources
  ACP sessions
  child processes
  containers
  filesystem sandboxes
  network connections

External Consumers
  dashboards
  approvers
  audit
  webhooks
  schedulers
```

---
