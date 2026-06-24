# 👩‍⚕️ Queue Cure '26 — Telemetry-Driven Clinic Engine

An ultra-performant, local-first queue synchronization system designed to replace traditional paper tokens and acoustic announcement patterns ("shouting") in modern Indian clinics. 

This system enforces strict state discipline, real-time reactive event streams over WebSockets, out-of-band telemetry diagnostics via a customized CLI companion, and fault-tolerant in-memory durability.

---

## 🏗️ System Architecture & Design Specification

Rather than utilizing heavy, multi-layered database abstractions or stateless REST structures prone to race conditions, the system approaches the challenge from an infrastructure perspective:

```text
[ Reception UI ]       [ Patient Display UI ]       [ Admin CLI Tool ]
       │                         ▲                         │
       │ (Socket.io)             │ (State Broadcast)       │ (Telemetry / Stress)
       ▼                         │                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           FASTIFY CORE ENGINE                           │
│                                                                         │
│  ┌───────────────────┐    ┌───────────────────┐   ┌──────────────────┐  │
│  │    AsyncMutex     │───>│ ClinicStateEngine │──>│ historyStack     │  │
│  │ (Operation Lock)  │    │  (WMA Wait Engine)│   │ (Rollback Stack) │  │
│  └───────────────────┘    └───────────────────┘   └──────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼ (Async Append Snapshot)
                              [ backup.json ]

1. Atomic Transaction Isolation (AsyncMutex)
Clinic operations require absolute sequence stability. If multiple receptionist clients or out-of-band administration channels issue state changes simultaneously, traditional asynchronous Node handlers risk state mutation overlap (race conditions).
The Solution: An explicit AsyncMutex pattern implemented natively on the backend core. Incoming events (patient:add, queue:callNext, queue:undo) are captured sequentially via a promise-backed callback chain, guaranteeing full thread serialization without blocking the main event loop.
2. High-Availability In-Memory Rehydration
To avoid the network latency of cloud-managed persistent caches, the core state (ClinicState) lives natively in the engine thread memory pool.
Durability Layer: To protect the transient state from runtime dropouts or process failures, any accepted state mutation triggers an asynchronous, zero-blocking IO stream handler that outputs serialized snapshots directly into backup.json.
Hydration Cycle: On system boot-up, an initialization process automatically flags and references the existing state log file, fully restoring historical tokens, averages, and indices to memory within milliseconds.
3. Dynamic Patient Wait Time Math (wmaBuffer)
Traditional clinic boards use static timers or manual estimations that fail to mirror the actual flow of medical check-ups.
The Solution: A moving Windowed Moving Average (WMA) running over a fixed ring buffer (WMA_WINDOW = 3) capturing historical appointment lengths.
Mathematical Progression:
When a patient is marked complete, their real cabin session duration is derived:
Duration=Date.now()−currentStartTime
This is pushed to the wmaBuffer array. Individual patient tokens waiting in line receive an immediate, unique computed delay value based directly on their real position index:
Estimated Wait Time=(Queue Position)× 
n
∑ 
i=1
n
​	
 Duration 
i
​	
 
​	
 
Edge Case Mitigation: If the calculation queue initializes with no history (e.g., first start of morning operations), the system safely cascades back to the receptionist's manual base parameters (avgConsultationTime).
4. Mistake-Proof Rollback History Ring Buffer
Accidental interface interactions ("double tapping") can disrupt clinical flow and confuse patients in the waiting lounge.
The Solution: An internal snapshot ring buffer (historyStack). Before any forward-moving mutation is executed, a clean serialized configuration map is pushed onto the stack layout (capped to a safety depth of 50 states). Invoking a queue:undo event pops the last configuration instantly, flashing structural parity across all client instances.
📁 Repository Blueprint
├── package.json          # Node engine specifications, runtime configurations & binaries
├── tsconfig.json         # Strict Type-Safe compiler rules for optimal memory execution
├── backup.json           # [Auto-Generated Output] State log snapshot file
├── src/
│   ├── server.ts         # Fastify core engine + Mutex lock processing + Socket.io hub
│   └── cli.ts            # Out-of-band admin automation, health dashboard & stress tool
└── public/
    ├── index.html        # 2D Kawaii/Neo-brutalist split screen display interface
    └── app.js            # Unified web layout logic + Browser Native Web Speech Audio Engine

⚙️ Local Infrastructure Installation
1. Initialize Dependecy Layers
Clone the repository path, open your directory terminal, and trigger installation scripts:
Bash
npm install
2. Activate the Main Engine Instance
Compile TypeScript scripts and engage the real-time background operational environment using the automated development runner script:
Bash
npm run dev
The host server console will acknowledge system activation:
Plaintext
╔══════════════════════════════════════╗
║  Queue Cure '26 — Clinic Engine      ║
╠══════════════════════════════════════╣
║  HTTP  → http://localhost:3000       ║
║  WS    → ws://localhost:3000         ║
╚══════════════════════════════════════╝
3. Execute Out-Of-Band Administration Telemetry CLI
Open a standalone alternative terminal split stream window to access structural control utilities:
Query Operational Health Matrix:
Bash
npm run cli -- --status
Trigger Emergency Queue Skip:
Bash
npm run cli -- --call-next
Run Chaos Monkey Concurrency Stress Engine:
Bash
npm run cli -- --stress
(Launches an asynchronous concurrent batch processing routine pushing 50 distinct requests inside a tight 1000ms window, verifying backend state processing containment and zero-data loss constraints).
