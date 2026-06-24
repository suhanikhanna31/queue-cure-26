Here is the complete, unified `src/server.ts` file containing your full state engine, custom async mutex, backup synchronization, and the dynamic network port configuration needed for your Railway container deployment.

```typescript
import Fastify from "fastify";
import { Server as SocketIOServer } from "socket.io";
import { createServer } from "http";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";

// ─── Interfaces ────────────────────────────────────────────────────────────────

export interface Patient {
  token: number;
  name: string;
  addedAt: number; // epoch ms
}

export interface CompletedSession {
  token: number;
  name: string;
  durationSeconds: number;
  completedAt: number;
}

export interface ClinicState {
  tokenCounter: number;
  queue: Patient[];
  current: Patient | null;
  currentStartTime: number | null; // epoch ms when current patient was called
  completed: CompletedSession[];
  avgConsultationTime: number; // receptionist-set baseline in seconds
  wmaBuffer: number[]; // last 3 actual durations (seconds)
  lastUpdated: number;
}

// ─── Constants ────────────────────────────────────────────────────────────────

// Dynamic environment port for Railway production, fallback to 3000 locally
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
const BACKUP_PATH = join(process.cwd(), "backup.json");
const WMA_WINDOW = 3;

// ─── Mutex / Transactional Queue ─────────────────────────────────────────────

class AsyncMutex {
  private queue: Array<() => void> = [];
  private locked = false;

  async acquire(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.locked) {
        this.locked = true;
        resolve();
      } else {
        this.queue.push(resolve);
      }
    });
  }

  release(): void {
    if (this.queue.length > 0) {
      const next = this.queue.shift()!;
      next();
    } else {
      this.locked = false;
    }
  }

  async run<T>(fn: () => T | Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }
}

// ─── State Engine ─────────────────────────────────────────────────────────────

class ClinicStateEngine {
  private state: ClinicState;
  private historyStack: string[] = [];
  private mutex = new AsyncMutex();
  private readonly MAX_HISTORY = 50;

  constructor(initialState?: ClinicState) {
    this.state = initialState ?? this.createDefaultState();
  }

  private createDefaultState(): ClinicState {
    return {
      tokenCounter: 0,
      queue: [],
      current: null,
      currentStartTime: null,
      completed: [],
      avgConsultationTime: 300, // default 5 minutes
      wmaBuffer: [],
      lastUpdated: Date.now(),
    };
  }

  // ── WMA Calculation ─────────────────────────────────────────────────────────

  private computeWMA(): number {
    const buf = this.state.wmaBuffer;
    if (buf.length === 0) {
      return this.state.avgConsultationTime;
    }
    const sum = buf.reduce((acc, val) => acc + val, 0);
    return sum / buf.length;
  }

  private pushWMABuffer(durationSeconds: number): void {
    this.state.wmaBuffer.push(durationSeconds);
    if (this.state.wmaBuffer.length > WMA_WINDOW) {
      this.state.wmaBuffer.shift();
    }
  }

  // ── Wait Time Estimation ────────────────────────────────────────────────────

  computeWaitForPosition(positionFromFront: number): number {
    const wma = this.computeWMA();
    return positionFromFront * wma;
  }

  // ── History / Undo ──────────────────────────────────────────────────────────

  private snapshotState(): void {
    const snap = JSON.stringify(this.state);
    this.historyStack.push(snap);
    if (this.historyStack.length > this.MAX_HISTORY) {
      this.historyStack.shift();
    }
  }

  async undo(): Promise<ClinicState> {
    return this.mutex.run(() => {
      if (this.historyStack.length === 0) {
        return this.getSnapshot();
      }
      const lastSnap = this.historyStack.pop()!;
      this.state = JSON.parse(lastSnap) as ClinicState;
      this.state.lastUpdated = Date.now();
      persistBackup(this.state);
      return this.getSnapshot();
    });
  }

  // ── Atomic State Mutations ──────────────────────────────────────────────────

  async addPatient(name: string): Promise<ClinicState> {
    return this.mutex.run(() => {
      this.snapshotState();
      this.state.tokenCounter += 1;
      const patient: Patient = {
        token: this.state.tokenCounter,
        name: name.trim() || `Patient ${this.state.tokenCounter}`,
        addedAt: Date.now(),
      };
      this.state.queue.push(patient);
      this.state.lastUpdated = Date.now();
      persistBackup(this.state);
      return this.getSnapshot();
    });
  }

  async callNext(): Promise<ClinicState> {
    return this.mutex.run(() => {
      this.snapshotState();

      if (this.state.current !== null && this.state.currentStartTime !== null) {
        const durationMs = Date.now() - this.state.currentStartTime;
        const durationSeconds = Math.round(durationMs / 1000);
        const session: CompletedSession = {
          token: this.state.current.token,
          name: this.state.current.name,
          durationSeconds,
          completedAt: Date.now(),
        };
        this.state.completed.push(session);
        this.pushWMABuffer(durationSeconds);
      }

      if (this.state.queue.length === 0) {
        this.state.current = null;
        this.state.currentStartTime = null;
      } else {
        const next = this.state.queue.shift()!;
        this.state.current = next;
        this.state.currentStartTime = Date.now();
      }

      this.state.lastUpdated = Date.now();
      persistBackup(this.state);
      return this.getSnapshot();
    });
  }

  async setAvgConsultationTime(seconds: number): Promise<ClinicState> {
    return this.mutex.run(() => {
      this.snapshotState();
      this.state.avgConsultationTime = Math.max(1, Math.round(seconds));
      this.state.lastUpdated = Date.now();
      persistBackup(this.state);
      return this.getSnapshot();
    });
  }

  // ── Read State ──────────────────────────────────────────────────────────────

  getSnapshot(): ClinicState {
    return JSON.parse(JSON.stringify(this.state)) as ClinicState;
  }

  getEnrichedSnapshot() {
    const snap = this.getSnapshot();
    const wma = this.computeWMA();
    const queueWithWait = snap.queue.map((p, idx) => ({
      ...p,
      estimatedWaitSeconds: this.computeWaitForPosition(idx + 1),
    }));

    return {
      ...snap,
      queue: queueWithWait,
      computedWMA: wma,
      historyDepth: this.historyStack.length,
      currentElapsedSeconds:
        snap.currentStartTime !== null
          ? Math.round((Date.now() - snap.currentStartTime) / 1000)
          : null,
    };
  }
}

// ─── Backup / Persistence ─────────────────────────────────────────────────────

function persistBackup(state: ClinicState): void {
  setImmediate(() => {
    try {
      writeFileSync(BACKUP_PATH, JSON.stringify(state, null, 2), "utf-8");
    } catch (err) {
      console.error("[BACKUP] Failed to write backup.json:", err);
    }
  });
}

function loadBackup(): ClinicState | null {
  try {
    if (existsSync(BACKUP_PATH)) {
      const raw = readFileSync(BACKUP_PATH, "utf-8");
      const parsed = JSON.parse(raw) as ClinicState;
      console.log(
        `[BOOT] Recovered state from backup.json — token counter at ${parsed.tokenCounter}, queue length: ${parsed.queue.length}`
      );
      return parsed;
    }
  } catch (err) {
    console.error("[BOOT] Failed to parse backup.json, starting fresh:", err);
  }
  return null;
}

// ─── Server Bootstrap ─────────────────────────────────────────────────────────

async function bootstrap() {
  const recoveredState = loadBackup();
  const engine = new ClinicStateEngine(recoveredState ?? undefined);

  const app = Fastify({ logger: false });

  await app.register(import("@fastify/static"), {
    root: join(process.cwd(), "public"),
    prefix: "/",
  });

  const httpServer = createServer(app.server);

  const io = new SocketIOServer(httpServer, {
    cors: {
      origin: "*",
      methods: ["GET", "POST"],
    },
    transports: ["websocket", "polling"],
  });

  function broadcastState() {
    const enriched = engine.getEnrichedSnapshot();
    io.emit("state:sync", enriched);
  }

  // ── REST endpoints ──────────────────────────────────────────────────────────

  app.get("/api/state", async (_req, reply) => {
    return reply.send(engine.getEnrichedSnapshot());
  });

  app.get("/health", async (_req, reply) => {
    return reply.send({ status: "ok", uptime: process.uptime() });
  });

  // ── Socket.io event handlers ────────────────────────────────────────────────

  io.on("connection", (socket) => {
    console.log(`[SOCKET] Client connected: ${socket.id}`);

    socket.emit("state:sync", engine.getEnrichedSnapshot());

    socket.on(
      "patient:add",
      async (payload: { name?: string }, ack?: (s: unknown) => void) => {
        const name = typeof payload?.name === "string" ? payload.name : "Anonymous";
        const state = await engine.addPatient(name);
        broadcastState();
        if (typeof ack === "function") {
          ack({ success: true, token: state.tokenCounter });
        }
      }
    );

    socket.on("queue:callNext", async (ack?: (s: unknown) => void) => {
      const state = await engine.callNext();
      broadcastState();
      if (typeof ack === "function") {
        ack({ success: true, current: state.current });
      }
    });

    socket.on("queue:undo", async (ack?: (s: unknown) => void) => {
      const state = await engine.undo();
      broadcastState();
      if (typeof ack === "function") {
        ack({ success: true, historyDepth: 0, state });
      }
    });

    socket.on(
      "config:setAvgTime",
      async (payload: { seconds?: number }, ack?: (s: unknown) => void) => {
        const seconds = typeof payload?.seconds === "number" ? payload.seconds : 300;
        await engine.setAvgConsultationTime(seconds);
        broadcastState();
        if (typeof ack === "function") {
          ack({ success: true, avgConsultationTime: seconds });
        }
      }
    );

    socket.on("cli:status", (ack?: (s: unknown) => void) => {
      if (typeof ack === "function") {
        ack(engine.getEnrichedSnapshot());
      }
    });

    socket.on("disconnect", () => {
      console.log(`[SOCKET] Client disconnected: ${socket.id}`);
    });
  });

  await app.ready();

  // Explicitly bound to host 0.0.0.0 for external cloud port proxy access
  httpServer.listen(PORT, "0.0.0.0", () => {
    console.log(`\n╔══════════════════════════════════════╗`);
    console.log(`║  Queue Cure '26 — Clinic Engine      ║`);
    console.log(`╠══════════════════════════════════════╣`);
    console.log(`║  PORT  → Running live on port: ${PORT}  ║`);
    console.log(`╚══════════════════════════════════════╝\n`);
  });
}

bootstrap().catch((err) => {
  console.error("[FATAL] Server failed to start:", err);
  process.exit(1);
});

```
