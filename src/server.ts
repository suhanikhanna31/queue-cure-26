import Fastify from "fastify";
import { Server as SocketIOServer } from "socket.io";
import { createServer } from "http";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";

export interface Patient {
  token: number;
  name: string;
  addedAt: number;
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
  currentStartTime: number | null;
  completed: CompletedSession[];
  avgConsultationTime: number;
  wmaBuffer: number[];
  lastUpdated: number;
}

const PORT = 3000;
const BACKUP_PATH = join(process.cwd(), "backup.json");
const WMA_WINDOW = 3;

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
      avgConsultationTime: 300,
      wmaBuffer: [],
      lastUpdated: Date.now(),
    };
  }

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

  computeWaitForPosition(positionFromFront: number): number {
    const wma = this.computeWMA();
    return positionFromFront * wma;
  }

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
      console.log(`[BOOT] Recovered state from backup.json — token counter at ${parsed.tokenCounter}`);
      return parsed;
    }
  } catch (err) {
    console.error("[BOOT] Failed to parse backup.json:", err);
  }
  return null;
}

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
    cors: { origin: "*", methods: ["GET", "POST"] },
    transports: ["websocket", "polling"],
  });

  function broadcastState() {
    const enriched = engine.getEnrichedSnapshot();
    io.emit("state:sync", enriched);
  }

  app.get("/api/state", async (_req, reply) => {
    return reply.send(engine.getEnrichedSnapshot());
  });

  io.on("connection", (socket) => {
    socket.emit("state:sync", engine.getEnrichedSnapshot());

    socket.on("patient:add", async (payload: { name?: string }, ack?: (s: unknown) => void) => {
      const name = typeof payload?.name === "string" ? payload.name : "Anonymous";
      const state = await engine.addPatient(name);
      broadcastState();
      if (typeof ack === "function") ack({ success: true, token: state.tokenCounter });
    });

    socket.on("queue:callNext", async (ack?: (s: unknown) => void) => {
      const state = await engine.callNext();
      broadcastState();
      if (typeof ack === "function") ack({ success: true, current: state.current });
    });

    socket.on("queue:undo", async (ack?: (s: unknown) => void) => {
      const state = await engine.undo();
      broadcastState();
      if (typeof ack === "function") ack({ success: true, state });
    });

    socket.on("config:setAvgTime", async (payload: { seconds?: number }) => {
      const seconds = typeof payload?.seconds === "number" ? payload.seconds : 300;
      await engine.setAvgConsultationTime(seconds);
      broadcastState();
    });

    socket.on("cli:status", (ack?: (s: unknown) => void) => {
      if (typeof ack === "function") ack(engine.getEnrichedSnapshot());
    });
  });

  await app.ready();
  httpServer.listen(PORT, "0.0.0.0", () => {
    console.log(`\nQueue Cure '26 Engine Online at http://localhost:${PORT}\n`);
  });
}

bootstrap().catch(err => console.error(err));
