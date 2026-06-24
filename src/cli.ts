import { io, Socket } from "socket.io-client";
import { Command } from "commander";

const SERVER_URL = process.env.QUEUE_CURE_URL ?? "http://localhost:3000";
const STRESS_COUNT = 50;
const STRESS_WINDOW_MS = 1000;

function createSocket(): Socket {
  return io(SERVER_URL, { transports: ["websocket"], reconnection: false, timeout: 5000 });
}

function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((_, reject) => setTimeout(() => reject(new Error("Timeout")), ms)),
  ]);
}

async function cmdStatus(): Promise<void> {
  const socket = createSocket();
  await withTimeout(new Promise<void>((resolve) => {
    socket.on("connect", () => {
      socket.emit("cli:status", (state: any) => {
        console.log("\n--- CLINIC TELEMETRY ---");
        console.log(`Now Serving: ${state.current ? `Token #${state.current.token} (${state.current.name})` : "None"}`);
        console.log(`Waiting in Queue: ${state.queue.length} patients`);
        console.log(`Pace (WMA): ${Math.round(state.computedWMA)} seconds`);
        console.log("------------------------\n");
        socket.disconnect();
        resolve();
      });
    });
  }), 6000);
}

async function cmdCallNext(): Promise<void> {
  const socket = createSocket();
  await withTimeout(new Promise<void>((resolve) => {
    socket.on("connect", () => {
      socket.emit("queue:callNext", (res: any) => {
        console.log(res?.success ? "✓ Next patient summoned successfully." : "✗ Execution failed.");
        socket.disconnect();
        resolve();
      });
    });
  }), 6000);
}

async function cmdStress(): Promise<void> {
  console.log(`\n☢ Chaos Monkey: Firing ${STRESS_COUNT} inputs in ${STRESS_WINDOW_MS}ms...\n`);
  const socket = createSocket();
  await withTimeout(new Promise<void>((resolve) => {
    socket.on("connect", async () => {
      const results: any[] = [];
      const promises: Promise<void>[] = [];

      for (let i = 0; i < STRESS_COUNT; i++) {
        const delay = Math.random() * STRESS_WINDOW_MS;
        promises.push(new Promise((res) => {
          setTimeout(() => {
            socket.emit("patient:add", { name: `Stress_${i+1}` }, (ack: any) => {
              results.push(ack);
              res();
            });
          }, delay);
        }));
      }

      await Promise.all(promises);
      const uniqueTokens = new Set(results.map(r => r.token));
      console.log(`Requests Transmitted: ${STRESS_COUNT}`);
      console.log(`Tokens Assigned: ${uniqueTokens.size}`);
      console.log(uniqueTokens.size === STRESS_COUNT ? "✓ PASS — Safe transactional isolation.\n" : "✗ FAIL — State Corrupted.\n");
      socket.disconnect();
      resolve();
    });
  }), 15000);
}

const program = new Command();
program.option("--status", "Status Check").option("--call-next", "Call Next").option("--stress", "Stress Test");
program.parse(process.argv);
const opts = program.opts();

async function main() {
  if (opts.status) await cmdStatus();
  else if (opts.callNext) await cmdCallNext();
  else if (opts.stress) await cmdStress();
  else program.outputHelp();
}
main();
