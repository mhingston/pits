import { DirectoryBackup, type DirectoryBackupRecord, type DirectoryBackupGatewayBinding } from "@cloudflare/sandbox";
import {
  addLostSequenceRange,
  decide,
  observationGate,
  forbidUncertainRedispatch,
  classifyObservationFailure,
  receiptWasInvalidated
} from "./recovery.mjs";
import { isSafeForegroundCommand, RUN, STATUS } from "./process-protocol.mjs";

const ROOT = "/var/lib/pits-processes";
const WORKSPACE = "/workspace/pits"; // Created after startup; directory restore works under wrangler dev.
const TIMEOUT_MS = 20 * 60_000;
const MAX_POLL_MS = 120_000;
const POLL_MS = 750;

// Adapted from Cloudflare Sandbox 1.0's background-process recipe.
// The directory is an atomic reservation. Never delete it within a boot.
interface Intent {
  id: string;
  bootId: string;
  digest: string;
  createdAt: number;
}
export interface Result {
  state: "exited" | "unknown" | "lost";
  commandId: string;
  exitCode?: number;
  stdout?: string;
  stderr?: string;
  reason?: string;
  sequence?: number;
}
interface Checkpoint {
  backup: DirectoryBackupRecord;
  bootId: string;
  commandId: string | null;
  createdAt: number;
  sequence: number;
  transcriptAnchor?: {
    sessionId: string;
    entryId: string | null;
    messageCount: number;
  };
}

export type FaultStage =
  | "before_intent"
  | "after_intent"
  | "after_reservation"
  | "after_launch"
  | "after_exit_before_receipt"
  | "after_receipt"
  | "during_backup"
  | "after_backup_before_checkpoint";

export async function commandIdFromTask(taskId: string): Promise<string> {
  return "p" + (await hash("task:" + taskId)).slice(0, 32);
}
async function hash(text: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, "0")).join("");
}
function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export class SandboxRunner {
  private readonly backups: DirectoryBackup;
  private readonly container: Container;
  private readonly storage: DurableObjectStorage;

  constructor(private readonly ctx: DurableObjectState) {
    if (!ctx.container) throw new Error("Sandbox container binding is missing");
    this.container = ctx.container;
    this.storage = ctx.storage;
    this.backups = new DirectoryBackup(
      ctx.container,
      // Wrangler types do not yet include this WorkerEntrypoint on Exports;
      // the gateway is exported by src/index.ts as required by SDK 1.0.
      (ctx.exports as unknown as { DirectoryBackupGateway: DirectoryBackupGatewayBinding }).DirectoryBackupGateway,
      { binding: "BACKUPS", prefix: "pits-s0/" }
    );
    // A DO restart terminates any in-flight DirectoryBackup helper. Therefore
    // a persisted maintenance marker at construction is stale and must not
    // wedge checkpoint/restore forever. PiHarness owns alarms; never set one.
    ctx.blockConcurrencyWhile(async () => {
      if (await this.storage.get<boolean>("maintenance")) await this.storage.delete("maintenance");
      if (this.container.running) await this.container.setInactivityTimeout(TIMEOUT_MS);
    });
  }

  private async ensureContainer(): Promise<{ bootId: string }> {
    if (!this.container.running) {
      this.container.start({
        image: this.container.images.sandbox,
        instance: "lite",
        enableInternet: false
      });
      await this.container.setInactivityTimeout(TIMEOUT_MS);
    }
    const directory = await this.sh(["mkdir", "-p", WORKSPACE]);
    if (directory.exitCode !== 0) throw new Error("Cannot initialise workspace: " + directory.stderr);
    const bootId = (await this.sh(["cat", "/proc/sys/kernel/random/boot_id"])).stdout.trim();
    if (!bootId) throw new Error("Cannot establish container boot identity");
    const previous = await this.storage.get<string>("boot");
    if (previous !== undefined && previous !== bootId) {
      // Do not implicitly restore: the previous instance may have left tool
      // effects past its last committed workspace snapshot.
      await this.storage.put("restore-required", true);
    }
    await this.storage.put("boot", bootId);
    return { bootId };
  }

  private async sh(argv: string[], cwd?: string): Promise<{ exitCode: number; stdout: string; stderr: string }> {
    const proc = await this.container.exec(argv, cwd ? { cwd } : undefined);
    const out = await proc.output();
    const decode = (bytes: Uint8Array | ArrayBuffer) => new TextDecoder().decode(bytes);
    return { exitCode: out.exitCode, stdout: decode(out.stdout), stderr: decode(out.stderr) };
  }

  private async status(id: string): Promise<{ kind: "missing" | "starting" | "running" | "exited" | "lost"; exitCode?: number }> {
    const { stdout } = await this.sh(["sh", "-c", STATUS, "sh", ROOT + "/" + id]);
    const [name, code] = stdout.trim().split(" ");
    if (name === "exited") return { kind: "exited", exitCode: Number(code) };
    if (name === "missing" || name === "running" || name === "starting" || name === "lost") {
      return { kind: name };
    }
    throw new Error("Unrecognised process state: " + stdout);
  }

  private async injectFault(requested: FaultStage | undefined, stage: FaultStage): Promise<void> {
    if (requested !== stage) return;

    this.ctx.abort("pits fault injection: " + stage, { retryAlarm: false });
    // abort() initiates a reset, but returning through fast, already-resolved
    // container/storage calls can otherwise let the request complete before
    // the runtime tears this instance down. Keep this invocation suspended so
    // the injected crash is deterministic at the requested boundary.
    await new Promise<void>(() => {});
  }

  async execute(taskId: string, command: string, faultAt?: FaultStage): Promise<Result> {
    if (!command.trim() || command.length > 4096) throw new Error("Command must be 1..4096 characters");
    if (!isSafeForegroundCommand(command)) {
      throw new Error("Command is outside the S0 foreground command subset");
    }
    const id = await commandIdFromTask(taskId);
    const digest = await hash(command);
    const receipt = await this.storage.get<Result>("receipt:" + id);
    if (receipt) {
      const original = await this.storage.get<Intent>("intent:" + id);
      if (!original || original.digest !== digest) throw new Error("Command ID reused for different arguments");
      const lostRanges = await this.storage.get<{ afterSequence: number; throughSequence: number }[]>("lost-sequence-ranges") ?? [];
      if (receiptWasInvalidated(receipt.sequence, lostRanges)) {
        return { state: "lost", commandId: id, reason: "Receipt postdates a restored workspace; never auto-replay" };
      }
      // Read the legacy single-range markers written by earlier S0 builds.
      const restoredFloor = await this.storage.get<number>("restore-floor-sequence");
      const restoredCeiling = await this.storage.get<number>("restore-ceiling-sequence");
      if (restoredFloor !== undefined && restoredCeiling !== undefined &&
          (receipt.sequence ?? Number.MAX_SAFE_INTEGER) > restoredFloor &&
          (receipt.sequence ?? Number.MAX_SAFE_INTEGER) <= restoredCeiling) {
        return { state: "lost", commandId: id, reason: "Receipt postdates restored workspace; never auto-replay" };
      }
      return receipt;
    }

    const { bootId } = await this.ensureContainer();
    let intent = await this.storage.get<Intent>("intent:" + id);
    if (intent && intent.digest !== digest) throw new Error("Command ID reused for different arguments");

    // Old intent on a replacement container is never redispatched.
    if (intent && intent.bootId !== bootId) {
      await this.storage.put("restore-required", true);
      return { state: "lost", commandId: id, reason: "Container boot changed; restore and reconcile" };
    }
    // An unknown or timed-out command retains the 'active' lock. A retry
    // of that exact task may observe its *existing* process and collect an
    // eventual exit receipt, but cannot launch a second command.
    // Workspace loss or an unacknowledged restore always blocks execution.
    if (observationGate({
      active: await this.storage.get<string>("active"),
      requested: id,
      restoreRequired: (await this.storage.get<boolean>("restore-required")) ?? false,
      reconciliationRequired: (await this.storage.get<boolean>("reconciliation-required")) ?? false
    }) === "blocked") {
      throw new Error("Workspace unavailable or another command is active; recovery required");
    }

    // Persistent single-writer gate shared with workspace checkpointing.
    // The Pi tool additionally requests executionMode: sequential.
    await this.injectFault(faultAt, "before_intent");
    await this.storage.transaction(async tx => {
      if (await tx.get<boolean>("maintenance")) throw new Error("Workspace maintenance in progress");
      const active = await tx.get<string>("active");
      if (active && active !== id) throw new Error("Another mutating command is still active");
      const existing = await tx.get<Intent>("intent:" + id);
      if (!existing) {
        const next: Intent = { id, bootId, digest, createdAt: Date.now() };
        await tx.put("intent:" + id, next);
        intent = next;
      } else if (existing.bootId !== bootId || existing.digest !== digest) {
        throw new Error("Intent changed during reservation");
      } else {
        intent = existing;
      }
      await tx.put("active", id);
    });
    if (!intent) throw new Error("Durable command intent was not established");
    await this.injectFault(faultAt, "after_intent");

    let observation: Awaited<ReturnType<SandboxRunner["status"]>>;
    try {
      observation = await this.status(id);
    } catch {
      return this.observationFailure(id, intent, "Process observation failed after intent commit");
    }
    const action = forbidUncertainRedispatch(
      decide(intent, bootId, observation.kind),
      (await this.storage.get<boolean>("no-redispatch:" + id)) ?? false
    );
    if (action === "lost" || action === "unknown") {
      return this.ambiguous(id, action, "Process state cannot be proven; never redispatch after uncertainty");
    }
    if (action === "dispatch") {
      await this.sh(["mkdir", "-p", ROOT]);
      const reserved = await this.sh(["mkdir", ROOT + "/" + id]);
      if (reserved.exitCode === 0) {
        // The DO intent was stored BEFORE the container's atomic mkdir.
        // A crash after mkdir but before exec is UNKNOWN, not a retry.
        await this.injectFault(faultAt, "after_reservation");
        try {
          await this.container.exec(
            ["sh", "-c", RUN, "sh", ROOT + "/" + id, "sh", "-lc", command],
            { cwd: WORKSPACE, stdout: "ignore", stderr: "ignore" }
          );
        } catch {
          // The launch acknowledgement can be lost after the container accepted
          // the process. Keep the reservation and never issue a second launch.
          return this.observationFailure(id, intent, "Process launch acknowledgement was lost");
        }
        await this.injectFault(faultAt, "after_launch");
      }
      // If mkdir lost a concurrent race, follow the winner.
    }
    const started = Date.now();
    while (Date.now() - started < MAX_POLL_MS) {
      let now: Awaited<ReturnType<SandboxRunner["status"]>>;
      try {
        now = await this.status(id);
      } catch {
        return this.observationFailure(id, intent, "Process state could not be observed after dispatch");
      }
      if (now.kind === "exited") {
        await this.injectFault(faultAt, "after_exit_before_receipt");
        const stdout = await this.sh(["sh", "-c", 'tail -c 32768 "$1/stdout.log"', "sh", ROOT + "/" + id]);
        const stderr = await this.sh(["sh", "-c", 'tail -c 32768 "$1/stderr.log"', "sh", ROOT + "/" + id]);
        const result: Result = {
          state: "exited", commandId: id, exitCode: now.exitCode,
          stdout: stdout.stdout, stderr: stderr.stdout
        };
        await this.storage.transaction(async tx => {
          const existing = await tx.get<Result>("receipt:" + id);
          if (existing) { Object.assign(result, existing); return; }
          const sequence = (await tx.get<number>("command-sequence") ?? 0) + 1;
          result.sequence = sequence;
          await tx.put("command-sequence", sequence);
          await tx.put("receipt:" + id, result);
          await tx.delete("no-redispatch:" + id);
          await tx.put("last-command", id);
          if (await tx.get<string>("active") === id) await tx.delete("active");
        });
        await this.injectFault(faultAt, "after_receipt");
        return result;
      }
      if (now.kind === "lost") return this.ambiguous(id, "unknown", "Launcher ended without exit receipt");
      if (now.kind === "missing") return this.ambiguous(id, "unknown", "Reservation disappeared on same boot");
      if (now.kind === "starting" && Date.now() - started > 10_000) {
        return this.ambiguous(id, "unknown", "Reservation has no PID after launch grace period");
      }
      await delay(POLL_MS);
    }
    return this.ambiguous(id, "unknown", "Execution deadline; process may still be running");
  }

  private async ambiguous(id: string, state: "unknown" | "lost", reason: string): Promise<Result> {
    // Fail closed. Keep the active lock and a durable no-dispatch marker.
    // Another request for this *same* task may poll and collect a late exit,
    // which releases the lock atomically with its receipt. Unlike a lost
    // container boot, an observation timeout is not workspace loss.
    await this.storage.put("no-redispatch:" + id, true);
    return { commandId: id, state, reason };
  }

  private async observationFailure(id: string, intent: Intent, reason: string): Promise<Result> {
    let observedBootId: string | undefined;
    try {
      observedBootId = (await this.ensureContainer()).bootId;
    } catch {
      // The existing process remains ambiguous if the container cannot be
      // restarted or inspected. The durable active lock is intentionally kept.
    }
    const classification = classifyObservationFailure(intent.bootId, observedBootId);
    if (classification === "lost") await this.storage.put("restore-required", true);
    if (classification === "lost") {
      return this.ambiguous(id, classification, "Container boot changed during process observation");
    }
    return this.ambiguous(id, "unknown", reason);
  }

  private async beginMaintenance(): Promise<void> {
    await this.storage.transaction(async tx => {
      if (await tx.get<boolean>("maintenance")) throw new Error("Maintenance already running");
      if (await tx.get<string>("active")) throw new Error("In-flight or ambiguous command; cannot snapshot");
      await tx.put("maintenance", true);
    });
  }

  async checkpoint(
    faultAt?: FaultStage,
    transcriptAnchor?: Checkpoint["transcriptAnchor"]
  ): Promise<Checkpoint> {
    await this.beginMaintenance();
    try {
      const { bootId } = await this.ensureContainer();
      if (await this.storage.get<boolean>("restore-required") ||
          await this.storage.get<boolean>("reconciliation-required")) {
        throw new Error("Cannot checkpoint a workspace requiring reconciliation");
      }
      // S0 forbids detached children and external workspace writers. The
      // active-command gate alone cannot establish their absence.
      if (faultAt === "during_backup") {
        await this.storage.delete("test-backup-interruption-fired-at");
        const fixture = await this.sh([
          // Keep the fixture visible: DirectoryBackup's archive traversal
          // excludes hidden implementation files, so a dotfile could make the
          // intended slow transfer finish before the injected interruption.
          "dd", "if=/dev/urandom", `of=${WORKSPACE}/backup-interruption-fixture.bin`,
          "bs=1M", "count=64"
        ]);
        if (fixture.exitCode !== 0) throw new Error("Cannot prepare backup interruption fixture: " + fixture.stderr);
      }
      const options = {
        dir: WORKSPACE, exclude: ["node_modules/", ".cache/"]
      };
      let backupCompleted = false;
      const backupPromise = this.backups.backup(options).then(record => {
        backupCompleted = true;
        return record;
      });
      if (faultAt === "during_backup") {
        const timer = setTimeout(async () => {
          if (backupCompleted) return;
          await this.storage.put("test-backup-interruption-fired-at", Date.now());
          this.ctx.abort("pits fault injection: during_backup", { retryAlarm: false });
        }, 1_000);
        try {
          await backupPromise;
        } finally {
          clearTimeout(timer);
        }
        if (!(await this.storage.get<number>("test-backup-interruption-fired-at"))) {
          throw new Error("Backup completed before the controlled interruption; increase the fixture size");
        }
        // ctx.abort() should have terminated this invocation before this point.
        throw new Error("DO interruption did not terminate the active backup");
      }
      const backup = await backupPromise;
      await this.injectFault(faultAt, "after_backup_before_checkpoint");
      const point: Checkpoint = {
        backup, bootId,
        commandId: await this.storage.get<string>("last-command") ?? null,
        createdAt: Date.now(),
        sequence: await this.storage.get<number>("command-sequence") ?? 0,
        ...(transcriptAnchor ? { transcriptAnchor } : {})
      };
      await this.storage.put("checkpoint", point);
      return point;
    } finally {
      await this.storage.delete("maintenance");
    }
  }

  async restore(): Promise<Checkpoint> {
    // Unlike snapshotting, a restore may follow an interrupted command on a
    // *different* container boot. Never restore over a live known process.
    await this.storage.transaction(async tx => {
      if (await tx.get<boolean>("maintenance")) throw new Error("Maintenance already running");
      await tx.put("maintenance", true);
    });
    try {
      const point = await this.storage.get<Checkpoint>("checkpoint");
      if (!point) throw new Error("No committed backup");
      const { bootId } = await this.ensureContainer();
      const active = await this.storage.get<string>("active");
      if (active) {
        const intent = await this.storage.get<Intent>("intent:" + active);
        if (intent?.bootId === bootId) {
          throw new Error("Cannot restore over a possibly running command in this boot");
        }
      }
      await this.backups.restore(point.backup);
      await this.storage.transaction(async tx => {
        await tx.delete("active");
        await tx.put("restore-required", false);
        const ceiling = await tx.get<number>("command-sequence") ?? 0;
        const lostRanges = await tx.get<{ afterSequence: number; throughSequence: number }[]>("lost-sequence-ranges") ?? [];
        await tx.put("lost-sequence-ranges", addLostSequenceRange(lostRanges, point.sequence, ceiling));
        await tx.put("restore-floor-sequence", point.sequence);
        await tx.put("restore-ceiling-sequence", ceiling);
        await tx.put("reconciliation-required", true);
        await tx.put("restored-checkpoint", point);
      });
      return point;
    } finally {
      await this.storage.delete("maintenance");
    }
  }

  async readEvidence(relativePath: string): Promise<{ path: string; text: string }> {
    // Deliberately read-only so operators can examine a restored workspace
    // before acknowledging divergence. No shell interpolation.
    if (!/^[a-zA-Z0-9._/-]{1,200}$/.test(relativePath) ||
        relativePath.split("/").some(part => part === "." || part === ".." || part === "")) {
      throw new Error("Invalid evidence path");
    }
    await this.ensureContainer();
    const path = WORKSPACE + "/" + relativePath;
    const result = await this.sh(["head", "-c", "8192", path]);
    if (result.exitCode !== 0) throw new Error("Cannot read evidence: " + result.stderr);
    return { path: relativePath, text: result.stdout };
  }

  async acknowledgeReconciliation(checkpointId: string): Promise<void> {
    const point = await this.storage.get<Checkpoint>("restored-checkpoint");
    if (!point || point.backup.id !== checkpointId) throw new Error("Checkpoint mismatch");
    if (await this.storage.get<boolean>("restore-required")) throw new Error("Restore still required");
    await this.storage.transaction(async tx => {
      await tx.put("reconciliation-required", false);
      await tx.put("reconciled-at", Date.now());
    });
  }

  async inspect() {
    const boot = await this.storage.get<string>("boot");
    const active = await this.storage.get<string>("active");
    // Inspection may overlap a container start/stop or replacement. A failed
    // observation is diagnostic uncertainty, not a reason to fail the whole
    // state request (and never evidence that the command is safe to replay).
    const processObservation = active
      ? await this.status(active).catch(() => ({ kind: "unknown" as const }))
      : null;
    return {
      boot,
      containerRunning: this.container.running,
      active,
      activeIntent: active ? await this.storage.get<Intent>("intent:" + active) : null,
      processObservation,
      backupInterruptionFiredAt: await this.storage.get<number>("test-backup-interruption-fired-at") ?? null,
      restoreRequired: (await this.storage.get<boolean>("restore-required")) ?? false,
      reconciliationRequired: (await this.storage.get<boolean>("reconciliation-required")) ?? false,
      commandSequence: await this.storage.get<number>("command-sequence") ?? 0,
      checkpoint: (await this.storage.get<Checkpoint>("checkpoint")) ?? null,
      restoredCheckpoint: (await this.storage.get<Checkpoint>("restored-checkpoint")) ?? null
    };
  }

  async destroyForTest(): Promise<void> {
    if (this.container.running) await this.container.destroy();
    await this.storage.put("restore-required", true);
  }

  abortForTest(): void {
    // Leave Lifecycle's pending alarm eligible to wake PiHarness after reset.
    this.ctx.abort("pits fault injection: Durable Object interruption");
  }
}
