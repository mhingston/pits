import { DurableObject } from "cloudflare:workers";
import { Type, fauxAssistantMessage, fauxProvider, fauxToolCall, type TranscriptContext } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import { createRegistry, defineTool, Harness } from "@earendil-works/pi-durable";
import { Lifecycle } from "agents/lifecycle";
import { PiHarness } from "agents/harness/pi";
import { createAI } from "agents/models/pi-ai";
import { SandboxRunner } from "./runner";
import type { FaultStage } from "./runner";

const FIXTURE_PROMPT = /^pits-fixture:([a-z0-9-]{1,63})$/;
const PI_REOBSERVE_DEADLINE_MS = 4 * 60_000;
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

function fixtureText(context: TranscriptContext): { marker: string; toolState?: string } | undefined {
  let index = -1;
  for (let i = context.messages.length - 1; i >= 0; i--) {
    if (context.messages[i].role === "user") { index = i; break; }
  }
  if (index < 0) return undefined;
  const content = context.messages[index].content;
  const text = typeof content === "string" ? content : content.map(block => block.type === "text" ? block.text : "").join("\n");
  const match = FIXTURE_PROMPT.exec(text.trim());
  if (!match) return undefined;
  const toolResult = context.messages.slice(index + 1).find(message =>
    message.role === "toolResult" && message.toolName === "sandbox_bash");
  if (!toolResult) return { marker: match[1] };
  const toolText = typeof toolResult.content === "string"
    ? toolResult.content
    : toolResult.content.filter(block => block.type === "text").map(block => block.text).join("\n");
  try {
    const result = JSON.parse(toolText) as { state?: unknown };
    return { marker: match[1], toolState: typeof result.state === "string" ? result.state : "unknown" };
  } catch {
    return { marker: match[1], toolState: "unknown" };
  }
}

export { DirectoryBackupGateway } from "@cloudflare/sandbox";

interface Env {
  PITS: DurableObjectNamespace<PitsAgent>;
  BACKUPS: R2Bucket;
  AI: Ai;
  PITS_API_TOKEN: string;
  PITS_ENABLE_FAULTS?: string;
  PITS_ENABLE_FIXTURE?: string;
  PITS_ENABLE_MODEL?: string;
}

const FAULT_STAGES: readonly FaultStage[] = [
  "before_intent", "after_intent", "after_reservation", "after_launch",
  "after_exit_before_receipt", "after_receipt", "during_backup", "after_backup_before_checkpoint"
];
const CHECKPOINT_FAULT_STAGES: readonly FaultStage[] = ["during_backup", "after_backup_before_checkpoint"];

export class PitsAgent extends DurableObject<Env> {
  private readonly runner: SandboxRunner;
  readonly ai = createAI({ binding: this.env.AI });
  readonly registry = createRegistry();
  readonly fixture = fauxProvider({
    api: "pits-s0-fixture",
    provider: "pits-s0-fixture",
    tokensPerSecond: 8,
    models: [{
      id: "deterministic",
      name: "PITS S0 deterministic fixture",
      reasoning: false,
      input: ["text"],
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
      contextWindow: 4096,
      maxTokens: 256
    }]
  });

  readonly harness = new PiHarness({
    harness: async ({ storage, context }) => {
      this.registry.install({
        name: "pits-s0",
        sections: [{
          key: "preamble",
          tag: false,
          render: () => [
            "You are the S0 recovery-test agent.",
            "Use sandbox_bash only for controlled, short-lived shell work.",
            "No background jobs, daemons, remote network access or secrets.",
            "A tool outcome of lost or unknown is NOT permission to retry.",
            "After workspace recovery, inspect and reverify changes."
          ].join(" ")
        }],
        tools: [
          defineTool({
            name: "sandbox_bash",
            description: "Execute a bounded foreground command in the isolated workspace. Never launch detached processes.",
            parameters: Type.Object({ command: Type.String({ minLength: 1, maxLength: 4096 }) }),
            // Pi re-enters the adapter after eviction using this durable task ID.
            // The runner reattaches a same-boot reservation/receipt and only
            // dispatches when reservation absence proves the command never started.
            replay: "safe",
            executionMode: "sequential",
            execute: async ({ command }, api) => {
              const taskId = String(api.taskId);
              const started = Date.now();
              let reobservations = 0;
              let result = await this.runner.execute(taskId, command);
              while (result.state === "unknown" && Date.now() - started < PI_REOBSERVE_DEADLINE_MS) {
                // Keep a possibly-running Pi tool open after an ambiguous
                // observation. The stable task ID can only reattach/collect;
                // runner no-redispatch state forbids a second launch.
                await delay(1_000);
                reobservations++;
                result = await this.runner.execute(taskId, command);
              }
              return {
                content: [{ type: "text", text: JSON.stringify({ ...result, reobservations }) }],
                isError: result.state !== "exited" || result.exitCode !== 0
              };
            }
          })
        ]
      });
      const models = createModels();
      if (this.env.PITS_ENABLE_FIXTURE === "true") {
        models.setProvider(this.fixture.provider);
        this.fixture.setResponses(Array.from({ length: 32 }, () => (context: TranscriptContext) => {
          const fixture = fixtureText(context);
          if (!fixture) return fauxAssistantMessage("fixture prompt was not recognised");
          if (fixture.toolState !== undefined) {
            if (fixture.toolState !== "exited") {
              return fauxAssistantMessage(`fixture observed ${fixture.toolState}; filesystem result is unverified and reconciliation is required`);
            }
            return fauxAssistantMessage(("fixture complete " + fixture.marker + " ").repeat(18));
          }
          const command = fixture.marker.startsWith("replace-")
            ? `sleep 15 && printf '%s\\n' ${fixture.marker} >> /workspace/pits/pi-fixture.txt`
            : `printf '%s\\n' ${fixture.marker} >> /workspace/pits/pi-fixture.txt`;
          return fauxAssistantMessage(fauxToolCall(
            "sandbox_bash",
            { command },
            { id: "pits-fixture-" + fixture.marker }
          ), { stopReason: "toolUse" });
        }));
      } else {
        models.setProvider(this.ai.provider);
      }
      return Harness.open(storage, { models, registry: this.registry }, context);
    },
    defaults: {
      model: this.env.PITS_ENABLE_FIXTURE === "true"
        ? this.fixture.getModel()
        : this.ai("@cf/moonshotai/kimi-k2.7-code"),
      thinkingLevel: "low"
    }
  });

  // Lifecycle owns the one Durable Object alarm; the process runner must not set one.
  readonly lifecycle = Lifecycle.install(this).use(this.harness);

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.runner = new SandboxRunner(ctx);
  }

  // Manual API is deliberately small: S0 can be run without an LLM.
  async runProbe(id: string, command: string, faultAt?: FaultStage) {
    if (!/^[a-z0-9-]{1,63}$/.test(id)) throw new Error("Invalid probe ID");
    if (faultAt && this.env.PITS_ENABLE_FAULTS !== "true") throw new Error("Fault injection is disabled");
    if (faultAt && CHECKPOINT_FAULT_STAGES.includes(faultAt)) throw new Error("Fault stage only applies to checkpointing");
    return this.runner.execute("probe:" + id, command, faultAt);
  }
  async checkpoint(faultAt?: FaultStage) {
    if (faultAt && this.env.PITS_ENABLE_FAULTS !== "true") throw new Error("Fault injection is disabled");
    if (faultAt && !CHECKPOINT_FAULT_STAGES.includes(faultAt)) throw new Error("Invalid checkpoint fault stage");
    const session = this.harness.session();
    if (await session.busy()) throw new Error("Pi session must be idle before checkpointing");
    const messages = await session.messages();
    return this.runner.checkpoint(faultAt, {
      sessionId: session.id,
      entryId: messages.at(-1) ? String(messages.at(-1)!.id) : null,
      messageCount: messages.length
    });
  }
  async restore() {
    const session = this.harness.session();
    if (await session.busy()) throw new Error("Pi session must be idle before restoring a workspace");
    const checkpoint = await this.runner.restore();
    const anchor = checkpoint.transcriptAnchor;
    const handoff = [
      `Workspace restored to committed checkpoint ${checkpoint.backup.id}.`,
      anchor
        ? `The checkpoint captured Pi session ${anchor.sessionId} after transcript entry ${anchor.entryId ?? "(empty transcript)"} (${anchor.messageCount} active entries).`
        : "The checkpoint predates transcript anchoring.",
      "This Pi context starts at the restore boundary; prior tool results after the checkpoint are not authoritative for the current filesystem.",
      "Re-inspect and re-verify relevant files. Workspace mutations remain blocked until the operator explicitly reconciles the restored checkpoint."
    ].join(" ");
    await session.reset(handoff);
    return checkpoint;
  }
  async readEvidence(path: string) { return this.runner.readEvidence(path); }
  async reconcile(checkpointId: string) { return this.runner.acknowledgeReconciliation(checkpointId); }
  async inspect() {
    const runner = await this.runner.inspect();
    const messages = await this.harness.messages();
    const pending = await this.harness.pending();
    return {
      ...runner,
      pi: {
        messageCount: messages.length,
        activeEntryKinds: messages.map(message => message.kind),
        pendingCount: pending.length,
        sessions: await this.harness.sessions.list(),
        lifecycleAlarm: await this.ctx.storage.getAlarm(),
        fixtureModelCalls: this.fixture.state.callCount
      }
    };
  }
  async destroyForTest() { return this.runner.destroyForTest(); }
  async ask(prompt: string) {
    const result = await this.harness.prompt(prompt);
    return { text: result.text };
  }
  async askFixture(marker: string) {
    if (this.env.PITS_ENABLE_FIXTURE !== "true") throw new Error("Deterministic fixture is disabled");
    if (!/^[a-z0-9-]{1,63}$/.test(marker)) throw new Error("Invalid fixture marker");
    const result = await this.harness.prompt("pits-fixture:" + marker, { operationId: "fixture-" + marker });
    const toolResult = result.messages
      .flatMap(entry => entry.model ?? [])
      .filter(message => message.role === "toolResult" && message.toolName === "sandbox_bash")
      .reverse()
      .find(message => message.role === "toolResult");
    let command: {
      commandId?: string; state?: string; exitCode?: number; reobservations?: number
    } | null = null;
    if (toolResult) {
      const text = toolResult.content.filter(block => block.type === "text").map(block => block.text).join("\n");
      try {
        const parsed = JSON.parse(text) as {
          commandId?: unknown; state?: unknown; exitCode?: unknown; reobservations?: unknown
        };
        command = {
          ...(typeof parsed.commandId === "string" ? { commandId: parsed.commandId } : {}),
          ...(typeof parsed.state === "string" ? { state: parsed.state } : {}),
          ...(typeof parsed.exitCode === "number" ? { exitCode: parsed.exitCode } : {}),
          ...(typeof parsed.reobservations === "number" ? { reobservations: parsed.reobservations } : {})
        };
      } catch { /* Pi keeps the tool entry even if its payload was truncated. */ }
    }
    return {
      text: result.text,
      messageCount: result.messages.length,
      modelCalls: this.fixture.state.callCount,
      command
    };
  }
  async abortForTest() {
    if (this.env.PITS_ENABLE_FAULTS !== "true") throw new Error("Fault injection is disabled");
    this.runner.abortForTest();
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const pathname = new URL(request.url).pathname;
    if (pathname === "/health") return Response.json({ service: "pits-s0" });
    // Fail closed: no default development token, no unauthenticated sandbox exec.
    if (!env.PITS_API_TOKEN || request.headers.get("Authorization") !== "Bearer " + env.PITS_API_TOKEN) {
      return new Response("Unauthorized", { status: 401 });
    }
    let objectName = "s0";
    if (env.PITS_ENABLE_FAULTS === "true" && env.PITS_ENABLE_FIXTURE === "true") {
      const testId = request.headers.get("X-PITS-Test-ID");
      if (!testId || !/^[a-z0-9-]{1,63}$/.test(testId)) {
        return new Response("Missing or invalid test object ID", { status: 400 });
      }
      // Reuse one isolated Worker/Container application while giving each
      // destructive integration run a clean Durable Object identity.
      objectName = "test-" + testId;
    }
    if (pathname === "/api/test/status" && request.method === "GET") {
      return Response.json({
        faultsEnabled: env.PITS_ENABLE_FAULTS === "true",
        fixtureEnabled: env.PITS_ENABLE_FIXTURE === "true",
        objectName,
        requestedTestId: request.headers.get("X-PITS-Test-ID")
      });
    }
    const agent = env.PITS.getByName(objectName);
    try {
      if (pathname === "/api/evidence" && request.method === "GET") {
        const path = new URL(request.url).searchParams.get("path");
        if (!path) return new Response("Missing path", { status: 400 });
        return Response.json(await agent.readEvidence(path));
      }
      if (pathname === "/api/state" && request.method === "GET") {
        return Response.json(await agent.inspect());
      }
      if (request.method !== "POST") return new Response("Not found", { status: 404 });
      if (pathname === "/api/probe") {
        const body = await request.json() as { id?: unknown; command?: unknown; faultAt?: unknown };
        if (typeof body.id !== "string" || typeof body.command !== "string") {
          return new Response("Expected id and command strings", { status: 400 });
        }
        if (body.faultAt !== undefined && typeof body.faultAt !== "string") {
          return new Response("faultAt must be a test fault stage", { status: 400 });
        }
        if (body.faultAt !== undefined && !FAULT_STAGES.includes(body.faultAt as FaultStage)) {
          return new Response("Unknown test fault stage", { status: 400 });
        }
        return Response.json(await agent.runProbe(body.id, body.command, body.faultAt as FaultStage | undefined));
      }
      if (pathname === "/api/checkpoint") {
        const body = request.headers.get("Content-Type")?.includes("application/json")
          ? await request.json() as { faultAt?: unknown }
          : {};
        if (body.faultAt !== undefined && typeof body.faultAt !== "string") {
          return new Response("faultAt must be a test fault stage", { status: 400 });
        }
        if (body.faultAt !== undefined && !FAULT_STAGES.includes(body.faultAt as FaultStage)) {
          return new Response("Unknown test fault stage", { status: 400 });
        }
        return Response.json(await agent.checkpoint(body.faultAt as FaultStage | undefined));
      }
      if (pathname === "/api/restore") return Response.json(await agent.restore());
      if (pathname === "/api/reconcile") {
        const body = await request.json() as { checkpointId?: unknown };
        if (typeof body.checkpointId !== "string") return new Response("Expected checkpointId", { status: 400 });
        await agent.reconcile(body.checkpointId);
        return Response.json({ reconciled: true });
      }
      if (pathname === "/api/ask" && env.PITS_ENABLE_MODEL === "true") {
        const body = await request.json() as { prompt?: unknown };
        if (typeof body.prompt !== "string" || !body.prompt.trim() || body.prompt.length > 2000) {
          return new Response("Expected prompt up to 2000 characters", { status: 400 });
        }
        return Response.json(await agent.ask(body.prompt));
      }
      if (pathname === "/api/ask-fixture" && env.PITS_ENABLE_FIXTURE === "true") {
        const body = await request.json() as { marker?: unknown };
        if (typeof body.marker !== "string") return new Response("Expected marker string", { status: 400 });
        return Response.json(await agent.askFixture(body.marker));
      }
      if (pathname === "/api/destroy" && env.PITS_ENABLE_FAULTS === "true") {
        await agent.destroyForTest();
        return Response.json({ destroyed: true });
      }
      if (pathname === "/api/abort" && env.PITS_ENABLE_FAULTS === "true") {
        await agent.abortForTest();
        return Response.json({ aborted: true });
      }
      return new Response("Not found", { status: 404 });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      console.error("pits.s0.failed", { path: pathname, message });
      return Response.json({ error: message }, { status: 409 });
    }
  }
} satisfies ExportedHandler<Env>;
