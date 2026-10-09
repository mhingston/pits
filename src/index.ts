import { DurableObject } from "cloudflare:workers";
import { Type } from "@earendil-works/pi-ai";
import { createModels } from "@earendil-works/pi-ai/models";
import { createRegistry, defineTool, Harness } from "@earendil-works/pi-durable";
import { Lifecycle } from "agents/lifecycle";
import { PiHarness } from "agents/harness/pi";
import { createAI } from "agents/models/pi-ai";
import { SandboxRunner } from "./runner";

export { DirectoryBackupGateway } from "@cloudflare/sandbox";

interface Env {
  PITS: DurableObjectNamespace<PitsAgent>;
  BACKUPS: R2Bucket;
  AI: Ai;
  PITS_API_TOKEN: string;
  PITS_ENABLE_FAULTS?: string;
}

export class PitsAgent extends DurableObject<Env> {
  private readonly runner: SandboxRunner;
  readonly ai = createAI({ binding: this.env.AI });
  readonly registry = createRegistry();

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
            replay: "safe", // Safe ONLY by virtue of SandboxRunner's durable deduplication.
            executionMode: "sequential",
            execute: async ({ command }, api) => {
              const result = await this.runner.execute(String(api.taskId), command);
              return {
                output: [{ type: "text", text: JSON.stringify(result) }],
                isError: result.state !== "exited" || result.exitCode !== 0
              };
            }
          })
        ]
      });
      const models = createModels();
      models.setProvider(this.ai.provider);
      return Harness.open(storage, { models, registry: this.registry }, context);
    },
    defaults: { model: this.ai("@cf/moonshotai/kimi-k2.7-code"), thinkingLevel: "low" }
  });

  // Lifecycle owns the one Durable Object alarm; the process runner must not set one.
  readonly lifecycle = Lifecycle.install(this).use(this.harness);

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.runner = new SandboxRunner(ctx);
  }

  // Manual API is deliberately small: S0 can be run without an LLM.
  async runProbe(id: string, command: string) {
    if (!/^[a-z0-9-]{1,63}$/.test(id)) throw new Error("Invalid probe ID");
    return this.runner.execute("probe:" + id, command);
  }
  async checkpoint() { return this.runner.checkpoint(); }
  async restore() { return this.runner.restore(); }
  async inspect() { return this.runner.inspect(); }
  async destroyForTest() { return this.runner.destroyForTest(); }
  async ask(prompt: string) {
    const result = await this.harness.prompt(prompt);
    return { text: result.text };
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
    const agent = env.PITS.getByName("s0");
    try {
      if (pathname === "/api/state" && request.method === "GET") {
        return Response.json(await agent.inspect());
      }
      if (request.method !== "POST") return new Response("Not found", { status: 404 });
      if (pathname === "/api/probe") {
        const body = await request.json() as { id?: unknown; command?: unknown };
        if (typeof body.id !== "string" || typeof body.command !== "string") {
          return new Response("Expected id and command strings", { status: 400 });
        }
        return Response.json(await agent.runProbe(body.id, body.command));
      }
      if (pathname === "/api/checkpoint") return Response.json(await agent.checkpoint());
      if (pathname === "/api/restore") return Response.json(await agent.restore());
      if (pathname === "/api/ask") {
        const body = await request.json() as { prompt?: unknown };
        if (typeof body.prompt !== "string" || !body.prompt.trim() || body.prompt.length > 2000) {
          return new Response("Expected prompt up to 2000 characters", { status: 400 });
        }
        return Response.json(await agent.ask(body.prompt));
      }
      if (pathname === "/api/destroy" && env.PITS_ENABLE_FAULTS === "true") {
        await agent.destroyForTest();
        return Response.json({ destroyed: true });
      }
      return new Response("Not found", { status: 404 });
    } catch (cause) {
      const message = cause instanceof Error ? cause.message : String(cause);
      console.error("pits.s0.failed", { path: pathname, message });
      return Response.json({ error: message }, { status: 409 });
    }
  }
} satisfies ExportedHandler<Env>;
