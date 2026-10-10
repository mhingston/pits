#!/usr/bin/env node
import assert from "node:assert/strict";
import { createHash, randomBytes } from "node:crypto";
import { mkdirSync, appendFileSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { spawn } from "node:child_process";
import { execFileSync } from "node:child_process";

const baseUrl = process.env.PITS_URL;
const token = process.env.PITS_API_TOKEN;
const accessCookie = process.env.PITS_ACCESS_COOKIE;
const workerName = process.env.PITS_WORKER_NAME ?? "pits-s0-test-recovery";
const predeployed = process.env.PITS_TEST_PREDEPLOYED === "true";
const iterations = Number(process.env.PITS_ITERATIONS ?? 10);
const onlyFaultStage = process.env.PITS_TEST_ONLY_FAULT_STAGE;
const testId = process.env.PITS_TEST_ID ??
  `${new Date().toISOString().replace(/[^0-9]/g, "").slice(0, 14)}-${randomBytes(4).toString("hex")}`;
const artifactPath = resolve(process.env.PITS_EVIDENCE ?? `artifacts/s0-${testId}.jsonl`);
const logsPath = resolve(process.env.PITS_CLOUDFLARE_LOGS ?? `artifacts/s0-${testId}.cloudflare.jsonl`);

assert.ok(baseUrl, "Set PITS_URL to the disposable Cloudflare Worker URL");
assert.ok(token && token.length >= 32, "Set PITS_API_TOKEN to the locally held test secret (at least 32 characters)");
assert.ok(Number.isInteger(iterations) && iterations >= 10 && iterations <= 100, "PITS_ITERATIONS must be 10..100");
assert.ok(onlyFaultStage === undefined || onlyFaultStage === "after_exit_before_receipt",
  "PITS_TEST_ONLY_FAULT_STAGE currently supports after_exit_before_receipt only");
const base = new URL(baseUrl);
assert.equal(base.protocol, "https:", "Live integration tests require HTTPS");
assert.ok(!["localhost", "127.0.0.1", "::1"].includes(base.hostname), "Local emulation is not live Cloudflare evidence");
const testConfig = JSON.parse(readFileSync(new URL("../wrangler.s0-test.jsonc", import.meta.url), "utf8"));
assert.equal(workerName, testConfig.name,
  "PITS_WORKER_NAME must match wrangler.s0-test.jsonc name so the Container application identity is stable");
assert.ok(base.hostname.startsWith(`${workerName}.`), `PITS_URL must target the isolated ${workerName} hostname`);

mkdirSync(dirname(artifactPath), { recursive: true });
mkdirSync(dirname(logsPath), { recursive: true });
const sourceCommit = execFileSync("git", ["rev-parse", "HEAD"], { encoding: "utf8" }).trim();
const lock = JSON.parse(readFileSync(new URL("../package-lock.json", import.meta.url), "utf8"));
const versions = Object.fromEntries(["@cloudflare/sandbox", "agents", "@earendil-works/pi-durable", "@earendil-works/pi-ai", "wrangler"]
  .map(name => [name, lock.packages[`node_modules/${name}`]?.version ?? "unknown"]));
const common = { testId, worker: workerName, sourceCommit, versions, containerImage: "cloudflare/sandbox:1.0.0" };
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const sha256 = value => createHash("sha256").update(value).digest("hex");
const redact = value => String(value)
  .replaceAll(token, "[REDACTED]")
  .replace(accessCookie ?? "\u0000", "[ACCESS-REDACTED]")
  .replace(/("(?:cf-connecting-ip|x-real-ip|x-forwarded-for)"\s*:\s*")[^"]+(\")/gi,
    (match, prefix, suffix) => prefix + "[IP-REDACTED]" + suffix)
  .replace(/(Bearer\s+)[^\s"']+/gi, "$1[REDACTED]");

function record(scenario, fields = {}) {
  const row = { ...common, at: new Date().toISOString(), scenario, ...fields };
  appendFileSync(artifactPath, JSON.stringify(row) + "\n", { mode: 0o600 });
  console.log(JSON.stringify(row));
  return row;
}

async function request(path, body, { auth = true, timeoutMs = 190_000 } = {}) {
  const headers = {};
  if (auth) headers.Authorization = `Bearer ${token}`;
  if (accessCookie) headers.Cookie = `CF_Authorization=${accessCookie}`;
  headers["X-PITS-Test-ID"] = testId;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  try {
    const response = await fetch(new URL(path, base), {
      method: body === undefined ? "GET" : "POST",
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(timeoutMs)
    });
    const text = await response.text();
    let parsed;
    try { parsed = JSON.parse(text); } catch { parsed = { text: redact(text).slice(0, 1000) }; }
    return { status: response.status, body: parsed };
  } catch (error) {
    return { status: 0, body: { error: error instanceof Error ? error.name : "request-failed" } };
  }
}

function expectStatus(response, status, scenario) {
  assert.equal(response.status, status,
    `${scenario}: HTTP ${response.status}; body=${JSON.stringify(response.body)}`);
  return response.body;
}

async function state() {
  return expectStatus(await request("/api/state"), 200, "state");
}

async function waitForState(predicate, label, timeoutMs = 45_000) {
  const started = Date.now();
  let last;
  while (Date.now() - started < timeoutMs) {
    last = await state();
    if (predicate(last)) return last;
    await sleep(500);
  }
  throw new Error(`Timed out waiting for ${label}; last state: ${JSON.stringify(last)}`);
}

async function reobserveProbeToTerminal(id, command, scenario, timeoutMs = 150_000) {
  const first = expectStatus(await request("/api/probe", { id, command }), 200, scenario);
  let result = first;
  const classifications = [first.state];
  const started = Date.now();
  while (result.state !== "exited" && Date.now() - started < timeoutMs) {
    if (result.state === "lost") break;
    // Re-observe the same durable command ID. The runner must collect its
    // existing process/receipt; this retry is not permission to redispatch.
    await sleep(500);
    result = expectStatus(await request("/api/probe", { id, command }), 200, `${scenario} reattachment`);
    classifications.push(result.state);
  }
  return {
    firstClassification: first.state,
    result,
    classifications,
    recoveryDurationMs: Date.now() - started
  };
}

async function collectProbeToExit(id, command, scenario, timeoutMs = 150_000) {
  const observed = await reobserveProbeToTerminal(id, command, scenario, timeoutMs);
  const { result } = observed;
  assert.equal(result.state, "exited",
    `${scenario}: did not settle as exited; last=${JSON.stringify(resultSummary(result))}`);
  const settled = await state();
  assert.equal(settled.activeIntent, null, `${scenario}: settled receipt must release the active writer gate`);
  return observed;
}

async function evidence(path) {
  const response = await request(`/api/evidence?path=${encodeURIComponent(path)}`);
  if (response.status === 409 || response.status === 404) return undefined;
  return expectStatus(response, 200, `evidence ${path}`).text;
}

async function markerCount(path, marker) {
  const text = await evidence(path);
  return text === undefined ? 0 : text.split(/\r?\n/).filter(line => line === marker).length;
}

function appendCommand(marker, path) {
  return `printf '%s\\n' ${marker} >> /workspace/pits/${path}`;
}

function resultSummary(body) {
  return {
    commandId: body?.commandId ?? null,
    classification: body?.state ?? null,
    exitCode: body?.exitCode ?? null,
    reason: body?.reason ?? null
  };
}

let logChild;
let logTail = "";
let logStatus = "not-started";
function startCloudflareLogs() {
  const env = { ...process.env };
  delete env.PITS_API_TOKEN;
  delete env.PITS_ACCESS_COOKIE;
  logChild = spawn("npx", ["wrangler", "tail", workerName, "--format", "json"], {
    cwd: process.cwd(), env, stdio: ["ignore", "pipe", "pipe"]
  });
  logChild.stdout.setEncoding("utf8");
  logChild.stderr.setEncoding("utf8");
  const writeLines = chunk => {
    logTail += chunk;
    const lines = logTail.split("\n");
    logTail = lines.pop() ?? "";
    for (const line of lines) if (line.trim()) appendFileSync(logsPath, redact(line) + "\n", { mode: 0o600 });
  };
  logChild.stdout.on("data", writeLines);
  logChild.stderr.on("data", chunk => { logStatus = "wrangler-tail-warning"; writeLines(chunk); });
  logChild.on("spawn", () => { logStatus = "capturing"; });
  logChild.on("error", () => { logStatus = "failed-to-start"; });
}

const RETRYABLE_CONTAINER_SETTINGS_ERROR =
  "could not finish applying its Durable Object-managed Container application settings";

async function deployTestWorker(faults, fixture) {
  let last;
  for (let attempt = 1; attempt <= 3; attempt++) {
    last = await new Promise(resolve => {
    const env = { ...process.env };
    delete env.PITS_API_TOKEN;
    delete env.PITS_ACCESS_COOKIE;
    const child = spawn("npx", [
      "wrangler", "deploy", "--config", "wrangler.s0-test.jsonc",
      "--name", workerName,
      "--var", `PITS_ENABLE_FAULTS:${faults}`,
      "--var", `PITS_ENABLE_FIXTURE:${fixture}`
    ], { cwd: process.cwd(), env, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    let timedOut = false;
    const timeout = setTimeout(() => {
      timedOut = true;
      child.kill("SIGINT");
    }, 8 * 60_000);
    const collect = chunk => {
      const safe = redact(chunk.toString());
      output += safe;
      appendFileSync(logsPath, safe, { mode: 0o600 });
    };
    child.stdout.on("data", collect);
    child.stderr.on("data", collect);
    child.on("error", error => {
      clearTimeout(timeout);
      resolve({ code: null, timedOut, output: redact(error.message) });
    });
    child.on("close", code => {
      clearTimeout(timeout);
      resolve({ code, timedOut, output: output.slice(-4000) });
    });
    });
    last.attempts = attempt;
    if (last.code === 0 || !last.output.includes(RETRYABLE_CONTAINER_SETTINGS_ERROR) || attempt === 3) {
      return last;
    }
    await sleep(1_000);
  }
  return last;
}

async function stopCloudflareLogs() {
  if (!logChild) return;
  if (logChild.exitCode === null) logChild.kill("SIGINT");
  await Promise.race([
    new Promise(resolve => logChild.once("close", resolve)),
    sleep(5000)
  ]);
  if (logTail.trim()) appendFileSync(logsPath, redact(logTail) + "\n", { mode: 0o600 });
  if (logStatus === "capturing") logStatus = "captured";
}

async function invokeFault(id, command, faultAt) {
  const response = await request("/api/probe", { id, command, faultAt });
  assert.notEqual(response.status, 200,
    `${faultAt}: injected DO abort unexpectedly returned HTTP ${response.status}; body=${JSON.stringify(response.body)}`);
  return response;
}

async function invokeCheckpointFault(faultAt) {
  const response = await request("/api/checkpoint", { faultAt });
  assert.notEqual(response.status, 200, `${faultAt}: injected DO abort unexpectedly returned a normal response`);
  return response;
}

async function restoreAndReconcile(checkpoint, expectedFiles = []) {
  const destroy = await request("/api/destroy", {});
  expectStatus(destroy, 200, "destroy test container");
  const started = Date.now();
  const restored = expectStatus(await request("/api/restore", {}), 200, "restore checkpoint");
  assert.equal(restored.backup.id, checkpoint.backup.id, "restore returned the committed checkpoint");
  const restoredState = await state();
  assert.equal(restoredState.reconciliationRequired, true, "restore sets reconciliation gate");
  const afterFiles = {};
  for (const [path, expectedHash] of expectedFiles) {
    const text = await evidence(path);
    assert.notEqual(text, undefined, `restored file ${path} exists`);
    const digest = sha256(text);
    assert.equal(digest, expectedHash, `restored file ${path} matches checkpoint hash`);
    afterFiles[path] = digest;
  }
  record("workspace-restore", {
    pass: true,
    checkpointId: restored.backup.id,
    sha256: restored.backup.sha256,
    size: restored.backup.size,
    durationMs: Date.now() - started,
    bootId: restoredState.boot,
    files: afterFiles
  });
  return restored;
}

let outcome = "passed";
let cleanupAttempted = false;
try {
  const enabled = predeployed ? { code: 0 } : await deployTestWorker(true, true);
  assert.equal(enabled.code, 0,
    `could not enable isolated test switches: ${"output" in enabled ? enabled.output : "predeployment was not verified"}`);
  record("test-switches-enabled", {
    pass: true, worker: workerName, deploymentExitCode: enabled.code,
    deploymentAttempts: enabled.attempts ?? 1,
    mode: predeployed ? "predeployed-and-verified-below" : "deployed-by-harness"
  });
  startCloudflareLogs();
  const routingStartedAt = Date.now();
  let routing;
  let lastRoutingResponse;
  while (Date.now() - routingStartedAt < 120_000) {
    const response = await request("/api/test/status");
    lastRoutingResponse = { status: response.status, body: response.body };
    if (response.status === 200 && response.body.faultsEnabled === true &&
      response.body.fixtureEnabled === true && response.body.workerFaultsEnabled === true &&
      response.body.workerFixtureEnabled === true && response.body.objectName === `test-${testId}` &&
      response.body.requestedTestId === testId) {
      routing = response.body;
      break;
    }
    await sleep(1_000);
  }
  assert.ok(routing,
    `test Worker deployment did not become active with the requested Durable Object routing; last=${JSON.stringify(lastRoutingResponse)}`);
  record("test-routing-ready", {
    pass: true, objectName: routing.objectName, propagationWaitMs: Date.now() - routingStartedAt
  });
  if (onlyFaultStage) {
    for (let i = 0; i < iterations; i++) {
      const id = `exit-before-receipt-${testId}-${i}`;
      const marker = `exit-before-receipt-${testId}-${i}`;
      const path = `exit-before-receipt-${i}.txt`;
      const command = appendCommand(marker, path);
      const fault = await invokeFault(id, command, onlyFaultStage);
      const bootAfterFault = (await state()).boot;
      const recovered = await collectProbeToExit(id, command, `focused ${onlyFaultStage} recovery`);
      const effects = await markerCount(path, marker);
      const after = await state();
      assert.equal(effects, 1, "recovered result corresponds to exactly one filesystem effect");
      assert.ok(bootAfterFault, "fault injection starts the execution container before process exit");
      assert.equal(after.boot, bootAfterFault, "same-boot exit recovery does not replace the container");
      record(`fault-${onlyFaultStage}`, {
        pass: true, iteration: i + 1, commandId: recovered.result.commandId,
        faultResponseStatus: fault.status, classificationProgression: recovered.classifications,
        classification: recovered.result.state, effects, bootIdBefore: bootAfterFault,
        bootIdAfter: after.boot, recoveryDurationMs: recovered.recoveryDurationMs
      });
    }
  } else {
  const health = expectStatus(await request("/health", undefined, { auth: false }), 200, "health");
  assert.equal(health.service, "pits-s0");
  assert.equal((await request("/api/state", undefined, { auth: false })).status, 401, "API rejects missing bearer token");
  let current = await state();
  record("preflight", { pass: true, containerBootId: current.boot ?? null, pi: current.pi ?? null });

  // Drive an actual pi-durable task with a zero-cost deterministic pi-ai provider.
  // Keep the Pi-issued tool process alive long enough to observe it in the same
  // DO/container pair before aborting the DO. This avoids sampling Pi's pending
  // alarm before its first shell tool has reserved or started the container.
  const fixtureMarker = `replace-pi-${testId}`;
  const fixturePending = request("/api/ask-fixture", { marker: fixtureMarker }).then(response => {
    record("piharness-request-settled", {
      pass: response.status === 200 || response.status === 409,
      status: response.status,
      disposition: response.status === 409 ? "interrupted-or-rejected-before-reattachment" : "completed-before-reattachment",
      error: typeof response.body?.error === "string" ? redact(response.body.error).slice(0, 500) : null,
      command: resultSummary(response.body?.command)
    });
    return response;
  });
  const duringFixture = await waitForState(
    value => value.pi.pendingCount > 0 && value.pi.lifecycleAlarm !== null &&
      Boolean(value.activeIntent) && value.processObservation?.kind === "running" && Boolean(value.boot),
    "PiHarness pending operation, Lifecycle alarm, and its running sandbox tool"
  );
  const fixtureBoot = duringFixture.boot;
  const fixtureCommandId = duringFixture.activeIntent.id;
  assert.ok(fixtureBoot, "Pi sandbox tool must already have started its container");
  const abortedFixture = await request("/api/abort", {});
  assert.notEqual(abortedFixture.status, 200, "test abort resets the DO instance");
  await fixturePending;
  const recoveredFixture = expectStatus(await request("/api/ask-fixture", { marker: fixtureMarker }), 200, "reattach Pi fixture");
  assert.match(recoveredFixture.text, new RegExp(`fixture complete ${fixtureMarker}`));
  assert.ok(recoveredFixture.command?.commandId, "Pi tool result retains its command receipt");
  assert.equal(recoveredFixture.command.commandId, fixtureCommandId,
    "PiHarness recovery reattaches the command running before DO abort");
  assert.equal(recoveredFixture.command.state, "exited");
  const fixtureEffects = await markerCount("pi-fixture.txt", fixtureMarker);
  assert.equal(fixtureEffects, 1, "PiHarness tool caused exactly one filesystem append");
  current = await state();
  assert.equal(current.boot, fixtureBoot, "DO restart preserved the existing container boot");
  assert.equal(current.pi.pendingCount, 0, "PiHarness recovered and settled the submitted task");
  record("piharness-lifecycle-recovery", {
    pass: true, commandId: recoveredFixture.command?.commandId ?? null,
    classification: recoveredFixture.command?.state ?? "tool result not exposed",
    effects: fixtureEffects, bootIdBefore: fixtureBoot, bootIdAfter: current.boot,
    piMessageCount: current.pi.messageCount, lifecycleAlarmAfter: current.pi.lifecycleAlarm
  });

  const baseFileMarker = `base-${testId}`;
  const baselineCommand = `printf '%s\\n' ${baseFileMarker} > /workspace/pits/baseline.txt`;
  const baselineResult = await collectProbeToExit(`base-${testId}`, baselineCommand, "create baseline");
  const baselineText = await evidence("baseline.txt");
  assert.equal(baselineText, baseFileMarker + "\n");
  const baselineHash = sha256(baselineText);
  record("baseline-command-settled", {
    pass: true, commandId: baselineResult.result.commandId,
    firstClassification: baselineResult.firstClassification,
    finalClassification: baselineResult.result.state, effects: 1,
    recoveryDurationMs: baselineResult.recoveryDurationMs
  });
  const checkpointStarted = Date.now();
  const checkpoint = expectStatus(await request("/api/checkpoint", {}), 200, "initial checkpoint");
  assert.ok(checkpoint.backup.size > 0);
  assert.match(checkpoint.backup.sha256, /^[a-f0-9]{64}$/);
  const checkpointDurationMs = Date.now() - checkpointStarted;
  const checkpointCommittedAt = Date.now();
  const fixtureWorkspaceText = await evidence("pi-fixture.txt");
  assert.notEqual(fixtureWorkspaceText, undefined, "checkpoint contains the Pi fixture marker");
  const fixtureWorkspaceHash = sha256(fixtureWorkspaceText);
  record("checkpoint-commit", {
    pass: true, checkpointId: checkpoint.backup.id, sha256: checkpoint.backup.sha256,
    size: checkpoint.backup.size, baselineHash, durationMs: checkpointDurationMs,
    bootId: checkpoint.bootId, transcriptAnchor: checkpoint.transcriptAnchor ?? null
  });

  const postCheckpointPiMarker = `post-checkpoint-pi-${testId}`;
  const postCheckpointPi = expectStatus(await request("/api/ask-fixture", {
    marker: postCheckpointPiMarker
  }), 200, "Pi tool result after checkpoint");
  assert.equal(postCheckpointPi.command?.state, "exited");
  assert.equal(await markerCount("pi-fixture.txt", postCheckpointPiMarker), 1);
  const piTranscriptBeforeRestore = await state();
  record("piharness-post-checkpoint-effect", {
    pass: true, commandId: postCheckpointPi.command?.commandId ?? null,
    classification: postCheckpointPi.command?.state, effects: 1,
    piReobservations: postCheckpointPi.command?.reobservations ?? null,
    activeTranscriptEntriesBeforeRestore: piTranscriptBeforeRestore.pi.messageCount,
    checkpointTranscriptAnchor: checkpoint.transcriptAnchor ?? null
  });

  // Replace the container while PiHarness is awaiting a long-running tool.
  // The durable transcript must retain a lost result and must not claim success.
  {
    const marker = `replace-${testId}`;
    const before = await state();
    const initialAsk = request("/api/ask-fixture", { marker });
    const active = await waitForState(value => Boolean(value.activeIntent), "Pi fixture sandbox command");
    await expectStatus(await request("/api/destroy", {}), 200, "replace container during Pi command");
    const initialResult = expectStatus(await initialAsk, 200, "settle Pi command after replacement");
    const recovered = expectStatus(await request("/api/ask-fixture", { marker }), 200, "reattach Pi operation after replacement");
    record("piharness-container-replacement-result", {
      pass: recovered.command?.state === "lost",
      initialStatus: initialResult.command?.state ?? "tool-result-unavailable",
      classification: recovered.command?.state ?? null,
      text: redact(recovered.text).slice(0, 300)
    });
    assert.equal(recovered.command?.state, "lost");
    assert.deepEqual(recovered.command, initialResult.command);
    assert.match(recovered.text, /fixture observed lost/);
    assert.match(recovered.text, /reconciliation is required/);
    const effects = await markerCount("pi-fixture.txt", marker);
    assert.equal(effects, 0, "lost Pi command was not replayed in replacement container");
    const after = await state();
    assert.notEqual(after.boot, before.boot);
    assert.equal(after.restoreRequired, true);
    const transcriptEntriesBeforeRestore = (await state()).pi.messageCount;
    await restoreAndReconcile(checkpoint, [
      ["baseline.txt", baselineHash], ["pi-fixture.txt", fixtureWorkspaceHash]
    ]);
    const transcriptAfterRestore = await state();
    assert.ok(transcriptAfterRestore.pi.activeEntryKinds.includes("pi.reset"),
      "restore starts a new Pi transcript context with a recovery handoff");
    assert.ok(transcriptAfterRestore.pi.messageCount < transcriptEntriesBeforeRestore,
      "post-checkpoint Pi tool results are no longer in the active transcript");
    await expectStatus(await request("/api/reconcile", { checkpointId: checkpoint.backup.id }), 200, "reconcile after Pi container replacement");
    record("piharness-container-replacement", {
      pass: true, commandId: recovered.command?.commandId ?? null,
      classification: recovered.command?.state, effects,
      bootIdBefore: before.boot, bootIdAfter: after.boot,
      transcriptMessageCountBeforeRestore: transcriptEntriesBeforeRestore,
      transcriptMessageCountAfterRestore: transcriptAfterRestore.pi.messageCount,
      activeTranscriptKindsAfterRestore: transcriptAfterRestore.pi.activeEntryKinds,
      recoveryDurationMs: Date.now() - checkpointCommittedAt
    });
  }

  // Same ID is idempotent, argument reuse is rejected, and simultaneous same-ID calls share one effect.
  for (let i = 0; i < iterations; i++) {
    const marker = `dedup-${testId}-${i}`;
    const id = `dedup-${testId}-${i}`;
    const command = appendCommand(marker, `dedup-${i}.txt`);
    const first = expectStatus(await request("/api/probe", { id, command }), 200, "dedup first");
    const duplicate = expectStatus(await request("/api/probe", { id, command }), 200, "dedup duplicate");
    assert.equal(duplicate.commandId, first.commandId, "duplicate observes the original command ID");
    assert.ok(["unknown", "exited"].includes(first.state), "first submission is either settled or conservatively ambiguous");
    const classifications = [first.state, duplicate.state];
    const settleStartedAt = Date.now();
    let settledDuplicate = duplicate;
    while (settledDuplicate.state !== "exited" && Date.now() - settleStartedAt < 150_000) {
      assert.equal(settledDuplicate.state, "unknown", "duplicate may only advance the same uncertain command");
      await sleep(500);
      settledDuplicate = expectStatus(await request("/api/probe", { id, command }), 200, "dedup result reattachment");
      assert.equal(settledDuplicate.commandId, first.commandId, "reattachment observes the same command ID");
      classifications.push(settledDuplicate.state);
    }
    assert.equal(settledDuplicate.state, "exited", "same-ID retry eventually collects the original result");
    if (first.state === "exited") assert.deepEqual(resultSummary(duplicate), resultSummary(first));
    const different = await request("/api/probe", { id, command: appendCommand("different", `dedup-${i}.txt`) });
    assert.equal(different.status, 409, "same ID with different arguments is rejected");
    const effects = await markerCount(`dedup-${i}.txt`, marker);
    assert.equal(effects, 1);
    assert.equal((await state()).activeIntent, null, "settled duplicate releases the active mutation gate");
    record("duplicate-command", {
      pass: true, iteration: i + 1, commandId: first.commandId,
      classificationProgression: classifications, finalClassification: settledDuplicate.state,
      effects, sameCommandId: true, differentArgumentsRejected: true,
      recoveryDurationMs: Date.now() - settleStartedAt
    });
  }

  const concurrentMarker = `concurrent-${testId}`;
  const concurrentId = `concurrent-${testId}`;
  const concurrentCommand = `sleep 2 && ${appendCommand(concurrentMarker, "concurrent.txt")}`;
  const concurrentStarted = Date.now();
  const concurrent = await Promise.all([
    request("/api/probe", { id: concurrentId, command: concurrentCommand }),
    request("/api/probe", { id: concurrentId, command: concurrentCommand })
  ]);
  concurrent.forEach((response, i) => expectStatus(response, 200, `concurrent submission ${i + 1}`));
  const concurrentEffects = await markerCount("concurrent.txt", concurrentMarker);
  assert.equal(concurrentEffects, 1);
  record("concurrent-deduplication", {
    pass: true, commandId: concurrent[0].body.commandId, classification: concurrent[0].body.state,
    effects: concurrentEffects, recoveryDurationMs: Date.now() - concurrentStarted
  });

  // Ten deterministic crashes after the SQLite intent commit but before container reservation.
  for (let i = 0; i < iterations; i++) {
    const marker = `intent-${testId}-${i}`;
    const id = `intent-${testId}-${i}`;
    const command = appendCommand(marker, `intent-${i}.txt`);
    const bootBefore = (await state()).boot;
    await invokeFault(id, command, "after_intent");
    const retryResult = await collectProbeToExit(id, command, "recover after intent commit");
    const retry = retryResult.result;
    const effects = await markerCount(`intent-${i}.txt`, marker);
    assert.equal(effects, 1);
    const after = await state();
    record("fault-after-intent", {
      pass: true, iteration: i + 1, commandId: retry.commandId,
      classificationProgression: retryResult.classifications, finalClassification: retry.state,
      effects, bootIdBefore: bootBefore, bootIdAfter: after.boot,
      recoveryDurationMs: retryResult.recoveryDurationMs
    });
  }

  // Before-intent interruption is retryable because no durable intent or dispatch exists.
  {
    const marker = `before-intent-${testId}`, id = `before-intent-${testId}`;
    const command = appendCommand(marker, "before-intent.txt");
    await invokeFault(id, command, "before_intent");
    const retryResult = await collectProbeToExit(id, command, "recover before intent");
    const retry = retryResult.result;
    const effects = await markerCount("before-intent.txt", marker);
    assert.equal(effects, 1);
    record("fault-before-intent", {
      pass: true, commandId: retry.commandId,
      classificationProgression: retryResult.classifications, classification: retry.state,
      effects, recoveryDurationMs: retryResult.recoveryDurationMs
    });
  }

  // A reservation with no published PID is ambiguous; it must not launch again.
  // Repeat this crash window 10 times, destroying the ambiguous container before
  // restoring so restore never overwrites a possibly live process on the same boot.
  for (let i = 0; i < iterations; i++) {
    const id = `reservation-${testId}-${i}`, marker = `reservation-${testId}-${i}`;
    const command = appendCommand(marker, `reservation-${i}.txt`);
    const bootBefore = (await state()).boot;
    const faultStarted = Date.now();
    await invokeFault(id, command, "after_reservation");
    const uncertain = expectStatus(await request("/api/probe", { id, command }), 200, "observe incomplete reservation");
    assert.equal(uncertain.state, "unknown");
    assert.equal(await markerCount(`reservation-${i}.txt`, marker), 0);
    const blocked = await request("/api/probe", {
      id: `blocked-${testId}-${i}`, command: appendCommand(`blocked-${i}`, `blocked-${i}.txt`)
    });
    assert.equal(blocked.status, 409, "uncertain command blocks another mutation");
    await expectStatus(await request("/api/destroy", {}), 200, "destroy ambiguous reservation container");
    const lostObservation = await reobserveProbeToTerminal(id, command, "classify replaced reservation");
    const lost = lostObservation.result;
    assert.equal(lost.state, "lost");
    assert.equal(await markerCount(`reservation-${i}.txt`, marker), 0);
    const restored = await restoreAndReconcile(checkpoint, [
      ["baseline.txt", baselineHash], ["pi-fixture.txt", fixtureWorkspaceHash]
    ]);
    current = await state();
    await expectStatus(await request("/api/reconcile", { checkpointId: checkpoint.backup.id }), 200, "reconcile after reservation restore");
    record("fault-after-reservation", {
      pass: true, iteration: i + 1, commandId: uncertain.commandId,
      classification: uncertain.state, afterReplacementClassification: lost.state,
      effects: 0, bootIdBefore: bootBefore, bootIdAfter: current.boot,
      restoreCheckpointId: restored.backup.id, otherMutationBlocked: true,
      recoveryDurationMs: Date.now() - faultStarted
    });
  }

  // Repeated DO aborts must preserve the container and reattach the original process.
  // The first run meets the required approximately-60-second process duration;
  // short runs keep ten controlled resets practical and independently observable.
  for (let i = 0; i < iterations; i++) {
    const id = `do-restart-${testId}-${i}`, marker = `do-restart-${testId}-${i}`;
    const path = `do-restart-${i}.txt`;
    const runSeconds = i === 0 ? 60 : 3;
    const command = `sleep ${runSeconds} && ${appendCommand(marker, path)}`;
    const before = await state();
    const initialRequest = request("/api/probe", { id, command });
    const expectedCommandId = `p${sha256(`task:probe:${id}`).slice(0, 32)}`;
    const running = await waitForState(value => value.activeIntent?.id === expectedCommandId, "60-second command intent");
    assert.equal(running.boot, before.boot);
    const checkpointDuringRun = await request("/api/checkpoint", {});
    assert.equal(checkpointDuringRun.status, 409, "checkpoint is blocked while command is active");
    const competing = await request("/api/probe", {
      id: `competing-${testId}`, command: appendCommand(`competing-${testId}`, "competing.txt")
    });
    assert.equal(competing.status, 409, "another mutation is blocked while command is active");
    const observedRunning = await waitForState(
      value => value.activeIntent?.id === expectedCommandId && value.processObservation?.kind === "running",
      "original command process running before DO abort"
    );
    const interrupted = await request("/api/abort", {});
    assert.notEqual(interrupted.status, 200, "DO abort interrupted the active request");
    await initialRequest;
    const started = Date.now();
    const reattachment = await collectProbeToExit(id, command, "reattach after DO restart");
    const reattached = reattachment.result;
    const after = await state();
    const effects = await markerCount(path, marker);
    assert.equal(reattached.state, "exited");
    assert.equal(effects, 1);
    assert.equal(after.boot, before.boot, "container boot remains the same after DO reset");
    record("do-restart-running-command", {
      iteration: i + 1, processRuntimeSeconds: runSeconds,
      pass: true, commandId: reattached.commandId, classification: reattached.state,
      classificationProgression: reattachment.classifications,
      effects, bootIdBefore: before.boot, bootIdAfter: after.boot,
      processStateAtAbort: observedRunning.processObservation?.kind,
      recoveryDurationMs: Date.now() - started
    });
  }

  // Repeat the post-launch and process-exit crash windows, using fresh command
  // IDs so each interruption exercises its own reservation and receipt.
  for (const faultAt of ["after_launch", "after_exit_before_receipt", "after_receipt"]) {
    for (let i = 0; i < iterations; i++) {
      const id = `${faultAt.replaceAll("_", "-")}-${testId}-${i}`;
      const marker = `${faultAt}-${testId}-${i}`;
      const path = `${faultAt}-${i}.txt`;
      const command = faultAt === "after_launch"
        ? `sleep 3 && ${appendCommand(marker, path)}`
        : appendCommand(marker, path);
      const bootBefore = (await state()).boot;
      const faultStarted = Date.now();
      await invokeFault(id, command, faultAt);
      const reattachment = await collectProbeToExit(id, command, `reattach ${faultAt}`);
      const reattached = reattachment.result;
      const after = await state();
      const effects = await markerCount(path, marker);
      assert.equal(effects, 1);
      assert.equal(after.boot, bootBefore);
      record(`fault-${faultAt}`, {
        pass: true, iteration: i + 1, commandId: reattached.commandId,
        classificationProgression: reattachment.classifications,
        classification: reattached.state, effects,
        bootIdBefore: bootBefore, bootIdAfter: after.boot,
        recoveryDurationMs: Date.now() - faultStarted
      });
    }
  }

  // Publish the same isolated configuration while a bounded mutation is active.
  // A deploy may or may not interrupt an in-flight DO; observe boot and classify
  // the command rather than presuming that it created the desired failure window.
  {
    const id = `redeploy-${testId}`, marker = `redeploy-${testId}`;
    const command = `sleep 65 && ${appendCommand(marker, "redeploy.txt")}`;
    const before = await state();
    const initialRequest = request("/api/probe", { id, command });
    const expectedCommandId = `p${sha256(`task:probe:${id}`).slice(0, 32)}`;
    const active = await waitForState(value => value.activeIntent?.id === expectedCommandId, "redeploy test command intent");
    const deployStartedAt = Date.now();
    const deployment = await deployTestWorker(true, true);
    assert.equal(deployment.code, 0, `test Worker redeployment failed: ${deployment.output}`);
    record("redeploy-during-active-command", {
      pass: true, worker: workerName, deployDurationMs: Date.now() - deployStartedAt,
      deploymentExitCode: deployment.code, commandId: active.activeIntent.id,
      classificationAtDeployStart: "running", bootIdBefore: before.boot,
      bootIdAtDeployStart: active.boot
    });
    await initialRequest;
    const reattachment = await reobserveProbeToTerminal(id, command, "observe command after redeploy");
    const reattached = reattachment.result;
    const after = await state();
    const effects = await markerCount("redeploy.txt", marker);
    assert.ok(["exited", "lost"].includes(reattached.state), "redeployed command is collected or conservatively classified");
    assert.ok(effects <= 1, "redeploy did not duplicate the filesystem effect");
    if (reattached.state === "exited") assert.equal(effects, 1);
    if (reattached.state === "lost") {
      assert.equal(after.restoreRequired, true);
      await restoreAndReconcile(checkpoint, [
        ["baseline.txt", baselineHash], ["pi-fixture.txt", fixtureWorkspaceHash]
      ]);
      await expectStatus(await request("/api/reconcile", { checkpointId: checkpoint.backup.id }), 200, "reconcile after redeploy loss");
    }
    record("redeploy-command-recovery", {
      pass: true, commandId: reattached.commandId, classification: reattached.state,
      effects, bootIdBefore: before.boot, bootIdAfter: after.boot,
      classificationProgression: reattachment.classifications,
      recoveryDurationMs: Date.now() - deployStartedAt
    });
  }

  // Repeat active container replacement; each old command must be lost, never replayed,
  // and the restored workspace remains write-blocked until explicit reconciliation.
  for (let i = 0; i < iterations; i++) {
    const id = `replace-${testId}-${i}`, marker = `replace-${testId}-${i}`;
    const path = `replace-${i}.txt`;
    const command = `sleep 3 && ${appendCommand(marker, path)}`;
    const before = await state();
    const initialRequest = request("/api/probe", { id, command });
    const expectedCommandId = `p${sha256(`task:probe:${id}`).slice(0, 32)}`;
    const running = await waitForState(
      value => value.activeIntent?.id === expectedCommandId && value.processObservation?.kind === "running",
      "container replacement command process running"
    );
    await expectStatus(await request("/api/destroy", {}), 200, "replace active command container");
    await initialRequest;
    const lostObservation = await reobserveProbeToTerminal(id, command, "classify replaced command");
    const lost = lostObservation.result;
    assert.equal(lost.state, "lost");
    assert.equal(await markerCount(path, marker), 0);
    const after = await state();
    assert.notEqual(after.boot, before.boot);
    assert.equal(after.restoreRequired, true);
    const otherMutation = await request("/api/probe", {
      id: `replace-blocked-${testId}-${i}`,
      command: appendCommand(`replace-blocked-${i}`, `replace-blocked-${i}.txt`)
    });
    assert.equal(otherMutation.status, 409, "replacement loss blocks new mutations until restore and reconciliation");
    const recoveryStarted = Date.now();
    await restoreAndReconcile(checkpoint, [["baseline.txt", baselineHash]]);
    await expectStatus(await request("/api/reconcile", { checkpointId: checkpoint.backup.id }), 200, "reconcile after manual replacement");
    record("container-replacement-active", {
      pass: true, iteration: i + 1, commandId: lost.commandId,
      classification: lost.state, effects: 0, processStateBeforeReplacement: running.processObservation?.kind,
      classificationProgression: lostObservation.classifications,
      bootIdBefore: before.boot, bootIdAfter: after.boot,
      otherMutationBlocked: true, recoveryDurationMs: Date.now() - recoveryStarted
    });
  }

  // Polling deadline remains observable; completion after 120 seconds releases the lock.
  {
    const id = `late-${testId}`, marker = `late-${testId}`;
    const command = `sleep 125 && ${appendCommand(marker, "late.txt")}`;
    const started = Date.now();
    const timedOut = expectStatus(await request("/api/probe", { id, command }, { timeoutMs: 160_000 }), 200, "120-second polling deadline");
    assert.equal(timedOut.state, "unknown");
    const blocked = await request("/api/probe", { id: `late-blocked-${testId}`, command: appendCommand("late-blocked", "late-blocked.txt") });
    assert.equal(blocked.status, 409);
    await sleep(7000);
    const lateObservation = await collectProbeToExit(id, command, "collect late result");
    const lateResult = lateObservation.result;
    assert.equal(lateResult.state, "exited");
    const effects = await markerCount("late.txt", marker);
    assert.equal(effects, 1);
    record("late-completion-after-deadline", {
      pass: true, commandId: lateResult.commandId, classification: lateResult.state,
      initialClassification: timedOut.state, effects, recoveryDurationMs: Date.now() - started,
      reattachmentClassifications: lateObservation.classifications
    });
  }

  // A completed post-checkpoint receipt is invalidated after restoring the prior file tree.
  const postMarker = `post-checkpoint-${testId}`, postId = `post-checkpoint-${testId}`;
  const postCommand = appendCommand(postMarker, "post-checkpoint.txt");
  const postResult = expectStatus(await request("/api/probe", { id: postId, command: postCommand }), 200, "post-checkpoint write");
  assert.equal(await markerCount("post-checkpoint.txt", postMarker), 1);

  // Interrupt after R2 receives the archive but before checkpoint metadata commits.
  expectStatus(await request("/api/probe", {
    id: `orphan-${testId}`, command: appendCommand(`orphan-${testId}`, "orphan.txt")
  }), 200, "create divergent file before interrupted backup");
  await invokeCheckpointFault("after_backup_before_checkpoint");
  current = await state();
  assert.equal(current.checkpoint.backup.id, checkpoint.backup.id, "previous checkpoint metadata remains committed");
  record("fault-backup-before-metadata", {
    pass: true, checkpointId: current.checkpoint.backup.id,
    sha256: current.checkpoint.backup.sha256, size: current.checkpoint.backup.size,
    orphanBackupPossible: true
  });

  for (let i = 0; i < iterations; i++) {
    const beforeBackupAbort = await state();
    await invokeCheckpointFault("during_backup");
    current = await state();
    assert.notEqual(current.backupInterruptionFiredAt, null, "controlled interruption fired while backup was in flight");
    assert.ok(current.backupInterruptionFiredAt > (beforeBackupAbort.backupInterruptionFiredAt ?? 0));
    assert.equal(current.checkpoint.backup.id, checkpoint.backup.id, "interrupted transfer cannot replace checkpoint metadata");
    assert.equal(current.boot, beforeBackupAbort.boot, "DO restart during backup preserves the container identity");
    record("fault-during-backup", {
      pass: true, iteration: i + 1, checkpointId: current.checkpoint.backup.id,
      sha256: current.checkpoint.backup.sha256, size: current.checkpoint.backup.size,
      bootIdBefore: beforeBackupAbort.boot, bootIdAfter: current.boot,
      interruptionAt: current.backupInterruptionFiredAt, mutationFixtureBytes: 64 * 1024 * 1024,
      maintenanceClearedAfterRestart: true
    });
  }

  await restoreAndReconcile(checkpoint, [
    ["baseline.txt", baselineHash], ["pi-fixture.txt", fixtureWorkspaceHash]
  ]);
  assert.equal(await evidence("post-checkpoint.txt"), undefined, "post-checkpoint file is absent after restore");
  assert.equal(await evidence("orphan.txt"), undefined, "interrupted-backup divergence is absent after restore");
  assert.equal(await evidence(".pits-backup-interruption.bin"), undefined,
    "the mutation fixture created for interrupted backup is absent after restore");
  const invalidated = expectStatus(await request("/api/probe", { id: postId, command: postCommand }), 200, "observe invalidated receipt");
  assert.equal(invalidated.state, "lost");
  const gate = await request("/api/probe", { id: `gate-${testId}`, command: appendCommand("gate", "gate.txt") });
  assert.equal(gate.status, 409, "restored workspace blocks writes before reconciliation");
  await expectStatus(await request("/api/reconcile", { checkpointId: checkpoint.backup.id }), 200, "reconcile restored workspace");
  const resumed = expectStatus(await request("/api/probe", {
    id: `reconciled-${testId}`, command: appendCommand(`reconciled-${testId}`, "reconciled.txt")
  }), 200, "mutation after reconciliation");
  assert.equal(resumed.state, "exited");
  assert.equal(await markerCount("reconciled.txt", `reconciled-${testId}`), 1);
  record("restore-divergence-and-reconcile", {
    pass: true, checkpointId: checkpoint.backup.id, checkpointHash: checkpoint.backup.sha256,
    fileHash: baselineHash, postCheckpointReceipt: resultSummary(invalidated),
    postCheckpointEffectsPresent: false, reconciliationGateBlocked: true,
    lostWorkWindowMs: Date.now() - checkpointCommittedAt,
    knownLostFiles: 2,
    knownLostBytes: Buffer.byteLength(postMarker + "\n") + Buffer.byteLength(`orphan-${testId}\n`),
    postReconciliationCommandId: resumed.commandId, postReconciliationEffects: 1
  });

  const rebasedCheckpoint = expectStatus(await request("/api/checkpoint", {}), 200, "checkpoint reconciled workspace");
  const rebaseMarker = `after-rebase-${testId}`;
  const rebaseResult = expectStatus(await request("/api/probe", {
    id: `after-rebase-${testId}`,
    command: appendCommand(rebaseMarker, "after-rebase.txt")
  }), 200, "mutate after rebased checkpoint");
  assert.equal(rebaseResult.state, "exited");
  await expectStatus(await request("/api/restore", {}), 200, "restore rebased checkpoint");
  const afterRebasedRestore = await state();
  assert.equal(afterRebasedRestore.reconciliationRequired, true);
  assert.equal(await evidence("after-rebase.txt"), undefined);
  const stillLost = expectStatus(await request("/api/probe", {
    id: postId, command: postCommand
  }), 200, "old post-checkpoint receipt remains lost after later restore");
  assert.equal(stillLost.state, "lost");
  assert.equal(afterRebasedRestore.pi.activeEntryKinds.includes("pi.reset"), true);
  const secondGate = await request("/api/probe", {
    id: `second-gate-${testId}`,
    command: appendCommand("second-gate", "second-gate.txt")
  });
  assert.equal(secondGate.status, 409);
  await expectStatus(await request("/api/reconcile", {
    checkpointId: rebasedCheckpoint.backup.id
  }), 200, "reconcile rebased checkpoint");
  record("lost-receipt-survives-subsequent-restore", {
    pass: true, originalLostCommandId: stillLost.commandId,
    classification: stillLost.state, laterCheckpointId: rebasedCheckpoint.backup.id,
    laterCheckpointSha256: rebasedCheckpoint.backup.sha256,
    postRestoreEffectPresent: false, reconciliationGateBlocked: true,
    transcriptEntryKinds: afterRebasedRestore.pi.activeEntryKinds
  });
  }
} catch (error) {
  outcome = "failed";
  record("suite-failure", { pass: false, error: error instanceof Error ? redact(error.message) : "unknown error" });
  process.exitCode = 1;
} finally {
  if (process.env.PITS_URL && token) {
    cleanupAttempted = true;
    const doMode = await request("/api/test/disable", {});
    const doModeDisabled = doMode.status === 200 &&
      doMode.body.faultsEnabled === false && doMode.body.fixtureEnabled === false;
    record("durable-test-mode-disabled", {
      pass: doModeDisabled, status: doMode.status,
      faultsEnabled: doMode.body?.faultsEnabled ?? null,
      fixtureEnabled: doMode.body?.fixtureEnabled ?? null
    });
    const disabled = await deployTestWorker(false, false);
    let verification;
    if (disabled.code === 0) {
      const status = await request("/api/test/status");
      verification = status.status === 200 && status.body.faultsEnabled === false &&
        status.body.fixtureEnabled === false && status.body.workerFaultsEnabled === false &&
        status.body.workerFixtureEnabled === false && status.body.objectName === `test-${testId}`;
    }
    const pass = disabled.code === 0 && doModeDisabled && verification === true;
    record("fault-injection-disabled", {
      pass, deploymentExitCode: disabled.code, timedOut: disabled.timedOut,
      deploymentAttempts: disabled.attempts ?? 1,
      durableModeVerifiedDisabled: doModeDisabled,
      postDeploySwitchesVerifiedDisabled: verification === true,
      output: disabled.output
    });
    if (!pass) {
      outcome = "failed-cleanup";
      process.exitCode = 1;
    }
  }
  await stopCloudflareLogs();
  record("suite-summary", {
    pass: outcome === "passed", outcome, artifactPath, cloudflareLogsPath: logsPath,
    cloudflareLogStatus: logStatus, cleanupAttempted
  });
}
