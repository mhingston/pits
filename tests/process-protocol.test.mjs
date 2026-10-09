import test from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { RUN, STATUS } from "../src/process-protocol.mjs";

const status = dir => execFileSync("sh", ["-c", STATUS, "sh", dir], { encoding: "utf8" }).trim();
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function awaitStatus(dir, expected) {
  for (let i = 0; i < 100; i++) {
    if (status(dir).startsWith(expected)) return;
    await sleep(20);
  }
  throw new Error("Never reached " + expected + " (actual " + status(dir) + ")");
}
test("atomic reservation gates dispatch and unique append marker appears once", async () => {
  const root = mkdtempSync(join(tmpdir(), "pits-s0-"));
  try {
    const dir = join(root, "cmd"), marker = join(root, "marker");
    assert.equal(status(dir), "missing");
    mkdirSync(dir);
    assert.throws(() => mkdirSync(dir), { code: "EEXIST" });
    const child = spawn("sh", ["-c", RUN, "sh", dir, "sh", "-c",
      "printf 'once\\n' >> '" + marker + "'; sleep 0.1"], { stdio: "ignore" });
    const closed = new Promise(resolve => child.once("close", resolve));
    await awaitStatus(dir, "exited 0");
    assert.equal(readFileSync(marker, "utf8"), "once\n");
    assert.match(readFileSync(join(dir, "pid"), "utf8"), /^\d+ [\da-f-]{36}\n$/);
    assert.equal(status(dir), "exited 0");
    await closed;
  } finally { rmSync(root, { recursive: true, force: true }); }
});
test("reservation without PID remains starting; do not automatically retry", () => {
  const root = mkdtempSync(join(tmpdir(), "pits-s0-"));
  try {
    const dir = join(root, "cmd"); mkdirSync(dir);
    assert.equal(status(dir), "starting");
    assert.ok(!existsSync(join(dir, "exit-code")));
  } finally { rmSync(root, { recursive: true, force: true }); }
});
