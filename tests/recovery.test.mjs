import test from "node:test";
import assert from "node:assert/strict";
import { decide } from "../src/recovery.mjs";

test("first invocation can dispatch", () => {
  assert.equal(decide(undefined, "boot-1", "missing"), "dispatch");
});
test("same-boot reservation absent: redispatch permitted", () => {
  assert.equal(decide({ bootId: "boot-1" }, "boot-1", "missing"), "dispatch");
});
test("same-boot running: reattach, never redispatch", () => {
  assert.equal(decide({ bootId: "boot-1" }, "boot-1", "running"), "reattach");
});
test("same-boot exited: collect stored result", () => {
  assert.equal(decide({ bootId: "boot-1" }, "boot-1", "exited"), "collect");
});
test("launch reservation without pid is ambiguous", () => {
  assert.equal(decide({ bootId: "boot-1" }, "boot-1", "starting"), "wait");
});
test("lost process without exit receipt is unknown", () => {
  assert.equal(decide({ bootId: "boot-1" }, "boot-1", "lost"), "unknown");
});
test("container replacement is lost even if new filesystem is empty", () => {
  for (const status of ["missing", "running", "starting", "exited", "lost"]) {
    assert.equal(decide({ bootId: "boot-1" }, "boot-2", status), "lost");
  }
});
