import test from "node:test";
import assert from "node:assert/strict";
import {
  addLostSequenceRange,
  decide,
  observationGate,
  forbidUncertainRedispatch,
  classifyObservationFailure,
  receiptWasInvalidated
} from "../src/recovery.mjs";

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

test("timed out command can observe its original running process without releasing the active lock", () => {
  assert.equal(observationGate({active:"command-1", requested:"command-1", restoreRequired:false, reconciliationRequired:false}), "observe");
  assert.equal(forbidUncertainRedispatch(decide({bootId:"boot-1"}, "boot-1", "running"), true), "reattach");
  assert.equal(forbidUncertainRedispatch(decide({bootId:"boot-1"}, "boot-1", "exited"), true), "collect");
});
test("unknown result never authorises a second dispatch if the reservation vanishes", () => {
  assert.equal(forbidUncertainRedispatch(decide({bootId:"boot-1"}, "boot-1", "missing"), true), "unknown");
});
test("while original command is uncertain all other commands remain blocked", () => {
  assert.equal(observationGate({active:"command-1", requested:"command-2", restoreRequired:false, reconciliationRequired:false}), "blocked");
});
test("workspace loss and pending reconciliation block even same-command observations", () => {
  for (const [restoreRequired, reconciliationRequired] of [[true,false],[false,true]]) {
    assert.equal(observationGate({active:"command-1", requested:"command-1", restoreRequired, reconciliationRequired}), "blocked");
  }
});

test("process observation failures are lost only after a proven boot change", () => {
  assert.equal(classifyObservationFailure("boot-1", undefined), "unknown");
  assert.equal(classifyObservationFailure("boot-1", "boot-1"), "unknown");
  assert.equal(classifyObservationFailure("boot-1", "boot-2"), "lost");
});

test("lost receipt ranges remain invalidated across subsequent checkpoints and restores", () => {
  const firstRestore = addLostSequenceRange([], 4, 9);
  assert.equal(receiptWasInvalidated(7, firstRestore), true);
  assert.equal(receiptWasInvalidated(4, firstRestore), false);

  const nextRestore = addLostSequenceRange(firstRestore, 12, 15);
  assert.deepEqual(nextRestore, [
    { afterSequence: 4, throughSequence: 9 },
    { afterSequence: 12, throughSequence: 15 }
  ]);
  assert.equal(receiptWasInvalidated(7, nextRestore), true);
  assert.equal(receiptWasInvalidated(14, nextRestore), true);
  assert.equal(receiptWasInvalidated(10, nextRestore), false);

  assert.deepEqual(addLostSequenceRange(nextRestore, 9, 12), [
    { afterSequence: 4, throughSequence: 15 }
  ]);
  assert.deepEqual(addLostSequenceRange(nextRestore, 20, 20), nextRestore);
});
