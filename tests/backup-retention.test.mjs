import test from "node:test";
import assert from "node:assert/strict";
import { checkpointKey, ownerBackupPrefix, orphanCandidates, collectOwnedOrphans, ORPHAN_GRACE_MS } from "../src/backup-retention.mjs";

const prefix = ownerBackupPrefix("a".repeat(64));
const id = "12345678-1234-1234-1234-123456789abc";
const key = prefix + id + ".tar.zst";
const now = Date.UTC(2026, 9, 10);
const object = (key, uploaded = now - 2 * ORPHAN_GRACE_MS) => ({ key, uploaded: new Date(uploaded) });

test("collector protects current and restored checkpoints regardless of age", () => {
  const current = checkpointKey({ backup: { id }, backupPrefix: prefix });
  const restored = prefix + "87654321-1234-1234-1234-123456789abc.tar.zst";
  assert.deepEqual(orphanCandidates([object(current), object(restored)], prefix, [current, restored], now), []);
});

test("collector deletes only old unreferenced archives of its own DO", () => {
  const foreign = ownerBackupPrefix("b".repeat(64)) + id + ".tar.zst";
  assert.deepEqual(orphanCandidates([
    object(key), object(foreign), object("pits-s0/" + id + ".tar.zst"),
    object(prefix + "marker"), object(prefix + "nested/" + id + ".tar.zst"),
    object(prefix + "ffffffff-1234-1234-1234-123456789abc.tar.zst", now),
    { key: prefix + "eeeeeeee-1234-1234-1234-123456789abc.tar.zst", uploaded: "invalid" }
  ], prefix, [], now - ORPHAN_GRACE_MS), [key]);
});

test("collector gives the full grace, fails closed on invalid scope, and supports legacy references", () => {
  assert.deepEqual(orphanCandidates([object(key, now - ORPHAN_GRACE_MS)], prefix, [], now - ORPHAN_GRACE_MS), []);
  assert.throws(() => orphanCandidates([object(key)], "pits-s0/", [], now));
  assert.throws(() => ownerBackupPrefix("../foreign"));
  assert.equal(checkpointKey({ backup: { id } }), "pits-s0/" + id + ".tar.zst");
  assert.equal(checkpointKey(null), null);
});


test("collector follows every page and protects handles on later pages", async () => {
  const deletes = [];
  const calls = [];
  const second = prefix + "aaaaaaaa-1234-1234-1234-123456789abc.tar.zst";
  const bucket = {
    async list(options) {
      calls.push(options);
      return options.cursor === undefined
        ? { objects: [object(key)], truncated: true, cursor: "page-2" }
        : { objects: [object(second)], truncated: false };
    },
    async delete(keys) { deletes.push(...keys); }
  };
  const result = await collectOwnedOrphans(bucket, prefix, [second], now);
  assert.equal(result.scanned, 2);
  assert.deepEqual(result.deleted, [key]);
  assert.deepEqual(deletes, [key]);
  assert.equal(calls[1].cursor, "page-2");
  assert.ok(calls.every(call => call.prefix === prefix));
});

test("collector refuses malformed pagination and propagates storage failure", async () => {
  let deleted = false;
  const bucket = {
    async list() { return { objects: [object(key)], truncated: true }; },
    async delete() { deleted = true; }
  };
  await assert.rejects(collectOwnedOrphans(bucket, prefix, [], now), /cursor/);
  assert.equal(deleted, false);
  bucket.list = async () => ({ objects: [object(key)], truncated: false });
  bucket.delete = async () => { throw new Error("R2 unavailable"); };
  await assert.rejects(collectOwnedOrphans(bucket, prefix, [], now), /R2 unavailable/);
});
