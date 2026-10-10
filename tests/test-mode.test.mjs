import test from "node:test";
import assert from "node:assert/strict";
import { loadTestMode, storeTestMode, TEST_MODE_STORAGE_KEY } from "../src/test-mode.mjs";

function storage() {
  const values = new Map();
  return {
    values,
    async get(key) { return values.get(key); },
    async put(key, value) { values.set(key, value); },
    async delete(key) { return values.delete(key); }
  };
}

test("test controls persist across Durable Object reconstruction and disable cleanly", async () => {
  const firstInstance = storage();
  assert.deepEqual(await loadTestMode(firstInstance), {
    faultsEnabled: false, fixtureEnabled: false
  });

  await storeTestMode(firstInstance, { faultsEnabled: true, fixtureEnabled: true });
  assert.deepEqual(firstInstance.values.get(TEST_MODE_STORAGE_KEY), {
    faultsEnabled: true, fixtureEnabled: true
  });

  const restartedInstance = {
    async get(key) { return firstInstance.values.get(key); },
    async put(key, value) { firstInstance.values.set(key, value); },
    async delete(key) { return firstInstance.values.delete(key); }
  };
  assert.deepEqual(await loadTestMode(restartedInstance), {
    faultsEnabled: true, fixtureEnabled: true
  });

  await storeTestMode(restartedInstance, { faultsEnabled: false, fixtureEnabled: false });
  assert.equal(firstInstance.values.has(TEST_MODE_STORAGE_KEY), false);
  assert.deepEqual(await loadTestMode(firstInstance), {
    faultsEnabled: false, fixtureEnabled: false
  });
});
