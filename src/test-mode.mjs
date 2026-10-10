export const TEST_MODE_STORAGE_KEY = "pits-s0-test-mode";

/**
 * @typedef {{ faultsEnabled: boolean, fixtureEnabled: boolean }} TestMode
 * @typedef {{ get(key: string): Promise<unknown>, put(key: string, value: unknown): Promise<void>, delete(key: string): Promise<unknown> }} TestModeStorage
 */

/** @param {TestModeStorage} storage @returns {Promise<TestMode>} */
export async function loadTestMode(storage) {
  const stored = await storage.get(TEST_MODE_STORAGE_KEY);
  if (!stored || typeof stored !== "object") {
    return { faultsEnabled: false, fixtureEnabled: false };
  }
  const mode = /** @type {Partial<TestMode>} */ (stored);
  return {
    faultsEnabled: mode.faultsEnabled === true,
    fixtureEnabled: mode.fixtureEnabled === true
  };
}

/** @param {TestModeStorage} storage @param {TestMode} mode @returns {Promise<TestMode>} */
export async function storeTestMode(storage, mode) {
  const normalized = {
    faultsEnabled: mode.faultsEnabled === true,
    fixtureEnabled: mode.fixtureEnabled === true
  };
  if (normalized.faultsEnabled || normalized.fixtureEnabled) {
    await storage.put(TEST_MODE_STORAGE_KEY, normalized);
  } else {
    await storage.delete(TEST_MODE_STORAGE_KEY);
  }
  return normalized;
}
