import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import ts from 'typescript';

// Execute the real runner with only the platform gateway substituted.
let source = readFileSync(new URL('../src/runner.ts', import.meta.url), 'utf8');
source = source.replace(/^import \{ DirectoryBackup[^\n]+\n/, 'const DirectoryBackup = globalThis.__pitsReviewDirectoryBackup;\n');
source = source.replace(/from "(\.\/[^\"]+)"/g, (_, path) =>
  `from "${new URL('../src/' + path.slice(2), import.meta.url).href}"`);
globalThis.__pitsReviewDirectoryBackup = class {
  constructor() {}
  async restore() { return globalThis.__pitsReviewRestore(); }
};
const { SandboxRunner } = await import('data:text/javascript;base64,' + Buffer.from(
  ts.transpileModule(source, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext } }).outputText
).toString('base64'));
delete globalThis.__pitsReviewDirectoryBackup;

function fixture(initial = {}) {
  const data = new Map(Object.entries(initial));
  const storage = {
    async get(key) { return data.get(key); },
    async put(key, value) { data.set(key, value); },
    async delete(key) { return data.delete(key); },
    async transaction(callback) { return callback(storage); }
  };
  let r2Calls = 0;
  const bucket = new Proxy({}, { get() { return async () => { r2Calls++; throw new Error('Unexpected R2 operation'); }; } });
  const container = { running: true, async setInactivityTimeout() {}, async destroy() { container.destroyed = true; } };
  const ctx = { storage, container, id: { toString: () => 'a'.repeat(64) }, exports: {}, blockConcurrencyWhile: callback => callback() };
  const runner = new SandboxRunner(ctx, bucket);
  runner.ensureContainer = async () => ({ bootId: 'boot' });
  return { runner, data, container, calls: () => r2Calls };
}
const point = { backup: { id: 'checkpoint' }, sequence: 2 };

test('production checkpoint cleanup performs no R2 fault-marker operations', async () => {
  const f = fixture();
  await f.runner.cleanupInterruptedBackupUpload();
  assert.equal(f.calls(), 0);
});
test('restore acquires maintenance before any upload cleanup', async () => {
  const f = fixture({ maintenance: true });
  await assert.rejects(f.runner.restore(), /Maintenance already running/);
  assert.equal(f.calls(), 0);
});
test('failed filesystem restore leaves unsafe flag and pending handoff', async () => {
  const f = fixture({ checkpoint: point });
  globalThis.__pitsReviewRestore = async () => { throw new Error('partial restore'); };
  await assert.rejects(f.runner.restore(), /partial restore/);
  assert.equal(f.data.get('restore-required'), true);
  assert.deepEqual(f.data.get('restore-handoff-pending'), point);
  assert.equal(f.data.has('maintenance'), false);
});
test('handoff retry skips filesystem restore and blocks reconciliation until reset completes', async () => {
  const f = fixture({ checkpoint: point, 'command-sequence': 5,
    'restore-floor-sequence': 0, 'restore-ceiling-sequence': 1 });
  let restores = 0;
  globalThis.__pitsReviewRestore = async () => { restores++; };
  await f.runner.restore();
  await f.runner.restore();
  assert.equal(restores, 1);
  await assert.rejects(f.runner.acknowledgeReconciliation('checkpoint'), /handoff pending/);
  assert.deepEqual(f.data.get('lost-sequence-ranges'), [
    { afterSequence: 0, throughSequence: 1 }, { afterSequence: 2, throughSequence: 5 }
  ]);
  assert.equal(f.data.has('restore-floor-sequence'), false);
  await f.runner.completeRestoreHandoff('checkpoint');
  await f.runner.acknowledgeReconciliation('checkpoint');
  assert.equal(f.data.get('reconciliation-required'), false);
});
test('explicit replacement requires uncertainty and keeps restore required', async () => {
  const f = fixture({ active: 'command' });
  await assert.rejects(f.runner.replaceContainerForRecovery(), /No ambiguous command/);
  f.data.set('no-redispatch:command', true);
  await f.runner.replaceContainerForRecovery();
  assert.equal(f.container.destroyed, true);
  assert.equal(f.data.get('restore-required'), true);
  assert.equal(f.data.get('active'), 'command');
});
test('test upload cleanup refuses another owner before touching R2', async () => {
  const f = fixture({
    'pits-s0-test-mode': { faultsEnabled: true, fixtureEnabled: true },
    'test-backup-interruption-upload': { key: 'pits-s0/owners/' + 'b'.repeat(64) + '/foreign.tar.zst', uploadId: 'foreign' }
  });
  await assert.rejects(f.runner.cleanupInterruptedBackupUpload(), /invalid interrupted backup/);
  assert.equal(f.calls(), 0);
});
