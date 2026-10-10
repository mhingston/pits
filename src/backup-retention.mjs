// Each DO exclusively owns this prefix. Legacy shared-prefix archives are
// deliberately outside automatic collection; old checkpoints remain restorable.
export const ORPHAN_GRACE_MS = 24 * 60 * 60 * 1000;
export const LEGACY_BACKUP_PREFIX = "pits-s0/";

export function ownerBackupPrefix(ownerId) {
  if (!/^[a-f0-9]{64}$/.test(ownerId)) throw new Error("Invalid backup owner ID");
  return `pits-s0/owners/${ownerId}/`;
}

export function checkpointKey(point) {
  if (!point) return null;
  return `${point.backupPrefix ?? LEGACY_BACKUP_PREFIX}${point.backup.id}.tar.zst`;
}

export function orphanCandidates(objects, prefix, protectedKeys, cutoff) {
  if (!/^pits-s0\/owners\/[a-f0-9]{64}\/$/.test(prefix) || !Number.isFinite(cutoff)) {
    throw new Error("Invalid backup collection scope");
  }
  const protectedSet = new Set(protectedKeys.filter(Boolean));
  return objects.filter(object => {
    const suffix = object.key.startsWith(prefix) ? object.key.slice(prefix.length) : "";
    const uploaded = new Date(object.uploaded).getTime();
    return /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}\.tar\.zst$/.test(suffix) &&
      Number.isFinite(uploaded) && uploaded < cutoff && !protectedSet.has(object.key);
  }).map(object => object.key);
}

// Caller must hold its DO's maintenance lock for this entire operation.
export async function collectOwnedOrphans(bucket, prefix, protectedKeys, cutoff) {
  let cursor;
  let scanned = 0;
  const deleted = [];
  do {
    const page = await bucket.list({ prefix, cursor, limit: 1000 });
    scanned += page.objects.length;
    const candidates = orphanCandidates(page.objects, prefix, protectedKeys, cutoff);
    // Check pagination before deleting anything from a malformed page.
    const next = page.truncated ? page.cursor : undefined;
    if (page.truncated && (!next || next === cursor)) throw new Error("Invalid backup continuation cursor");
    if (candidates.length) {
      await bucket.delete(candidates);
      deleted.push(...candidates);
    }
    cursor = next;
  } while (cursor);
  return { prefix, scanned, deleted, protectedKeys };
}
